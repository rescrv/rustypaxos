use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Range;
use std::rc;

use super::configuration::Configuration;
use super::configuration::QuorumTracker;
use super::configuration::ReplicaID;
use super::types::Ballot;
use super::types::Command;
use super::types::PValue;
use super::Environment;
use super::Message;
use super::Misbehavior;
use super::PaxosPhase;

pub struct Proposer {
    // The configuration under which this proposer operates.  Proposers will not operate under
    // different configs; they will die and a new one will rise in their place.
    config: rc::Rc<Configuration>,
    // The Ballot which this proposer is shepherding forward.
    ballot: Ballot,

    // Phase of paxos in which this proposer believes itself to be.
    phase: PaxosPhase,
    // Acceptors that follow this proposer.
    followers: QuorumTracker<FollowerState>,
    // Values to be proposed and pushed forward by this proposer.
    proposals: HashMap<u64, PValueState>,
    // The lower and upper bounds of the range this proposer will push forward.
    lower_slot: u64,
    upper_slot: u64,
    // Commands that are enqueued, but not yet turned into proposals.
    commands: VecDeque<Command>,
}

// A Proposer drives a single ballot from being unused to being superseded.
impl Proposer {
    pub fn new(config: &rc::Rc<Configuration>, ballot: Ballot) -> Proposer {
        Proposer {
            config: config.clone(),
            ballot,
            phase: PaxosPhase::ONE,
            followers: QuorumTracker::new(config.clone()),
            proposals: HashMap::new(),
            lower_slot: config.first_valid_slot(),
            upper_slot: u64::max_value(),
            commands: VecDeque::new(),
        }
    }

    // Add a command to be proposed.
    pub fn enqueue_command(&mut self, env: &mut Environment, cmd: Command) {
        // Enqueue the command and then shift commands to the proposals.
        self.commands.push_back(cmd);
        if self.phase == PaxosPhase::TWO && (self.proposals.len() as u64) < self.config.alpha() {
            self.bind_commands_to_slots(env);
        }
    }

    // Only issue proposals for slots less than the stop slot.
    //
    // # Panics
    //
    // * This value is initialized to u64 max may only decrease.  Passing a value greater than the
    //   previous value will panic.
    pub fn stop_at_slot(&mut self, slot: u64) {
        // TODO(rescrv): In practice, we should never have a caller move this to a slot that's
        // been assigned a pvalue under this ballot.  It would be good to enforce that.
        assert!(slot <= self.upper_slot);
        self.upper_slot = slot;
    }

    // Advance the window of contiguously learned commands to at least slot.
    pub fn advance_window(&mut self, env: &mut Environment, slot: u64) {
        assert!(self.lower_slot < slot);
        while self.lower_slot < slot && self.lower_slot < self.upper_slot {
            self.proposals.remove(&self.lower_slot);
            self.lower_slot += 1;
        }
        self.bind_commands_to_slots(env);
    }

    // Slots for which this proposer will currently generate pvalues
    pub fn active_slots(&self) -> Range<u64> {
        assert!(self.lower_slot <= self.upper_slot);
        let lower = self.lower_slot;
        let alpha = self.lower_slot + self.config.alpha();
        let upper = if self.upper_slot < alpha {
            self.upper_slot
        } else {
            alpha
        };
        lower..upper
    }

    // Take actions that would make progress on this ballot in all phases.
    pub fn make_progress(&mut self, env: &mut Environment) {
        // Send phase one messages to replicas we haven't had join our quorum.
        for replica in self.followers.waiting_for() {
            env.send(Message::Phase1A {
                acceptor: replica,
                ballot: self.ballot,
            });
        }
        // If in phase two, do the work for phase two.
        if self.phase == PaxosPhase::TWO {
            self.make_progress_phase_two(env);
        }
    }

    // Process receipt of a single phase 1b message.
    pub fn process_phase_1b_message(
        &mut self,
        env: &mut Environment,
        acceptor: &ReplicaID,
        current: &Ballot,
        pvalues: &[PValue],
    ) {
        // Provide basic protection against misbehaving servers participating in the protocol.
        if !self.config.is_member(acceptor) {
            env.report_misbehavior(Misbehavior::NotAReplica(*acceptor));
            return;
        }
        // We expect the ballot to be exactly equal.  Where the current ballot of the acceptor does
        // not equal this acceptor, we expect whoever owns the proposer to deal with it, possibly
        // by killing this proposer, rather than passing it downward.
        if *current != self.ballot {
            env.report_misbehavior(Misbehavior::ProposerWrongBallot(*acceptor, *current));
            return;
        }
        // If already accepted, do not go through it again.
        if self.followers.is_follower(acceptor) {
            // TODO(rescrv): might be useful to log this.
            return;
        }
        let mut state = FollowerState::new(self.lower_slot);
        // Validate that all pvalues from the acceptor are less than the current acceptor's ballot.
        for pval in pvalues {
            if self.ballot < pval.ballot() {
                // TODO(rescrv): write a test for this.
                env.report_misbehavior(Misbehavior::Phase1PValueAboveBallot(
                    *acceptor,
                    self.ballot,
                    pval.clone(),
                ));
                return;
            }
        }
        // Integrate the pvalues from the acceptor.
        for pval in pvalues {
            if pval.slot() < self.lower_slot || self.upper_slot <= pval.slot() {
                // TODO(rescrv): warn if this happens because it is inefficient
                continue;
            }
            match self.proposals.entry(pval.slot()) {
                // The acceptor's highest seen pvalue for this slot is less than what we have seen
                // from another acceptor.  The pvalue should be ignored because a higher ballot
                // proposed a different value.
                Entry::Occupied(ref entry) if pval.ballot() < entry.get().pval.ballot() => {}
                // Otherwise we know the pval from the acceptor has a ballot at least as high as
                // the entry we already have in the map and we should adopt this as our proposal
                // for this particular slot.
                Entry::Occupied(mut entry) => {
                    // The catch is that if we've moved onto phase two, we should not just adopt
                    // here.  Instead, maybe pretend that the acceptor does not join this phase one
                    // until slots at least as great as this one.
                    if self.phase == PaxosPhase::TWO && entry.get().pval.ballot() < pval.ballot() {
                        state.start_slot = pval.slot() + 1
                    } else {
                        // It's a protocol invariant that two pvalues with the same ballot must have
                        // the same command.  Because we enforce this in a distributed fashion across
                        // all replicas, we cannot do anything except check to make sure this is true
                        // and log when it is violated.
                        if entry.get().pval.ballot() == pval.ballot()
                            && entry.get().pval.command() != pval.command()
                        {
                            env.report_misbehavior(Misbehavior::PValueConflict(
                                entry.get().pval.clone(),
                                pval.clone(),
                            ));
                        }
                        *entry.get_mut() = PValueState::wrap(self.config.clone(), pval.clone());
                    }
                }
                // If there is nothing for this slot yet, the protocol obligates us to use this
                // provided pvalue.
                Entry::Vacant(entry) => {
                    entry.insert(PValueState::wrap(self.config.clone(), pval.clone()));
                }
            }
        }
        // Record that the acceptor follows us.
        self.followers.add(acceptor, state);
        // If we are in phase one
        if self.phase == PaxosPhase::ONE {
            // maybe advance to phase two
            if self.followers.has_quorum() {
                self.phase = PaxosPhase::TWO;
                self.make_progress_phase_two(env);
                self.bind_commands_to_slots(env);
            }
        }
    }

    // Process receipt of a single phase 2b message.
    pub fn process_phase_2b_message(
        &mut self,
        env: &mut Environment,
        acceptor: &ReplicaID,
        ballot: &Ballot,
        slot: u64,
    ) {
        // Provide basic protection against misbehaving servers participating in the protocol.
        if !self.config.is_member(acceptor) {
            env.report_misbehavior(Misbehavior::NotAReplica(*acceptor));
            return;
        }
        // Make sure the other server is responding to the right ballot.
        if self.ballot != *ballot {
            // TODO(rescrv): write a test for this
            env.report_misbehavior(Misbehavior::Phase2WrongBallot(
                *acceptor,
                self.ballot,
                *ballot,
            ));
            return;
        }
        // Make sure we are in phase two.
        if self.phase != PaxosPhase::TWO {
            // TODO(rescrv): write a test for this
            env.report_misbehavior(Misbehavior::NotInPhase2(*acceptor, self.ballot));
            return;
        }
        // If they didn't follow phase one, they have no business responding in phase two.
        if !self.followers.is_follower(acceptor) {
            // TODO(rescrv): write a test for this
            env.report_misbehavior(Misbehavior::Phase2First(*acceptor, *ballot));
            return;
        }
        // Get the pvalue state.
        let pval_state = match self.proposals.get_mut(&slot) {
            Some(x) => x,
            None => {
                if self.lower_slot <= slot {
                    // TODO(rescrv): write a test for this
                    env.report_misbehavior(Misbehavior::Phase2LostPValue(*acceptor, *ballot, slot));
                }
                return;
            }
        };
        // If this replica has already accepted.
        if pval_state.quorum.is_follower(acceptor) {
            // TODO(rescrv): might be useful to log this.
            return;
        }
        pval_state.quorum.add(acceptor, ());
        // TODO(rescrv): Decide how to handle this case, because proposer needs to move forward.
        if pval_state.quorum.has_quorum() {
            // SUCCESS!
            // TODO(rescrv): do something here
            print!("LEARNED {:?}\n", pval_state.pval);
        }
    }

    // Take actions that will make progress for phase two of this ballot.
    //
    // This will retransmit every bound pvalue, but will not bind additional pvalues.
    fn make_progress_phase_two(&mut self, env: &mut Environment) {
        assert!(self.phase == PaxosPhase::TWO);
        for slot in self.active_slots() {
            let pval_state = match self.proposals.get_mut(&slot) {
                Some(v) => v,
                None => continue,
            };
            pval_state.make_progress(env, &self.ballot);
        }
    }

    fn bind_commands_to_slots(&mut self, env: &mut Environment) {
        // TODO(rescrv):  I know this is inefficient, but it is safe and clean.  Make it all three.
        // The risk of being tricky here is that there's a hole in the pvalues from phase one, and
        // that hole leads to slots not being filled and the window not moving forward.
        for slot in self.active_slots() {
            let entry = self.proposals.entry(slot);
            if let Entry::Vacant(entry) = entry {
                let command = match self.commands.pop_front() {
                    Some(cmd) => cmd,
                    None => return,
                };
                let mut pval_state = PValueState::wrap(
                    self.config.clone(),
                    PValue::new(slot, self.ballot, command),
                );
                pval_state.make_progress(env, &self.ballot);
                entry.insert(pval_state);
            }
        }
    }

    #[cfg(test)]
    fn peek_slot(&self, slot: u64) -> Option<&PValue> {
        match self.proposals.get(&slot) {
            Some(p) => Some(&p.pval),
            None => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
struct FollowerState {
    start_slot: u64,
}

impl FollowerState {
    fn new(start_slot: u64) -> FollowerState {
        FollowerState { start_slot }
    }
}

#[derive(Debug)]
struct PValueState {
    pval: PValue,
    quorum: QuorumTracker<()>,
}

impl PValueState {
    fn wrap(config: rc::Rc<Configuration>, pval: PValue) -> PValueState {
        PValueState {
            pval,
            quorum: QuorumTracker::new(config),
        }
    }

    fn make_progress(&mut self, env: &mut Environment, ballot: &Ballot) {
        for replica in self.quorum.waiting_for() {
            let pval = self.pval.for_new_ballot(*ballot);
            env.send(Message::Phase2A {
                acceptor: replica,
                pval,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::rc::Rc;

    use super::*;
    use crate::configuration::GroupID;
    use crate::configuration::DEFAULT_ALPHA;
    use crate::testutil::*;
    use crate::PValue;
    use crate::AcceptorAction;

    fn compare_slices<T: PartialEq + Debug>(expected: &[T], returned: &[T]) {
        for e in expected.iter() {
            assert!(returned.contains(e), "{:?} expected but not seen", e);
        }
        for r in returned.iter() {
            assert!(expected.contains(r), "{:?} returned but not expected", r);
        }
    }

    struct TestEnvironment {
        config: Rc<Configuration>,
        ballot: Ballot,

        misbehaviors: Vec<Misbehavior>,
        phase_1a_messages: Vec<(ReplicaID, Ballot)>,
        phase_2a_messages: Vec<(ReplicaID, PValue)>,
    }

    impl TestEnvironment {
        fn new(
            group: GroupID,
            replicas: &[ReplicaID],
            shadows: &[ReplicaID],
            ballot: Ballot,
        ) -> TestEnvironment {
            let config = Configuration::bootstrap(group, replicas, shadows);
            TestEnvironment::from_config(config, ballot)
        }

        fn from_config(config: Rc<Configuration>, ballot: Ballot) -> TestEnvironment {
            TestEnvironment {
                config: config,
                ballot: ballot,

                misbehaviors: Vec::new(),
                phase_1a_messages: Vec::new(),
                phase_2a_messages: Vec::new(),
            }
        }

        fn proposer(&self) -> Proposer {
            Proposer::new(&self.config, self.ballot)
        }

        fn ballot(&self) -> Ballot {
            self.ballot
        }

        fn assert_ok(&self) {
            assert_eq!(0, self.misbehaviors.len(), "check there was no misbehavior");
        }

        fn assert_misbehaviors(&self, misbehaviors: &[Misbehavior]) {
            compare_slices(misbehaviors, self.misbehaviors.as_slice());
        }

        fn clear_phase_1a_messages(&mut self) {
            self.phase_1a_messages.clear();
        }

        fn assert_phase_1a_messages(&self, expect: &[(ReplicaID, Ballot)]) {
            compare_slices(expect, self.phase_1a_messages.as_slice());
        }

        fn clear_phase_2a_messages(&mut self) {
            self.phase_2a_messages.clear();
        }

        fn assert_phase_2a_messages(&self, expect: &[(ReplicaID, PValue)]) {
            compare_slices(expect, self.phase_2a_messages.as_slice());
        }
    }

    impl Environment for TestEnvironment {
        fn send(&mut self, msg: Message) {
            match msg {
                Message::Phase1A { acceptor, ballot } => {
                    self.phase_1a_messages.push((acceptor, ballot));
                }
                Message::Phase2A { acceptor, pval } => {
                    self.phase_2a_messages.push((acceptor, pval));
                }
                _ => {
                    panic!("unexpected message {:?}", msg);
                }
            };
        }

        fn persist_acceptor(&mut self, _action: AcceptorAction) {
            // If you're wondering why this is true, think about the following observation of the
            // Paxos protocol:  the acceptors form the durable memory of the system and a proposer
            // makes things durable by talking to acceptors.
            //
            // If you find yourself needing to persist state within the acceptor, it is a bad smell
            // and may be a strong indicator of unsafe behavior.
            panic!("proposers should not persist acceptor data");
        }

        fn send_when_persistent(&mut self, _msg: Message) {
            // See the above comment on `persist` for why this panic is in place.
            panic!("proposers should always invoke send, not send_when_persistent")
        }

        fn report_misbehavior(&mut self, m: Misbehavior) {
            self.misbehaviors.push(m);
        }
    }

    // Test that three replicas responding to phase one will achieve quorum.
    #[test]
    fn three_replicas_phase_one_no_pvalues() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // One
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert!(!proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Two makes quorum
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        env.assert_ok();
        assert!(proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Three is a bonus
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert!(proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::TWO);
    }

    // Test that five replicas responding to phase one will achieve quorum.
    #[test]
    fn five_replicas_phase_one_no_pvalues() {
        let mut env = TestEnvironment::new(GROUP, FIVE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // One
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert!(!proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Two
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        env.assert_ok();
        assert!(!proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Three makes quorum
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert!(proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Four is a bonus
        proposer.process_phase_1b_message(&mut env, &REPLICA4, &ballot, &[]);
        env.assert_ok();
        assert!(proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Five is perfect
        proposer.process_phase_1b_message(&mut env, &REPLICA5, &ballot, &[]);
        env.assert_ok();
        assert!(proposer.followers.has_quorum());
        assert_eq!(proposer.phase, PaxosPhase::TWO);
    }

    // Test that an acceptor that's not part of the ensemble cannot accept.
    // Replicas one, two, three are included, four is a shadow, and five is excluded.
    #[test]
    fn outside_acceptors_not_allowed() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[REPLICA4], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // A phase one response from a member should always be good.
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // From a shadow should be good too, but should not advance the phase.
        proposer.process_phase_1b_message(&mut env, &REPLICA4, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // From a non-member should cause misbehaving server error and not advance the phase.
        proposer.process_phase_1b_message(&mut env, &REPLICA5, &ballot, &[]);
        env.assert_misbehaviors(&[Misbehavior::NotAReplica(REPLICA5)]);
    }

    // Test that a stale phase one response has zero effect.
    #[test]
    fn stale_phase_one_response() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // One response.
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Stale second response should not be counted.
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &BALLOT_4_REPLICA1, &[]);
        assert_eq!(proposer.phase, PaxosPhase::ONE);
    }

    // Test that an acceptor cannot sybil their way to having a leader.
    #[test]
    fn double_commitment_to_follow() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // One
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Can't do that twice
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Or three times
        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
    }

    // Test that acceptors with the same pvalues become accepted.
    #[test]
    fn normal_acceptors_with_pvalues() {
        let pval1 = PValue::new(1, BALLOT_5_REPLICA1, Command::data("command"));
        let pval2 = PValue::new(2, BALLOT_6_REPLICA2, Command::data("command"));
        let pval3 = PValue::new(3, BALLOT_6_REPLICA2, Command::data("command"));

        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_7_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(
            &mut env,
            &REPLICA1,
            &ballot,
            &[pval1.clone(), pval2.clone()],
        );
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
        assert_eq!(proposer.proposals.len(), 2);
        assert_eq!(proposer.peek_slot(1), Some(&pval1));
        assert_eq!(proposer.peek_slot(2), Some(&pval2));

        proposer.process_phase_1b_message(
            &mut env,
            &REPLICA2,
            &ballot,
            &[pval2.clone(), pval3.clone()],
        );
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 3);
        assert_eq!(proposer.peek_slot(1), Some(&pval1));
        assert_eq!(proposer.peek_slot(2), Some(&pval2));
        assert_eq!(proposer.peek_slot(3), Some(&pval3));
    }

    // Test that the highest pvalue for the same slot gets retained.
    #[test]
    fn pvalues_for_the_same_slot() {
        let pval1 = PValue::new(1, BALLOT_5_REPLICA1, Command::data("command"));
        let pval2a = PValue::new(2, BALLOT_6_REPLICA1, Command::data("command"));
        let pval2b = PValue::new(2, BALLOT_6_REPLICA2, Command::data("command"));
        let pval3a = PValue::new(3, BALLOT_6_REPLICA1, Command::data("command"));
        let pval3b = PValue::new(3, BALLOT_6_REPLICA2, Command::data("command"));

        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_7_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(
            &mut env,
            &REPLICA1,
            &ballot,
            &[pval1.clone(), pval2a.clone(), pval3b.clone()],
        );
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
        assert_eq!(proposer.proposals.len(), 3);
        assert_eq!(proposer.peek_slot(1), Some(&pval1));
        assert_eq!(proposer.peek_slot(2), Some(&pval2a));
        assert_eq!(proposer.peek_slot(3), Some(&pval3b));

        proposer.process_phase_1b_message(
            &mut env,
            &REPLICA2,
            &ballot,
            &[pval1.clone(), pval2b.clone(), pval3a.clone()],
        );
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 3);
        assert_eq!(proposer.peek_slot(1), Some(&pval1));
        assert_eq!(proposer.peek_slot(2), Some(&pval2b));
        assert_eq!(proposer.peek_slot(3), Some(&pval3b));
    }

    // Test that pvalues with same ballot/slot, but different command will be logged.
    #[test]
    fn pvalue_conflicts_are_logged() {
        let pval1a = PValue::new(1, BALLOT_6_REPLICA1, Command::data("red fish"));
        let pval1b = PValue::new(1, BALLOT_6_REPLICA1, Command::data("blue fish"));

        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_7_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[pval1a.clone()]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.peek_slot(1), Some(&pval1a));

        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[pval1b.clone()]);
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.peek_slot(1), Some(&pval1b));
        env.assert_misbehaviors(&[Misbehavior::PValueConflict(pval1a.clone(), pval1b.clone())]);
    }

    // Test that a late-arriving acceptor will be excluded until after the slot for which the
    // conflict happened.
    #[test]
    fn late_acceptors_will_delay() {
        let pval1a = PValue::new(1, BALLOT_6_REPLICA1, Command::data("red fish"));
        let pval1b = PValue::new(1, BALLOT_7_REPLICA1, Command::data("blue fish"));

        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_7_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[pval1a.clone()]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[pval1a.clone()]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.peek_slot(1), Some(&pval1a));

        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[pval1b.clone()]);
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.peek_slot(1), Some(&pval1a));
        assert!(match proposer.followers.follower_state(&REPLICA3) {
            Some(r) => r.start_slot == 2,
            None => false,
        });
    }

    // Test that make_progess will send a phase one message to every unaccepted acceptor.
    #[test]
    fn make_progress_sends_phase_one() {
        let mut env =
            TestEnvironment::new(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS, BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // At first, we must message everyone to make progress.
        proposer.make_progress(&mut env);
        env.assert_phase_1a_messages(&[
            (REPLICA1, ballot),
            (REPLICA2, ballot),
            (REPLICA3, ballot),
            (REPLICA4, ballot),
            (REPLICA5, ballot),
        ]);

        // If we were to get a response from REPLICA3, we should not pester it again.
        env.clear_phase_1a_messages();
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        proposer.make_progress(&mut env);
        env.assert_phase_1a_messages(&[
            (REPLICA1, ballot),
            (REPLICA2, ballot),
            (REPLICA4, ballot),
            (REPLICA5, ballot),
        ]);
    }

    // Test that the proposer discards values outside the slots it was chosen for.
    #[test]
    fn pvalues_before_configuration_start_are_discarded() {
        let pval = PValue::new(1, BALLOT_6_REPLICA1, Command::data("red fish"));
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let config = config.reconfigure();
        let config = config.commit(128);
        let mut env = TestEnvironment::from_config(config, BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[pval]);
        assert_eq!(proposer.proposals.len(), 0);
    }

    // Test that advance window panics if it decreases.
    #[test]
    #[should_panic]
    fn advance_window_monotonic() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        proposer.advance_window(&mut env, 5);
        proposer.advance_window(&mut env, 4);
    }

    // Test that stop_at_slot panics if it increases.
    #[test]
    #[should_panic]
    fn stop_at_slot_monotonic() {
        let env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        proposer.stop_at_slot(4);
        proposer.stop_at_slot(5);
    }

    // Test the sliding window for enqueued commands.
    #[test]
    fn sliding_window_over_commands() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        for i in 0u64..DEFAULT_ALPHA + 5u64 {
            proposer.enqueue_command(&mut env, Command::data("command"));
            if i < DEFAULT_ALPHA {
                assert_eq!(proposer.proposals.len() as u64, i + 1);
                assert!(proposer.proposals.contains_key(&(i + 1)));
                assert_eq!(proposer.commands.len() as u64, 0);
            } else {
                assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA);
                assert!(!proposer.proposals.contains_key(&(i + 1)));
                assert_eq!(proposer.commands.len() as u64, i - DEFAULT_ALPHA + 1);
            }
            assert_eq!(proposer.active_slots(), 1u64..(DEFAULT_ALPHA + 1));
        }

        proposer.advance_window(&mut env, 2);
        assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA);
        assert!(!proposer.proposals.contains_key(&1));
        assert!(proposer.proposals.contains_key(&(DEFAULT_ALPHA + 1)));
        assert!(!proposer.proposals.contains_key(&(DEFAULT_ALPHA + 2)));
        assert_eq!(proposer.active_slots(), 2u64..(DEFAULT_ALPHA + 2));

        proposer.advance_window(&mut env, 7);
        assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA - 1);
        assert!(!proposer.proposals.contains_key(&6));
        assert!(proposer.commands.len() == 0);
        assert_eq!(proposer.active_slots(), 7u64..(DEFAULT_ALPHA + 7));

        proposer.enqueue_command(&mut env, Command::data("command"));
        assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA);
        assert!(proposer.proposals.contains_key(&(DEFAULT_ALPHA + 6)));
        assert!(proposer.commands.len() == 0);
        assert_eq!(proposer.active_slots(), 7u64..(DEFAULT_ALPHA + 7));
    }

    // Test that commands get enqueued on transition to phase two.
    #[test]
    fn enqueue_prior_to_phase_two() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        for i in 0..7 {
            proposer.enqueue_command(&mut env, Command::data("command"));
            assert_eq!(proposer.proposals.len(), 0);
            assert_eq!(proposer.commands.len(), (i + 1) as usize);
        }

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 7);
    }

    // Test the sliding window stops enqueuing at the stop slot.
    #[test]
    fn sliding_window_stop_at_slot() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // If this fails, adjust ITERS downward or DEFAULT_ALPHA upward.
        const ITERS: u64 = 10;
        assert!(ITERS < DEFAULT_ALPHA);

        proposer.stop_at_slot(ITERS + 1);

        for i in 0u64..ITERS {
            proposer.enqueue_command(&mut env, Command::data("command"));
            assert_eq!(proposer.proposals.len() as u64, i + 1);
            assert_eq!(proposer.commands.len() as u64, 0);
        }

        for i in 0u64..ITERS {
            proposer.enqueue_command(&mut env, Command::data("command"));
            assert_eq!(proposer.proposals.len() as u64, ITERS);
            assert_eq!(proposer.commands.len() as u64, i + 1);
        }
    }

    // Test that transition to phase two sends messages with pvalues.
    #[test]
    fn phase_two_sends_previous_enqueues() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        let cmd1 = Command::data("command 1");
        let cmd2 = Command::data("command 2");
        proposer.enqueue_command(&mut env, cmd1.clone());
        proposer.enqueue_command(&mut env, cmd2.clone());

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // check that the messages were sent
        env.assert_phase_2a_messages(&[
            (REPLICA1, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA1, PValue::new(2, ballot, cmd2.clone())),
            (REPLICA2, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA2, PValue::new(2, ballot, cmd2.clone())),
            (REPLICA3, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA3, PValue::new(2, ballot, cmd2.clone())),
        ]);

        // Replica one responds to pvalue for slot two.
        env.clear_phase_2a_messages();
        proposer.process_phase_2b_message(&mut env, &REPLICA1, &ballot, 2);
        proposer.make_progress_phase_two(&mut env);

        // check that the messages were re-sent
        env.assert_phase_2a_messages(&[
            (REPLICA1, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA2, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA2, PValue::new(2, ballot, cmd2.clone())),
            (REPLICA3, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA3, PValue::new(2, ballot, cmd2.clone())),
        ]);
    }


    // Test that enqueues after phase two immediately send phase 2a messages.
    #[test]
    fn enqueue_after_phase_two_sends_immediately() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        env.assert_phase_2a_messages(&[]);

        let cmd1 = Command::data("command 1");
        let cmd2 = Command::data("command 2");
        proposer.enqueue_command(&mut env, cmd1.clone());
        proposer.enqueue_command(&mut env, cmd2.clone());

        // check that the messages were sent
        env.assert_phase_2a_messages(&[
            (REPLICA1, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA1, PValue::new(2, ballot, cmd2.clone())),
            (REPLICA2, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA2, PValue::new(2, ballot, cmd2.clone())),
            (REPLICA3, PValue::new(1, ballot, cmd1.clone())),
            (REPLICA3, PValue::new(2, ballot, cmd2.clone())),
        ]);
    }

    // Test that pvalues returned in phase one go out in phase two with the proposer's ballot.
    #[test]
    fn all_proposals_come_from_us() {
        let mut env = TestEnvironment::new(GROUP, THREE_REPLICAS, &[], BALLOT_5_REPLICA1);
        let mut proposer = env.proposer();
        let ballot = env.ballot();

        // Note: ballot.number=4
        let pval = PValue::new(1, BALLOT_4_REPLICA1, Command::data("red fish"));

        proposer.process_phase_1b_message(&mut env, &REPLICA1, &ballot, &[pval]);
        proposer.process_phase_1b_message(&mut env, &REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&mut env, &REPLICA3, &ballot, &[]);
        env.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Note: ballot.number=5
        let pval = PValue::new(1, BALLOT_5_REPLICA1, Command::data("red fish"));

        // check that the messages were sent
        env.assert_phase_2a_messages(&[
            (REPLICA1, pval.clone()),
            (REPLICA2, pval.clone()),
            (REPLICA3, pval.clone()),
        ]);
    }

    // TODO:
    // - check all the cases of process_phase_two
    // - check that when there are existing pvalues our phase two sends them out as our own
    // - check that phase two does not message a server that didn't follow phase one
    // - test someone not an acceptor phase two
    // - test wrong ballot phase two
    // - test phase two in phase one
    // - test phase two when didn't follow phase one
    // - artificially lose pvalue state and then see reported misbehavior
    // - double phase two follower
}
