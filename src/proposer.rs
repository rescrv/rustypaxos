use super::configuration::Configuration;
use super::configuration::ReplicaID;
use super::Ballot;
use super::PValue;
use super::Command;
use super::PaxosPhase;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::VecDeque;

pub trait Logger {
    // transitioning to lame duck
    fn become_lame_duck(&mut self, superceded_by: &Ballot);

    // stale proposals are possible, but should be minimal in steady state
    // TODO(rescrv) rename this
    fn stale_proposal(&mut self, superceded_by: &Ballot);

    // someone sent a bad message
    fn error_misbehaving_server(&mut self, msg: &str);
    // no one should call us in lame duck, so log if they do
    fn error_in_lame_duck(&mut self);
    // pvalues have invariants
    fn error_pvalue_conflict(&mut self, p1: &PValue, p2: &PValue);
}

pub trait Messenger {
    fn send_phase_1a_message(&mut self, acceptor: &ReplicaID, ballot: &Ballot);
}

pub struct Proposer<'a, L: Logger, M: Messenger> {
    // The configuration under which this proposer operates.  Proposers will not operate under
    // different configs; they will die and a new one will rise in their place.
    config: &'a Configuration,
    // The Ballot which this proposer is shepherding forward.
    ballot: &'a Ballot,
    // The Logger to which state transitions and notable events will be recorded.
    logger: &'a mut L,
    // The Messenger over which outbound communication will be sent
    messenger: &'a mut M,

    // Is this proposer a lame_duck now because another higher ballot is floating around?
    // Note that this proposer technically doesn't have to even acknowledge this and the protocol
    // should still be safe because that's the point of Paxos.
    is_lame_duck: bool,
    // Phase of paxos in which this proposer believes itself to be.
    phase: PaxosPhase,
    // Acceptors that follow this proposer.
    followers: HashMap<&'a ReplicaID, FollowerState>,
    // Values to be proposed and pushed forward by this proposer.
    proposals: HashMap<u64, PValueState>,
    // The lower and upper bounds of the range this proposer will push forward.
    lower_slot: u64,
    upper_slot: u64,
    // Commands that are enqueued, but not yet turned into proposals.
    commands: VecDeque<Command>,
}

// A Proposer drives a single ballot from being unused to being superceded.
impl<'a, L: Logger, M: Messenger> Proposer<'a, L, M> {
    pub fn new(
        config: &'a Configuration,
        ballot: &'a Ballot,
        logger: &'a mut L,
        messenger: &'a mut M,
    ) -> Proposer<'a, L, M> {
        Proposer {
            config,
            ballot,
            logger,
            messenger,
            is_lame_duck: false,
            phase: PaxosPhase::ONE,
            followers: HashMap::new(),
            proposals: HashMap::new(),
            lower_slot: config.first_valid_slot(),
            upper_slot: u64::max_value(),
            commands: VecDeque::new(),
        }
    }

    // Add a command to be proposed.
    pub fn enqueue_command(&mut self, cmd: Command) {
        // Protection against misuse by owner
        if self.is_lame_duck {
            self.logger.error_in_lame_duck();
            return;
        }
        // Enqueue the command and then shift commands to the proposals.
        self.commands.push_back(cmd);
        if self.phase == PaxosPhase::TWO && (self.proposals.len() as u64) < self.config.alpha() {
            self.bind_commands_to_slots();
        }
    }

    // Only issue proposals for slots less than the stop slot.
    //
    // # Panics
    //
    // * This value is initialized to u64 and must not increase.
    pub fn stop_at_slot(&mut self, slot: u64) {
        // TODO(rescrv): In practice, we should never have a caller move this to a slot that's
        // been assigned a pvalue under this ballot.  It would be good to enforce that.
        assert!(slot <= self.upper_slot);
        self.upper_slot = slot;
    }

    // Advance the window of contiguously learned commands to at least slot.
    pub fn advance_window(&mut self, slot: u64) {
        assert!(self.lower_slot < slot);
        while self.lower_slot < slot && self.lower_slot < self.upper_slot {
            self.proposals.remove(&self.lower_slot);
            self.lower_slot += 1;
        }
        self.bind_commands_to_slots();
    }

    // Take actions that would make progress on this ballot in all phases.
    pub fn make_progress(&mut self) {
        // Protection against misuse by owner
        if self.is_lame_duck {
            self.logger.error_in_lame_duck();
            return;
        }
        // Send a phase one message to each replica
        for replica in self.config.replicas() {
            if !self.followers.contains_key(replica) {
                self.send_phase_1a_message(replica);
            }
        }
        // Send a phase one message to each shadow
        for shadow in self.config.shadows() {
            if !self.followers.contains_key(shadow) {
                self.send_phase_1a_message(shadow);
            }
        }
        if self.phase == PaxosPhase::TWO {
            self.make_progress_phase_two();
        }
    }

    // Process receipt of a single phase 1b message.
    pub fn process_phase_1b_message(
        &mut self,
        acceptor: &ReplicaID,
        current: &Ballot,
        pvalues: &[PValue],
    ) {
        // Protection against misuse by owner
        if self.is_lame_duck {
            self.logger.error_in_lame_duck();
            return;
        }
        // Provide basic protection against misbehaving servers participating in the protocol.
        if !self.config.is_member(acceptor) {
            self.logger.error_misbehaving_server("not an acceptor");
            return;
        }
        // Become a lame duck if this proposer has been superceded.
        if self.ballot < current {
            self.is_lame_duck = true;
            self.logger.become_lame_duck(current);
            return;
        }
        // Drop this message if it is old and out of date.
        if current < self.ballot {
            self.logger.stale_proposal(current);
            return;
        }
        // Protect against future programmers.
        assert!(
            current == self.ballot,
            "we can only proceed if the ballots are equal"
        );
        // If already accepted, do not go through it again.
        if self.followers.contains_key(acceptor) {
            // TODO(rescrv): might be useful to log this.
            return;
        }
        let mut state = FollowerState::new(self.lower_slot);
        // Validate that all pvalues from the acceptor are less than the current acceptor's ballot.
        for pval in pvalues {
            if self.ballot < &pval.ballot {
                self.logger.error_misbehaving_server("acceptor reports pval higher than current ballot");
                return;
            }
        }
        // Integrate the pvalues from the acceptor.
        for pval in pvalues {
            if pval.slot < self.lower_slot || self.upper_slot <= pval.slot {
                // TODO(rescrv): warn if this happens because it is inefficient
                continue;
            }
            let v = self.proposals.entry(pval.slot);
            match v {
                // The acceptor's highest seen pvalue for this slot is less than what we have seen
                // from another acceptor.  The pvalue should be ignored because a higher ballot
                // proposed a different value.
                Entry::Occupied(ref entry) if pval.ballot < entry.get().pval.ballot => {}
                // Otherwise we know the pval from the acceptor has a ballot at least as high as
                // the entry we already have in the map and we should adopt this as our proposal
                // for this particular slot.
                Entry::Occupied(mut entry) => {
                    // The catch is that if we've moved onto phase two, we should not just adopt
                    // here.  Instead, maybe pretend that the acceptor does not join this phase one
                    // until slots at least as great as this one.
                    if self.phase == PaxosPhase::TWO && entry.get().pval.ballot < pval.ballot {
                        state.start_slot = pval.slot + 1
                    } else {
                        // It's a protocol invariant that two pvalues with the same ballot must have
                        // the same command.  Because we enforce this in a distributed fashion across
                        // all replicas, we cannot do anything except check to make sure this is true
                        // and log when it is violated.
                        if entry.get().pval.ballot == pval.ballot && entry.get().pval.command != pval.command
                        {
                            self.logger.error_pvalue_conflict(&entry.get().pval, pval);
                        }
                        *entry.get_mut() = PValueState::wrap(pval.clone());
                    }
                }
                // If there is nothing for this slot yet, the protocol obligates us to use this
                // provided pvalue.
                Entry::Vacant(entry) => {
                    entry.insert(PValueState::wrap(pval.clone()));
                }
            }
        }
        // Record that the acceptor follows us.
        self.followers
            .insert(self.config.member_reference(acceptor), state);
        // If we are in phase one
        if self.phase == PaxosPhase::ONE {
            // maybe advance to phase two
            if self.config.is_quorum(&|x| self.followers.contains_key(x)) {
                self.phase = PaxosPhase::TWO;
                self.bind_commands_to_slots();
                self.make_progress_phase_two();
            }
        }
    }

    // Process receipt of a single phase 2b message.
    pub fn process_phase_2b_message(&mut self) {
        // Protection against misuse by owner
        if self.is_lame_duck {
            self.logger.error_in_lame_duck();
            return;
        }
    }

    // Take actions that will make progress for phase two of this ballot.
    fn make_progress_phase_two(&mut self) {
        assert!(self.phase == PaxosPhase::TWO);
        // TODO(rescrv)
    }

    fn send_phase_1a_message(&mut self, acceptor: &ReplicaID) {
        self.messenger.send_phase_1a_message(acceptor, self.ballot);
    }

    fn bind_commands_to_slots(&mut self) {
        // TODO(rescrv):  I know this is inefficient, but it is safe and clean.  Make it all three.
        let mut slot = self.lower_slot;
        while slot < self.upper_slot && slot < self.lower_slot + self.config.alpha() {
            let entry = self.proposals.entry(slot);
            if let Entry::Vacant(entry) = entry {
                let command = match self.commands.pop_front() {
                    Some(cmd) => cmd,
                    None => return,
                };
                entry.insert(PValueState::wrap(PValue {
                    slot,
                    ballot: self.ballot.clone(),
                    command,
                }));
            }
            slot += 1;
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

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
struct PValueState {
    pval: PValue,
}

impl PValueState {
    fn wrap(pval: PValue) -> PValueState {
        PValueState {
            pval
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::DEFAULT_ALPHA;
    use crate::testutil::*;
    use crate::PValue;

    #[derive(Eq, PartialEq)]
    struct TestLogger {
        lame_duck_superceded_by: Option<Ballot>,
        last_stale_proposal: Option<Ballot>,
        saw_misbehaving_server_error: bool,
        saw_in_lame_duck_error: bool,
        saw_pvalue_conflict_error: bool,
    }

    impl TestLogger {
        fn new() -> TestLogger {
            TestLogger {
                lame_duck_superceded_by: None,
                last_stale_proposal: None,
                saw_misbehaving_server_error: false,
                saw_in_lame_duck_error: false,
                saw_pvalue_conflict_error: false,
            }
        }

        fn assert_ok(&self) {
            assert!(self.lame_duck_superceded_by == None);
            assert!(self.last_stale_proposal == None);
            assert!(!self.saw_misbehaving_server_error);
            assert!(!self.saw_in_lame_duck_error);
            assert!(!self.saw_pvalue_conflict_error);
        }
    }

    struct TestMessenger {
        phase_1a_messages: Vec<(ReplicaID, Ballot)>,
    }

    impl TestMessenger {
        fn new() -> TestMessenger {
            TestMessenger {
                phase_1a_messages: Vec::new(),
            }
        }
    }

    impl Messenger for TestMessenger {
        fn send_phase_1a_message(&mut self, acceptor: &ReplicaID, ballot: &Ballot) {
            self.phase_1a_messages
                .push((acceptor.clone(), ballot.clone()));
        }
    }

    impl Logger for TestLogger {
        fn become_lame_duck(&mut self, superceded_by: &Ballot) {
            self.lame_duck_superceded_by = Some(superceded_by.clone());
        }

        fn stale_proposal(&mut self, stale: &Ballot) {
            self.last_stale_proposal = Some(stale.clone());
        }

        fn error_misbehaving_server(&mut self, _msg: &str) {
            self.saw_misbehaving_server_error = true;
        }

        fn error_in_lame_duck(&mut self) {
            self.saw_in_lame_duck_error = true;
        }

        fn error_pvalue_conflict(&mut self, _p1: &PValue, _p2: &PValue) {
            self.saw_pvalue_conflict_error = true;
        }
    }

    // Test that three replicas responding to phase one will achieve quorum.
    #[test]
    fn three_replicas_phase_one_no_pvalues() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // One
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Two makes quorum
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Three is a bonus
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
    }

    // Test that five replicas responding to phase one will achieve quorum.
    #[test]
    fn five_replicas_phase_one_no_pvalues() {
        let config = Configuration::bootstrap(&GROUP, FIVE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // One
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Two
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Three makes quorum
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Four is a bonus
        proposer.process_phase_1b_message(&REPLICA4, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        // Five is perfect
        proposer.process_phase_1b_message(&REPLICA5, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
    }

    // Test that proposer will enter into lame duck and self-enforce lame duck.
    #[test]
    fn lame_duck() {
        let config = Configuration::bootstrap(&GROUP, FIVE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // Seeing a higher ballot in phase one will cause it to lame-duck itself.
        proposer.process_phase_1b_message(&REPLICA1, &ballot_6_replica1(), &[]);
        assert_eq!(
            proposer.logger.lame_duck_superceded_by,
            Some(ballot_6_replica1())
        );
        proposer.logger.lame_duck_superceded_by = None;
        assert!(!proposer.logger.saw_in_lame_duck_error);
        proposer.logger.assert_ok();

        // Which means that if we try to make progress it will error out with a lame duck error
        proposer.make_progress();
        assert!(proposer.logger.saw_in_lame_duck_error);
        proposer.logger.saw_in_lame_duck_error = false;
        proposer.logger.assert_ok();

        // And subsequent phase one responses will get dropped.
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        assert!(proposer.logger.saw_in_lame_duck_error);
        proposer.logger.saw_in_lame_duck_error = false;
        proposer.logger.assert_ok();

        // As will phase two response.
        proposer.process_phase_2b_message();
        assert!(proposer.logger.saw_in_lame_duck_error);
        proposer.logger.saw_in_lame_duck_error = false;
        proposer.logger.assert_ok();

        // And of course that means no more proposals
        proposer.enqueue_command(Command{command: String::from("command")});
        assert!(proposer.logger.saw_in_lame_duck_error);
        proposer.logger.saw_in_lame_duck_error = false;
        proposer.logger.assert_ok();
    }

    // Test that an acceptor that's not part of the ensemble cannot accept.
    // Replicas one, two, three are included, four is a shadow, and five is excluded.
    #[test]
    fn outside_acceptors_not_allowed() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[REPLICA4]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // A phase one response from a member should always be good.
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // From a shadow should be good too, but should not advance the phase.
        proposer.process_phase_1b_message(&REPLICA4, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // From a non-member should cause misbehaving server error and not advance the phase.
        proposer.process_phase_1b_message(&REPLICA5, &ballot, &[]);
        assert!(proposer.logger.saw_misbehaving_server_error);
        assert_eq!(proposer.phase, PaxosPhase::ONE);
    }

    // Test that a stale phase one response has zero effect.
    #[test]
    fn stale_phase_one_response() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // One response.
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Stale second response should not be counted.
        proposer.process_phase_1b_message(&REPLICA2, &ballot_4_replica1(), &[]);
        assert_eq!(
            proposer.logger.last_stale_proposal,
            Some(ballot_4_replica1())
        );
        assert_eq!(proposer.phase, PaxosPhase::ONE);
    }

    // Test that an acceptor cannot sybil their way to having a leader.
    #[test]
    fn double_commitment_to_follow() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // One
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Can't do that twice
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);

        // Or three times
        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
    }

    // Test that acceptors with the same pvalues become accepted.
    #[test]
    fn normal_acceptors_with_pvalues() {
        let pval1 = PValue::new(1, ballot_5_replica1(), String::from("command"));
        let pval2 = PValue::new(2, ballot_6_replica2(), String::from("command"));
        let pval3 = PValue::new(3, ballot_6_replica2(), String::from("command"));

        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_7_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[pval1.clone(), pval2.clone()]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
        assert_eq!(proposer.proposals.len(), 2);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1.clone())));
        assert_eq!(proposer.proposals.get(&2), Some(&PValueState::wrap(pval2.clone())));

        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[pval2.clone(), pval3.clone()]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 3);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1.clone())));
        assert_eq!(proposer.proposals.get(&2), Some(&PValueState::wrap(pval2.clone())));
        assert_eq!(proposer.proposals.get(&3), Some(&PValueState::wrap(pval3.clone())));
    }

    // Test that the highest pvalue for the same slot gets retained.
    #[test]
    fn pvalues_for_the_same_slot() {
        let pval1 = PValue::new(1, ballot_5_replica1(), String::from("command"));
        let pval2a = PValue::new(2, ballot_6_replica1(), String::from("command"));
        let pval2b = PValue::new(2, ballot_6_replica2(), String::from("command"));
        let pval3a = PValue::new(3, ballot_6_replica1(), String::from("command"));
        let pval3b = PValue::new(3, ballot_6_replica2(), String::from("command"));

        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_7_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        proposer.process_phase_1b_message(
            &REPLICA1,
            &ballot,
            &[pval1.clone(), pval2a.clone(), pval3b.clone()],
        );
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
        assert_eq!(proposer.proposals.len(), 3);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1.clone())));
        assert_eq!(proposer.proposals.get(&2), Some(&PValueState::wrap(pval2a.clone())));
        assert_eq!(proposer.proposals.get(&3), Some(&PValueState::wrap(pval3b.clone())));

        proposer.process_phase_1b_message(
            &REPLICA2,
            &ballot,
            &[pval1.clone(), pval2b.clone(), pval3a.clone()],
        );
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 3);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1.clone())));
        assert_eq!(proposer.proposals.get(&2), Some(&PValueState::wrap(pval2b.clone())));
        assert_eq!(proposer.proposals.get(&3), Some(&PValueState::wrap(pval3b.clone())));
    }

    // Test that pvalues with same ballot/slot, but different command will be logged.
    #[test]
    fn pvalue_conflicts_are_logged() {
        let pval1a = PValue::new(1, ballot_6_replica1(), String::from("red fish"));
        let pval1b = PValue::new(1, ballot_6_replica1(), String::from("blue fish"));

        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_7_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[pval1a.clone()]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::ONE);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1a.clone())));

        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[pval1b.clone()]);
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1b.clone())));
        assert!(proposer.logger.saw_pvalue_conflict_error);
    }

    // Test that a late-arriving acceptor will be excluded until after the slot for which the
    // conflict happened.
    #[test]
    fn late_acceptors_will_delay() {
        let pval1a = PValue::new(1, ballot_6_replica1(), String::from("red fish"));
        let pval1b = PValue::new(1, ballot_7_replica1(), String::from("blue fish"));

        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_7_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[pval1a.clone()]);
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[pval1a.clone()]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1a.clone())));

        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[pval1b.clone()]);
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 1);
        assert_eq!(proposer.proposals.get(&1), Some(&PValueState::wrap(pval1a.clone())));
        assert!(match proposer.followers.get(&REPLICA3) {
            Some(r) => r.start_slot == 2,
            None => false,
        });
    }

    // Test that make_progess will send a phase one message to every unaccepted acceptor.
    #[test]
    fn make_progress_sends_phase_one() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        // At first, we must message everyone to make progress.
        proposer.make_progress();
        assert_eq!(proposer.messenger.phase_1a_messages.len(), 5);
        assert_eq!(
            proposer.messenger.phase_1a_messages[0],
            (REPLICA1.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[1],
            (REPLICA2.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[2],
            (REPLICA3.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[3],
            (REPLICA4.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[4],
            (REPLICA5.clone(), ballot.clone())
        );

        // If we were to get a response from REPLICA3, we should not pester it again.
        proposer.messenger.phase_1a_messages.clear();
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.make_progress();
        assert_eq!(proposer.messenger.phase_1a_messages.len(), 4);
        assert_eq!(
            proposer.messenger.phase_1a_messages[0],
            (REPLICA1.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[1],
            (REPLICA2.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[2],
            (REPLICA4.clone(), ballot.clone())
        );
        assert_eq!(
            proposer.messenger.phase_1a_messages[3],
            (REPLICA5.clone(), ballot.clone())
        );
    }

    // Test that the proposer discards values outside the slots it was chosen for.
    #[test]
    fn pvalues_before_configuration_start_are_discarded() {
        let pval = PValue::new(1, ballot_6_replica1(), String::from("red fish"));
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let config = config.reconfigure();
        let config = config.commit(128);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[pval]);
        assert_eq!(proposer.proposals.len(), 0);
    }

    // Test the sliding window for enqueued commands.
    #[test]
    fn sliding_window_over_commands() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);

        for i in 0u64..DEFAULT_ALPHA + 5u64 {
            proposer.enqueue_command(Command{command: String::from("command")});
            if i < DEFAULT_ALPHA {
                assert_eq!(proposer.proposals.len() as u64, i + 1);
                assert!(proposer.proposals.contains_key(&(i + 1)));
                assert_eq!(proposer.commands.len() as u64, 0);
            } else {
                assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA);
                assert!(!proposer.proposals.contains_key(&(i + 1)));
                assert_eq!(proposer.commands.len() as u64, i - DEFAULT_ALPHA + 1);
            }
        }

        proposer.advance_window(2);
        assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA);
        assert!(!proposer.proposals.contains_key(&1));
        assert!(proposer.proposals.contains_key(&(DEFAULT_ALPHA + 1)));
        assert!(!proposer.proposals.contains_key(&(DEFAULT_ALPHA + 2)));

        proposer.advance_window(7);
        assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA - 1);
        assert!(!proposer.proposals.contains_key(&6));
        assert!(proposer.commands.len() == 0);

        proposer.enqueue_command(Command{command: String::from("command")});
        assert_eq!(proposer.proposals.len() as u64, DEFAULT_ALPHA);
        assert!(proposer.proposals.contains_key(&(DEFAULT_ALPHA + 6)));
        assert!(proposer.commands.len() == 0);
    }

    // Test that commands get enqueued on transition to phase two.
    #[test]
    fn enqueue_prior_to_phase_two() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = ballot_5_replica1();
        let mut logger = TestLogger::new();
        let mut messenger = TestMessenger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger, &mut messenger);

        for i in 0..7 {
            proposer.enqueue_command(Command{command: String::from("command")});
            assert_eq!(proposer.proposals.len(), 0);
            assert_eq!(proposer.commands.len(), (i + 1) as usize);
        }

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.logger.assert_ok();
        assert_eq!(proposer.phase, PaxosPhase::TWO);
        assert_eq!(proposer.proposals.len(), 7);
    }

    // TODO(rescrv):
    // - clean up redundancy
    // - enqueue prior to phase two
    // - advance window aborts if backwards
    // - stop_at_slot
}
