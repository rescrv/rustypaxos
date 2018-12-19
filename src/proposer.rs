use super::Ballot;
use super::configuration::ReplicaID;
use super::configuration::Configuration;
use super::PValue;
use super::PaxosPhase;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub trait Logger {
    // TODO(rescrv) figure out details

    // transitioning to lame duck
    fn become_lame_duck(&mut self, superceded_by: &Ballot);

    // someone sent a bad message
    fn error_misbehaving_server(&mut self, msg: &str);
    // no one should call us in lame duck, so log if they do
    fn error_in_lame_duck(&mut self);
    // pvalues have invariants
    fn error_pvalue_conflict(&mut self, p1: &PValue, p2: &PValue);
}

pub struct Proposer<'a, L: Logger> {
    // The configuration under which this proposer operates.  Proposers will not operate under
    // different configs; they will die and a new one will rise in their place.
    config: &'a Configuration,
    // The Ballot which this proposer is shepherding forward.
    ballot: &'a Ballot,
    // The Logger to which state transitions and notable events will be logged.
    logger: &'a mut L,

    // Is this proposer a lame_duck now because another higher ballot is floating around?
    // Note that this proposer technically doesn't have to even acknowledge this and the protocol
    // should still be safe because that's the point of Paxos.
    is_lame_duck: bool,
    // Phase of paxos in which this proposer believes itself to be.
    phase: PaxosPhase,
    // Values to be proposed and pushed forward by this proposer.
    proposals: HashMap<u64, PValue>,
    // Acceptors that follow this proposer.
    followers: HashMap<&'a ReplicaID, FollowerState>,
    // The lower and upper bounds of the range this proposer will push forward.
    lower_slot: u64,
    upper_slot: u64,
}

impl<'a, L: Logger> Proposer<'a, L> {
    pub fn new(
        config: &'a Configuration,
        ballot: &'a Ballot,
        logger: &'a mut L,
    ) -> Proposer<'a, L> {
        Proposer {
            config,
            ballot,
            logger,
            is_lame_duck: false,
            phase: PaxosPhase::ONE,
            proposals: HashMap::new(),
            followers: HashMap::new(),
            lower_slot: config.first_valid_slot(),
            upper_slot: u64::max_value(),
        }
    }

    pub fn make_progress(&mut self) {
        if self.is_lame_duck {
            self.logger.error_in_lame_duck();
            return;
        }

        // TODO(rescrv)

        // for each acceptor that has not yet passed phase1:
        // send a phase1 with some amount of backoff

        // if we have quorum, move onto phase2
    }

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
            self.logger.become_lame_duck(current);
            return;
        }
        // Drop this message if it is old and out of date.
        if current < self.ballot {
            // TODO(rescrv):  this is a bad condition, but whether it's an error depends on if the
            // layer routing to the proposer is guaranteeing that old proposer's messages won't be
            // sent to new proposers
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
        let state = FollowerState::new(self.lower_slot);
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
                Entry::Occupied(ref entry) if pval.ballot < entry.get().ballot => {
                    // TODO(rescrv): if we have moved onto phase two, we must not involve the
                    // acceptor until pval.slot + 1 because it would break the protocol
                }
                // Otherwise we know the pval from the acceptor has a ballot at least as high as
                // the entry we already have in the map and we should adopt this as our proposal
                // for this particular slot.
                Entry::Occupied(mut entry) => {
                    // It's a protocol invariant that two pvalues with the same ballot must have
                    // the same command.  Because we enforce this in a distributed fashion across
                    // all replicas, we cannot do anything except check to make sure this is true
                    // and log when it is violated.
                    if entry.get().ballot == pval.ballot && entry.get().command != pval.command {
                        self.logger.error_pvalue_conflict(entry.get(), pval);
                    }
                    *entry.get_mut() = pval.clone();
                }
                // If there is nothing for this slot yet, the protocol obligates us to use this
                // provided pvalue.
                Entry::Vacant(entry) => {
                    entry.insert(pval.clone());
                }
            }
        }
        // Record that the acceptor follows us.
        self.followers.insert(self.config.member_reference(acceptor), state);
    }
}

struct FollowerState {
    start_slot: u64,
}

impl FollowerState {
    fn new(start_slot: u64) -> FollowerState {
        FollowerState { start_slot }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::*;

    #[derive(Eq, PartialEq)]
    struct TestLogger {
        lame_duck_superceded_by: Option<Ballot>,
        saw_misbehaving_server_error: bool,
        saw_in_lame_duck_error: bool,
        saw_pvalue_conflict_error: bool,
    }

    impl TestLogger {
        fn new() -> TestLogger {
            TestLogger {
                lame_duck_superceded_by: None,
                saw_misbehaving_server_error: false,
                saw_in_lame_duck_error: false,
                saw_pvalue_conflict_error: false,
            }
        }

        fn assert_ok(&self) {
            assert!(self.lame_duck_superceded_by == None);
            assert!(!self.saw_misbehaving_server_error);
            assert!(!self.saw_in_lame_duck_error);
            assert!(!self.saw_pvalue_conflict_error);
        }
    }

    impl Logger for TestLogger {
        fn become_lame_duck(&mut self, superceded_by: &Ballot) {
            self.lame_duck_superceded_by = Some(superceded_by.clone());
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

    #[test]
    fn three_replicas_phase_one_no_pvalues() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, &[]);
        let ballot = Ballot::new(1, &THREE_REPLICAS[0]);
        let mut logger = TestLogger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger);

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.logger.assert_ok();
    }

    #[test]
    fn five_replicas_phase_one_no_pvalues() {
        let config = Configuration::bootstrap(&GROUP, FIVE_REPLICAS, &[]);
        let ballot = Ballot::new(5, &FIVE_REPLICAS[0]);
        let mut logger = TestLogger::new();
        let mut proposer = Proposer::new(&config, &ballot, &mut logger);

        proposer.process_phase_1b_message(&REPLICA1, &ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA2, &ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA3, &ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA4, &ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA5, &ballot, &[]);
        proposer.logger.assert_ok();
    }

    // TODO(rescrv):
    // - test proposer enters lame duck
    // - test lame duck will error_in_lame_duck
    // - test with an acceptor not part of the ensemble
    // - test that stale phase 1b has zero effect
    // - test an acceptor double-accepting
    // - test slots outside the mandate of the proposer
    // - test acceptors same pval
    // - test acceptors different pvals with diff ballot same slot
    // - test acceptors "same" pvals different commands
    // - add helper to look at the followers
}
