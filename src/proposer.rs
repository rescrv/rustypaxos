use super::Ballot;
use super::PValue;
use super::ReplicaID;
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

pub struct ProposerConstants<'a> {
    // The Ballot which this proposer is shepherding forward.
    ballot: Ballot,
    // The acceptors this proposer is courting.
    acceptors: &'a [ReplicaID],
    // The range of slots this proposer is valid for.  [0, 1).
    slots: (u64, u64),
    // The number of proposals that may be issued concurrently.
    alpha: u64,
}

pub struct Proposer<'a, L: Logger> {
    // ProposerConstants that govern this particular proposer
    constants: &'a ProposerConstants<'a>,
    // The Logger to which state transitions and notable events will be logged.
    logger: &'a mut L,
    // Is this proposer a lame_duck now because another higher ballot is floating around?
    // Note that this proposer technically doesn't have to even acknowledge this and the protocol
    // should still be safe because acceptors will ignore this proposer.
    is_lame_duck: bool,
    // Values to be proposed and pushed forward by this proposer.
    proposals: HashMap<u64, PValue>,
    // Acceptors that follow this proposer.
    followers: HashMap<&'a ReplicaID, FollowerState>,
}

impl<'a, L: Logger> Proposer<'a, L> {
    pub fn new(
        constants: &'a ProposerConstants,
        logger: &'a mut L,
    ) -> Proposer<'a, L> {
        Proposer {
            constants,
            logger,
            is_lame_duck: false,
            proposals: HashMap::new(),
            followers: HashMap::new(),
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
        if self.is_lame_duck {
            self.logger.error_in_lame_duck();
            return;
        }
        // Provide basic protection against misbehaving servers participating in the protocol.
        if !self.is_acceptor(acceptor) {
            self.logger.error_misbehaving_server("not an acceptor");
            return;
        }
        // Become a lame duck if this proposer has been superceded.
        if self.constants.ballot < *current {
            self.logger.become_lame_duck(current);
            return;
        }
        // Drop this message if it is old and out of date.
        if *current < self.constants.ballot {
            // TODO(rescrv):  this is a bad condition, but whether it's an error depends on if the
            // layer routing to the proposer is guaranteeing that old proposer's messages won't be
            // sent to new proposers
            return;
        }
        // Protect against future programmers.
        assert!(
            *current == self.constants.ballot,
            "we can only proceed if the ballots are equal"
        );
        // If already accepted, do not go through it again.
        if self.followers.contains_key(acceptor) {
            // TODO(rescrv): might be useful to log this.
            return;
        }
        let state = FollowerState::new(self.constants.slots.0);
        // Integrate the pvalues from the acceptor.
        for pval in pvalues {
            if pval.slot < self.constants.slots.0 || self.constants.slots.1 <= pval.slot {
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
        self.followers.insert(self.get_acceptor(acceptor), state);
    }

    // Is the provided ReplicaID among the set of acceptors this proposer is courting?
    fn is_acceptor(&self, acceptor: &ReplicaID) -> bool {
        self.constants.acceptors.iter().any(|a| a == acceptor)
    }

    // Turn the reference to a ReplicaID into an equivalently-valued ReplicaID that outlasts the
    // proposer's lifetime.
    fn get_acceptor(&self, acceptor: &ReplicaID) -> &'a ReplicaID {
        for i in 0..self.constants.acceptors.len() {
            if self.constants.acceptors[i] == *acceptor {
                return &self.constants.acceptors[i];
            }
        }
        panic!("cannot call get_acceptor(A) when !is_acceptor(A)")
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

    // TODO(rescrv): dedupe
    pub const REPLICA1: ReplicaID = ReplicaID { XXX: 1 };
    pub const REPLICA2: ReplicaID = ReplicaID { XXX: 2 };
    pub const REPLICA3: ReplicaID = ReplicaID { XXX: 3 };
    pub const REPLICA4: ReplicaID = ReplicaID { XXX: 4 };
    pub const REPLICA5: ReplicaID = ReplicaID { XXX: 5 };

    pub const THREE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3];
    pub const FIVE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3, REPLICA4, REPLICA5];

    #[derive(Eq,PartialEq)]
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
        let PC = ProposerConstants {
            ballot : Ballot::new(5, &THREE_REPLICAS[0]),
            acceptors : THREE_REPLICAS,
            slots: (0, u64::max_value()),
            alpha: 16,
        };
        let mut logger = TestLogger::new();
        let mut proposer = Proposer::new(&PC, &mut logger);

        proposer.process_phase_1b_message(&REPLICA1, &PC.ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA2, &PC.ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA3, &PC.ballot, &[]);
        proposer.logger.assert_ok();
    }

    #[test]
    fn five_replicas_phase_one_no_pvalues() {
        let PC = ProposerConstants {
            ballot : Ballot::new(5, &FIVE_REPLICAS[0]),
            acceptors : FIVE_REPLICAS,
            slots: (0, u64::max_value()),
            alpha: 16,
        };
        let mut logger = TestLogger::new();
        let mut proposer = Proposer::new(&PC, &mut logger);

        proposer.process_phase_1b_message(&REPLICA1, &PC.ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA2, &PC.ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA3, &PC.ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA4, &PC.ballot, &[]);
        proposer.logger.assert_ok();
        proposer.process_phase_1b_message(&REPLICA5, &PC.ballot, &[]);
        proposer.logger.assert_ok();
    }
}
