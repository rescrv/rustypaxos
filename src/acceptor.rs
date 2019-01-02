use crate::types::Ballot;
use crate::types::PValue;
use crate::AcceptorAction;
use crate::Environment;
use crate::Message;

// An acceptor is the durable memory of the system.  In phase one, the acceptor commits to follow a
// ballot for a range of slots if and only if no higher ballots have been accepted for those slots.
// In phase two, the acceptor commits to remember proposed values for slots (pvalues) if and only
// if the ballot under which the pvalue is proposed is the highest ballot the acceptor has
// committed to following for that slot.
pub struct Acceptor {
    ballots: BallotTracker,
    pvalues: Vec<PValue>,
}

impl Acceptor {
    pub fn new() -> Acceptor {
        Acceptor {
            ballots: BallotTracker::new(),
            pvalues: Vec::new(),
        }
    }

    pub fn highest_ballot(&self) -> Ballot {
        self.ballots.highest_ballot()
    }

    pub fn process_phase_1a_message(
        &mut self,
        env: &mut Environment,
        ballot: &Ballot,
        start: u64,
        limit: u64,
    ) {
        if self.ballots.maybe_adopt(*ballot, start, limit) {
            env.persist_acceptor(AcceptorAction::FollowBallot {
                ballot: *ballot,
                start,
                limit,
            });
            env.send_when_persistent(Message::Phase1B {
                ballot: *ballot,
                pvalues: self.pvalues_for(start, limit),
            });
        } else {
            // There's some nuance here.  Normal acceptor actions are to update state in memory,
            // persist it on disk, and then expose the decisions made in memory only after the
            // acceptor state is persistent, which is done by using the send_when_persistent call
            // on the Environment object
            //
            // Normally, we never expose the effects of the in-memory state until everything is
            // persistent because we need to remember the decisions we made.  NACK is an advisory
            // state that's akin to saying, "I'm giving you positive confirmation that I received
            // your request, but I cannot actually act on it.
            //
            // Theoretically we could just never send these requests and the protocol would still
            // be safe.  Similarly, we could spuriously send these requests and sabotage the
            // progress of a well-meaning proposer.
            //
            // Put another way:  If we forget that we ever sent this NACK, the protocol is fine.
            // If we send a NACK spuriously, the protocol is fine.  We could flip a coin on every
            // message and send a NACK if it comes up tails without any effect on safety
            //
            // Because NACK does not affect safety, we can send without waiting for persistence.
            env.send(Message::ProposerNACK { ballot: *ballot });
        }
    }

    pub fn process_phase_2a_message(&mut self, env: &mut Environment, pval: PValue) {
        if self.ballots.ballot_for_slot(pval.slot()) == pval.ballot() {
            env.persist_acceptor(AcceptorAction::AcceptProposal { pval: pval.clone() });
            env.send_when_persistent(Message::Phase2B {
                ballot: pval.ballot(),
                slot: pval.slot(),
            });
            // send phase1b when durable =d
            self.pvalues.push(pval);
        } else {
            // See the comment above the other NACK.
            env.send(Message::ProposerNACK {
                ballot: pval.ballot(),
            });
        }
    }

    fn pvalues_for(&self, start: u64, limit: u64) -> Vec<PValue> {
        let mut values = Vec::with_capacity(self.pvalues.len());
        for pval in self.pvalues.iter() {
            if start <= pval.slot() && pval.slot() < limit {
                values.push(pval.clone());
            }
        }
        values
    }
}

// BallotTracker keeps an unsorted vector of the active ballots.  It guarantees that no two
// elements [start, limit) ranges overlap.  The data structure uses a vector over anything more
// complicated because we expect the number of active ballots to always be small and a linear scan
// is both easy to reason about and will probably be on-par with pointer-rich structures.
struct BallotTracker {
    ballots: Vec<ActiveBallot>,
}

impl BallotTracker {
    fn new() -> BallotTracker {
        BallotTracker {
            ballots: Vec::new(),
        }
    }

    fn highest_ballot(&self) -> Ballot {
        let mut ballot = Ballot::BOTTOM;
        for ab in self.ballots.iter() {
            if ballot < ab.ballot {
                ballot = ab.ballot;
            }
        }
        ballot
    }

    fn maybe_adopt(&mut self, ballot: Ballot, start: u64, limit: u64) -> bool {
        // First pass, scan for hard conflicts that stop us from taking up this proposal
        // No sense trying to do anything if there is a conflict.
        //
        // Note that we could have a more complicated system where we partially accept the ballot
        // and nack it at phase two, but that's much more complicated to reason about.
        for ab in self.ballots.iter() {
            if ab.overlaps(start, limit) && ballot < ab.ballot {
                return false;
            }
        }
        // Second pass, scan for soft conflicts that require us to adjust the boundaries of
        // previous commitments.
        // TODO(rescrv):  Make this more efficient than copying.
        let mut new_ballots = Vec::with_capacity(self.ballots.len() + 2);
        for ab in self.ballots.iter() {
            // Take the simple case of no overlap
            if !ab.overlaps(start, limit) {
                new_ballots.push(*ab);
                continue;
            }
            // When there is overlap, we can break it down into zero or more of these cases:
            // LOWER:  The existing ab was promised for slots less than start
            if ab.start < start && start < ab.limit {
                new_ballots.push(ActiveBallot {
                    ballot: ab.ballot,
                    start: ab.start,
                    limit: start,
                });
            }
            // UPPER:  The existing ab was promised for slots greater than limit
            if ab.start <= limit && limit < ab.limit {
                new_ballots.push(ActiveBallot {
                    ballot: ab.ballot,
                    start: limit,
                    limit: ab.limit,
                });
            }
            // To convince yourself this is correct, consider the case of an existing active ballot
            // that covers the whole range [start-i, limit+j) for i,j > 0.  We can see that in the
            // first case, it will fill in the range [ab.start, start) and in the second case it
            // will fill in the range [limit, ab.limit).  This will cover the original range of
            // [ab.start, ab.limit) with a nice hole of [start, limit) cut out at the center.  Then
            // look at the border cases where ab.start approaches start and ab.limit approaches
            // limit to see that they are handled correctly too.
        }
        new_ballots.push(ActiveBallot {
            ballot,
            start,
            limit,
        });
        self.ballots = new_ballots;
        true
    }

    fn ballot_for_slot(&self, slot: u64) -> Ballot {
        for ab in self.ballots.iter() {
            if ab.start <= slot && slot < ab.limit {
                return ab.ballot;
            }
        }
        Ballot::BOTTOM
    }

    #[cfg(test)]
    fn ordered_promises(&self) -> Vec<ActiveBallot> {
        let mut ballots = self.ballots.clone();
        ballots.sort_unstable_by(|lhs, rhs| lhs.start.cmp(&rhs.start));
        ballots
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
struct ActiveBallot {
    ballot: Ballot,
    start: u64,
    limit: u64,
}

impl ActiveBallot {
    fn overlaps(&self, start: u64, limit: u64) -> bool {
        (start <= self.start && self.start < limit) || (self.start <= start && start < self.limit)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::testutil::*;
    use crate::configuration::ReplicaID;
    use crate::types::Command;
    use crate::Misbehavior;

    use super::*;

    struct TestEnvironment {
        send_persistent: bool,
        nacked: Vec<Ballot>,
        actions: Vec<AcceptorAction>,
        phase_1b_messages: Vec<(Ballot, Vec<PValue>)>,
        phase_2b_messages: Vec<(Ballot, u64)>,
    }

    impl TestEnvironment {
        fn new() -> TestEnvironment {
            TestEnvironment {
                send_persistent: false,
                nacked: Vec::new(),
                actions: Vec::new(),
                phase_1b_messages: Vec::new(),
                phase_2b_messages: Vec::new(),
            }
        }
    }

    impl Environment for TestEnvironment {
        fn send(&mut self, msg: Message) {
            match msg {
                Message::ProposerNACK { ballot } => {
                    self.nacked.push(ballot);
                }
                _ => {
                    panic!("unexpected non-durable message {:?}", msg);
                }
            }
        }

        fn persist_acceptor(&mut self, action: AcceptorAction) {
            if self.send_persistent {
                panic!("persist_acceptor called after send_when_persistent");
            }
            self.actions.push(action);
        }

        fn send_when_persistent(&mut self, msg: Message) {
            self.send_persistent = true;
            match msg {
                Message::Phase1B { ballot, pvalues } => {
                    self.phase_1b_messages.push((ballot, pvalues));
                }
                Message::Phase2B { ballot, slot } => {
                    self.phase_2b_messages.push((ballot, slot));
                }
                _ => {
                    panic!("unexpected durable message {:?}", msg);
                }
            }
        }

        fn report_misbehavior(&mut self, _m: Misbehavior) {
            panic!("no misbehavior in acceptor");
        }
    }

    // Tests that the acceptor adopts a ballot and persists/sends.
    #[test]
    fn acceptor_follows() {
        let mut acc = Acceptor::new();
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_5_REPLICA1, 0, 64);
        assert_eq!(&env.nacked, &[]);
        assert_eq!(
            &env.actions,
            &[AcceptorAction::FollowBallot {
                ballot: BALLOT_5_REPLICA1,
                start: 0,
                limit: 64
            }],
        );
        assert_eq!(&env.phase_1b_messages, &[(BALLOT_5_REPLICA1, [].to_vec()),]);
        assert_eq!(&env.phase_2b_messages, &[]);
    }

    // Tests that the acceptor will NACK when a ballot is not accepted.
    #[test]
    fn acceptor_nacks_phase_1() {
        let mut acc = Acceptor::new();
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_5_REPLICA1, 0, 64);
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_4_REPLICA1, 0, 64);
        assert_eq!(&env.nacked, &[BALLOT_4_REPLICA1]);
        assert_eq!(&env.actions, &[]);
        assert_eq!(&env.phase_1b_messages, &[]);
        assert_eq!(&env.phase_2b_messages, &[]);
    }

    // Tests that the acceptor adopts a ballot and persists/sends.
    #[test]
    fn acceptor_accepts_proposal() {
        let pval = PValue::new(32, BALLOT_5_REPLICA1, Command::data("command"));
        let mut acc = Acceptor::new();
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_5_REPLICA1, 0, 64);
        let mut env = TestEnvironment::new();
        acc.process_phase_2a_message(&mut env, pval.clone());
        assert_eq!(&env.nacked, &[]);
        assert_eq!(
            &env.actions,
            &[AcceptorAction::AcceptProposal { pval: pval.clone() }]
        );
        assert_eq!(&env.phase_1b_messages, &[]);
        assert_eq!(&env.phase_2b_messages, &[(BALLOT_5_REPLICA1, 32)]);
        assert_eq!(acc.pvalues, &[pval.clone()]);
    }

    // Test that the acceptor will NACK a proposal
    #[test]
    fn acceptor_nacks_proposal() {
        let pval = PValue::new(32, BALLOT_5_REPLICA1, Command::data("command"));
        let mut acc = Acceptor::new();
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_5_REPLICA1, 0, 64);
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_6_REPLICA2, 0, 64);
        let mut env = TestEnvironment::new();
        acc.process_phase_2a_message(&mut env, pval.clone());
        assert_eq!(&env.nacked, &[BALLOT_5_REPLICA1]);
        assert_eq!(&env.actions, &[]);
        assert_eq!(&env.phase_1b_messages, &[]);
        assert_eq!(&env.phase_2b_messages, &[]);
        assert_eq!(acc.pvalues, &[]);
    }

    // Test that the acceptor returns previous pvalues
    #[test]
    fn acceptor_returns_pvalues() {
        let pval = PValue::new(32, BALLOT_5_REPLICA1, Command::data("command"));
        let mut acc = Acceptor::new();
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_5_REPLICA1, 0, 64);
        let mut env = TestEnvironment::new();
        acc.process_phase_2a_message(&mut env, pval.clone());
        let mut env = TestEnvironment::new();
        acc.process_phase_1a_message(&mut env, &BALLOT_6_REPLICA1, 0, 64);
        assert_eq!(&env.nacked, &[]);
        assert_eq!(
            &env.actions,
            &[AcceptorAction::FollowBallot {
                ballot: BALLOT_6_REPLICA1,
                start: 0,
                limit: 64
            }],
        );
        assert_eq!(
            &env.phase_1b_messages,
            &[(BALLOT_6_REPLICA1, [pval.clone()].to_vec()),]
        );
        assert_eq!(&env.phase_2b_messages, &[]);
        assert_eq!(acc.pvalues, &[pval.clone()]);
    }

    fn check_ordered_promises(bt: &BallotTracker, promises: &[ActiveBallot]) {
        let observed: &[ActiveBallot] = &bt.ordered_promises();
        assert_eq!(promises, observed);
        for ab in observed {
            for slot in ab.start..ab.limit {
                assert_eq!(ab.ballot, bt.ballot_for_slot(slot));
            }
        }
    }

    // Test that an empty ballot tracker has BOTTOM as its highest ballot.
    #[test]
    fn ballot_tracker_highest_ballot_bottom() {
        let bt = BallotTracker::new();
        assert_eq!(Ballot::BOTTOM, bt.highest_ballot());
        check_ordered_promises(&bt, &[]);
    }

    // Test that a single adopted ballot becomes the highest ballot.
    #[test]
    fn ballot_tracker_highest_ballot() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert_eq!(BALLOT_5_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[ActiveBallot {
                ballot: BALLOT_5_REPLICA1,
                start: 0,
                limit: 64,
            }],
        );
    }

    // Test that the highest ballot wins the highest ballot contest.
    #[test]
    fn ballot_tracker_adopt_success() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_6_REPLICA1, 0, 64));
        assert_eq!(BALLOT_6_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[ActiveBallot {
                ballot: BALLOT_6_REPLICA1,
                start: 0,
                limit: 64,
            }],
        );
    }

    // Test that a lower ballot cannot be accepted after a higher ballot.
    #[test]
    fn ballot_tracker_adopt_failure() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(!bt.maybe_adopt(BALLOT_4_REPLICA1, 0, 64));
        assert_eq!(BALLOT_5_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[ActiveBallot {
                ballot: BALLOT_5_REPLICA1,
                start: 0,
                limit: 64,
            }],
        );
    }

    // Test that a higher ballot will override the start range of a lower ballot.
    #[test]
    fn ballot_tracker_adopt_start_range() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_6_REPLICA1, 0, 32));
        assert_eq!(BALLOT_6_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[
                ActiveBallot {
                    ballot: BALLOT_6_REPLICA1,
                    start: 0,
                    limit: 32,
                },
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 32,
                    limit: 64,
                },
            ],
        );
    }

    // Test that a higher ballot will override the end range of a lower ballot.
    #[test]
    fn ballot_tracker_adopt_end_range() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_6_REPLICA1, 32, 64));
        assert_eq!(BALLOT_6_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 0,
                    limit: 32,
                },
                ActiveBallot {
                    ballot: BALLOT_6_REPLICA1,
                    start: 32,
                    limit: 64,
                },
            ],
        );
    }

    // Test that a higher ballot will override the center of the range of a lower ballot.
    #[test]
    fn ballot_tracker_adopt_split_range() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_6_REPLICA1, 16, 48));
        assert_eq!(BALLOT_6_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 0,
                    limit: 16,
                },
                ActiveBallot {
                    ballot: BALLOT_6_REPLICA1,
                    start: 16,
                    limit: 48,
                },
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 48,
                    limit: 64,
                },
            ],
        );
    }

    // Test that a ballot that's not the highest, but is higher than all ballots for its slots, can
    // get adopted by the ballot tracker.
    #[test]
    fn ballot_tracker_adopt_backfill_not_highest() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_7_REPLICA1, 48, 64));
        assert!(bt.maybe_adopt(BALLOT_6_REPLICA1, 8, 12));
        assert_eq!(BALLOT_7_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 0,
                    limit: 8,
                },
                ActiveBallot {
                    ballot: BALLOT_6_REPLICA1,
                    start: 8,
                    limit: 12,
                },
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 12,
                    limit: 48,
                },
                ActiveBallot {
                    ballot: BALLOT_7_REPLICA1,
                    start: 48,
                    limit: 64,
                },
            ],
        );
    }

    // Test that a backfill can be the highest ballot.
    #[test]
    fn ballot_tracker_adopt_backfill_highest() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_6_REPLICA1, 48, 64));
        assert!(bt.maybe_adopt(BALLOT_7_REPLICA1, 8, 12));
        assert_eq!(BALLOT_7_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 0,
                    limit: 8,
                },
                ActiveBallot {
                    ballot: BALLOT_7_REPLICA1,
                    start: 8,
                    limit: 12,
                },
                ActiveBallot {
                    ballot: BALLOT_5_REPLICA1,
                    start: 12,
                    limit: 48,
                },
                ActiveBallot {
                    ballot: BALLOT_6_REPLICA1,
                    start: 48,
                    limit: 64,
                },
            ],
        );
    }

    // Test that a ballot can be adopted twice.
    #[test]
    fn ballot_tracker_adopt_twice() {
        let mut bt = BallotTracker::new();
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert!(bt.maybe_adopt(BALLOT_5_REPLICA1, 0, 64));
        assert_eq!(BALLOT_5_REPLICA1, bt.highest_ballot());
        check_ordered_promises(
            &bt,
            &[ActiveBallot {
                ballot: BALLOT_5_REPLICA1,
                start: 0,
                limit: 64,
            }],
        );
    }

    // Test that some random assignments of ballots matches our expectations.
    #[test]
    fn ballot_tracker_random() {
        const ITERS: u64 = 1000;
        const SLOTS: usize = 100;
        let mut rng = rand::thread_rng();
        let mut bt = BallotTracker::new();
        let mut active = [Ballot::BOTTOM; SLOTS];
        for i in 0..ITERS {
            let b = Ballot::new(i, ReplicaID::BOTTOM);
            let start = rng.gen_range(0, SLOTS);
            let limit = rng.gen_range(start, SLOTS);
            if start == limit {
                continue;
            }
            for s in start..limit {
                active[s] = b;
            }
            assert!(bt.maybe_adopt(b, start as u64, limit as u64));
            for s in 0..SLOTS {
                assert_eq!(active[s], bt.ballot_for_slot(s as u64));
            }
        }
    }
}
