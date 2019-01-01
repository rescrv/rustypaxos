use super::PValue;
use crate::configuration::ReplicaID;
use crate::Ballot;
use crate::Environment;

// An acceptor is the durable memory of the system.  In phase one, the acceptor commits to follow a
// ballot for a range of slots if and only if no higher ballots have been accepted for those slots.
// In phase two, the acceptor commits to remember proposed values for slots (pvalues) if and only
// if the ballot under which the pvalue is proposed is the highest ballot the acceptor has
// committed to following for that slot.
pub struct Acceptor {
    ballots: BallotTracker,
}

impl Acceptor {
    pub fn new() -> Acceptor {
        Acceptor {
            ballots: BallotTracker::new(),
        }
    }

    pub fn highest_ballot(&self) -> Ballot {
        self.ballots.highest_ballot()
    }

    pub fn process_phase_1a_message(
        &mut self,
        env: &mut Environment,
        proposer: &ReplicaID,
        ballot: &Ballot,
        start: u64,
        limit: u64,
    ) {
        if self.ballots.maybe_adopt(*ballot, start, limit) {
            // make it durable
            // d = env.persist(phase1b(ballot, start, limit))
            // a.durable = d
            // send phase1b when durable=d
        } else {
            // send nack
        }
    }

    pub fn process_phase_2a_message(
        &mut self,
        env: &mut Environment,
        proposer: &ReplicaID,
        pval: &PValue,
    ) {
        if self.ballots.ballot_for_slot(pval.slot) == pval.ballot {
            // make it durable
            // d = env.persist(phase1b(pval))
            // a.durable =d
            // send phase1b when durable =d
            // self.pvales = append(self.pvalues, pval);
        } else {
            // send nack
        }
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

    use super::*;

    fn check_ordered_promises(bt: &BallotTracker, promises: &[ActiveBallot]) {
        let observed: &[ActiveBallot] = &bt.ordered_promises();
        assert_eq!(promises, observed);
        for ab in observed {
            for slot in ab.start..ab.limit {
                assert_eq!(ab.ballot, bt.ballot_for_slot(slot));
            }
        }
    }

    #[test]
    fn ballot_tracker_highest_ballot_bottom() {
        let bt = BallotTracker::new();
        assert_eq!(Ballot::BOTTOM, bt.highest_ballot());
        check_ordered_promises(&bt, &[]);
    }

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

    #[test]
    fn ballot_tracker_random() {
        const ITERS: u64 = 1000;
        const SLOTS: usize = 100;
        let mut rng = rand::thread_rng();
        let mut bt = BallotTracker::new();
        let mut active = [Ballot::BOTTOM; SLOTS];
        for i in 0..ITERS {
            let b = Ballot {
                number: i,
                leader: ReplicaID::BOTTOM,
            };
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
