//! The core data types of Paxos as it would be described academically.

use std::fmt;

use crate::configuration::ReplicaID;

/// A ballot is synonymous with the ballot described in the Paxos literature.
///
/// Ballots are neither created nor destroyed, they just exist.  The overall protocol must
/// guarantee that no two replicas ever work the synod protocol using the same ballot.  To
/// accomplish this, a ballot is the ordered pair of (number, leader), where only the listed leader
/// is allowed to issue proposals under the ballot.
///
/// Ballots are comparable.  When `ballot1` < `ballot2`, we will say that ballot2 supersedes
/// ballot1.  The comparison is lexicographic by (number, leader), which ensures that a proposer
/// whose ballot is superseded by a competing proposer can select another ballot to supersede
/// either of the first two.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Ballot {
    number: u64,
    leader: ReplicaID,
}

impl Ballot {
    /// The smallest possible ballot.
    pub const BOTTOM: Ballot = Ballot {
        number: 0,
        leader: ReplicaID::BOTTOM,
    };

    /// The largest possible ballot.
    pub const TOP: Ballot = Ballot {
        number: u64::max_value(),
        leader: ReplicaID::TOP,
    };

    /// Create a new Ballot for (number, leader).
    pub fn new(number: u64, leader: ReplicaID) -> Ballot {
        Ballot {
            number,
            leader: leader,
        }
    }

    /// The number of this ballot.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The leader for this ballot.
    pub fn leader(&self) -> ReplicaID {
        self.leader
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ballot:{}:{}", self.number, self.leader.viewable_id())
    }
}

/// A Proposed Value, or PValue, is commonly referred to as a "decree" in the Paxos papers.
///
/// PValues are triples of (slot, ballot, command) and can be interpreted as, "The proposer
/// championing `ballot` proposes putting `command` into `slot`".
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct PValue {
    slot: u64,
    ballot: Ballot,
    command: Command,
}

impl PValue {
    /// Create a new PValue for (slot, ballot, command).
    pub fn new(slot: u64, ballot: Ballot, command: Command) -> PValue {
        PValue {
            slot,
            ballot,
            command,
        }
    }

    /// The slot for which this pvalue proposes a value.
    pub fn slot(&self) -> u64 {
        self.slot
    }

    /// The ballot the proposer is using to claim authority to propose.
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The command to be proposed.
    pub fn command(&self) -> &Command {
        &self.command
    }
}

impl fmt::Display for PValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "pvalue:{}:{}:{}",
            self.slot,
            self.ballot.number,
            self.ballot.leader.viewable_id()
        )
    }
}

/// A Command is the "value" to be decided in the Paxos protocol.
///
/// Commands can come from outside the system as "data", or can be generated by the system as part
/// of its own internal maintenance and operation.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct Command {
    command: String,
}

impl Command {
    /// Create a new command that agrees upon user data in the system.
    ///
    /// TODO(rescrv): the command should not be str by default, but arbitrary bytes.
    pub fn data(s: &str) -> Command {
        Command {
            command: s.to_string(),
        }
    }
}

#[cfg(test)]
pub mod testutil {
    use super::Ballot;

    pub use crate::configuration::testutil::*;

    pub const BALLOT_4_REPLICA1: Ballot = Ballot {
        number: 4,
        leader: REPLICA1,
    };

    pub const BALLOT_5_REPLICA1: Ballot = Ballot {
        number: 5,
        leader: REPLICA1,
    };

    pub const BALLOT_6_REPLICA1: Ballot = Ballot {
        number: 6,
        leader: REPLICA1,
    };

    pub const BALLOT_6_REPLICA2: Ballot = Ballot {
        number: 6,
        leader: REPLICA2,
    };

    pub const BALLOT_7_REPLICA1: Ballot = Ballot {
        number: 7,
        leader: REPLICA1,
    };
}

#[cfg(test)]
mod tests {
    use super::testutil::*;
    use super::*;

    // Test that the Ballot string looks like what we expect.
    #[test]
    fn ballot_string() {
        assert_eq!(
            BALLOT_5_REPLICA1.to_string(),
            "ballot:5:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        );
    }

    // Test that ballots are ordered first by their number and then by their leader.
    #[test]
    fn ballot_order() {
        assert!(BALLOT_5_REPLICA1 < BALLOT_6_REPLICA1);
        assert!(BALLOT_6_REPLICA1 < BALLOT_6_REPLICA2);
        assert!(BALLOT_6_REPLICA2 < BALLOT_7_REPLICA1);
    }

    // Test that the PValue string looks like what we expect.
    #[test]
    fn pvalue_string() {
        let pval = PValue::new(32, BALLOT_5_REPLICA1, Command::data("command"));
        assert_eq!(
            pval.to_string(),
            "pvalue:32:5:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        );
    }

    // Test that ballots are ordered first by their number and then by their leader.
    #[test]
    fn pvalue_order() {
        let pval1 = PValue::new(1, BALLOT_7_REPLICA1, Command::data("command7"));
        let pval2 = PValue::new(2, BALLOT_6_REPLICA2, Command::data("command6"));
        let pval3 = PValue::new(3, BALLOT_6_REPLICA1, Command::data("command6"));
        let pval4 = PValue::new(4, BALLOT_5_REPLICA1, Command::data("command5"));
        let pval5 = PValue::new(5, BALLOT_4_REPLICA1, Command::data("command4"));

        assert!(pval1 < pval2);
        assert!(pval2 < pval3);
        assert!(pval3 < pval4);
        assert!(pval4 < pval5);
    }
}