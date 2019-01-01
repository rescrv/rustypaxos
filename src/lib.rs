pub mod acceptor;
pub mod configuration;
pub mod proposer;
pub mod simulator;

use std::fmt;

use crate::configuration::GroupID;
use crate::configuration::ReplicaID;

pub trait Environment {
    // Send a message.
    // There is explicitly no return value.  See "Error Handling" in the README.
    fn send(&mut self, msg: Message);

    // Report some form of misbehavior.
    // These are hard errors, and each one indicates a problem that should be investigated.
    fn report_misbehavior(&mut self, m: Misbehavior);
}

#[derive(Debug, Eq, PartialEq)]
pub enum PaxosPhase {
    ONE,
    TWO,
}

// A Ballot matches the Paxos terminology and consists of an ordered pair of (number,leader).
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Copy)]
pub struct Ballot {
    number: u64,
    leader: ReplicaID,
}

impl Ballot {
    pub fn bottom() -> Ballot {
        Ballot {
            number: 0,
            leader: ReplicaID::bottom(),
        }
    }

    pub fn new(number: u64, leader: &ReplicaID) -> Ballot {
        Ballot {
            number,
            leader: leader.clone(),
        }
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn leader(&self) -> ReplicaID {
        self.leader
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ballot:{}:{}", self.number, self.leader.viewable_id())
    }
}

// A PValue is referred to as a decree in the part time parliament paper.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct PValue {
    slot: u64,
    ballot: Ballot,
    command: Command,
}

impl PValue {
    pub fn new(slot: u64, ballot: Ballot, command: String) -> PValue {
        PValue {
            slot,
            ballot,
            command: Command { command },
        }
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

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct Command {
    command: String,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Misbehavior {
    ProposerInLameDuck,
    PValueConflict(PValue, PValue),
    NotAReplica(ReplicaID), // TODO(rescrv): does config matter here?
    NotInPhase2(ReplicaID, Ballot),
    Phase1PValueAboveBallot(ReplicaID, Ballot, PValue),
    Phase2WrongBallot(ReplicaID, Ballot, Ballot),
    Phase2LostPValue(ReplicaID, Ballot, u64),
    ProposerWrongBallot(ReplicaID, Ballot),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Phase1A{ acceptor: ReplicaID, ballot: Ballot },
    Phase1B,
    Phase2A{ acceptor: ReplicaID, pval: PValue },
    Phase2B,
}

pub struct Paxos {
    group: GroupID,
    id: ReplicaID,
}

impl Paxos {
    pub fn new_cluster(group: GroupID, id: ReplicaID) -> Paxos {
        Paxos { group, id }
    }

    pub fn id(&self) -> ReplicaID {
        self.id
    }

    // XXX
    pub fn highest_ballot(&self) -> Ballot {
        Ballot {
            number: 1,
            leader: self.id,
        }
    }

    pub fn start_proposer(&mut self, env: &mut Environment, ballot: &Ballot) {
        /*
        pub fn new(
            config: &'a Configuration,
            ballot: &'a Ballot,
            env: &'a Environment,
        ) -> Proposer<'a> {
        */
    }
}

#[cfg(test)]
mod testutil {
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
        let pval = PValue::new(32, BALLOT_5_REPLICA1, String::from("command"));
        assert_eq!(
            pval.to_string(),
            "pvalue:32:5:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        );
    }

    // Test that ballots are ordered first by their number and then by their leader.
    #[test]
    fn pvalue_order() {
        let pval1 = PValue::new(1, BALLOT_7_REPLICA1, String::from("command7"));
        let pval2 = PValue::new(2, BALLOT_6_REPLICA2, String::from("command6"));
        let pval3 = PValue::new(3, BALLOT_6_REPLICA1, String::from("command6"));
        let pval4 = PValue::new(4, BALLOT_5_REPLICA1, String::from("command5"));
        let pval5 = PValue::new(5, BALLOT_4_REPLICA1, String::from("command4"));

        assert!(pval1 < pval2);
        assert!(pval2 < pval3);
        assert!(pval3 < pval4);
        assert!(pval4 < pval5);
    }
}
