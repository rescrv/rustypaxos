use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::rc;

use crate::configuration::GroupID;
use crate::configuration::ReplicaID;

pub mod acceptor;
pub mod configuration;
pub mod proposer;
pub mod simulator;

// TODO(rescrv): name this better
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
struct DurabilityThreshold(u64);

pub trait Environment {
    // Send a message.
    // There is explicitly no return value.  See "Error Handling" in the README.
    fn send(&mut self, msg: Message);

    // Persist the given state somewhere durable.
    // The return value here is of type DurabilityThreshold.  See the section in the README about
    // durability for more information on what this means and how it fits into the overall design.
    // Errors in I/O cannot be returned here, but should instead show up as the durable threshold
    // never passing this mark.
    // TODO(rescrv) describe this ^ better
    //fn persist(&mut self);

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
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Ballot {
    number: u64,
    leader: ReplicaID,
}

impl Ballot {
    pub const BOTTOM: Ballot = Ballot {
        number: 0,
        leader: ReplicaID::BOTTOM,
    };

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
    Phase1A { acceptor: ReplicaID, ballot: Ballot },
    Phase1B,
    Phase2A { acceptor: ReplicaID, pval: PValue },
    Phase2B,
}

impl Message {
    pub fn intended_recipient(&self) -> ReplicaID {
        match self {
            Message::Phase1A {
                acceptor: a,
                ballot: _,
            } => *a,
            Message::Phase1B => panic!("not implemented"), // XXX
            Message::Phase2A {
                acceptor: a,
                pval: _,
            } => *a,
            Message::Phase2B => panic!("not implemented"), // XXX
        }
    }
}

pub struct Paxos {
    group: GroupID,
    id: ReplicaID,
    configs: HashMap<u64, rc::Rc<configuration::Configuration>>,
    proposers: HashMap<Ballot, proposer::Proposer>,
}

impl Paxos {
    pub fn new_cluster(group: GroupID, id: ReplicaID) -> Paxos {
        let config = configuration::Configuration::bootstrap(group, &[id], &[]);
        let mut configs = HashMap::new();
        configs.insert(config.epoch(), config);
        Paxos {
            group,
            id,
            configs,
            proposers: HashMap::new(),
        }
    }

    pub fn group(&self) -> GroupID {
        self.group
    }

    pub fn id(&self) -> ReplicaID {
        self.id
    }

    pub fn highest_ballot(&self) -> Ballot {
        let mut ballot = Ballot::BOTTOM;
        for &k in self.proposers.keys() {
            if ballot < k {
                ballot = k;
            }
        }
        // TODO(rescrv) acceptor
        ballot
    }

    pub fn start_proposer(&mut self, env: &mut Environment, ballot: &Ballot) {
        let config = self.current_configuration();
        if let Entry::Vacant(entry) = self.proposers.entry(*ballot) {
            let mut proposer = proposer::Proposer::new(&config, *ballot);
            proposer.make_progress(env);
            entry.insert(proposer);
        }
    }

    pub fn process_message(&mut self, env: &mut Environment, src: ReplicaID, msg: &Message) {
        match msg {
            Message::Phase1A {
                acceptor: a,
                ballot: b,
            } => {
                self.process_phase_1a_message(env, &src, a, b);
            }
            Message::Phase2A {
                acceptor: a,
                pval: p,
            } => {
                self.process_phase_2a_message(env, &src, a, p);
            }
            _ => panic!("not implemented"), // XXX
        };
    }

    fn process_phase_1a_message(
        &mut self,
        env: &mut Environment,
        proposer: &ReplicaID,
        acceptor: &ReplicaID,
        current: &Ballot,
    ) {
        print!("PHASE 1A: {}->{} {}\n", proposer, acceptor, current);
    }

    fn process_phase_1b_message(
        &mut self,
        env: &mut Environment,
        acceptor: &ReplicaID,
        proposer: &ReplicaID,
        current: &Ballot,
        pvalues: &[PValue],
    ) {
        print!("PHASE 1B: {}->{} {}\n", acceptor, proposer, current);
    }

    fn process_phase_2a_message(
        &mut self,
        env: &mut Environment,
        proposer: &ReplicaID,
        acceptor: &ReplicaID,
        pval: &PValue,
    ) {
        print!("PHASE 2A: {}->{} {}\n", proposer, acceptor, pval);
    }

    fn process_phase_2b_message(
        &mut self,
        env: &mut Environment,
        acceptor: &ReplicaID,
        proposer: &ReplicaID,
        ballot: &Ballot,
        slot: u64,
    ) {
        print!("PHASE 2B: {}->{} {}@{}\n", acceptor, proposer, ballot, slot);
    }

    fn current_configuration(&self) -> rc::Rc<configuration::Configuration> {
        let mut epoch = 0;
        for &e in self.configs.keys() {
            if epoch < e {
                epoch = e;
            }
        }
        if epoch == 0 {
            panic!("Paxos initialized without a configuration");
        }
        rc::Rc::clone(self.configs.get(&epoch).unwrap())
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
