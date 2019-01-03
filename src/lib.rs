//! An implementation of Paxos.
//!
//! Start by reading the README file distributed with the source.  It will provide an overview of
//! Paxos, provide some motivation for why this implementation exists, and discuss some
//! "philosophical" parts of the design that have a very strong influence on the code.
//!
//! Once you've understood and embraced the README, there's probably no real good place to start in
//! the documentation.  Maybe start with the simulator documentation and code and BFS from there.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::rc;

use crate::configuration::GroupID;
use crate::configuration::ReplicaID;
use crate::types::Ballot;
use crate::types::Command;
use crate::types::PValue;

pub mod acceptor;
pub mod configuration;
pub mod proposer;
pub mod simulator;
pub mod types;

/// The hooks this Paxos implementation has into the environment to perform stateful I/O.
///
/// This is abstracted away behind the trait to facilitate testing, simulation, and fault
/// injection.  By keeping this contract with the environment minimal, we constrain the extent to
/// which errors in the environment can inadvertently affect safety.
///
/// See "Error Handling" in the README for more details.
pub trait Environment {
    /// Send a message.
    /// There is explicitly no return value.  See "Error Handling" in the README.
    fn send(&mut self, msg: Message);

    /// Persist acceptor state on durable storage.
    ///
    /// There is explicitly no return value.  See "Error Handling" in the README.
    fn persist_acceptor(&mut self, action: AcceptorAction);

    /// Send a message once all previously persisted state is durable.
    ///
    /// It is necessary that every `persist*` call made prior to this call be fully durable before
    /// the messages are sent to the network.  Failure to adhere to this ordering will violate the
    /// safety guarantees of the protocol and will lead to an opportunity for data loss.
    ///
    /// Although the call has `persistent` in its name, this call does no local I/O and only sends
    /// messages over the network.
    ///
    /// There is explicitly no return value.  See "Error Handling" in the README.
    fn send_when_persistent(&mut self, msg: Message);

    /// Report some form of misbehavior.
    /// These are hard errors that should never happen.  Every single one should indicate a problem
    /// that should be investigated.
    fn report_misbehavior(&mut self, m: Misbehavior);
}

#[derive(Debug, Eq, PartialEq)]
pub enum PaxosPhase {
    ONE,
    TWO,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Misbehavior {
    ProposerInLameDuck,
    PValueConflict(PValue, PValue),
    NotAReplica(ReplicaID),
    NotInPhase2(ReplicaID, Ballot),
    Phase1PValueAboveBallot(ReplicaID, Ballot, PValue),
    Phase2WrongBallot(ReplicaID, Ballot, Ballot),
    Phase2LostPValue(ReplicaID, Ballot, u64),
    ProposerWrongBallot(ReplicaID, Ballot),
    ProposerDoesNotMatchLeader(ReplicaID, Ballot),
    WrongRecipient(ReplicaID, ReplicaID, ReplicaID),
    Phase2First(ReplicaID, Ballot),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Phase1A {
        acceptor: ReplicaID,
        ballot: Ballot,
    },
    Phase1B {
        ballot: Ballot,
        pvalues: Vec<PValue>,
    },
    Phase2A {
        acceptor: ReplicaID,
        pval: PValue,
    },
    Phase2B {
        ballot: Ballot,
        slot: u64,
    },
    ProposerNACK {
        ballot: Ballot,
    },
}

impl Message {
    pub fn intended_recipient(&self) -> ReplicaID {
        match self {
            Message::Phase1A { acceptor, ballot: _ } => *acceptor,
            Message::Phase1B { ballot, pvalues: _ } => ballot.leader(),
            Message::Phase2A {
                acceptor: a,
                pval: _,
            } => *a,
            Message::Phase2B { ballot, slot: _ } => ballot.leader(),
            Message::ProposerNACK { ballot } => ballot.leader(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptorAction {
    FollowBallot {
        ballot: Ballot,
        start: u64,
        limit: u64,
    },
    AcceptProposal {
        pval: PValue,
    },
}

pub struct Paxos {
    group: GroupID,
    id: ReplicaID,
    configs: HashMap<u64, rc::Rc<configuration::Configuration>>,
    acceptor: acceptor::Acceptor,
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
            acceptor: acceptor::Acceptor::new(),
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

    pub fn enqueue_command(&mut self, env: &mut Environment, cmd: Command) {
        if let Entry::Occupied(mut entry) = self.proposers.entry(self.highest_ballot()) {
            entry.get_mut().enqueue_command(env, cmd);
        }
    }

    pub fn process_message(&mut self, env: &mut Environment, src: ReplicaID, msg: &Message) {
        match msg {
            Message::Phase1A {
                acceptor: a,
                ballot: b,
            } => {
                self.process_phase_1a_message(env, &src, a, b);
            },
            Message::Phase1B { ballot, pvalues } => {
                self.process_phase_1b_message(env, &src, &ballot.leader(), ballot, pvalues);
            },
            Message::Phase2A {
                acceptor: a,
                pval: p,
            } => {
                self.process_phase_2a_message(env, &src, a, p);
            },
            Message::Phase2B { ballot, slot } => {
                self.process_phase_2b_message(env, &src, &ballot.leader(), ballot, *slot);
            },
            Message::ProposerNACK { ballot } => {
                self.process_proposer_nack(&src, ballot);
            },
        };
    }

    fn process_phase_1a_message(
        &mut self,
        env: &mut Environment,
        proposer: &ReplicaID,
        acceptor: &ReplicaID,
        current: &Ballot,
    ) {
        // Make sure that we are the intended recipient.
        if *acceptor != self.id {
            env.report_misbehavior(Misbehavior::WrongRecipient(*proposer, *acceptor, self.id));
            return;
        }
        // Make sure the sender is authorized to act on this ballot.
        if *proposer != current.leader() {
            env.report_misbehavior(Misbehavior::ProposerDoesNotMatchLeader(*proposer, *current));
            return;
        }
        // Delegate to the acceptor.
        self.acceptor
            .process_phase_1a_message(env, current, 0, u64::max_value());
    }

    fn process_phase_1b_message(
        &mut self,
        env: &mut Environment,
        acceptor: &ReplicaID,
        proposer: &ReplicaID,
        current: &Ballot,
        pvalues: &[PValue],
    ) {
        // Make sure that we are the intended recipient.
        if *proposer != self.id {
            env.report_misbehavior(Misbehavior::WrongRecipient(*acceptor, *proposer, self.id));
            return;
        }
        // Get the proposer and let them know about the new follower.
        match self.get_proposer(current) {
            Some(p) => {
                p.process_phase_1b_message(env, acceptor, current, pvalues);
            }
            None => {},
        }
    }

    fn process_phase_2a_message(
        &mut self,
        env: &mut Environment,
        proposer: &ReplicaID,
        acceptor: &ReplicaID,
        pval: &PValue,
    ) {
        // Make sure that we are the intended recipient.
        if *acceptor != self.id {
            env.report_misbehavior(Misbehavior::WrongRecipient(*proposer, *acceptor, self.id));
            return;
        }
        // Make sure the sender is authorized to act on this proposal.
        if *proposer != pval.ballot().leader() {
            env.report_misbehavior(Misbehavior::ProposerDoesNotMatchLeader(
                *proposer,
                pval.ballot(),
            ));
            return;
        }
        // Delegate to the acceptor.
        self.acceptor.process_phase_2a_message(env, pval.clone());
    }

    fn process_phase_2b_message(
        &mut self,
        env: &mut Environment,
        acceptor: &ReplicaID,
        proposer: &ReplicaID,
        ballot: &Ballot,
        slot: u64,
    ) {
        // Make sure that we are the intended recipient.
        if *proposer != self.id {
            env.report_misbehavior(Misbehavior::WrongRecipient(*acceptor, *proposer, self.id));
            return;
        }
        // Get the proposer and let them know about the accepted proposal.
        match self.get_proposer(ballot) {
            Some(p) => {
                p.process_phase_2b_message(env, acceptor, ballot, slot);
            }
            None => {},
        }
    }

    fn process_proposer_nack(
        &mut self,
        acceptor: &ReplicaID,
        ballot: &Ballot,
    ) {
        // TODO(rescrv) do something here
        print!("{} NACK'd {}\n", acceptor, ballot);
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

    fn get_proposer(&mut self, ballot: &Ballot) -> Option<&mut proposer::Proposer> {
        self.proposers.get_mut(ballot)
    }
}

#[cfg(test)]
mod testutil {
    pub use crate::configuration::testutil::*;
    pub use crate::types::testutil::*;
}
