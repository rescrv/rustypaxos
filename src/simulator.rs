use std::cmp::max;

use rand;
use rand::seq::SliceRandom;
use rand::Rng;

use crate::types::Ballot;
use crate::types::Command;
use crate::AcceptorAction;
use crate::Environment;
use crate::GroupID;
use crate::Message;
use crate::Misbehavior;
use crate::Paxos;
use crate::ReplicaID;

#[derive(Debug, Eq, PartialEq)]
pub enum Transition {
    NOP,

    Introduce(Command), // TODO(rescrv): name better

    StartProposer(Ballot),

    DeliverMessage(InFlightMessage),
    DuplicateMessage(InFlightMessage),
    DropMessage(InFlightMessage),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InFlightMessage {
    src: ReplicaID,
    message: Message,
}

pub struct TransitionGenerator {}

impl TransitionGenerator {
    pub fn new() -> TransitionGenerator {
        TransitionGenerator {}
    }

    pub fn next(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        loop {
            let t = match rng.gen_range(0, 20) {
                3 => self.generate_introduce(),

                5 => self.generate_start_proposer(sim),

                10 => self.generate_deliver_message(sim),
                11 => self.generate_duplicate_message(sim),
                12 => self.generate_drop_message(sim),

                _ => Transition::NOP,
            };
            if t == Transition::NOP {
                continue;
            }
            return t;
        }
    }

    fn generate_introduce(&mut self) -> Transition {
        let mut rng = rand::thread_rng();
        let x: u64 = rng.gen();
        Transition::Introduce(Command::data(&format!("number={}", x)))
    }

    fn generate_start_proposer(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        let mut ballot = Ballot::BOTTOM;
        for replica in sim.replicas.iter() {
            ballot = max(ballot, replica.highest_ballot());
        }
        // ballot holds the highest ballot in the simulator.
        //
        // Use it as the basis of a new ballot:
        // 25% of the time, pick a ballot we know should fail
        // 25% of the time, pick a ballot we know should succeed in the absence of
        //     any subsequent ballots
        // 50% of the time, pick a ballot we know has the same number (default case)
        //     so that we force arbitration by leader
        // but first, make sure we move away from bottom.
        let mut number = ballot.number();
        if number < 2 {
            number = 2;
        }
        let x: u64 = rng.gen();
        match x % 4 {
            0 => {
                number -= 1;
            }
            1 => {
                number += 1;
            }
            _ => {}
        }
        let x: usize = rng.gen();
        let leader = self.choose_replica(sim);
        Transition::StartProposer(Ballot::new(number, leader))
    }

    fn generate_deliver_message(&mut self, sim: &Simulator) -> Transition {
        match self.choose_message(sim) {
            Some(msg) => Transition::DeliverMessage(msg.clone()),
            None => Transition::NOP,
        }
    }

    fn generate_duplicate_message(&mut self, sim: &Simulator) -> Transition {
        match self.choose_message(sim) {
            Some(msg) => Transition::DuplicateMessage(msg.clone()),
            None => Transition::NOP,
        }
    }

    fn generate_drop_message(&mut self, sim: &Simulator) -> Transition {
        match self.choose_message(sim) {
            Some(msg) => Transition::DropMessage(msg.clone()),
            None => Transition::NOP,
        }
    }

    fn choose_message<'a>(&mut self, sim: &'a Simulator) -> Option<&'a InFlightMessage> {
        let mut rng = rand::thread_rng();
        sim.messages.choose(&mut rng)
    }

    fn choose_replica(&mut self, sim: &Simulator) -> ReplicaID {
        let mut rng = rand::thread_rng();
        let x: usize = rng.gen();
        sim.replicas[x % sim.replicas.len()].id()
    }
}

pub struct Simulator {
    commands: Vec<Command>,
    replicas: Vec<Paxos>,
    messages: Vec<InFlightMessage>,
}

impl Simulator {
    pub fn new() -> Simulator {
        let group = GroupID::new([5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]);
        let replica = ReplicaID::new([7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7]);
        Simulator {
            commands: Vec::new(),
            // TODO(rescrv): cleanup
            replicas: vec![Paxos::new_cluster(group, replica)],
            messages: Vec::new(),
        }
    }

    pub fn apply(&mut self, trans: &Transition) {
        match trans {
            Transition::NOP => {}

            Transition::Introduce(cmd) => self.apply_introduce(cmd),

            Transition::StartProposer(ballot) => self.apply_start_proposer(ballot),

            Transition::DeliverMessage(msg) => self.apply_deliver_message(msg),
            Transition::DuplicateMessage(msg) => self.apply_duplicate_message(msg),
            Transition::DropMessage(msg) => self.apply_drop_message(msg),
        }
    }

    fn apply_introduce(&mut self, cmd: &Command) {
        self.commands.push(cmd.clone());
    }

    fn apply_start_proposer(&mut self, ballot: &Ballot) {
        let replica = self.get_replica(ballot.leader());
        let mut env = SimulatorEnvironment::new(replica.id());
        replica.start_proposer(&mut env, ballot);
        self.merge(env);
    }

    fn apply_deliver_message(&mut self, msg: &InFlightMessage) {
        let replica = self.get_replica(msg.message.intended_recipient());
        let mut env = SimulatorEnvironment::new(replica.id());
        replica.process_message(&mut env, msg.src, &msg.message);
        self.remove_message(msg);
        self.merge(env);
    }

    fn apply_duplicate_message(&mut self, msg: &InFlightMessage) {
        self.messages.push(msg.clone());
    }

    fn apply_drop_message(&mut self, msg: &InFlightMessage) {
        self.remove_message(msg);
    }

    fn get_replica(&mut self, id: ReplicaID) -> &mut Paxos {
        for replica in self.replicas.iter_mut() {
            if replica.id() == id {
                return replica;
            }
        }
        panic!("could not get_replica {}", id);
    }

    fn remove_message(&mut self, msg: &InFlightMessage) {
        for i in 0..self.messages.len() {
            if self.messages[i] == *msg {
                self.messages.swap_remove(i);
                return;
            }
        }
        panic!("tried to remove message {}, but it does not exist");
    }

    fn merge(&mut self, mut env: SimulatorEnvironment) {
        self.messages.append(&mut env.messages);
    }
}

pub fn equivalent(sim1: &Simulator, sim2: &Simulator) -> bool {
    // TODO(rescrv): obviously not true, but not a priority
    true
}

struct SimulatorEnvironment {
    id: ReplicaID,
    messages: Vec<InFlightMessage>,
}

impl SimulatorEnvironment {
    fn new(id: ReplicaID) -> SimulatorEnvironment {
        SimulatorEnvironment {
            id,
            messages: Vec::new(),
        }
    }
}

impl Environment for SimulatorEnvironment {
    fn send(&mut self, message: Message) {
        self.messages.push(InFlightMessage {
            src: self.id,
            message,
        });
    }

    fn persist_acceptor(&mut self, action: AcceptorAction) {
        // TODO(rescrv);
    }

    fn send_when_persistent(&mut self, msg: Message) {
        // TODO(rescrv)
    }

    // TODO(rescrv): make sure this shows up
    fn report_misbehavior(&mut self, _m: Misbehavior) {}
}
