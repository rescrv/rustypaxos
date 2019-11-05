use std::cmp::max;
use std::fmt;

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

    NewCommand(Command), // TODO(rescrv): name better
    SubmitCommand(Command, ReplicaID),

    StartProposer(Ballot),

    DeliverMessage(InFlightMessage),
    DuplicateMessage(InFlightMessage),
    DropMessage(InFlightMessage),

    MakeDurable(ReplicaID, usize),
}

impl fmt::Display for Transition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Transition::NOP => write!(f, "NOP"),

            Transition::NewCommand(c) => write!(f, "new command {:?}", c), // XXX
            Transition::SubmitCommand(c, id) => write!(f, "submit {:?}->{}", c, id), // XXX

            Transition::StartProposer(ballot) => write!(f, "start proposer {}", ballot),

            Transition::DeliverMessage(msg) => write!(f, "deliver {}", msg),
            Transition::DuplicateMessage(msg) => write!(f, "duplicate {}", msg),
            Transition::DropMessage(msg) => write!(f, "drop {}", msg),

            Transition::MakeDurable(id, thresh) => write!(f, "make durable {} on {}", thresh, id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InFlightMessage {
    src: ReplicaID,
    msg: Message,
}

impl fmt::Display for InFlightMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let src = self.src;
        let dst = self.msg.intended_recipient();
        match &self.msg {
            Message::Phase1A {
                acceptor: _,
                ballot,
            } => write!(f, "{}->{} 1A {}", src, dst, ballot),
            Message::Phase1B {
                ballot,
                pvalues,
            } => write!(f, "{}->{} 1B {} {:?}", src, dst, ballot, pvalues),
            Message::Phase2A {
                acceptor: _,
                pval,
            } => write!(f, "{}->{} 2A {}", src, dst, pval),
            Message::Phase2B {
                ballot,
                slot,
            } => write!(f, "{}->{} 2B pvalue:{}:{}:{}", src, dst, slot, ballot.number(), ballot.leader().viewable_id()),
            Message::ProposerNACK { ballot } => write!(f, "{}->{} NACK {}", src, dst, ballot),
        }
    }
}

trait TransitionGenerator {
    fn next(&mut self, sim: &Simulator) -> Transition;
}

pub struct RealtimeTransitionGenerator {}

impl RealtimeTransitionGenerator {
    pub fn new() -> RealtimeTransitionGenerator {
        RealtimeTransitionGenerator {}
    }

    fn generate_new_command(&mut self) -> Transition {
        let mut rng = rand::thread_rng();
        let x: u64 = rng.gen();
        Transition::NewCommand(Command::data(&format!("number={}", x)))
    }

    fn generate_submit_command(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        let id = self.choose_replica(sim).paxos.id();
        match sim.commands.choose(&mut rng) {
            Some(c) => Transition::SubmitCommand(c.clone(), id),
            None => Transition::NOP,
        }
    }

    fn generate_start_proposer(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        let mut ballot = Ballot::BOTTOM;
        for replica in sim.replicas.iter() {
            ballot = max(ballot, replica.paxos.highest_ballot());
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
        let leader = self.choose_replica(sim).paxos.id();
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

    fn generate_make_durable(&mut self, sim: &Simulator) -> Transition {
        let replica = self.choose_replica(sim);
        let mut rng = rand::thread_rng();
        let start = replica.durable + 1;
        let limit = replica.actions.len();
        if start < limit {
            Transition::MakeDurable(replica.paxos.id(), rng.gen_range(start, limit))
        } else {
            Transition::NOP
        }
    }

    fn choose_message<'a>(&mut self, sim: &'a Simulator) -> Option<&'a InFlightMessage> {
        let mut rng = rand::thread_rng();
        sim.messages.choose(&mut rng)
    }

    fn choose_replica<'a>(&mut self, sim: &'a Simulator) -> &'a Process {
        let mut rng = rand::thread_rng();
        let x: usize = rng.gen();
        &sim.replicas[x % sim.replicas.len()]
    }
}

impl TransitionGenerator for RealtimeTransitionGenerator {
    fn next(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        loop {
            let t = match rng.gen_range(0, 37) {
                3 => self.generate_new_command(),
                4 => self.generate_submit_command(sim),

                5 => self.generate_start_proposer(sim),

                10 => self.generate_deliver_message(sim),
                11 => self.generate_duplicate_message(sim),
                12 => self.generate_drop_message(sim),

                15 => self.generate_make_durable(sim),

                16 => self.generate_deliver_message(sim),
                17 => self.generate_deliver_message(sim),
                18 => self.generate_deliver_message(sim),
                19 => self.generate_deliver_message(sim),
                20 => self.generate_deliver_message(sim),
                21 => self.generate_deliver_message(sim),
                22 => self.generate_deliver_message(sim),
                23 => self.generate_deliver_message(sim),
                24 => self.generate_deliver_message(sim),
                25 => self.generate_deliver_message(sim),
                26 => self.generate_deliver_message(sim),
                27 => self.generate_deliver_message(sim),
                28 => self.generate_deliver_message(sim),
                29 => self.generate_deliver_message(sim),
                30 => self.generate_deliver_message(sim),
                31 => self.generate_deliver_message(sim),
                32 => self.generate_deliver_message(sim),
                33 => self.generate_deliver_message(sim),
                34 => self.generate_deliver_message(sim),
                35 => self.generate_deliver_message(sim),
                36 => self.generate_deliver_message(sim),

                37 => self.generate_make_durable(sim),
                38 => self.generate_make_durable(sim),
                39 => self.generate_make_durable(sim),
                40 => self.generate_make_durable(sim),
                41 => self.generate_make_durable(sim),
                42 => self.generate_make_durable(sim),
                43 => self.generate_make_durable(sim),
                44 => self.generate_make_durable(sim),
                45 => self.generate_make_durable(sim),
                46 => self.generate_make_durable(sim),
                47 => self.generate_make_durable(sim),


                _ => Transition::NOP,
            };
            if t == Transition::NOP {
                continue;
            }
            return t;
        }
    }
}

pub struct Simulator {
    commands: Vec<Command>,
    replicas: Vec<Process>,
    messages: Vec<InFlightMessage>,
}

impl Simulator {
    pub fn new() -> Simulator {
        let group = GroupID::new([5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]);
        let replica = ReplicaID::new([7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7]);
        Simulator {
            commands: Vec::new(),
            // TODO(rescrv): cleanup
            replicas: vec![Process::new_cluster(group, replica)],
            messages: Vec::new(),
        }
    }

    pub fn apply(&mut self, trans: &Transition) {
        match trans {
            Transition::NOP => {}

            Transition::NewCommand(cmd) => self.apply_new_command(cmd),
            Transition::SubmitCommand(cmd, id) => self.apply_submit_command(cmd, id),

            Transition::StartProposer(ballot) => self.apply_start_proposer(ballot),

            Transition::DeliverMessage(msg) => self.apply_deliver_message(msg),
            Transition::DuplicateMessage(msg) => self.apply_duplicate_message(msg),
            Transition::DropMessage(msg) => self.apply_drop_message(msg),

            Transition::MakeDurable(id, thresh) => self.apply_make_durable(*id, *thresh),
        }
    }

    fn apply_new_command(&mut self, cmd: &Command) {
        self.commands.push(cmd.clone());
    }

    fn apply_submit_command(&mut self, cmd: &Command, id: &ReplicaID) {
        let replica = self.get_replica(*id);
        let mut env = SimulatorEnvironment::new(replica.paxos.id());
        replica.paxos.enqueue_command(&mut env, cmd.clone());
        self.merge(env);
    }

    fn apply_start_proposer(&mut self, ballot: &Ballot) {
        let replica = self.get_replica(ballot.leader());
        let mut env = SimulatorEnvironment::new(replica.paxos.id());
        replica.paxos.start_proposer(&mut env, ballot);
        self.merge(env);
    }

    fn apply_deliver_message(&mut self, msg: &InFlightMessage) {
        let replica = self.get_replica(msg.msg.intended_recipient());
        let mut env = SimulatorEnvironment::new(replica.paxos.id());
        replica.paxos.process_message(&mut env, msg.src, &msg.msg);
        self.remove_message(msg);
        self.merge(env);
    }

    fn apply_duplicate_message(&mut self, msg: &InFlightMessage) {
        self.messages.push(msg.clone());
    }

    fn apply_drop_message(&mut self, msg: &InFlightMessage) {
        self.remove_message(msg);
    }

    fn apply_make_durable(&mut self, rid: ReplicaID, thresh: usize) {
        let replica = self.get_replica(rid);
        let mut still_not_durable = Vec::new();
        let mut now_in_flight = Vec::new();
        for (when, msg) in replica.when_persistent.iter() {
            if *when <= thresh {
                now_in_flight.push(msg.clone());
            } else {
                still_not_durable.push((*when, msg.clone()));
            }
        }
        replica.durable = thresh;
        replica.when_persistent = still_not_durable;
        self.messages.append(&mut now_in_flight);
    }

    fn get_replica(&mut self, id: ReplicaID) -> &mut Process {
        for replica in self.replicas.iter_mut() {
            if replica.paxos.id() == id {
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
        let replica = self.get_replica(env.id);
        replica.actions.append(&mut env.actions);
        // actions must be modified before when_persistent
        for action in env.when_persistent {
            replica
                .when_persistent
                .push((replica.actions.len(), action));
        }
    }
}

struct Process {
    paxos: Paxos,
    actions: Vec<AcceptorAction>,
    durable: usize,
    when_persistent: Vec<(usize, InFlightMessage)>,
}

impl Process {
    fn new_cluster(group: GroupID, replica: ReplicaID) -> Process {
        Process {
            paxos: Paxos::new_cluster(group, replica),
            actions: Vec::new(),
            durable: 0,
            when_persistent: Vec::new(),
        }
    }
}

struct SimulatorEnvironment {
    id: ReplicaID,
    messages: Vec<InFlightMessage>,
    actions: Vec<AcceptorAction>,
    when_persistent: Vec<InFlightMessage>,
}

impl SimulatorEnvironment {
    fn new(id: ReplicaID) -> SimulatorEnvironment {
        SimulatorEnvironment {
            id,
            messages: Vec::new(),
            actions: Vec::new(),
            when_persistent: Vec::new(),
        }
    }
}

impl Environment for SimulatorEnvironment {
    fn send(&mut self, msg: Message) {
        self.messages.push(InFlightMessage { src: self.id, msg });
    }

    fn persist_acceptor(&mut self, action: AcceptorAction) {
        self.actions.push(action);
    }

    fn send_when_persistent(&mut self, msg: Message) {
        self.when_persistent
            .push(InFlightMessage { src: self.id, msg });
    }

    // TODO(rescrv): be more advanced here
    fn report_misbehavior(&mut self, m: Misbehavior) {
        panic!("Oh No! {:?}\n", m);
    }
}

pub fn run() {
    let mut gen = RealtimeTransitionGenerator::new();
    let mut sim = Simulator::new();
    for _i in 0..10 {
        let trans = gen.next(&sim);
        print!("{}\n", trans);
        sim.apply(&trans);
    }
    ::std::process::exit(0);
}
