use std::cmp::max;

use rand;
use rand::Rng;

use crate::Ballot;
use crate::Command;
use crate::Paxos;
use crate::ReplicaID;
use crate::GroupID;

#[derive(Debug, Eq, PartialEq)]
pub enum Transition {
    Introduce(Command), // TODO(rescrv): name better

    StartProposer(Ballot),
}

pub struct TransitionGenerator {}

impl TransitionGenerator {
    pub fn new() -> TransitionGenerator {
        TransitionGenerator {}
    }

    pub fn next(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        loop {
            return match rng.gen_range(0, 7) {
                0 => self.generate_introduce(),
                5 => self.generate_start_proposer(sim),
                _ => continue,
            };
        }
    }

    fn generate_introduce(&mut self) -> Transition {
        let mut rng = rand::thread_rng();
        let x: u64 = rng.gen();
        Transition::Introduce(Command {
            command: format!("number={}", x),
        })
    }

    fn generate_start_proposer(&mut self, sim: &Simulator) -> Transition {
        let mut rng = rand::thread_rng();
        let mut ballot = Ballot::bottom();
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
        if ballot.number < 2 {
            ballot.number = 2;
        }
        let x: u64 = rng.gen();
        match x % 4 {
            0 => {
                ballot.number -= 1;
            }
            1 => {
                ballot.number += 1;
            }
            _ => {}
        }
        let x: usize = rng.gen();
        ballot.leader = sim.replicas[x % sim.replicas.len()].id();
        Transition::StartProposer(ballot)
    }
}

#[derive(Eq, PartialEq)]
pub struct Simulator {
    commands: Vec<Command>,
    replicas: Vec<Paxos>,
}

impl Simulator {
    pub fn new() -> Simulator {
        let group = GroupID::new([
                5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            ]);
        let replica = ReplicaID::new([
                7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
            ]);
        Simulator {
            commands: Vec::new(),
            // TODO(rescrv): cleanup
            replicas: vec![Paxos::new_cluster(group, replica)],
        }
    }

    pub fn apply(&mut self, trans: &Transition) {
        match trans {
            Transition::Introduce(cmd) => self.apply_introduce(cmd),
            Transition::StartProposer(ballot) => self.apply_start_proposer(ballot),
        }
    }

    fn apply_introduce(&mut self, cmd: &Command) {
        self.commands.push(cmd.clone());
    }

    fn apply_start_proposer(&mut self, ballot: &Ballot) {
        for replica in self.replicas.iter_mut() {
            if replica.id() == ballot.leader {
                replica.start_proposer(ballot);
                return;
            }
        }
        panic!(
            "tried to apply {}, but {} is not known",
            ballot, ballot.leader
        );
    }
}
