use std::process::exit;

use rustypaxos::simulator::TransitionGenerator;
use rustypaxos::simulator::equivalent;
use rustypaxos::simulator::Simulator;

fn main() {
    let mut gen = TransitionGenerator::new();
    let mut sim1 = Simulator::new();
    let mut sim2 = Simulator::new();

    for _i in 0..1000 {
        let trans = gen.next(&sim1);
        if !equivalent(&sim1, &sim2) {
            print!("non-deterministic execution found:\n");
            exit(1);
        }
        print!("{:?}\n", trans);
        sim1.apply(&trans);
        sim2.apply(&trans);
    }
    exit(0);
}
