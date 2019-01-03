use std::process::exit;

use rustypaxos::simulator::Simulator;
use rustypaxos::simulator::TransitionGenerator;

fn main() {
    let mut gen = TransitionGenerator::new();
    let mut sim = Simulator::new();
    for _i in 0..100000 {
        let trans = gen.next(&sim);
        print!("{}\n", trans);
        sim.apply(&trans);
    }
    exit(0);
}
