use std::process::exit;

use rustypaxos::simulator::*;

fn main() {
    let mut gen = TransitionGenerator::new();
    let mut sim1 = Simulator::new();
    let mut sim2 = Simulator::new();

    for _i in 0..1000 {
        let trans = gen.next();
        let res1 = sim1.apply(&trans);
        let res2 = sim2.apply(&trans);
        if sim1.checksum() != sim2.checksum() {
            print!("non-deterministic execution found:\n");
            print!("{}: {}\n", sim1.checksum(), res1);
            print!("{}: {}\n", sim2.checksum(), res2);
            exit(1);
        }
        print!("{}\n", res1);
    }
    exit(0);
}
