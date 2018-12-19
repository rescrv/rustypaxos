pub mod configuration;
pub mod proposer;

use std::fmt;

use crate::configuration::ReplicaID;

pub enum PaxosPhase {
    ONE,
    TWO,
}

// A Ballot matches the Paxos terminology and consists of an ordered pair of (number,leader).
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct Ballot {
    number: u64,
    leader: ReplicaID,
}

impl Ballot {
    fn new(number: u64, leader: &ReplicaID) -> Ballot {
        Ballot {
            number,
            leader: leader.clone(),
        }
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ballot:{}:{}", self.number, self.leader.viewable_id())
    }
}

// A PValue is referred to as a decree in the part time parliament paper.
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct PValue {
    slot: u64,
    ballot: Ballot,
    // TODO(rescrv): type this
    command: String,
}

// XXX type Command string
// XXX type DedupeToken uint64
//type DurabilityWatermark uint64
//
//type DurableStorage interface {
//	PersistPhase1(promise Ballot) DurabilityWatermark
//	PersistPhase2(pvalue PValue) DurabilityWatermark
//	WaitUntilDurable(offset DurabilityWatermark)
//	CheckInvariants()
//}
//package paxos
//
//import (
//	"log"
//
//	"hack.systems/util/assert"
//)
//
//type Paxos struct {
//	thisServer ReplicaID
//	storage    DurableStorage
//	acceptor   *Acceptor
//	proposer   *Proposer
//	config     *Configuration
//	ticker     WeightedTicker
//	log        bool
//	// XXX proposals waiting to go out
//	// XXX proposals waiting for success
//	/*
//	   uint64_t m_scout_wait_cycles;
//	*/
//}
//
//func New(c Configuration, id ReplicaID, storage DurableStorage) *Paxos {
//	p := &Paxos{
//		thisServer: id,
//		storage:    storage,
//		acceptor:   NewAcceptor(storage),
//		config:     &c,
//	}
//	p.ticker.Configure(p.config.ReplicaIndex(id))
//	return p
//}
//
//func (p *Paxos) EnableLog() {
//	p.log = true
//}
//
//func (p *Paxos) DisableLog() {
//	p.log = false
//}
//
//func (p *Paxos) Logf(format string, v ...interface{}) {
//	if !p.log {
//		return
//	}
//	args := make([]interface{}, len(v)+1)
//	args[0] = p.thisServer
//	copy(args[1:], v)
//	log.Printf("replica %s: "+format, args...)
//}
//
//func (p *Paxos) Tick() {
//	p.ticker.Tick()
//	if !p.ticker.Active() {
//		p.Logf("inactive tick")
//	}
//	if p.proposer == nil {
//		b := p.AcceptorMaxBallot()
//		b.Number++
//		b.Leader = p.thisServer
//		/*
//			for _, p := range s.replicas {
//				b := p.AcceptorMaxBallot()
//				if b.Number > number {
//					number = b.Number
//				}
//				if p.HasProposer() {
//					b = p.ProposerBallot()
//					if b.Number > number {
//						number = b.Number
//					}
//				}
//			}
//		*/
//	}
//	p.Logf("ACTIVE!")
//}
//
//func (p *Paxos) GenerateToken() DedupeToken {
//	return 0 // XXX
//}
//
//// XXX this is a bad API; should return token, slot
//func (p *Paxos) RefreshToken(tok DedupeToken) DedupeToken {
//	return tok // XXX
//}
//
//func (p *Paxos) Propose(tok DedupeToken, cmd Command) {
//}
//
//func (p *Paxos) AcceptorMaxBallot() Ballot {
//	return p.acceptor.MaxBallot()
//}
//
///*
//func (p *Paxos) AcceptorPhase1(b Ballot, start, limit Slot) (Ballot, []PValue, DurabilityWatermark) {
//}
//
//func (p *Paxos) AcceptorPhase2(pval PValue) Ballot {
//}
//*/
//
//func (p *Paxos) HasProposer() bool {
//	return p.proposer != nil
//}
//
//func (p *Paxos) ProposerBallot() Ballot {
//	assert.True(p.HasProposer(),
//		"HasProposer() must be true when calling ProposerBallot")
//	return p.proposer.ballot
//}
//
//func (p *Paxos) ProposerStart(b Ballot) {
//	assert.True(p.proposer == nil || b.Supercedes(p.proposer.ballot),
//		"new proposers must have a higher ballot than old proposers")
//}
//
//func (p *Paxos) ProposerStep() {
//	assert.True(p.proposer != nil, "cannot step non-nil proposer")
//	p.proposer.WorkStateMachine(nil)
//}
//
//func (p *Paxos) CheckInvariants() {
//	p.acceptor.CheckInvariants()
//	p.proposer.CheckInvariants()
//	p.storage.CheckInvariants()
//}
//
//
//package paxos
//
//// WeightedTicker biases the output of a time.Ticker to reduce the likelihood
//// that two live instances of Paxos race to being a round of Paxos with a higher
//// ballot.  It is probabilistic in nature:  Races can still happen, but the
//// probability of the race continuing goes to zero so long as the tick interval
//// of all instances is longer than the expected latency of Phase 1 and all tick
//// intervals are within a factor of two of each other.
//type WeightedTicker struct {
//	index uint
//	count int
//}
//
//func (t *WeightedTicker) Configure(index int) {
//	if index >= 0 && index < MaxReplicas {
//		t.index = uint(index)
//	} else {
//		t.index = MaxReplicas
//	}
//	t.count = -1
//}
//
//func (t *WeightedTicker) Tick() {
//	t.count--
//	if t.count < 0 {
//		t.count = 1<<t.index - 1
//	}
//}
//
//func (t *WeightedTicker) Active() bool {
//	return t.count == 0
//}

#[cfg(test)]
mod testutil {
    use crate::configuration::GroupID;
    use crate::configuration::ReplicaID;

    pub use crate::configuration::testutil::*;

    pub const THREE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3];
    pub const FIVE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3, REPLICA4, REPLICA5];
}

#[cfg(test)]
mod tests {
    use super::testutil::*;
    use super::*;

    // Test that the Ballot string looks like what we expect.
    #[test]
    fn ballot_string() {
        const B: Ballot = Ballot {
            number: 5,
            leader: REPLICA1,
        };
        assert_eq!(
            B.to_string(),
            "ballot:5:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        );
    }

    // Test that ballots are ordered first by their number and then by their leader.
    #[test]
    fn ballot_order() {
        let b1 = Ballot {
            number: 5,
            leader: REPLICA1,
        };
        let b2 = Ballot {
            number: 6,
            leader: REPLICA1,
        };
        let b3 = Ballot {
            number: 6,
            leader: REPLICA2,
        };
        let b4 = Ballot {
            number: 7,
            leader: REPLICA1,
        };
        assert!(b1 < b2);
        assert!(b2 < b3);
        assert!(b3 < b4);
    }

    // Test that ballots are ordered first by their number and then by their leader.
    #[test]
    fn pvalue_order() {
        // TODO(rescrv)
    }
}
