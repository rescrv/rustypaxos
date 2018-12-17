mod proposer;

use std::fmt;

// Represents a single replica in the Paxos ensemble.
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub struct ReplicaID {
    // TODO(rescrv) make this real
    XXX: u64,
}

impl fmt::Display for ReplicaID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "replica({})", self.XXX)
    }
}

// A Ballot matches the Paxos terminology and consists of an ordered pair of (number,leader).
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct Ballot {
    number: u64,
    leader: ReplicaID,
}

impl Ballot {
    fn new(number: u64, leader: &ReplicaID) -> Ballot {
        Ballot{ 
            number,
            leader: leader.clone(),
        }
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ballot:{}:{}", self.number, self.leader)
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

//const (
//	DefaultAlpha Slot = 16
//	MaxReplicas       = 9
//)
//
//type GroupID [16]byte
//type ReplicaID [16]byte
//
//type Configuration struct {
//	GroupID
//	Version   uint64
//	FirstSlot Slot
//	Alpha     Slot
//	Replicas  []ReplicaID
//	Excluded  []ReplicaID
//}
//
//func NewClusterConfiguration() (*Configuration, error) {
//	var buf [32]byte
//	if _, err := rand.Read(buf[:]); err != nil {
//		return nil, err
//	}
//	c := NewCustomClusterConfiguration(buf)
//	return &c, nil
//}
//
//func NewCustomClusterConfiguration(random [32]byte) Configuration {
//	c := Configuration{
//		Version:   1,
//		FirstSlot: 1,
//		Alpha:     DefaultAlpha,
//		Replicas:  make([]ReplicaID, 1),
//	}
//	copy(c.GroupID[:], random[:16])
//	copy(c.Replicas[0][:], random[16:])
//	return c
//}
//
//func (c *Configuration) ReplicaIndex(replica ReplicaID) int {
//	for i := range c.Replicas {
//		if c.Replicas[i] == replica {
//			return i
//		}
//	}
//	return MaxReplicas
//}
//
//func (g GroupID) String() string {
//	return encode([16]byte(g))
//}
//
//func (r ReplicaID) String() string {
//	return encode([16]byte(r))
//}
//
//func encode(id [16]byte) string {
//	buf := make([]byte, 36)
//	hex.Encode(buf[0:8], id[0:4])
//	hex.Encode(buf[9:13], id[4:6])
//	hex.Encode(buf[14:18], id[6:8])
//	hex.Encode(buf[19:23], id[8:10])
//	hex.Encode(buf[24:], id[10:])
//	buf[8] = '-'
//	buf[13] = '-'
//	buf[18] = '-'
//	buf[23] = '-'
//	return string(buf)
//}

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
mod tests {
    use super::*;

    pub const REPLICA1: ReplicaID = ReplicaID { XXX: 1 };
    pub const REPLICA2: ReplicaID = ReplicaID { XXX: 2 };
    pub const REPLICA3: ReplicaID = ReplicaID { XXX: 3 };
    pub const REPLICA4: ReplicaID = ReplicaID { XXX: 4 };
    pub const REPLICA5: ReplicaID = ReplicaID { XXX: 5 };

    pub const THREE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3];
    pub const FIVE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3, REPLICA4, REPLICA5];

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

    // TODO(rescrv): test the ballot format
}
