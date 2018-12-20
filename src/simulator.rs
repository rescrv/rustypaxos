//use rand;
//use rand::Rng;
//use rand::rngs::EntropyRng;
//
//#[derive(Eq, PartialEq)]
//pub struct DeterministicChooser {
//    hash: [u8; 32]
//}
//
//impl DeterministicChooser {
//    pub fn new() -> DeterministicChooser {
//        let mut x = DeterministicChooser{ hash: [0; 32] };
//        let mut r = EntropyRng::new();
//        r.try_fill(&mut x.hash);
//        return x;
//    }
//}
//
//#[derive(Eq, PartialEq)]
//pub enum Transition {
//    // Every good state machine under development has a NOP
//    NOP(),
//
//    // Create a new proposal in the system, issued by a client.
//    Propose(),
//    // Repropose a command already in the system.
//    Repropose(DeterministicChooser),
//    // Inform that a completed proposal succeeded so that the client stops proposing it.
//    ClientSuccess(DeterministicChooser),
//
//    // Tick the chosen process in the system.
//    Tick(DeterministicChooser),
//
//    // Start a proposer on a deterministically chosen replica.
//    StartProposer(DeterministicChooser),
//    // Step the processing of a deterministically chosen replica's proposer.
//    StepProposer(DeterministicChooser),
//}
//
//pub struct TransitionGenerator {}
//
//impl TransitionGenerator {
//    pub fn new() -> TransitionGenerator {
//        TransitionGenerator {}
//    }
//
//    pub fn next(&mut self) -> Transition {
//        let mut rng = rand::thread_rng();
//        loop {
//            let trans = match rng.gen_range(0, 7) {
//                0 => Transition::NOP(),
//                1 => Transition::Propose(),
//                2 => Transition::Repropose(DeterministicChooser::new()),
//                3 => Transition::ClientSuccess(DeterministicChooser::new()),
//                4 => Transition::Tick(DeterministicChooser::new()),
//                5 => Transition::StartProposer(DeterministicChooser::new()),
//                6 => Transition::StepProposer(DeterministicChooser::new()),
//
//                _ => Transition::NOP(),
//            };
//            if trans != Transition::NOP() {
//                return trans
//            }
//        }
//    }
//}
//
//pub struct Simulator {}
//
//impl Simulator {
//    pub fn new() -> Simulator {
//        Simulator {}
//    }
//
//    pub fn apply(&mut self, trans: &Transition) -> String {
//        match trans {
//            Transition::NOP() => self.apply_nop(),
//            Transition::Propose() => self.apply_propose(),
//            Transition::Repropose(C) => self.apply_repropose(C),
//            Transition::ClientSuccess(C) => self.apply_client_success(C),
//            Transition::Tick(C) => self.apply_tick(C),
//            Transition::StartProposer(C) => self.apply_start_proposer(C),
//            Transition::StepProposer(C) => self.apply_step_proposer(C),
//        }
//    }
//
//    fn apply_nop(&mut self) -> String {
//        String::from("NOP")
//    }
//
//    fn apply_propose(&mut self) -> String {
//        String::from("PROPOSE")
//    }
//
//    fn apply_repropose(&mut self, chooser: &DeterministicChooser) -> String {
//        String::from("RE-PROPOSE")
//    }
//
//    fn apply_client_success(&mut self, chooser: &DeterministicChooser) -> String {
//        String::from("CLIENT SUCCESS")
//    }
//
//    fn apply_tick(&mut self, chooser: &DeterministicChooser) -> String {
//        String::from("TICK")
//    }
//
//    fn apply_start_proposer(&mut self, chooser: &DeterministicChooser) -> String {
//        String::from("START PROPOSER")
//    }
//
//    fn apply_step_proposer(&mut self, chooser: &DeterministicChooser) -> String {
//        String::from("STEP PROPOSER")
//    }
//
//    pub fn checksum(&self) -> String {
//        String::from("checksum")
//    }
//}
//
//type Simulator struct {
//	index      uint64
//	checksum   []byte
//	transition transition
//	log        bool
//	// random number generation
//	guac      *guacamole.Guacamole
//	scrambler *guacamole.Scrambler
//	// all configurations used in this cluster
//	configs []*paxos.Configuration
//	// replica state
//	replicas map[paxos.ReplicaID]*paxos.Paxos
//	// in-flight client requests
//	proposals []*proposal
//	committed map[*proposal]struct{}
//}
//
//func New(seed uint64, log bool) *Simulator {
//	s := &Simulator{
//		checksum:   make([]byte, sha256.Size),
//		transition: &nop{},
//		log:        log,
//		guac:       guacamole.New(),
//		scrambler:  guacamole.NewScrambler(),
//		replicas:   make(map[paxos.ReplicaID]*paxos.Paxos),
//		committed:  make(map[*proposal]struct{}),
//	}
//	s.scrambler.Change(seed)
//	s.guac.Seed(s.scrambler.Scramble(^uint64(0)))
//	var buf [32]byte
//	s.guac.Fill(buf[:])
//	c := paxos.NewCustomClusterConfiguration(buf)
//	for _, replica := range c.Replicas {
//		s.replicas[replica] = paxos.New(c, replica, nil)
//		if s.log {
//			s.replicas[replica].EnableLog()
//		} else {
//			s.replicas[replica].DisableLog()
//		}
//	}
//	return s
//}
//
//func (s *Simulator) Step() {
//	s.index++
//	s.checksum = s.rawChecksum()
//	s.transition = s.generate()
//}
//
//func (s *Simulator) AbbrevChecksum() string {
//	raw := s.rawChecksum()
//	return fmt.Sprintf("%d:%s", s.index, hex.EncodeToString(raw)[:8])
//}
//
//func (s *Simulator) FullChecksum() string {
//	raw := s.rawChecksum()
//	return fmt.Sprintf("%d:%s", s.index, hex.EncodeToString(raw))
//}
//
//func (s *Simulator) HumanReadable() string {
//	return s.transition.human()
//}
//
//func (s *Simulator) Apply() {
//	s.transition.apply(s)
//}
//
//func (s *Simulator) rawChecksum() []byte {
//	hasher := hmac.New(sha256.New, s.checksum)
//	hasher.Write([]byte(s.HumanReadable()))
//	return hasher.Sum(make([]byte, 0, sha256.Size))
//}
//
//func (t *tick) apply(s *Simulator) {
//	r := s.replicas[t.replica]
//	r.Tick()
//}
//
//func (s *Simulator) pickReplica() paxos.ReplicaID {
//	ids := make([]paxos.ReplicaID, 0, len(s.replicas))
//	for id, _ := range s.replicas {
//		ids = append(ids, id)
//	}
//	sort.Slice(ids, func(i, j int) bool {
//		return bytes.Compare(ids[i][:], ids[j][:]) < 0
//	})
//	return ids[s.guac.Uint64()%uint64(len(ids))]
//}
//
//func (s *Simulator) generateStartProposer() *startProposer {
//	number := uint64(0)
//	for _, p := range s.replicas {
//		b := p.AcceptorMaxBallot()
//		if b.Number > number {
//			number = b.Number
//		}
//		if p.HasProposer() {
//			b = p.ProposerBallot()
//			if b.Number > number {
//				number = b.Number
//			}
//		}
//	}
//	if number < 2 {
//		number = 2
//	}
//	// 25% of the time, pick a ballot we know should fail
//	// 25% of the time, pick a ballot we know should succeed in the absence of
//	// 		any subsequent ballots
//	// 50% of the time, pick a ballot we know has the same number (default case)
//	// 		so that we force arbitration by leader
//	x := s.guac.Uint64()
//	switch x & 0x3 {
//	case 0:
//		number--
//	case 1:
//		number++
//	default:
//	}
//	// create the state we want to advance
//	p := &startProposer{
//		ballot: paxos.Ballot{
//			Number: number,
//			Leader: s.pickReplica(),
//		},
//	}
//	// check that this ballot would be one the proposer could take up
//	r := s.replicas[p.ballot.Leader]
//	if !r.HasProposer() || p.ballot.Supercedes(r.ProposerBallot()) {
//		return p
//	}
//	return nil
//}
//
