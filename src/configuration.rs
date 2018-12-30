use std::collections::HashMap;
use std::fmt;
use std::fmt::Write;
use std::rc;

use rand;
use rand::rngs::EntropyRng;
use rand::Rng;

pub const ID_BYTES: usize = 16;
pub const DEFAULT_ALPHA: u64 = 16;
pub const MAXIMUM_ALPHA: u64 = 1000000;

// Generate 16B unique identifier.
fn gen_id() -> Result<[u8; ID_BYTES], rand::Error> {
    let mut id: [u8; ID_BYTES] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut r = EntropyRng::new();
    match r.try_fill(&mut id) {
        Ok(_) => Ok(id),
        Err(e) => Err(e),
    }
}

// Encode 16B of random data in something aesthetically better.
fn encode_id(id: &[u8; ID_BYTES]) -> String {
    let mut s = String::with_capacity(36);
    const SLICES: [(usize, usize); 5] = [(0, 4), (4, 6), (6, 8), (8, 10), (10, 16)];
    for &(start, limit) in SLICES.iter() {
        if start > 0 {
            s.push_str("-");
        }
        for i in start..limit {
            write!(&mut s, "{:x}", id[i]).expect("unable to write to string");
        }
    }
    s
}

// GroupID uniquely identifies a Paxos ensemble.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct GroupID {
    id: [u8; ID_BYTES],
}

impl GroupID {
    pub fn bottom() -> GroupID {
        GroupID { id: [0; ID_BYTES] }
    }

    pub fn new(id: [u8; ID_BYTES]) -> GroupID {
        GroupID { id }
    }

    pub fn generate() -> Result<GroupID, rand::Error> {
        let id = gen_id();
        match id {
            Ok(id) => Ok(GroupID { id }),
            Err(e) => Err(e),
        }
    }

    pub fn viewable_id(&self) -> String {
        encode_id(&self.id)
    }
}

impl fmt::Display for GroupID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "paxos:{}", self.viewable_id())
    }
}

// ReplicaID represents a single replica in the Paxos ensemble.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct ReplicaID {
    id: [u8; ID_BYTES],
}

impl ReplicaID {
    pub fn bottom() -> ReplicaID {
        ReplicaID { id: [0; ID_BYTES] }
    }

    pub fn new(id: [u8; ID_BYTES]) -> ReplicaID {
        ReplicaID { id }
    }

    pub fn generate() -> Result<ReplicaID, rand::Error> {
        let id = gen_id();
        match id {
            Ok(id) => Ok(ReplicaID { id }),
            Err(e) => Err(e),
        }
    }

    pub fn viewable_id(&self) -> String {
        encode_id(&self.id)
    }
}

impl fmt::Display for ReplicaID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "replica:{}", self.viewable_id())
    }
}

// A configuration of the Paxos ensemble.
#[derive(Debug, Clone)]
pub struct Configuration {
    // The Paxos ensemble this configuration represents.
    group: GroupID,
    // A sequence number indicating how many configurations this ensemble has had.  The first
    // configuration is epoch=1.
    epoch: u64,
    // The smallest slot this configuration is allowed to address.  The first slot=1.
    slot: u64,
    // The number of proposals that may be issued concurrently.
    alpha: u64,
    // Replicas involved in this ensemble.  These will be used for acceptors, learners and provide
    // durability guarantees for the cluster.
    replicas: Vec<ReplicaID>,
    // Shadows will be included in cluster operations as if they were replicas, but will not
    // contribute to durability.
    shadows: Vec<ReplicaID>,
}

impl Configuration {
    pub fn bootstrap(
        group: GroupID,
        replicas: &[ReplicaID],
        shadows: &[ReplicaID],
    ) -> rc::Rc<Configuration> {
        let c = rc::Rc::new(Configuration {
            group: group,
            epoch: 1,
            slot: 1,
            alpha: DEFAULT_ALPHA,
            replicas: replicas.to_vec(),
            shadows: shadows.to_vec(),
        });
        c
    }

    pub fn reconfigure(&self) -> Reconfiguration {
        let alpha = self.alpha;
        Reconfiguration {
            config: self.clone(),
            alpha,
        }
    }

    pub fn group(&self) -> &GroupID {
        &self.group
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn first_valid_slot(&self) -> u64 {
        self.slot
    }

    pub fn alpha(&self) -> u64 {
        self.alpha
    }

    pub fn is_member(&self, member: &ReplicaID) -> bool {
        self.is_replica(member) || self.is_shadow(member)
    }

    pub fn is_replica(&self, replica: &ReplicaID) -> bool {
        self.replicas.iter().any(|r| r == replica)
    }

    pub fn is_shadow(&self, shadow: &ReplicaID) -> bool {
        self.shadows.iter().any(|s| s == shadow)
    }

    pub fn members(&self) -> impl Iterator<Item = &ReplicaID> {
        self.replicas().chain(self.shadows())
    }

    pub fn replicas(&self) -> impl Iterator<Item = &ReplicaID> {
        self.replicas.iter()
    }

    pub fn shadows(&self) -> impl Iterator<Item = &ReplicaID> {
        self.shadows.iter()
    }

    pub fn has_quorum(&self, f: &Fn(&ReplicaID) -> bool) -> bool {
        let mut count = 0;
        for r in self.replicas() {
            if f(r) {
                count += 1;
            }
        }
        self.replicas.len() / 2 + 1 <= count
    }
}

// Reconfiguration of the Paxos ensemble.
pub struct Reconfiguration {
    config: Configuration,
    alpha: u64,
}

impl Reconfiguration {
    // Commit the configuration starting with the given slot.
    //
    // # Panics
    //
    // * The slot parameter must be at least as great as the present configuration's start slot
    //   plus its original alpha value.
    pub fn commit(mut self, slot: u64) -> rc::Rc<Configuration> {
        assert!(
            self.config.slot + self.alpha <= slot,
            "alpha window size not respected"
        );
        self.config.epoch += 1;
        self.config.slot = slot;
        rc::Rc::new(self.config)
    }

    pub fn set_alpha(&mut self, alpha: u64) -> &mut Reconfiguration {
        assert!(0 < alpha && alpha <= MAXIMUM_ALPHA);
        self.config.alpha = alpha;
        self
    }
}

// Track a quorum of the Paxos ensemble.
#[derive(Debug)]
pub struct QuorumTracker<S> {
    config: rc::Rc<Configuration>,
    followers: HashMap<ReplicaID, S>,
}

impl<S> QuorumTracker<S> {
    pub fn new(config: rc::Rc<Configuration>) -> QuorumTracker<S> {
        QuorumTracker {
            config,
            followers: HashMap::new(),
        }
    }

    pub fn add(&mut self, member: &ReplicaID, state: S) {
        if !self.is_follower(member) {
            self.followers
                .insert(*member,  state);
        }
    }

    pub fn is_follower(&self, member: &ReplicaID) -> bool {
        self.followers.contains_key(member)
    }

    pub fn waiting_for(&self) -> Vec<ReplicaID> {
        let mut nf: Vec<ReplicaID> = Vec::new();
        for &replica in self.config.replicas() {
            if !self.is_follower(&replica) {
                nf.push(replica);
            }
        }
        for &shadow in self.config.shadows() {
            if !self.is_follower(&shadow) {
                nf.push(shadow);
            }
        }
        nf
    }

    pub fn follower_state(&self, member: &ReplicaID) -> Option<&S> {
        self.followers.get(member)
    }

    pub fn follower_state_mut(&mut self, member: &ReplicaID) -> Option<&mut S> {
        self.followers.get_mut(member)
    }

    pub fn has_quorum(&self) -> bool {
        self.config.has_quorum(&|x| self.followers.contains_key(x))
    }
}

#[cfg(test)]
pub mod testutil {
    use super::GroupID;
    use super::ReplicaID;

    pub const GROUP: GroupID = GroupID {
        id: [
            0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55,
        ],
    };

    pub const REPLICA1: ReplicaID = ReplicaID {
        id: [
            0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
            0xaa, 0xaa,
        ],
    };
    pub const REPLICA2: ReplicaID = ReplicaID {
        id: [
            0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb,
            0xbb, 0xbb,
        ],
    };
    pub const REPLICA3: ReplicaID = ReplicaID {
        id: [
            0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc,
            0xcc, 0xcc,
        ],
    };
    pub const REPLICA4: ReplicaID = ReplicaID {
        id: [
            0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd,
            0xdd, 0xdd,
        ],
    };
    pub const REPLICA5: ReplicaID = ReplicaID {
        id: [
            0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
            0xee, 0xee,
        ],
    };

    pub const THREE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3];
    pub const FIVE_REPLICAS: &[ReplicaID] = &[REPLICA1, REPLICA2, REPLICA3, REPLICA4, REPLICA5];
    pub const LAST_TWO_REPLICAS: &[ReplicaID] = &[REPLICA4, REPLICA5];
}

#[cfg(test)]
mod tests {
    use super::testutil::*;
    use super::*;

    // Test that this constant does not change and document why.
    #[test]
    fn id_bytes_is_sixteen() {
        assert_eq!(
            ID_BYTES, 16,
            "if you change the constant, fix this test and the *_string methods tests"
        );
    }

    // Test that id generation does not fail often.
    #[test]
    fn id_generation() {
        let mut success = 0;
        let mut failure = 0;
        for _i in 0..10000 {
            let x = match gen_id() {
                Ok(_) => &mut success,
                Err(_) => &mut failure,
            };
            *x += 1;
        }
        assert!(failure * 1000 < success);
    }

    // Test that the GroupID string looks like what we expect.
    #[test]
    fn group_id_string() {
        assert_eq!(
            GROUP.to_string(),
            "paxos:55555555-5555-5555-5555-555555555555"
        );
    }

    // Test that the ReplicaID string looks like what we expect.
    #[test]
    fn replica_id_string() {
        assert_eq!(
            REPLICA1.to_string(),
            "replica:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        );
    }

    // Test that the first config has epoch=1
    #[test]
    fn first_epoch() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        assert_eq!(config.epoch(), 1);
    }

    // Test that is replica returns true for every replica and false for every shadow.
    #[test]
    fn config_is_star() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);

        assert!(config.is_member(&REPLICA1));
        assert!(config.is_member(&REPLICA2));
        assert!(config.is_member(&REPLICA3));
        assert!(config.is_member(&REPLICA4));
        assert!(config.is_member(&REPLICA5));

        assert!(config.is_replica(&REPLICA1));
        assert!(config.is_replica(&REPLICA2));
        assert!(config.is_replica(&REPLICA3));
        assert!(!config.is_replica(&REPLICA4));
        assert!(!config.is_replica(&REPLICA5));

        assert!(!config.is_shadow(&REPLICA1));
        assert!(!config.is_shadow(&REPLICA2));
        assert!(!config.is_shadow(&REPLICA3));
        assert!(config.is_shadow(&REPLICA4));
        assert!(config.is_shadow(&REPLICA5));
    }

    // Test that membership iteration works
    #[test]
    fn iter_members() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let mut iter = config.members();
        assert_eq!(iter.next(), Some(&REPLICA1));
        assert_eq!(iter.next(), Some(&REPLICA2));
        assert_eq!(iter.next(), Some(&REPLICA3));
        assert_eq!(iter.next(), Some(&REPLICA4));
        assert_eq!(iter.next(), Some(&REPLICA5));
        assert_eq!(iter.next(), None);
    }

    // Test that replica iteration works
    #[test]
    fn iter_replicas() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let mut iter = config.replicas();
        assert_eq!(iter.next(), Some(&REPLICA1));
        assert_eq!(iter.next(), Some(&REPLICA2));
        assert_eq!(iter.next(), Some(&REPLICA3));
        assert_eq!(iter.next(), None);
    }

    // Test that shadow iteration works
    #[test]
    fn iter_shadows() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let mut iter = config.shadows();
        assert_eq!(iter.next(), Some(&REPLICA4));
        assert_eq!(iter.next(), Some(&REPLICA5));
        assert_eq!(iter.next(), None);
    }

    // Test that quorum computation works and excludes shadows
    #[test]
    fn quorum_computation_three_replicas() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        // all nodes form a quorum
        assert!(config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => true,
                &REPLICA2 => true,
                &REPLICA3 => true,
                _ => false,
            }
        }));
        // two out of three is still quorum
        assert!(config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => true,
                &REPLICA2 => true,
                &REPLICA3 => false,
                _ => false,
            }
        }));
        // one out of three is not quorum
        assert!(!config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => true,
                &REPLICA2 => false,
                &REPLICA3 => false,
                _ => false,
            }
        }));
        // adding a shadow should not change things
        assert!(!config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => true,
                &REPLICA2 => false,
                &REPLICA3 => false,
                &REPLICA4 => true,
                &REPLICA5 => true,
                _ => false,
            }
        }));
    }

    // Test quorums with five nodes
    #[test]
    fn quorum_computation_five_replicas() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        // all nodes form a quorum
        assert!(config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => true,
                &REPLICA2 => true,
                &REPLICA3 => true,
                &REPLICA4 => true,
                &REPLICA5 => true,
                _ => false,
            }
        }));
        // three nodes form a quorum
        assert!(config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => false,
                &REPLICA2 => true,
                &REPLICA3 => false,
                &REPLICA4 => true,
                &REPLICA5 => true,
                _ => false,
            }
        }));
        // two nodes do not a quorum make
        assert!(!config.has_quorum(&|r| -> bool {
            match r {
                &REPLICA1 => false,
                &REPLICA2 => true,
                &REPLICA3 => false,
                &REPLICA4 => true,
                &REPLICA5 => false,
                _ => false,
            }
        }));
    }

    // Test that quorum object works and excludes shadows
    #[test]
    fn quorum_object_three_replicas() {
        let config = Configuration::bootstrap(GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);
        let mut quorum: QuorumTracker<()> = QuorumTracker::new(config);

        // None is not a quorum.
        assert!(!quorum.has_quorum());
        // One is not a quorum.
        quorum.add(&REPLICA2, ());
        assert!(!quorum.has_quorum());
        // One plus shadows is not a quorum.
        quorum.add(&REPLICA4, ());
        quorum.add(&REPLICA5, ());
        assert!(!quorum.has_quorum());
        // Two becomes a quorum.
        quorum.add(&REPLICA1, ());
        assert!(quorum.has_quorum());
        // Three remains a quorum.
        quorum.add(&REPLICA3, ());
        assert!(quorum.has_quorum());
    }

    // Test quorum objects with five nodes
    #[test]
    fn quorum_object_five_replicas() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        let mut quorum: QuorumTracker<()> = QuorumTracker::new(config);

        // Two is not a quorum.
        quorum.add(&REPLICA2, ());
        quorum.add(&REPLICA4, ());
        assert!(!quorum.has_quorum());
        // Three is a quorum.
        quorum.add(&REPLICA3, ());
        assert!(quorum.has_quorum());
        // Four and five are also quorums
        quorum.add(&REPLICA1, ());
        assert!(quorum.has_quorum());
        quorum.add(&REPLICA5, ());
        assert!(quorum.has_quorum());
    }

    // Test reconfigure increments epoch
    #[test]
    fn reconfigure_epoch() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        let config = config.reconfigure();
        let config = config.commit(1 + DEFAULT_ALPHA);
        assert!(config.epoch() == 2);
    }

    // Test reconfigure respects alpha
    #[test]
    #[should_panic]
    fn reconfigure_respects_alpha() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        let config = config.reconfigure();
        config.commit(DEFAULT_ALPHA);
    }

    // Test reconfigure alpha
    #[test]
    fn reconfigure_alpha() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        let mut config = config.reconfigure();
        config.set_alpha(1);
        let config = config.commit(1 + DEFAULT_ALPHA);
        assert!(config.alpha() == 1);
    }

    // Test reconfigure will not allow a zero alpha
    #[test]
    #[should_panic]
    fn reconfigure_alpha_zero() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        let mut config = config.reconfigure();
        config.set_alpha(0);
    }

    // Test reconfigure will not allow a too-large alpha
    #[test]
    #[should_panic]
    fn reconfigure_alpha_maximum() {
        let config = Configuration::bootstrap(GROUP, FIVE_REPLICAS, &[]);
        let mut config = config.reconfigure();
        config.set_alpha(MAXIMUM_ALPHA + 1);
    }
}
