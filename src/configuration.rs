use std::fmt;
use std::fmt::Write;

use rand;
use rand::rngs::EntropyRng;
use rand::Rng;

const ID_BYTES: usize = 16;
const DEFAULT_ALPHA: u64 = 16;

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
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub struct GroupID {
    id: [u8; ID_BYTES],
}

impl GroupID {
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
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub struct ReplicaID {
    id: [u8; ID_BYTES],
}

impl ReplicaID {
    pub fn viewable_id(&self) -> String {
        encode_id(&self.id)
    }
}

impl fmt::Display for ReplicaID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "replica:{}", self.viewable_id())
    }
}

pub struct Configuration {
    // The Paxos ensemble this configuration represents.
    group: GroupID,
    // A sequence number indicating how many configurations this ensemble has had.  The first
    // configuration is version=1.
    version: u64,
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
        group: &GroupID,
        replicas: &[ReplicaID],
        shadows: &[ReplicaID],
    ) -> Configuration {
        Configuration {
            group: group.clone(),
            version: 1,
            slot: 1,
            alpha: DEFAULT_ALPHA,
            replicas: replicas.to_vec(),
            shadows: shadows.to_vec(),
        }
    }

    pub fn first_valid_slot(&self) -> u64 {
        self.slot
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

    pub fn member_reference(&self, member: &ReplicaID) -> &ReplicaID {
        for i in 0..self.replicas.len() {
            if self.replicas[i] == *member {
                return &self.replicas[i];
            }
        }
        for i in 0..self.shadows.len() {
            if self.shadows[i] == *member {
                return &self.shadows[i];
            }
        }
        panic!("cannot call get_acceptor(A) when !is_acceptor(A)")
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
    use super::*;
    use crate::testutil::*;

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

    // Test that is replica returns true for every replica and false for every shadow.
    #[test]
    fn config_is_star() {
        let config = Configuration::bootstrap(&GROUP, THREE_REPLICAS, LAST_TWO_REPLICAS);

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
}
