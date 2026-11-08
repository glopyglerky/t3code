//! Transport admission is separate from the T3 application's session permissions.

#[derive(Clone, Debug, Default)]
pub struct PeerPolicy {
    peers: Vec<Grant>,
    next_generation: u64,
}

#[derive(Clone, Debug)]
struct Grant {
    peer: [u8; 32],
    generation: u64,
}

impl PeerPolicy {
    pub fn new() -> Self {
        Self {
            peers: Vec::new(),
            next_generation: 0,
        }
    }

    pub fn grant(&mut self, peer: [u8; 32]) -> bool {
        if self.permits(&peer) {
            return false;
        }
        let Some(generation) = self.next_generation.checked_add(1) else {
            return false;
        };
        self.next_generation = generation;
        self.peers.push(Grant { peer, generation });
        true
    }

    pub fn revoke(&mut self, peer: &[u8; 32]) -> bool {
        let mut index = 0;
        while index < self.peers.len() {
            if self.peers[index].peer == *peer {
                self.peers.remove(index);
                return true;
            }
            index += 1;
        }
        false
    }

    pub fn permits(&self, peer: &[u8; 32]) -> bool {
        self.generation(peer).is_some()
    }

    /// Connections bind this generation so a revoke/regrant cannot revive an old session.
    pub fn generation(&self, peer: &[u8; 32]) -> Option<u64> {
        for grant in &self.peers {
            if grant.peer == *peer {
                return Some(grant.generation);
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::PeerPolicy;

    #[test]
    fn admission_denies_unknown_peers_and_revocation_removes_the_grant() {
        let mut policy = PeerPolicy::new();
        let alice = [1; 32];
        let bob = [2; 32];
        assert!(policy.is_empty());
        assert!(!policy.permits(&alice));
        assert!(policy.grant(alice));
        let first_grant = policy.generation(&alice);
        assert!(!policy.grant(alice));
        assert_eq!(policy.generation(&alice), first_grant);
        assert!(policy.grant(bob));
        assert!(policy.revoke(&alice));
        assert!(!policy.permits(&alice));
        assert!(policy.permits(&bob));
        assert!(!policy.revoke(&alice));
        assert!(policy.revoke(&bob));
        assert!(policy.is_empty());
        assert!(policy.grant(alice));
        assert_ne!(policy.generation(&alice), first_grant);
    }

    #[test]
    fn exhausted_generations_cannot_reuse_authority() {
        let mut policy = PeerPolicy::new();
        policy.next_generation = u64::MAX;
        assert!(!policy.grant([3; 32]));
        assert!(policy.is_empty());
    }
}
