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

    /// Replace the complete peer allowlist as one transaction. Unchanged peers retain
    /// their connection generation; rejected input cannot partly revoke or grant.
    /// The bounded list is administrative state, not a hot-path packet operation.
    pub fn reconcile(&mut self, wanted: &[[u8; 32]]) -> Result<bool, &'static str> {
        if wanted.len() > 1024 {
            return Err("peer allowlist exceeds 1024 entries");
        }
        for index in 0..wanted.len() {
            if wanted[..index].contains(&wanted[index]) {
                return Err("duplicate endpoint in peer allowlist");
            }
        }
        let mut additions = 0_u64;
        for peer in wanted {
            if !self.permits(peer) {
                additions += 1;
            }
        }
        if self.next_generation.checked_add(additions).is_none() {
            return Err("peer generation space exhausted");
        }
        let before = self.peers.len();
        self.peers.retain(|grant| wanted.contains(&grant.peer));
        let mut changed = self.peers.len() != before;
        for peer in wanted {
            if !self.permits(peer) {
                self.next_generation += 1;
                self.peers.push(Grant {
                    peer: *peer,
                    generation: self.next_generation,
                });
                changed = true;
            }
        }
        Ok(changed)
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
    fn reconciled_allowlists_revoke_and_add_without_regranting_survivors() {
        let mut policy = PeerPolicy::new();
        let alice = [1; 32];
        let bob = [2; 32];
        let carol = [3; 32];
        assert!(policy.reconcile(&[alice, bob]).unwrap());
        let original = policy.generation(&alice).unwrap();
        let revoked = policy.generation(&bob).unwrap();
        assert!(!policy.reconcile(&[alice, bob]).unwrap());
        assert_eq!(policy.generation(&alice), Some(original));
        assert!(policy.reconcile(&[carol, alice]).unwrap());
        assert_eq!(policy.generation(&alice), Some(original));
        assert!(!policy.permits(&bob));
        assert!(policy.permits(&carol));
        assert!(policy.reconcile(&[alice, bob]).unwrap());
        assert_ne!(policy.generation(&bob), Some(revoked));
    }

    #[test]
    fn invalid_reconciliation_is_atomic_including_generation_overflow() {
        let mut policy = PeerPolicy::new();
        let alice = [1; 32];
        let bob = [2; 32];
        assert!(policy.grant(alice));
        let generation = policy.generation(&alice);
        assert_eq!(
            policy.reconcile(&[alice, alice]),
            Err("duplicate endpoint in peer allowlist")
        );
        assert_eq!(
            policy.reconcile(&vec![[3; 32]; 1025]),
            Err("peer allowlist exceeds 1024 entries")
        );
        assert_eq!(policy.generation(&alice), generation);
        assert!(!policy.permits(&bob));
        policy.next_generation = u64::MAX;
        assert_eq!(
            policy.reconcile(&[bob]),
            Err("peer generation space exhausted")
        );
        assert_eq!(policy.generation(&alice), generation);
        assert!(!policy.permits(&bob));
        assert!(policy.reconcile(&[]).unwrap());
        assert!(!policy.permits(&alice));
        assert!(!policy.reconcile(&[]).unwrap());
    }

    #[test]
    fn exhausted_generations_cannot_reuse_authority() {
        let mut policy = PeerPolicy::new();
        policy.next_generation = u64::MAX;
        assert!(!policy.grant([3; 32]));
        assert!(policy.is_empty());
    }
}
