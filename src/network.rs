//! [`MemNetwork`] — the explicit in-process "wire" mailboxes live on.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use iroh_base::CustomAddr;

/// Datagrams a single mailbox buffers before dropping, mirroring a UDP
/// socket's receive buffer. At iroh's ~1500-byte datagrams this is
/// roughly 1.5 MiB per endpoint — QUIC's congestion control keeps a
/// healthy flow well under it, and a burst that overflows it is
/// recovered by QUIC loss recovery exactly as on a real network.
const MAILBOX_CAPACITY: usize = 1024;

/// One received datagram: the sender's address and its payload.
pub(crate) type Datagram = (CustomAddr, Vec<u8>);

/// An isolated in-process datagram network.
///
/// Cloning is cheap and shares the network: every
/// [`MemTransport`](crate::MemTransport) created with a clone of the
/// same `MemNetwork` can reach every other. Two separately created
/// `MemNetwork`s share nothing — there is deliberately no process-wide
/// global registry.
#[derive(Debug, Clone, Default)]
pub struct MemNetwork {
    registry: Arc<Mutex<HashMap<CustomAddr, Arc<Mailbox>>>>,
}

impl MemNetwork {
    /// Create an empty, isolated network.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `addr`, creating its mailbox.
    ///
    /// Fails with [`io::ErrorKind::AddrInUse`] when the address is
    /// already bound on this network — the same contract as binding a
    /// socket.
    pub(crate) fn register(&self, addr: CustomAddr) -> io::Result<Arc<Mailbox>> {
        let mut registry = self.registry.lock().expect("registry lock poisoned");
        if registry.contains_key(&addr) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("mem transport address already bound: {addr:?}"),
            ));
        }
        let mailbox = Arc::new(Mailbox::default());
        registry.insert(addr, Arc::clone(&mailbox));
        Ok(mailbox)
    }

    /// Remove `addr` from the network (endpoint drop).
    pub(crate) fn unregister(&self, addr: &CustomAddr) {
        self.registry
            .lock()
            .expect("registry lock poisoned")
            .remove(addr);
    }

    /// Deliver one datagram from `src` to `dst`.
    ///
    /// UDP semantics: an unregistered destination or a full mailbox
    /// silently drops the datagram — QUIC loss recovery is the
    /// retransmission layer, exactly as on a lossy network path.
    pub(crate) fn send(&self, dst: &CustomAddr, src: CustomAddr, payload: Vec<u8>) {
        let mailbox = {
            let registry = self.registry.lock().expect("registry lock poisoned");
            registry.get(dst).cloned()
        };
        if let Some(mailbox) = mailbox {
            mailbox.push((src, payload));
        }
    }
}

/// A bounded receive queue plus the waker of the endpoint draining it.
#[derive(Debug, Default)]
pub(crate) struct Mailbox {
    queue: Mutex<MailboxQueue>,
}

#[derive(Debug, Default)]
struct MailboxQueue {
    datagrams: VecDeque<Datagram>,
    recv_waker: Option<Waker>,
}

impl Mailbox {
    /// Enqueue a datagram (dropping it when full) and wake the receiver.
    fn push(&self, datagram: Datagram) {
        let waker = {
            let mut queue = self.queue.lock().expect("mailbox lock poisoned");
            if queue.datagrams.len() >= MAILBOX_CAPACITY {
                // Full: drop, exactly like a UDP receive-buffer overflow.
                return;
            }
            queue.datagrams.push_back(datagram);
            queue.recv_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Dequeue up to `max` datagrams; when empty, park `waker` for the
    /// next [`push`](Self::push).
    pub(crate) fn drain(&self, max: usize, waker: &Waker) -> Vec<Datagram> {
        let mut queue = self.queue.lock().expect("mailbox lock poisoned");
        if queue.datagrams.is_empty() {
            queue.recv_waker = Some(waker.clone());
            return Vec::new();
        }
        let take = queue.datagrams.len().min(max);
        queue.datagrams.drain(..take).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use super::*;
    use crate::mem_custom_addr;

    #[test]
    fn register_conflict_is_addr_in_use() {
        let network = MemNetwork::new();
        let addr = mem_custom_addr("a");
        network.register(addr.clone()).expect("first bind");
        let err = network.register(addr).expect_err("second bind must fail");
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn unregistered_destination_drops_silently() {
        let network = MemNetwork::new();
        // Must not panic or error — UDP semantics.
        network.send(&mem_custom_addr("ghost"), mem_custom_addr("src"), vec![1]);
    }

    #[test]
    fn full_mailbox_drops_new_datagrams() {
        let network = MemNetwork::new();
        let addr = mem_custom_addr("b");
        let mailbox = network.register(addr.clone()).expect("bind");
        for i in 0..(MAILBOX_CAPACITY + 10) {
            network.send(&addr, mem_custom_addr("src"), vec![i as u8]);
        }
        let drained = mailbox.drain(usize::MAX, &Waker::noop().clone());
        assert_eq!(drained.len(), MAILBOX_CAPACITY);
    }

    #[test]
    fn separate_networks_are_isolated() {
        let net_a = MemNetwork::new();
        let net_b = MemNetwork::new();
        let addr = mem_custom_addr("shared-name");
        let mailbox = net_a.register(addr.clone()).expect("bind on a");
        net_b.send(&addr, mem_custom_addr("src"), vec![7]);
        assert!(
            mailbox.drain(usize::MAX, &Waker::noop().clone()).is_empty(),
            "a datagram on network B must not arrive on network A"
        );
    }
}
