//! The in-memory [`CustomEndpoint`] and [`CustomSender`].

use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use iroh::endpoint::transports::{CustomEndpoint, CustomSender, RecvInfo, Transmit};
use iroh_base::CustomAddr;

use crate::MEM_TRANSPORT_ID;
use crate::network::{Mailbox, MemNetwork};

/// A bound in-memory endpoint: one registered mailbox on a
/// [`MemNetwork`], advertising its name as its local [`CustomAddr`].
#[derive(Debug)]
pub(crate) struct MemEndpoint {
    network: MemNetwork,
    mailbox: Arc<Mailbox>,
    local_addr: CustomAddr,
    watchable: n0_watcher::Watchable<Vec<CustomAddr>>,
}

impl MemEndpoint {
    pub(crate) fn new(network: MemNetwork, mailbox: Arc<Mailbox>, local_addr: CustomAddr) -> Self {
        let watchable = n0_watcher::Watchable::new(vec![local_addr.clone()]);
        Self {
            network,
            mailbox,
            local_addr,
            watchable,
        }
    }
}

impl Drop for MemEndpoint {
    fn drop(&mut self) {
        // Free the name: later datagrams to it drop silently (UDP
        // semantics), and the name can be re-bound.
        self.network.unregister(&self.local_addr);
    }
}

impl CustomEndpoint for MemEndpoint {
    fn watch_local_addrs(&self) -> n0_watcher::Direct<Vec<CustomAddr>> {
        self.watchable.watch()
    }

    fn create_sender(&self) -> Arc<dyn CustomSender> {
        Arc::new(MemSender {
            network: self.network.clone(),
            local_addr: self.local_addr.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [noq_udp::RecvMeta],
        recv_infos: &mut [RecvInfo],
    ) -> Poll<io::Result<usize>> {
        // Drain as many queued datagrams as the caller's buffers allow
        // in one poll — iroh hands us batched slices for exactly this;
        // one-datagram-per-wakeup throttling starves QUIC under bursts
        // (measured on the sibling IPC transport as multi-second
        // stalls).
        let cap = bufs.len().min(metas.len()).min(recv_infos.len());
        if cap == 0 {
            return Poll::Ready(Ok(0));
        }
        let datagrams = self.mailbox.drain(cap, cx.waker());
        if datagrams.is_empty() {
            // `drain` parked our waker under the queue lock, so a send
            // racing this poll wakes us — no lost wakeup.
            return Poll::Pending;
        }
        let mut n = 0;
        for (src, payload) in datagrams {
            let buf = &mut bufs[n];
            if payload.len() > buf.len() {
                // Datagram larger than the receive buffer: drop it, as
                // UDP would truncate-or-drop. QUIC keeps datagrams
                // within its max_udp_payload_size, so this is a
                // misbehaving-sender guard, not a normal path.
                continue;
            }
            buf[..payload.len()].copy_from_slice(&payload);
            recv_infos[n] = RecvInfo::new(src, Some(self.local_addr.clone()));
            metas[n].len = payload.len();
            metas[n].stride = payload.len();
            n += 1;
        }
        if n == 0 {
            // Everything drained was oversized; try again on the next
            // wakeup rather than reporting an empty success.
            return Poll::Pending;
        }
        Poll::Ready(Ok(n))
    }
}

/// Sends datagrams to mailboxes on the shared [`MemNetwork`].
#[derive(Debug)]
struct MemSender {
    network: MemNetwork,
    local_addr: CustomAddr,
}

impl CustomSender for MemSender {
    fn is_valid_send_addr(&self, addr: &CustomAddr) -> bool {
        addr.id() == MEM_TRANSPORT_ID
    }

    fn poll_send(
        &self,
        _cx: &mut Context<'_>,
        dst: &CustomAddr,
        _src: Option<&CustomAddr>,
        transmit: &Transmit<'_>,
    ) -> Poll<io::Result<()>> {
        if dst.id() != MEM_TRANSPORT_ID {
            return Poll::Ready(Err(io::Error::other("not a mem-transport address")));
        }
        // max_transmit_segments defaults to 1, so `contents` is a single
        // datagram — deliver it whole. Full/unregistered destinations
        // drop silently (UDP semantics; QUIC recovers).
        self.network
            .send(dst, self.local_addr.clone(), transmit.contents.to_vec());
        Poll::Ready(Ok(()))
    }
}
