//! In-memory custom transport for [iroh](https://docs.rs/iroh).
//!
//! Endpoints in the **same process** connect over in-memory datagram
//! queues as a native iroh path: QUIC still multiplexes, encrypts, and
//! authenticates exactly as it would over UDP — but no socket is bound,
//! no file is created, and nothing touches the network stack. The
//! transport is fully portable and needs no elevated permissions.
//!
//! Intended uses:
//!
//! - **In-process test harnesses**: many iroh endpoints in one test
//!   binary, with no ports to collide, no `/tmp` socket files, and no
//!   file descriptors consumed per endpoint.
//! - **Single-binary deployments**: several logical nodes (each with
//!   its own identity) co-hosted in one process, talking to each other
//!   through the same protocol stack they would use across machines.
//!
//! Note that iroh refuses to connect an endpoint to its **own**
//! id (`SelfConnect`) on every transport, including this one — the
//! peers must be distinct identities, which is exactly the
//! two-endpoints-in-one-process case this crate serves.
//!
//! # Wiring
//!
//! Create one [`MemNetwork`] — the explicit, shared "wire" — and give
//! each endpoint a [`MemTransport`] bound to a unique name on it:
//!
//! ```no_run
//! # async fn wiring() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//!
//! use iroh::endpoint::{Endpoint, presets};
//! use iroh_mem_transport::{MemNetwork, MemTransport};
//!
//! let network = MemNetwork::new();
//! let transport_a = MemTransport::new(&network, "a");
//! let endpoint_a = Endpoint::builder(presets::N0)
//!     .add_custom_transport(Arc::new(transport_a.clone()))
//!     .clear_ip_transports()
//!     .clear_relay_transports()
//!     .bind()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Dial by including the peer's [`MemTransport::local_addr`] in its
//! `EndpointAddr`. Two [`MemNetwork`]s are two isolated universes —
//! there is no process-global registry.
//!
//! # Datagram semantics
//!
//! The queues deliberately behave like UDP: each mailbox holds a
//! bounded number of datagrams, and a datagram sent to a full mailbox
//! or an unregistered name is **dropped**, not an error — QUIC's loss
//! recovery handles it, exactly as it would packet loss on a real
//! network path.

mod addr;
mod endpoint;
mod network;
mod transport;

pub use addr::{mem_custom_addr, name_from_custom_addr};
pub use network::MemNetwork;
pub use transport::MemTransport;

/// The custom-transport id for in-memory addresses: `"mem_tr\0\0"` as
/// big-endian bytes. [`CustomAddr`](iroh_base::CustomAddr)s carrying
/// any other id are not this transport's to handle.
pub const MEM_TRANSPORT_ID: u64 = 0x6d65_6d5f_7472_0000;
