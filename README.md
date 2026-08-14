# iroh-mem-transport

[![Crates.io](https://img.shields.io/crates/v/iroh-mem-transport.svg)](https://crates.io/crates/iroh-mem-transport)
[![Documentation](https://docs.rs/iroh-mem-transport/badge.svg)](https://docs.rs/iroh-mem-transport)
[![CI](https://github.com/legra-ai/iroh-mem-transport/actions/workflows/ci.yml/badge.svg)](https://github.com/legra-ai/iroh-mem-transport/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![Downloads](https://img.shields.io/crates/d/iroh-mem-transport.svg)](https://crates.io/crates/iroh-mem-transport)

In-memory custom transport for [iroh](https://github.com/n0-computer/iroh):
endpoints in the **same process** connect over in-memory datagram queues as a
native iroh path. QUIC still multiplexes, encrypts, and authenticates exactly
as it would over UDP — but no socket is bound, no file is created, and nothing
touches the network stack.

Sibling of
[`iroh-ipc-transport`](https://github.com/legra-ai/iroh-ipc-transport)
(same-machine peers over a Unix socket); this crate is same-**process** peers
over shared memory.

## Why

- **In-process test harnesses** — many iroh endpoints in one test binary,
  with no ports to collide, no socket files, and no file descriptors consumed
  per endpoint. Fully portable (no `cfg(unix)`).
- **Single-binary deployments** — several logical nodes, each with its own
  identity, co-hosted in one process and talking through the same protocol
  stack they would use across machines.

Note: iroh refuses to connect an endpoint to its *own* id (`SelfConnect`) on
every transport, including this one. The peers must be distinct identities —
which is exactly the two-endpoints-in-one-process case this crate serves.

## Use

```rust,no_run
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
use std::sync::Arc;

use iroh::endpoint::{Endpoint, presets};
use iroh::{EndpointAddr, TransportAddr};
use iroh_mem_transport::{MemNetwork, MemTransport};

// One MemNetwork is one isolated universe — no global registry.
let network = MemNetwork::new();

let transport_a = MemTransport::new(&network, "a");
let ep_a = Endpoint::builder(presets::N0)
    .add_custom_transport(Arc::new(transport_a.clone()))
    .clear_ip_transports()      // optional: fully sockets-free
    .clear_relay_transports()
    .bind()
    .await?;

let transport_b = MemTransport::new(&network, "b");
let ep_b = Endpoint::builder(presets::N0)
    .alpns(vec![b"my-proto".to_vec()])
    .add_custom_transport(Arc::new(transport_b.clone()))
    .clear_ip_transports()
    .clear_relay_transports()
    .bind()
    .await?;

// Dial by the peer's mem address.
let b_addr = EndpointAddr::from_parts(
    ep_b.id(),
    [TransportAddr::Custom(transport_b.local_addr())],
);
let conn = ep_a.connect(b_addr, b"my-proto").await?;
# drop(conn); Ok(())
# }
```

## Semantics

The queues deliberately behave like UDP: each endpoint's mailbox holds a
bounded number of datagrams, and a datagram sent to a full mailbox or an
unregistered name is **dropped**, not an error — QUIC's loss recovery handles
it, exactly as it would packet loss on a real path. Dropping an endpoint
unregisters its name.

Requires iroh's `unstable-custom-transports` feature (this crate enables it).

## License

Copyright © 2026 DataRoad Inc, Delaware, USA, trading as Legra.

Licensed under either the [MIT license](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option.
