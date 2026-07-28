//! The in-memory [`CustomTransport`] factory.

use std::io;

use iroh::endpoint::transports::{CustomEndpoint, CustomTransport};
use iroh_base::CustomAddr;

use crate::addr::mem_custom_addr;
use crate::endpoint::MemEndpoint;
use crate::network::MemNetwork;

/// Factory that binds an in-memory endpoint named `name` on a shared
/// [`MemNetwork`].
///
/// Install on an iroh endpoint builder with
/// [`add_custom_transport`](iroh::endpoint::Builder::add_custom_transport);
/// for a sockets-free endpoint also clear the IP and relay transports.
/// The `Clone` exists so the caller can keep a handle for
/// [`local_addr`](Self::local_addr) after handing the transport to the
/// builder.
#[derive(Debug, Clone)]
pub struct MemTransport {
    network: MemNetwork,
    local_addr: CustomAddr,
}

impl MemTransport {
    /// A transport that will bind `name` on `network`.
    ///
    /// The name must be unique on the network at
    /// [`bind`](CustomTransport::bind) time — binding a taken name
    /// fails with [`io::ErrorKind::AddrInUse`].
    #[must_use]
    pub fn new(network: &MemNetwork, name: &str) -> Self {
        Self {
            network: network.clone(),
            local_addr: mem_custom_addr(name),
        }
    }

    /// The [`CustomAddr`] this transport binds — the address peers dial.
    #[must_use]
    pub fn local_addr(&self) -> CustomAddr {
        self.local_addr.clone()
    }
}

impl CustomTransport for MemTransport {
    fn bind(&self) -> io::Result<Box<dyn CustomEndpoint>> {
        let mailbox = self.network.register(self.local_addr.clone())?;
        Ok(Box::new(MemEndpoint::new(
            self.network.clone(),
            mailbox,
            self.local_addr.clone(),
        )))
    }
}
