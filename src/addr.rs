//! Name ↔ [`CustomAddr`] conversion for the in-memory transport.

use iroh_base::CustomAddr;

use crate::MEM_TRANSPORT_ID;

/// The [`CustomAddr`] for an in-memory mailbox `name`.
pub fn mem_custom_addr(name: &str) -> CustomAddr {
    CustomAddr::from_parts(MEM_TRANSPORT_ID, name.as_bytes())
}

/// The mailbox name carried by `addr`, if it is an in-memory transport
/// address (its id is [`MEM_TRANSPORT_ID`]) with UTF-8 name bytes.
pub fn name_from_custom_addr(addr: &CustomAddr) -> Option<String> {
    if addr.id() != MEM_TRANSPORT_ID {
        return None;
    }
    String::from_utf8(addr.data().to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addr_roundtrip() {
        let addr = mem_custom_addr("harness-node-1");
        assert_eq!(addr.id(), MEM_TRANSPORT_ID);
        assert_eq!(
            name_from_custom_addr(&addr).as_deref(),
            Some("harness-node-1")
        );
    }

    #[test]
    fn foreign_id_is_not_ours() {
        let foreign = CustomAddr::from_parts(0xdead_beef, b"x");
        assert_eq!(name_from_custom_addr(&foreign), None);
    }
}
