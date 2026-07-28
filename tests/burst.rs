//! Burst proof: a multi-megabyte transfer over the in-memory transport
//! completes promptly — the batched `poll_recv` drain and the bounded
//! mailbox (with QUIC loss recovery over drops) sustain real throughput.

use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::{Endpoint, presets};
use iroh::{EndpointAddr, SecretKey, TransportAddr};
use iroh_mem_transport::{MemNetwork, MemTransport};

const BURST_ALPN: &[u8] = b"iroh-mem-transport/burst";
const PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
/// In-memory should move 8 MiB in well under a second; a generous
/// ceiling fails loudly on a batching regression without flaking on a
/// loaded CI box.
const DEADLINE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread")]
async fn large_transfer_over_memory_completes_promptly() {
    let network = MemNetwork::new();

    let secret_b = SecretKey::from([2u8; 32]);
    let b_id = secret_b.public();

    let transport_a = MemTransport::new(&network, "a");
    let ep_a = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::from([1u8; 32]))
        .add_custom_transport(Arc::new(transport_a))
        .clear_ip_transports()
        .clear_relay_transports()
        .bind()
        .await
        .expect("bind a");

    let transport_b = MemTransport::new(&network, "b");
    let b_local = transport_b.local_addr();
    let ep_b = Endpoint::builder(presets::N0)
        .secret_key(secret_b)
        .alpns(vec![BURST_ALPN.to_vec()])
        .add_custom_transport(Arc::new(transport_b))
        .clear_ip_transports()
        .clear_relay_transports()
        .bind()
        .await
        .expect("bind b");

    let server = tokio::spawn(async move {
        let incoming = ep_b.accept().await.expect("incoming");
        let conn = incoming.await.expect("accept connection");
        let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi");
        let received = recv
            .read_to_end(PAYLOAD_BYTES + 1)
            .await
            .expect("read payload");
        send.write_all(&(received.len() as u64).to_le_bytes())
            .await
            .expect("ack write");
        send.finish().expect("finish");
        conn.closed().await;
        received.len()
    });

    let b_addr = EndpointAddr::from_parts(b_id, [TransportAddr::Custom(b_local)]);
    let conn = ep_a
        .connect(b_addr, BURST_ALPN)
        .await
        .expect("connect over memory");
    let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");

    let payload = vec![0xa5u8; PAYLOAD_BYTES];
    let sent = tokio::time::timeout(DEADLINE, async {
        send.write_all(&payload).await.expect("payload write");
        send.finish().expect("finish payload");
        let ack = recv.read_to_end(8).await.expect("read ack");
        u64::from_le_bytes(ack.try_into().expect("8-byte ack"))
    })
    .await
    .expect("transfer within deadline");

    assert_eq!(sent as usize, PAYLOAD_BYTES);
    conn.close(0u32.into(), b"done");
    assert_eq!(server.await.expect("server task"), PAYLOAD_BYTES);
}
