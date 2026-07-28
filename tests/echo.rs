//! End-to-end proof: two iroh endpoints in one process, connected over
//! the in-memory transport **only** — no sockets, no relay, no network.

use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::{Endpoint, presets};
use iroh::{EndpointAddr, SecretKey, TransportAddr};
use iroh_mem_transport::{MemNetwork, MemTransport};

const ECHO_ALPN: &[u8] = b"iroh-mem-transport/echo";
const DEADLINE: Duration = Duration::from_secs(10);

async fn mem_only_endpoint(
    network: &MemNetwork,
    name: &str,
    secret: SecretKey,
    alpns: Vec<Vec<u8>>,
) -> (Endpoint, MemTransport) {
    let transport = MemTransport::new(network, name);
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret)
        .alpns(alpns)
        .add_custom_transport(Arc::new(transport.clone()))
        .clear_ip_transports()
        .clear_relay_transports()
        .bind()
        .await
        .expect("bind mem-only endpoint");
    (endpoint, transport)
}

#[tokio::test(flavor = "multi_thread")]
async fn echo_round_trip_over_memory_only() {
    let network = MemNetwork::new();

    let secret_b = SecretKey::from([2u8; 32]);
    let b_id = secret_b.public();

    let (ep_a, _ta) = mem_only_endpoint(&network, "a", SecretKey::from([1u8; 32]), vec![]).await;
    let (ep_b, tb) = mem_only_endpoint(&network, "b", secret_b, vec![ECHO_ALPN.to_vec()]).await;

    let server = tokio::spawn(async move {
        let incoming = ep_b.accept().await.expect("incoming");
        let conn = incoming.await.expect("accept connection");
        let (mut send, mut recv) = conn.accept_bi().await.expect("accept_bi");
        let msg = recv.read_to_end(1024).await.expect("read request");
        send.write_all(&msg).await.expect("echo write");
        send.finish().expect("finish");
        conn.closed().await;
        msg
    });

    let b_addr = EndpointAddr::from_parts(b_id, [TransportAddr::Custom(tb.local_addr())]);
    let conn = tokio::time::timeout(DEADLINE, ep_a.connect(b_addr, ECHO_ALPN))
        .await
        .expect("connect within deadline")
        .expect("connect over memory");

    let (mut send, mut recv) = conn.open_bi().await.expect("open_bi");
    send.write_all(b"ping over shared memory")
        .await
        .expect("request write");
    send.finish().expect("finish request");

    let reply = tokio::time::timeout(DEADLINE, recv.read_to_end(1024))
        .await
        .expect("reply within deadline")
        .expect("read reply");
    assert_eq!(reply, b"ping over shared memory");

    conn.close(0u32.into(), b"done");
    let served = server.await.expect("server task");
    assert_eq!(served, b"ping over shared memory");
}

#[tokio::test(flavor = "multi_thread")]
async fn isolated_networks_cannot_connect() {
    let net_a = MemNetwork::new();
    let net_b = MemNetwork::new();

    let secret_b = SecretKey::from([4u8; 32]);
    let b_id = secret_b.public();

    let (ep_a, _ta) = mem_only_endpoint(&net_a, "a", SecretKey::from([3u8; 32]), vec![]).await;
    let (_ep_b, tb) = mem_only_endpoint(&net_b, "b", secret_b, vec![ECHO_ALPN.to_vec()]).await;

    // Same name-addressing, but a different MemNetwork: the dial must
    // not complete — the datagrams fall on the floor of network A.
    let b_addr = EndpointAddr::from_parts(b_id, [TransportAddr::Custom(tb.local_addr())]);
    let outcome =
        tokio::time::timeout(Duration::from_secs(3), ep_a.connect(b_addr, ECHO_ALPN)).await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "endpoints on isolated MemNetworks must not reach each other"
    );
}
