use super::{DiscoveryOptions, transport::connect};
use std::{fs, path::Path, process::Command};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Server, ServerTlsConfig};
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

fn openssl(dir: &Path, args: &[&str]) {
    let result = Command::new("openssl")
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[tokio::test]
async fn tls_checks_authority_and_peer_name() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    openssl(
        dir,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=RPC test CA",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
        ],
    );
    openssl(
        dir,
        &[
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=rpc.test",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
        ],
    );
    fs::write(
        dir.join("ext"),
        "subjectAltName=DNS:rpc.test\nextendedKeyUsage=serverAuth\n",
    )
    .unwrap();
    openssl(
        dir,
        &[
            "x509",
            "-req",
            "-days",
            "1",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-extfile",
            "ext",
            "-out",
            "server.pem",
        ],
    );
    let identity = Identity::from_pem(
        fs::read(dir.join("server.pem")).unwrap(),
        fs::read(dir.join("server.key")).unwrap(),
    );
    let authority = Certificate::from_pem(fs::read(dir.join("ca.pem")).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (_reporter, service) = tonic_health::server::health_reporter();
    let (stop, done) = oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))
            .unwrap()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = done.await;
            })
            .await
            .unwrap();
    });
    for (name, trusted) in [
        ("rpc.test", true),
        ("incorrect.test", true),
        ("rpc.test", false),
    ] {
        let mut tls = ClientTlsConfig::new().domain_name(name);
        if trusted {
            tls = tls.ca_certificate(authority.clone());
        }
        let options = DiscoveryOptions {
            tls: Some(tls),
            ..Default::default()
        };
        let result = connect(&address, &options).await;
        if name == "rpc.test" && trusted {
            let (channel, _) = result.unwrap();
            let reply = HealthClient::new(channel)
                .check(HealthCheckRequest::default())
                .await
                .unwrap();
            assert_eq!(reply.into_inner().status, 1);
        } else {
            assert!(result.is_err());
        }
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}
