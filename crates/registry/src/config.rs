use std::time::Duration;

/// Connection and lease settings for one etcd namespace.
#[derive(Clone)]
pub struct EtcdConfig {
    /// etcd v3 gRPC endpoints.
    pub endpoints: Vec<String>,
    /// Prefix without leading or trailing slash.
    pub namespace: String,
    /// Deadline for establishing connections and individual etcd operations.
    pub timeout: Duration,
    /// Requested lease lifetime, in whole seconds.
    pub lease_ttl: Duration,
    /// Optional etcd username and password.
    pub auth: Option<(String, String)>,
    /// Optional CA, identity and server-name configuration for etcd.
    pub tls: Option<etcd_client::TlsOptions>,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://127.0.0.1:2379".into()],
            namespace: "grpc".into(),
            timeout: Duration::from_secs(2),
            lease_ttl: Duration::from_secs(5),
            auth: None,
            tls: None,
        }
    }
}

impl EtcdConfig {
    /// Connect with the same authentication, TLS and request timeouts as the registry.
    pub async fn connect_client(&self) -> Result<etcd_client::Client, crate::RegistryError> {
        if self.namespace.trim_matches('/').is_empty()
            || self.timeout.is_zero()
            || self.lease_ttl.as_secs() == 0
            || self.lease_ttl.subsec_nanos() != 0
            || i64::try_from(self.lease_ttl.as_secs()).is_err()
        {
            return Err(crate::RegistryError::Invalid(
                "invalid namespace, timeout or lease lifetime",
            ));
        }
        let mut options = etcd_client::ConnectOptions::new()
            .with_connect_timeout(self.timeout)
            .with_timeout(self.timeout);
        if let Some(tls) = self.tls.clone() {
            options = options.with_tls(tls);
        }
        if let Some((user, password)) = &self.auth {
            options = options.with_user(user, password);
        }
        Ok(tokio::time::timeout(
            self.timeout,
            etcd_client::Client::connect(&self.endpoints, Some(options)),
        )
        .await??)
    }
}
