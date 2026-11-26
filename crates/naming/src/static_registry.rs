//! Read-only JSON service directory shared by server registration and client resolution.
use crate::{
    DiscoveryError, DiscoverySubscription, Registration, RegistrationHandle, RegistrationOptions,
    Registry, RegistryError, Resolver, ServiceMetadata, ServiceStatus, Snapshot,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader, Read},
    net::{IpAddr, SocketAddr},
    num::{NonZeroU16, NonZeroU32},
    path::Path,
    time::Duration,
};
use tokio::sync::watch;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// One entry per advertised endpoint. A service may have multiple entries.
    instances: Vec<Instance>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Instance {
    /// Discovery name used by the client and by the server during registration.
    service_name: String,
    /// Advertised IPv4 or IPv6 address, without brackets or a port.
    ip: IpAddr,
    /// Nonzero TCP port used by the gRPC listener.
    port: NonZeroU16,
    /// Relative share when weighted round robin is selected; defaults to 100.
    weight: Option<NonZeroU32>,
}

/// A directory loaded once at startup. Configuration owns every registration.
/// Share a directory with `Arc` or load the same file independently in each process.
pub struct StaticRegistry {
    /// Group by discovery name, then by canonical socket address (the stable instance ID).
    services: BTreeMap<String, BTreeMap<String, ServiceMetadata>>,
}

impl StaticRegistry {
    /// Load and validate the entire JSON file before making any service visible.
    pub fn from_file(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_reader(BufReader::new(File::open(path)?))
    }

    /// Read JSON from an existing configuration source. Reject invalid or duplicate endpoints.
    pub fn from_reader(reader: impl Read) -> io::Result<Self> {
        let config: Config = serde_json::from_reader(reader)?;
        let mut services = BTreeMap::<String, BTreeMap<String, ServiceMetadata>>::new();
        for instance in config.instances {
            let name = instance.service_name;
            if name.is_empty() || name.trim() != name || instance.ip.is_unspecified() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid service name or advertised IP: {name:?} at {}",
                        instance.ip
                    ),
                ));
            }
            let address = SocketAddr::new(instance.ip, instance.port.get()).to_string();
            let members = services.entry(name.clone()).or_default();
            if members.contains_key(&address) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("duplicate static instance: {name} at {address}"),
                ));
            }
            members.insert(
                address.clone(),
                ServiceMetadata {
                    service_name: name,
                    instance_id: address.clone(),
                    address,
                    status: ServiceStatus::Up,
                    weight: instance.weight.map_or(100, |w| i64::from(w.get())),
                    ..Default::default()
                },
            );
        }
        Ok(Self { services })
    }
}

#[tonic::async_trait]
impl Registry for StaticRegistry {
    async fn register(&self, options: RegistrationOptions) -> Result<Registration, RegistryError> {
        let address = options.address.parse::<SocketAddr>().map_err(|e| {
            RegistryError::Backend(
                format!(
                    "invalid advertised address for {}: {} ({e})",
                    options.service_name, options.address
                )
                .into(),
            )
        })?;
        if self
            .services
            .get(&options.service_name)
            .is_some_and(|m| m.contains_key(&address.to_string()))
        {
            // Existing grants no withdrawal handle: server shutdown cannot remove configured entries.
            Ok(Registration::Existing)
        } else {
            Err(RegistryError::Backend(
                format!(
                    "static directory has no instance of {} at {address}",
                    options.service_name
                )
                .into(),
            ))
        }
    }

    async fn unregister_handle(&self, _: &RegistrationHandle) -> Result<(), RegistryError> {
        Ok(())
    }
}

#[tonic::async_trait]
impl Resolver for StaticRegistry {
    async fn subscribe(
        &self,
        service: &str,
        _: Duration,
    ) -> Result<Box<dyn DiscoverySubscription>, DiscoveryError> {
        let members = self
            .services
            .get(service)
            .ok_or_else(|| DiscoveryError(format!("unknown static service: {service}")))?;
        let (sender, updates) = watch::channel(Ok(members.clone()));
        Ok(Box::new(StaticSubscription {
            updates,
            sender: Some(sender),
        }))
    }
}

struct StaticSubscription {
    /// Single immutable snapshot; normal transport workers track actual connection availability.
    updates: watch::Receiver<Snapshot>,
    /// Keep the subscription live until close or drop without spawning a watch task.
    sender: Option<watch::Sender<Snapshot>>,
}

#[tonic::async_trait]
impl DiscoverySubscription for StaticSubscription {
    fn snapshots(&self) -> watch::Receiver<Snapshot> {
        self.updates.clone()
    }
    async fn refresh(&self) -> Result<(), DiscoveryError> {
        self.sender
            .as_ref()
            .map(|_| ())
            .ok_or_else(|| DiscoveryError("static subscription closed".into()))
    }
    async fn close(&mut self) {
        self.sender.take();
    }
}
