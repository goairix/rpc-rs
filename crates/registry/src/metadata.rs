use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

/// Service governance state. Only `Up` instances accept new traffic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    /// Eligible for discovery once the transport is ready.
    Up,
    /// Administratively disabled.
    Down,
    /// Initialization is in progress.
    Starting,
    /// Draining in-flight requests.
    Stopping,
    /// A missing or unrecognized Go status must stay out of routing.
    #[serde(other)]
    #[default]
    Unknown,
}

/// Application-supplied values for a new registration.
#[derive(Clone, Debug, Default)]
pub struct RegistrationOptions {
    /// Fully qualified protobuf service name.
    pub service_name: String,
    /// Advertised host and port.
    pub address: String,
    /// Application version, which may be empty.
    pub version: String,
    /// Deployment environment.
    pub env: String,
    /// Labels carried in the Go registration record.
    pub tags: Vec<String>,
}

fn null_as_default<'de, D: Deserializer<'de>, T: Deserialize<'de> + Default>(
    deserializer: D,
) -> Result<T, D::Error> {
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn default_weight() -> i64 {
    100
}

/// Go-compatible persistent record stored under a leased instance key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceMetadata {
    /// Fully qualified protobuf service name.
    pub service_name: String,
    /// Application version.
    pub version: String,
    /// Advertised host and port.
    pub address: String,
    /// Deployment environment.
    pub env: String,
    /// Routing weight. Nonpositive values use the Go default of 100.
    #[serde(default = "default_weight")]
    pub weight: i64,
    /// Labels; Go nil slices decode as empty.
    #[serde(deserialize_with = "null_as_default")]
    pub tags: Vec<String>,
    /// Current governance state.
    pub status: ServiceStatus,
    /// Unix seconds of the original registration.
    pub register_time: i64,
    /// Stable ID reused through lease recovery.
    pub instance_id: String,
    /// Application properties; Go nil maps decode as empty.
    #[serde(deserialize_with = "null_as_default")]
    pub properties: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_go_marshalled_service_metadata() {
        let record: ServiceMetadata =
            serde_json::from_str(include_str!("../tests/fixtures/go-record.json")).unwrap();
        assert_eq!(record.service_name, "example.Echo");
        assert_eq!(record.instance_id, "go-instance");
        assert_eq!(record.register_time, 1_700_000_000);
        assert_eq!(record.status, ServiceStatus::Up);
        assert!(record.tags.is_empty() && record.properties.is_empty());
    }
    #[test]
    fn go_nil_collections_and_missing_fields() {
        let record: ServiceMetadata = serde_json::from_str(
            r#"{"service_name":"Echo","tags":null,"properties":null,"extra":1}"#,
        )
        .unwrap();
        assert!(record.tags.is_empty() && record.properties.is_empty());
        assert_eq!(record.weight, 100);
        assert_eq!(record.status, ServiceStatus::Unknown);
        assert!(record.version.is_empty());
    }
}
