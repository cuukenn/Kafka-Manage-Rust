use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ClusterId;
use crate::TimestampMs;
use crate::error::DriverError;

/// Transport security mode, mirrors librdkafka `security.protocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Ssl => "ssl",
            Self::SaslPlaintext => "sasl_plaintext",
            Self::SaslSsl => "sasl_ssl",
        }
    }
    pub fn uses_sasl(&self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
    pub fn uses_tls(&self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SaslMechanism {
    #[default]
    Plain,
    ScramSha256,
    ScramSha512,
    Kafka,
    GssApi,
    Oauthbearer,
}

impl SaslMechanism {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::Kafka => "KAFKA",
            Self::GssApi => "GSSAPI",
            Self::Oauthbearer => "OAUTHBEARER",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AuthConfig {
    pub mechanism: SaslMechanism,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    /// SASL handshake protocol name override (`PLAIN` / `SASL_PLAINTEXT`), rarely needed.
    #[serde(default)]
    pub sasl_protocol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SslConfig {
    #[serde(default)]
    pub ca_pem: Option<String>,
    #[serde(default)]
    pub ca_path: Option<String>,
    #[serde(default)]
    pub cert_pem: Option<String>,
    #[serde(default)]
    pub key_pem: Option<String>,
    #[serde(default = "bool_true")]
    pub verify_certificate: bool,
    #[serde(default = "bool_true")]
    pub verify_hostname: bool,
}

fn bool_true() -> bool {
    true
}

fn default_connect_ms() -> u64 {
    10_000
}
fn default_request_ms() -> u64 {
    15_000
}
fn default_metadata_ms() -> u64 {
    10_000
}
fn default_poll_ms() -> u64 {
    500
}
fn default_client_id() -> String {
    "rs-kafka-ui-client".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TimeoutConfig {
    #[serde(default = "default_connect_ms")]
    pub connect_ms: u64,
    #[serde(default = "default_request_ms")]
    pub request_ms: u64,
    #[serde(default = "default_metadata_ms")]
    pub metadata_ms: u64,
    /// Consumer poll interval; also the cadence of the mock driver's message pump.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_ms: default_connect_ms(),
            request_ms: default_request_ms(),
            metadata_ms: default_metadata_ms(),
            poll_ms: default_poll_ms(),
        }
    }
}

/// One saved cluster. `mock` selects the driver implementation, so a single
/// application can hold both real and mock clusters side by side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ClusterConfig {
    #[serde(default)]
    pub id: ClusterId,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub bootstrap_servers: Vec<String>,
    #[serde(default)]
    pub mock: bool,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default)]
    pub security_protocol: SecurityProtocol,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub ssl: Option<SslConfig>,
    #[serde(default)]
    pub timeouts: TimeoutConfig,
    /// Escape hatch for arbitrary driver properties (librdkafka conf keys).
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
    #[serde(default)]
    pub created_at: TimestampMs,
    #[serde(default)]
    pub updated_at: TimestampMs,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            id: ClusterId::new(),
            name: String::new(),
            bootstrap_servers: Vec::new(),
            mock: false,
            client_id: default_client_id(),
            security_protocol: SecurityProtocol::Plaintext,
            auth: None,
            ssl: None,
            timeouts: TimeoutConfig::default(),
            extra: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl ClusterConfig {
    pub fn real(name: impl Into<String>, servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            name: name.into(),
            bootstrap_servers: servers.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }

    pub fn mock(name: impl Into<String>) -> Self {
        Self { name: name.into(), mock: true, bootstrap_servers: vec!["mock://localhost:9092".into()], ..Default::default() }
    }

    /// Rejects configs the UI would otherwise persist and fail on later.
    pub fn validate(&self) -> Result<(), DriverError> {
        if self.name.trim().is_empty() {
            return Err(DriverError::invalid("name", "cluster name must not be empty"));
        }
        if self.bootstrap_servers.is_empty() {
            return Err(DriverError::invalid("bootstrapServers", "at least one bootstrap server is required"));
        }
        for server in &self.bootstrap_servers {
            let host = server.trim();
            if host.is_empty() || host.contains(' ') {
                return Err(DriverError::invalid("bootstrapServers", format!("'{server}' is not a valid host[:port]")));
            }
            if !self.mock {
                let (_, port) = host.rsplit_once(':').unwrap_or((host, "9092"));
                if port.parse::<u16>().is_err() {
                    return Err(DriverError::invalid(
                        "bootstrapServers",
                        format!("'{server}' has an invalid port (expected host:port)"),
                    ));
                }
            }
        }
        if self.security_protocol.uses_sasl() {
            let auth = self.auth.as_ref().ok_or_else(|| {
                DriverError::invalid("auth", format!("{} requires SASL credentials", self.security_protocol.as_str()))
            })?;
            if auth.username.trim().is_empty() {
                return Err(DriverError::invalid("auth.username", "username is required for SASL"));
            }
        }
        Ok(())
    }

    pub fn bootstrap_joined(&self) -> String {
        self.bootstrap_servers.join(",")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BrokerInfo {
    #[serde(default)]
    pub node_id: i32,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub rack: Option<String>,
}

impl BrokerInfo {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Live cluster metadata returned by `connect` / `healthcheck`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ClusterInfo {
    #[serde(default)]
    pub cluster_id: Option<String>,
    #[serde(default)]
    pub controller_id: Option<i32>,
    #[serde(default)]
    pub brokers: Vec<BrokerInfo>,
    /// Best-effort broker version string (real driver: `broker.version`).
    #[serde(default)]
    pub kafka_version: Option<String>,
    #[serde(default)]
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    #[default]
    Disconnected,
    Connecting,
    Connected,
    /// Connected but with degraded health (leader down, ISR shrink, auth partial).
    Degraded,
    Error,
}
