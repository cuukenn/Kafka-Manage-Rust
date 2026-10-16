//! Maps a saved [`ClusterConfig`] onto a librdkafka [`ClientConfig`].
//!
//! Kept apart from the driver so every rdkafka-backed client (the metadata
//! `Client`, and later the admin/producer/consumer clients T071+ adds) is built
//! from one place and cannot drift in how it interprets the same saved cluster.

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::DriverError;
use rdkafka::ClientConfig;

/// Translates `cfg` into librdkafka properties.
///
/// `cfg.extra` is applied **last**, so the escape hatch always wins over the
/// derived values — the UI's "arbitrary driver property" field is the final word.
///
/// Security in this card is the `security.protocol` switch only; the SASL/SSL
/// credential plumbing is T076.
pub fn client_config(cfg: &ClusterConfig) -> Result<ClientConfig, DriverError> {
    cfg.validate()?;

    let mut conf = ClientConfig::new();
    conf.set("bootstrap.servers", cfg.bootstrap_joined())
        .set("client.id", &cfg.client_id)
        // librdkafka defaults are minutes long; a dead broker should fail on the
        // timeout the user configured, not librdkafka's patience.
        .set("socket.connection.setup.timeout.ms", cfg.timeouts.connect_ms.to_string())
        .set("request.timeout.ms", cfg.timeouts.request_ms.to_string())
        .set("security.protocol", cfg.security_protocol.as_str())
        // A producer-typed client (and `AdminClient` is one) defaults this to
        // `true`, which would let any metadata probe for a topic that does not
        // exist create it as a side effect (R42).
        .set("allow.auto.create.topics", "false");

    for (key, value) in &cfg.extra {
        conf.set(key, value);
    }

    Ok(conf)
}
