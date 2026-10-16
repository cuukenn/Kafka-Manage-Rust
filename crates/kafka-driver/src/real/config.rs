//! Maps a saved [`ClusterConfig`] onto a librdkafka [`ClientConfig`].
//!
//! Kept apart from the driver so every rdkafka-backed client (the metadata
//! `Client`, the admin client, the producer and the consumer) is built from one
//! place and cannot drift in how it interprets the same saved cluster.

use kafka_core::cluster::{ClusterConfig, SslConfig};
use kafka_core::error::DriverError;
use rdkafka::ClientConfig;

/// Translates `cfg` into librdkafka properties.
///
/// `cfg.extra` is applied **last**, so the escape hatch always wins over the
/// derived values — the UI's "arbitrary driver property" field is the final word.
pub fn client_config(cfg: &ClusterConfig) -> Result<ClientConfig, DriverError> {
    cfg.validate()?;

    // Resolved first: the `auth.saslProtocol` override can turn SASL on for a
    // config whose saved protocol still says PLAINTEXT, and the credential
    // branch below has to follow the protocol actually in force (R65).
    let protocol = security_protocol(cfg);

    let mut conf = ClientConfig::new();
    conf.set("bootstrap.servers", cfg.bootstrap_joined())
        .set("client.id", &cfg.client_id)
        // librdkafka defaults are minutes long; a dead broker should fail on the
        // timeout the user configured, not librdkafka's patience.
        .set("socket.connection.setup.timeout.ms", cfg.timeouts.connect_ms.to_string())
        .set("request.timeout.ms", cfg.timeouts.request_ms.to_string())
        .set("security.protocol", protocol.clone())
        // A producer-typed client (and `AdminClient` is one) defaults this to
        // `true`, which would let any metadata probe for a topic that does not
        // exist create it as a side effect (R42).
        .set("allow.auto.create.topics", "false");

    // Credentials are only handed over when the protocol in force uses them, so
    // switching a cluster back to PLAINTEXT leaves nothing behind (R65).
    if protocol.contains("sasl") {
        apply_sasl(&mut conf, cfg)?;
    }
    if protocol.contains("ssl") {
        apply_tls(&mut conf, cfg.ssl.as_ref())?;
    }

    for (key, value) in &cfg.extra {
        conf.set(key, value);
    }

    Ok(conf)
}

/// The transport switch, unless the saved `auth.saslProtocol` overrides it.
///
/// That field sits next to the SASL credentials in the UI's cluster form and
/// holds `security.protocol` names (`PLAIN`, `SASL_PLAINTEXT`); librdkafka has
/// no separate handshake switch to map it onto (R64).
fn security_protocol(cfg: &ClusterConfig) -> String {
    let saved = cfg.auth.as_ref().and_then(|auth| auth.sasl_protocol.as_deref()).unwrap_or("").trim();
    if saved.is_empty() {
        cfg.security_protocol.as_str().to_string()
    } else {
        saved.to_ascii_lowercase()
    }
}

fn apply_sasl(conf: &mut ClientConfig, cfg: &ClusterConfig) -> Result<(), DriverError> {
    let auth = cfg.auth.as_ref().ok_or_else(|| {
        DriverError::invalid("auth", format!("{} requires SASL credentials", cfg.security_protocol.as_str()))
    })?;
    conf.set("sasl.mechanism", auth.mechanism.as_str())
        .set("sasl.username", &auth.username)
        .set("sasl.password", &auth.password);
    Ok(())
}

/// Writes the TLS properties for an `ssl`-carrying protocol.
///
/// An absent section means "broker cert verified against librdkafka's own trust
/// store", which is its default; nothing is written then, and in particular the
/// verification switches are not derived from `SslConfig::default()` — that
/// derive disagrees with the serde default and would silently disable them (R67).
fn apply_tls(conf: &mut ClientConfig, ssl: Option<&SslConfig>) -> Result<(), DriverError> {
    let Some(ssl) = ssl else {
        return Ok(());
    };

    if let Some(pem) = present(ssl.ca_pem.as_deref()) {
        conf.set("ssl.ca.pem", pem);
    }
    if let Some(path) = present(ssl.ca_path.as_deref()) {
        conf.set("ssl.ca.location", path);
    }

    // librdkafka rejects a certificate without its key, so catch the half-set
    // pair here rather than at the first handshake (R66).
    match (present(ssl.cert_pem.as_deref()), present(ssl.key_pem.as_deref())) {
        (Some(cert), Some(key)) => {
            conf.set("ssl.certificate.pem", cert).set("ssl.key.pem", key);
        }
        (Some(_), None) => {
            return Err(DriverError::invalid("ssl.keyPem", "a client certificate needs its private key"));
        }
        (None, Some(_)) => {
            return Err(DriverError::invalid("ssl.certPem", "a private key needs its client certificate"));
        }
        (None, None) => {}
    }

    conf.set("enable.ssl.certificate.verification", if ssl.verify_certificate { "true" } else { "false" })
        .set("ssl.endpoint.identification.algorithm", if ssl.verify_hostname { "https" } else { "none" });
    Ok(())
}

/// A field that was left blank behaves like one that was never filled in.
fn present(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|text| !text.is_empty())
}
