#!/usr/bin/env bash
# Generates the self-signed TLS material the dev broker's SSL listener needs.
#
# Dev-only material: throwaway cert, password "changeit", no CA chain. The
# output directory is git-ignored, so run this once after a fresh clone:
#
#     ./gen-secrets.sh && docker compose up -d
#
# keytool comes from the Kafka image itself, so no JDK is needed on the host.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$here/secrets"
image="apache/kafka:3.9.1"

if [ -f "$out/kafka.keystore.jks" ]; then
    echo "secrets already present in $out (delete the directory to regenerate)"
    exit 0
fi

mkdir -p "$out"
docker run --rm -u root -v "$out:/secrets" "$image" bash -euc '
  keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 -validity 3650 \
    -dname "CN=localhost, OU=dev, O=rs-kafka-ui, L=dev, ST=dev, C=US" \
    -ext "SAN=DNS:localhost,IP:127.0.0.1" \
    -keystore /secrets/kafka.keystore.jks -storepass changeit -keypass changeit
  keytool -exportcert -rfc -alias kafka \
    -keystore /secrets/kafka.keystore.jks -storepass changeit -file /secrets/ca.pem
  keytool -importcert -noprompt -alias kafka -file /secrets/ca.pem \
    -keystore /secrets/kafka.truststore.jks -storepass changeit
  # The Kafka image reads the passwords from files named by env, not from env
  # directly (`KAFKA_SSL_KEY_CREDENTIALS` / `KAFKA_SSL_KEYSTORE_CREDENTIALS`).
  printf "changeit" > /secrets/key_credentials
  printf "changeit" > /secrets/keystore_credentials
  chmod 644 /secrets/kafka.keystore.jks /secrets/kafka.truststore.jks \
    /secrets/ca.pem /secrets/key_credentials /secrets/keystore_credentials
'

echo "wrote $out/{kafka.keystore.jks,kafka.truststore.jks,ca.pem,key_credentials,keystore_credentials}"
