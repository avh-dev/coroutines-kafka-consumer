#!/usr/bin/env sh

set -eu

LAB_ROOT="${LAB_ROOT:-/opt/ckc-lab}"
LAB_NODE_IP="${LAB_NODE_IP:?LAB_NODE_IP is required}"
TLS_DIR="${LAB_ROOT}/state/kafka-tls"

if [ -f "${TLS_DIR}/ca.crt" ] && [ -f "${TLS_DIR}/kafka.keystore.p12" ] && [ -f "${TLS_DIR}/client.truststore.p12" ]; then
  exit 0
fi

command -v openssl >/dev/null 2>&1 || { echo "openssl is required to create internal Kafka TLS material." >&2; exit 1; }
command -v keytool >/dev/null 2>&1 || { echo "keytool is required to create the Kafka client truststore." >&2; exit 1; }
umask 077
mkdir -p "${TLS_DIR}"
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -subj "/CN=ckc-internal-kafka-ca" -keyout "${TLS_DIR}/ca.key" -out "${TLS_DIR}/ca.crt"
openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=ckc-internal-kafka" \
  -keyout "${TLS_DIR}/kafka.key" -out "${TLS_DIR}/kafka.csr"
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1,IP:%s\n' "${LAB_NODE_IP}" > "${TLS_DIR}/kafka.ext"
openssl x509 -req -sha256 -days 3650 -in "${TLS_DIR}/kafka.csr" -CA "${TLS_DIR}/ca.crt" -CAkey "${TLS_DIR}/ca.key" \
  -CAcreateserial -out "${TLS_DIR}/kafka.crt" -extfile "${TLS_DIR}/kafka.ext"
openssl pkcs12 -export -name kafka -in "${TLS_DIR}/kafka.crt" -inkey "${TLS_DIR}/kafka.key" -certfile "${TLS_DIR}/ca.crt" \
  -out "${TLS_DIR}/kafka.keystore.p12" -passout pass:changeit
keytool -importcert -noprompt -alias ckc-internal-kafka-ca -file "${TLS_DIR}/ca.crt" \
  -keystore "${TLS_DIR}/client.truststore.p12" -storetype PKCS12 -storepass changeit
cat > "${TLS_DIR}/client.properties" <<'EOF'
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/client.truststore.p12
ssl.truststore.password=changeit
ssl.truststore.type=PKCS12
ssl.endpoint.identification.algorithm=
EOF
