#!/bin/sh -e

PROTO=$(echo "${1:-tcp}" | tr '[:upper:]' '[:lower:]')

if [ "$PROTO" = "udp" ]; then
    PROTO_FLAG="-u"
elif [ "$PROTO" = "tcp" ]; then
    PROTO_FLAG=""
else
    echo "Unsupport protocol: $PROTO"
    exit 1
fi

nc -lk $PROTO_FLAG \
    -p 514 \
    -e /go/bin/log-shuttle \
        -appname "${APP_NAME:-log-shuttle}" \
        -batch-size 150 \
        -hostname "${HOSTNAME:-shuttle}" \
        -input-format rfc5424 \
        -kinesis-shards "${KINESIS_SHARDS:-200000000}" \
        -logs-url "$KINESIS_URL" \
        -max-line-length 32000 \
        -outlet-token "$OUTLET_API_TOKEN" \
        -verbose
