#!/bin/bash
nc -lk \
    -p 514 \
    -e /go/bin/log-shuttle \
        -appname "${APP_NAME:-log-shuttle}" \
        -batch-size 150 \
        -hostname "${HOSTNAME:-shuttle}" \
        -input-format rfc5424 \
        -kinesis-shards ${KINESIS_SHARDS:-200000000} \
        -logs-url "$KINESIS_URL" \
        -max-line-length 32000 \
        -outlet-token "$OUTLET_API_TOKEN" \
        -verbose
