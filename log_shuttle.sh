#!/bin/sh -e

python select_server.py \
    -v \
    -e /bin/log-shuttle \
        -logs-url $KINESIS_URL \
        -max-line-length ${MAX_LINE_LENGTH:-32000} \
        -batch-size ${BATCH_SIZE:-150} \
        -input-format rfc5424 \
        -kinesis-shards $KINESIS_SHARDS
