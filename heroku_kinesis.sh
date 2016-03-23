#!/bin/bash
socat -u udp-recv:514 - | /bin/log-shuttle -logs-url $KINESIS_URL -max-line-length 35000 -batch-size 498 -input-format rfc5424 -kinesis-shards $KINESIS_SHARDS
