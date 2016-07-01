#!/bin/bash

nc -lk -p 514 -e /bin/log-shuttle -verbose -logs-url $KINESIS_URL -max-line-length 32000 -batch-size 498 -input-format rfc5424 -kinesis-shards $KINESIS_SHARDS
