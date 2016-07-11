#!/bin/bash

nc -lk -p 514 | tee >(/bin/log-shuttle -verbose -logs-url $KINESIS_URL -max-line-length 32000 -batch-size 150 -input-format rfc5424 -kinesis-shards $KINESIS_SHARDS) | \
  /bin/log-shuttle -verbose -logs-url $OTHER_URL -max-line-length 32000 -batch-size 498 -input-format rfc5424
