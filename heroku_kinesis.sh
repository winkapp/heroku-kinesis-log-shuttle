#!/bin/bash

ncat -l 514 | /bin/log-shuttle -logs-url $KINESIS_URL -max-line-length 35000 -batch-size 498
