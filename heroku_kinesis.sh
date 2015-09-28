#!/bin/bash

nc -lk -p 514 -e /bin/log-shuttle -logs-url $KINESIS_URL -max-line-length 35000 -batch-size 498
