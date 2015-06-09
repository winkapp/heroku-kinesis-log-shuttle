#!/bin/bash

source ~/.bash_profile
heroku login <<< $HEROKU_USER$'\n'$HEROKU_PASS$'\n'
heroku logs --tail -a $APP_NAME | /bin/log-shuttle -logs-url $KINESIS_URL -max-line-length 35000 -batch-size 498
