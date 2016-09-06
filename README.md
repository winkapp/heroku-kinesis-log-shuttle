# Kinesis Heroku Log-Shuttle

This is an implementation of the Heroku [Log-Shuttle](https://github.com/heroku/log-shuttle) that takes logs from in from applications and funnels them to an AWS Kinesis stream.

To use this log-shuttle:

* Send your logs via TCP to port `514`.
* In the ENV for the log-shuttle set:
  * `KINESIS_URL`, the url of the Kinesis stream with url encoded credentials included.
  * `KINESIS_SHARDS`, the number of shards the Kinesis stream has.

[![Docker Repository on Quay.io](https://quay.io/repository/winkapp/kinesis-log-shuttle/status "Docker Repository on Quay")](https://quay.io/repository/winkapp/kinesis-log-shuttle)
