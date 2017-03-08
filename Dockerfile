FROM golang:1.8.0-alpine
MAINTAINER Jonathan Hosmer <jonathan@wink.com>

RUN mkdir -p /go/src/app
WORKDIR /go/src/app

EXPOSE 514

COPY entrypoint.sh /entrypoint.sh

COPY . /go/src/app
RUN go-wrapper download
RUN go-wrapper install -v cmd/log-shuttle

ENTRYPOINT ["/entrypoint.sh"]
