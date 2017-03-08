FROM golang:1.8.0-alpine
MAINTAINER Jonathan Hosmer <jonathan@wink.com>

RUN mkdir -p /go/src/github.com/winkapp/log-shuttle
WORKDIR /go/src/github.com/winkapp/log-shuttle

EXPOSE 514

COPY entrypoint.sh /entrypoint.sh

COPY . /go/src/github.com/winkapp/log-shuttle
RUN go-wrapper download
RUN go-wrapper install ./cmd/log-shuttle

ENTRYPOINT ["/entrypoint.sh"]
