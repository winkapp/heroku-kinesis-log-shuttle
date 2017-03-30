FROM golang:1.8.0-alpine
LABEL maintainer "Jonathan Hosmer <jonathan@wink.com>"

RUN set -ex \
        && apk add --no-cache --virtual .build-deps \
            git \
        && apk del .build-deps

RUN mkdir -p /go/src/github.com/winkapp/log-shuttle
WORKDIR /go/src/github.com/winkapp/log-shuttle

EXPOSE 514/tcp 514/udp

COPY . /go/src/github.com/winkapp/log-shuttle
RUN go-wrapper download
RUN go-wrapper install ./cmd/log-shuttle

ENTRYPOINT ["/go/bin/log-shuttle"]
