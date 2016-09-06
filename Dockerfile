FROM quay.io/winkapp/log-shuttle:v0.16.0
MAINTAINER Jonathan Hosmer <jonathan@wink.com>

RUN apk update && apk-install 'python=2.7.12-r0'

EXPOSE 514

COPY select_server.py /root/select_server.py
COPY log_shuttle.sh /root/log_shuttle.sh

WORKDIR /root

ENTRYPOINT ["/root/log_shuttle.sh"]
CMD ["/bin/sh"]

# Docker 1.12+
# HEALTHCHECK --timeout=3s \
#     CMD /usr/bin/nc -vz localhost 514
