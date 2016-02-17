FROM heroku/log-shuttle:0.12.0

RUN apk-install wget sudo bash socat

ADD ./heroku_kinesis.sh /root/

EXPOSE 514/udp

ENTRYPOINT ["/bin/bash"]
CMD ["/root/heroku_kinesis.sh"]
