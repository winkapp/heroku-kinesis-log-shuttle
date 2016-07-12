FROM heroku/log-shuttle:0.16.0

RUN apk update

RUN apk-install wget sudo bash

ADD ./heroku_kinesis.sh /root/

EXPOSE 514

ENTRYPOINT ["/bin/bash"]
CMD ["/root/heroku_kinesis.sh"]
