FROM heroku/log-shuttle:0.12.0

RUN apk-install wget sudo bash
RUN apk-install ruby ruby-dev

RUN wget -qO- https://toolbelt.heroku.com/install.sh | sh

RUN echo 'PATH="/usr/local/heroku/bin:$PATH"' >> ~/.bash_profile
ADD ./heroku_kinesis.sh /root/

ENTRYPOINT ["/bin/bash"]
CMD ["/root/heroku_kinesis.sh"]
