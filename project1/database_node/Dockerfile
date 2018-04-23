# Author: Haoji Liu
# Referenced from MongoDB Dockerfile

FROM cmpe275_db_base_image

# Expose ports.
#   - 27017: process
#   - 28017: http
EXPOSE 27017 28017 9003

VOLUME /data/db /data/configdb

# Define working directory.
WORKDIR /data

ADD ./src /srv/src
RUN mkdir /srv/logs

# We mount /srv/src as Volume, need to move the shell script to /tmp
#COPY ./src/run.sh /tmp/run.sh
#RUN chmod +x /tmp/run.sh
#CMD /tmp/run.sh


# Copy config files over
ADD ./config/supervisord.conf /etc/supervisord.conf
CMD /usr/bin/supervisord -c /etc/supervisord.conf
