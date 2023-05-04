# ################### VER 1 of Docker ########################
# FROM redis:alpine3.17

# CMD ["redis-server"]


################### VER 2 of Docker (Compose) ########################
FROM python:3.10.4-slim
WORKDIR /usr/src/app/scraper

# install supervisord
RUN apt-get update && apt-get install -y supervisor

# Install Chrome.
RUN apt-get update && apt-get -y install google-chrome-stable

# copy requirements and install (so that changes to files do not mean rebuild cannot be cached)
COPY requirements.txt /usr/src/app
RUN pip install -r /usr/src/app/requirements.txt

# copy all files into the container
COPY . /usr/src/app/scraper

# needs to be set else Celery gives an error (because docker runs commands inside container as root)
ENV C_FORCE_ROOT=1

# run supervisord
CMD ["/usr/bin/supervisord"]


################### VER 3 of Docker ########################
# FROM rabbitmq:3.10.20-alpine


# # Define environment variables.
# ENV RABBITMQ_USER myuser
# ENV RABBITMQ_PASSWORD mypassword
# ENV RABBITMQ_VHOST myvhost
# ENV RABBITMQ_PID_FILE /var/lib/rabbitmq/mnesia/rabbitmq


# ADD init.sh /init.sh
# RUN chmod +x /init.sh
# EXPOSE 5672

# # Define default command
# CMD ["/init.sh"]
