FROM ubuntu:18.04
LABEL mantainer="giovanni.basolu@manchester.ac.uk"

WORKDIR /app

# Define root folder
RUN mkdir -p /root/.hydra/

## COPY . /app
COPY install.sh /app
COPY Pipfile /app
COPY Pipfile.lock /app

#Tell docker to expect these arguments, and make them available to the install.sh script.
ARG SSH_PRIVATE_KEY
ARG SSH_KNOWN_HOSTS

RUN chmod 755 install.sh
RUN ./install.sh

# Cleaning file system
RUN rm ./install.sh
RUN rm Pipfile
RUN rm Pipfile.lock

CMD ["/bin/sleep", "86400"]
