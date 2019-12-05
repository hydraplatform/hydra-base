#!/bin/bash

which ssh-agent || ( apt-get update -y && apt-get install openssh-client -y )
eval $(ssh-agent -s)

#Make the ssh folder
mkdir -p ~/.ssh
chmod 700 ~/.ssh

#Add the private key which has been passed in from the make file, via the docker file
echo $SSH_PRIVATE_KEY | tr '#' '\n' > ~/.ssh/key.pem
chmod 400 ~/.ssh/key.pem
ssh-add ~/.ssh/key.pem

#Add the known hosts environment variable to the known_hosts file. Ssh won't work automatically without this
echo "$SSH_KNOWN_HOSTS" > ~/.ssh/known_hosts
chmod 644 ~/.ssh/known_hosts

pip install --upgrade pip

pip install pipenv
# Install all the dependencies
pipenv install --system --deploy --verbose

# Clean up the cache
rm -rf ~/.cache/pip

#Remove the ssh key so it doesn't get saved in the image
rm ~/.ssh/key.pem
