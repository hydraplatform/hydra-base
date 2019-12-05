# Deployment/Docker folder

This folder contains the scripts and configuration files to:
- build images
- login to AWS
- push them to AWS ECR
- pull them from AWS ECR
- inspect an image without running the default entrypoint

## Install the command to build an image
This command is necessary at the first install and at any "templates/aws-images-manager.sh" change
```bash
./command-installer.sh
```

## Build an image
```bash
hydra-base-repo-images-manager -d dev -c build
```

## Push an image to AWS#
```bash
hydra-base-repo-images-manager -d dev -c push
```

## Pull an image from AWS
```bash
hydra-base-repo-images-manager -d dev -c pull
```

## Inspect an image
```bash
hydra-base-repo-images-manager -d dev -c inspect
```
