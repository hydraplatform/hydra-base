# This file contains env variables specific to the project
export SPECIFIC_PROJECT_NAME="base"
export SPECIFIC_IMAGE_NAME="hydra-${SPECIFIC_PROJECT_NAME}"
export AWS_REGION_NAME="eu-west-1"
export AWS_ACCOUNT_NAME="560078195495"

export REPOSITORY_IMAGE_BASENAME="${AWS_ACCOUNT_NAME}.dkr.ecr.${AWS_REGION_NAME}.amazonaws.com/${SPECIFIC_IMAGE_NAME}"
