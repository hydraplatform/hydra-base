# <NOTES>
BASEDIR=$PWD
repo_folder="<REPO_FOLDER>"
if [[ ${repo_folder} =~ "REPO_FOLDER" ]]; then
  echo "You must install the script as command"
  exit 1
fi

NOW=$(date '+%d/%m/%Y %H:%M:%S')
BASEDIR=${repo_folder}
source "$BASEDIR/env.sh"
echo "Repository Images Manager v 0.5"
usage() {
  echo "Usage: $0 [-d <dev|test|live|latest>] [-c <build|push|pull|inspect>]" 1>&2;
  echo " " 1>&2;
  echo "Software Required:" 1>&2;
  echo "- Aws cli" 1>&2;
  echo "- jq" 1>&2;
  echo "- kubectl client" 1>&2;
  exit 2;
}

show_title () {
  str=$1
  current_folder_path="Contextual FOLDER: '$BASEDIR'"
  str_len=${#str}
  divider=$(printf %${str_len}s |tr " " "-")
  echo "+-${divider}-+"
  echo "| ${str} |"
  echo "+-${divider}-+"
}

cd $BASEDIR
show_title "Contextual FOLDER: '$BASEDIR'"


# Checking that the current version is younger than the template
TEMPLATE_UPDATE_TIME=$(stat -c %y ./templates/repo-images-manager.sh)
CURRENT_UPDATE_TIME=$(stat -c %y ./current/repo-images-manager.sh)
if [[ $TEMPLATE_UPDATE_TIME > $CURRENT_UPDATE_TIME ]]; then
  echo "The template is newer than the current version."
  echo "run the following command before continuing:"
  echo "cd ${BASEDIR}; ./command-installer.sh"
  exit 1
fi

# Getting parameters
suppress_usage=0
while getopts ":d:c:s" o; do
  case "${o}" in
    d)
      dist_name=`echo ${OPTARG} | tr '[:upper:]' '[:lower:]'`
      ;;
    c)
      command_name=`echo ${OPTARG} | tr '[:upper:]' '[:lower:]'`
      ;;
    s)
      suppress_usage=1
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND-1))


if [ \( -z "${dist_name}" \) -o \
     \( "${dist_name}" != "dev" -a "${dist_name}" != "test" -a "${dist_name}" != "live" -a "${dist_name}" != "latest" \) -o \
     \( -z "${command_name}" \) -o \
     \( "${command_name}" != "build" -a "${command_name}" != "push" -a "${command_name}" != "pull" -a "${command_name}" != "inspect" \) ]; then
  if [ $suppress_usage -eq 0 ];
  then
    usage
  else
    exit 2
  fi
fi

CURRENT_REPOSITORY_IMAGE_NAME="${REPOSITORY_IMAGE_BASENAME}:${dist_name}"

if [[ "${command_name}" == "push" ]]; then
  # This pushes the current image to ECR
  ${SPECIFIC_IMAGE_NAME}-login-to-remote-repository
  docker tag ${SPECIFIC_IMAGE_NAME}:${dist_name} $CURRENT_REPOSITORY_IMAGE_NAME
  docker push $CURRENT_REPOSITORY_IMAGE_NAME
elif [[ "${command_name}" == "pull" ]]; then
  # This pulls the current image from ECR
  ${SPECIFIC_IMAGE_NAME}-login-to-remote-repository
  docker pull $CURRENT_REPOSITORY_IMAGE_NAME
elif [[ "${command_name}" == "build" ]]; then
  # This builds the image with the provided dist name
  TAG=$(git rev-parse --short=8 HEAD)
  FINAL_IMG=${SPECIFIC_IMAGE_NAME}:${TAG}
  FINAL_DIST_NAME=${SPECIFIC_IMAGE_NAME}:${dist_name}

  # Let's test if the image with the same TAG already exists. If yes. the rebuild is not necessary
  existing_tag_images=$(docker images -aq ${FINAL_IMG}*)
  if [[ ${existing_tag_images[@]} ]]; then
    # The source is not changed. cancel the build
    echo "The git source has not changed. No Need to rebuild anything! Exit!"; \
    exit 0
  fi

  # Updating Pipfile.lock
  cd $BASEDIR
  pipenv lock
  git commit -m "New Packages Lock at ${NOW}" Pipfile.lock
  git push
  # pipenv update

  # SSH_PRIVATE_KEY and SSH_KNOWN_HOSTS should be environment variables on your machine.
  # See the relative section in the wiki on adding the SSH access details
  if [ "${SSH_PRIVATE_KEY}" = "" ]; then \
		echo "Environment variable SSH_PRIVATE_KEY not set"; \
		exit 1
  elif [ "${SSH_KNOWN_HOSTS}" = "" ]; then \
		echo "Environment variable SSH_PRIVATE_KEY not set"; \
		exit 1
	fi

	echo "Building Dist ${dist_name} of ${SPECIFIC_IMAGE_NAME}"

	# Cleaning the old image with the current dist name
  existing_dist_images=$(docker images -aq ${FINAL_DIST_NAME}*)
  if [[ ${existing_dist_images[@]} ]]; then
    # Deletes eventually ONLY the previous image associated with the provided dist name
    echo $CURRENT_REPOSITORY_IMAGE_NAME
    docker image rm ${CURRENT_REPOSITORY_IMAGE_NAME}
    docker image rm ${FINAL_DIST_NAME}
    docker image rm ${existing_dist_images[@]}
  fi

  # Replaces the <DIST-NAME> with the current working one
  sed "s|<DIST-NAME>|${dist_name}|" ${BASEDIR}/templates/Dockerfile > ${BASEDIR}/current/Dockerfile

  # Effectively builds the image
	docker build --build-arg SSH_PRIVATE_KEY --build-arg SSH_KNOWN_HOSTS . -f ${BASEDIR}/current/Dockerfile -t ${FINAL_IMG}


  # Tags the built image with the dist name
	docker tag ${FINAL_IMG} ${FINAL_DIST_NAME}

elif [[ "${command_name}" == "inspect" ]]; then
  # Inspecting an image without running default entry point
  # generate a random ID in case we have multiple running
  ID=$(env LC_CTYPE=C tr -dc "a-z0-9" < /dev/urandom | head -c 10)

  IMAGE_DIST_NAME=${SPECIFIC_IMAGE_NAME}:${dist_name}

  # sanitise the name a little
  NAME=$(echo ${IMAGE_DIST_NAME} | tr '/:' '-')

  docker run -it \
    --rm \
    --name $NAME-inspecting-$ID  \
    -v $HOME/.ssh:/root/.ssh  \
    -v ${APPS_DIR}:${APPS_DIR} \
    -e APPS_DIR=${APPS_DIR} \
    -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
    -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    --entrypoint=/bin/bash ${IMAGE_DIST_NAME} \
    -i

fi
