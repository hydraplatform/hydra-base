#BASEDIR=$(dirname "$0")
BASEDIR=$PWD
repo_folder="<REPO_FOLDER>"
if [[ ${repo_folder} =~ "REPO_FOLDER" ]]; then
  echo "You must install the script as command"
  exit 1
fi

BASEDIR=${repo_folder}
source "$BASEDIR/env.sh"

$(aws ecr get-login --no-include-email --region ${AWS_REGION_NAME})
