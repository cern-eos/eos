#!/bin/bash -ve

function usage() {
  echo "usage: $(basename $0) --type 'docker'|'k8s <k8s_namespace>'"
  echo "        docker : script runs in a Docker based setup"
  echo "        k8s    : script runs in a Kubernetes setup and requires a namespace argument"
  echo "<k8s_namespace>: must be a DNS-1123 label and must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character"
  echo "                 in practice, it is parsed like [[ <k8s_namespace> =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]"
}

# Forward the given command to the proper executor Docker or Kubernetes. Gets
# as argument a container name and a shell command to be executed
function exec_cmd() {
  if [[ $IS_DOCKER == true ]]; then
    exec_cmd_docker "$@"
  else
    exec_cmd_k8s "$@"
  fi
}

# Execute command in Docker setup where the first argument is the name of the container
# and the rest is the command to be executed
function exec_cmd_docker() {
  set -o xtrace
  docker exec -i $1 /bin/bash -c -l "${@:2}"
  set +o xtrace
}

# Execute command in Kubernetes setup where the first argument is the name of the Pod
# and the rest is the command to be executed
function exec_cmd_k8s() {
  set -o xtrace
  kubectl exec --namespace=$K8S_NAMESPACE $(get_podname $1) -- /bin/bash -c -l "${@:2}"
  set +o xtrace
}

function get_podname () {
    local app=$1
    kubectl get pods --namespace=$K8S_NAMESPACE -l app=$app | grep -E '([0-9]+)/\1' | awk '{print $1}' # Get only READY pods
}


# Set up global variables
IS_DOCKER=""
K8S_NAMESPACE=""


if [[ "$1" != "--type" ]]; then
  echo "error: unknown argument \"$1\""
  usage && exit 1
fi

if [[ "$2" == "docker" ]]; then
  IS_DOCKER=true

elif [[ "$2" == "k8s" ]]; then

  IS_DOCKER=false
  # For the Kubernetes setup we also need a namespace argument
  if [[ -z $3 ]]; then
    echo "error: missing Kubernetes namespace argument"
    usage && exit 1
  else
    if [[ $3 =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
	  K8S_NAMESPACE=$3
    else
      usage && exit 1
    fi
  fi

else
  echo "error: unknown type of executor \"$2\""
  usage
  exit 1
fi

