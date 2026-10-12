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
  docker exec -i $1 /bin/bash -lic "${@:2}"
  set +o xtrace
}

# Execute command in Kubernetes setup where the first argument is the name of the Pod
# and the rest is the command to be executed
function exec_cmd_k8s() {
  set -o xtrace
  kubectl exec --namespace=${K8S_NAMESPACE} $(get_podname ${1}) -- /bin/bash -lc "${@:2}"
  set +o xtrace
}

function cp_to_local_cmd() {
  if [[ $IS_DOCKER == true ]]; then
    cp_to_local_cmd_docker "$@"
  else
    cp_to_local_cmd_k8s "$@"
  fi
}

function cp_to_local_cmd_docker() {
  docker cp $1 $2
}

function cp_to_local_cmd_k8s() {
  local substr=":"
  kubectl cp "${K8S_NAMESPACE}/$(get_podname ${1%$substr*})${substr}${1#*$substr}" ${2}
}

function get_podname () {
  local pods
  pods=$(kubectl get pods --namespace="$K8S_NAMESPACE" -l "app=$1" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{.metadata.deletionTimestamp}{" "}{.status.phase}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}') || return $?
  # A deletion timestamp shifts the remaining columns and excludes terminating pods.
  awk '$2 == "Running" && $3 == "True" {print $1}' <<< "$pods"
}

# Use concrete pod identities and preserve each command argument. In particular,
# a temporarily unready pod must still be reachable during daemon recovery.
function k8s_cmd() {
  timeout -s KILL 30 kubectl --namespace "$K8S_NAMESPACE" "$@"
}

function exec_cmd_argv() {
  local container=$1
  shift
  if [[ $IS_DOCKER == true ]]; then
    docker exec -i "$container" "$@"
  else
    k8s_cmd exec "$container" -- "$@"
  fi
}

function monitor_value() {
  awk -v key="$2" '
    { for (i=1; i<=NF; i++) if (index($i,key "=")==1) {
        value=substr($i,length(key)+2); count++
      } }
    END { if (count!=1 || value=="") exit 1; print value }' <<< "$1"
}

# The caller owns failure reporting. The optional callback can report progress
# after a failed predicate without putting test-specific state in this helper.
function poll_until() {
  [[ $# -ge 4 ]] || return 2
  local deadline=$1 interval=$2 pending=$3 remaining
  shift 3
  [[ $deadline =~ ^[0-9]+$ && $interval =~ ^[1-9][0-9]*$ && $# -gt 0 ]] || return 2
  while (( SECONDS < deadline )); do
    if "$@"; then return 0; fi
    if [[ -n $pending ]]; then "$pending" || return $?; fi
    remaining=$((deadline - SECONDS))
    (( remaining > 0 )) || break
    if (( remaining < interval )); then
      sleep "$remaining"
    else
      sleep "$interval"
    fi
  done
  return 1
}

function cleanup_k8s_fixture() {
  local namespace=${1:-} log_directory=${2:-} status cleanup_status=0
  [[ $# == 2 && -n $log_directory && $IS_DOCKER == false && -n ${KUBECONFIG:-} && $namespace != default &&
      ${#namespace} -le 63 && $namespace =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
    echo 'error: refusing empty, default or invalid Kubernetes cleanup namespace' >&2
    return 2
  }
  if timeout -s KILL 120 bash ./eos-on-k8s/collect_logs.sh "$namespace" "$log_directory"; then
    :
  else
    status=$?
    printf 'error: Kubernetes log collection failed or timed out (rc=%s)\n' "$status" >&2
    cleanup_status=1
  fi
  # Always delete, including when collecting logs failed or timed out. The
  # fixture scripts handle a namespace that a previous finalizer already deleted.
  if timeout -s KILL 180 bash -e ./eos-on-k8s/delete-all.sh "$namespace"; then
    :
  else
    status=$?
    printf 'error: Kubernetes namespace deletion failed or timed out (rc=%s)\n' "$status" >&2
    cleanup_status=1
  fi
  return "$cleanup_status"
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
  usage && exit 1
fi
