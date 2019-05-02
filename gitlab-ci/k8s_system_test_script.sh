#!/bin/bash -ve


NAMESPACE=""
if [[ $1 =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; 
	then NAMESPACE=$1
	else echo "! Wrong arg $1: arg1 must be a DNS-1123 label and must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character"; exit 1
fi

# get_podname() : Return the name of the Pods tagged with $1. Suppose it return just one result.
#            $1 : is a label selector with key="app", specifying identifying attributes for a Kubernetes object.
# Example of admitted labels are {eos-mgm, eos-mq, eos-fst1, eos-fst2 ... }, mirroring mirrors eos-roles
# refs :https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
function get_podname () { 
	kubectl get pods --namespace=${NAMESPACE} --no-headers -o custom-columns=":metadata.name" -l app=$1
}


kubectl exec --namespace=${NAMESPACE} $(get_podname eos-mgm) \
	-- eos chmod 2777 /eos/dockertest/
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-mgm) \
	-- eos vid enable krb5
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-mgm) \
	-- sed -i 's/eos-mq-test.eoscluster.cern.ch/eos-mq/g' /usr/sbin/eos-instance-test-ci # @todo tmp, then re-code the files
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-mgm) \
	-- sed -i "s/eos-fst4-test.eoscluster.cern.ch/eos-fst4.eos-fst4.${NAMESPACE}.svc.cluster.local/g" /usr/sbin/eos-drain-test # @todo tmp, then re-code the files
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-mgm) \
	-- eos-instance-test-ci

kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- git clone https://gitlab.cern.ch/dss/eosclient-tests.git
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- /bin/bash -c 'atd; at now <<< "mkdir /eos1/; mount -t fuse eosxd /eos1/; mkdir /eos2/; mount -t fuse eosxd /eos2/;"'
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- /bin/bash -c 'count=0; while [[ $count -le 10 ]] && ( [[ ! -d /eos1/dockertest/ ]] || [[ ! -d /eos2/dockertest/ ]] ); do echo "Wait for mount... $count"; (( count++ )); sleep 1; done;'
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- su - eos-user -c 'mkdir /eos1/dockertest/fusex_tests/; cd /eos1/dockertest/fusex_tests/; /usr/sbin/fusex-benchmark' # workaround for docker exec '-u' flag

# @todo(esindril): run "all" tests in schedule mode once these are properly supported
# if [ "$CI_PIPELINE_SOURCE" == "schedule" ];
# then
# 	kubectl exec --namespace=${NAMESPACE} $(get_podname eos-mgm) \
# 	-- eos vid add gateway "eos-cli1.eos-cli1.${NAMESPACE}.svc.cluster.local" unix;
# 	kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
# 	-- env EOS_FUSE_NO_ROOT_SQUASH=1 python /eosclient-tests/run.py --workdir="/eos1/dockertest /eos2/dockertest" ci;
# fi
# until then just run the "ci" tests
kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- su - eos-user -c 'python /eosclient-tests/run.py --workdir="/eos1/dockertest /eos2/dockertest" ci'

if [ "$CI_JOB_NAME" != k8s_ubuntu_system_test ]; then
	kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- /bin/bash -c 'eos fuse mount /eos_fuse; eos fuse mount /eos_fuse2;';
	kubectl exec --namespace=${NAMESPACE} $(get_podname eos-cli1) \
	-- python /eosclient-tests/run.py --workdir="/eos_fuse/dockertest /eos_fuse2/dockertest" ci;
fi
