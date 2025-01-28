#!/bin/bash

EOS_PROJECT_ROOT_DIR="$(git rev-parse --show-toplevel)"


die() {
    echo "$@" 1>&2
    test -z $TAILPID || kill ${TAILPID} &>/dev/null
    exit 1
}


setuprepos()
{

    local EOS_CODENAME="${1:-diopside}"
    dnf install -y dnf-plugins-core &&
        echo -e "[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/${EOS_CODENAME}-depend/el-9/$(uname -m)/\ngpgcheck=0\nenabled=1\npriority=4" > /etc/yum.repos.d/eos-depend.repo

    dnf install -y ccache cmake gcc-c++ make rpm-build rpm-sign which moreutils ninja-build
}

installdeps()
{
    cd $EOS_PROJECT_ROOT_DIR
    mkdir -p build
    cd build
    cmake ../ -DPACKAGEONLY=1 -Wno-dev && make srpm || die "Unable to create the SRPMS."
    dnf builddep -y SRPMS/*
}

setuprepos
installdeps
