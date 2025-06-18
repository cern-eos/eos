#
# Prebuild EL10 docker image for EOS
#

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base
LABEL maintainer="Elvin Sindrilaru, esindril[@].cern.ch, CERN 2025"

ARG EOS_CODENAME

WORKDIR /builds/dss/eos/

# If the working directory is a not the top-level dir of a git repo OR git
# remote is not set to the EOS repo url. On Gitlab CI, the test won't
# (and don't have to) pass.
RUN dnf install --nogpg -y git && dnf clean all \
    && if [[ $(git rev-parse --git-dir) != .git ]] || [[ $(git config --get remote.origin.url) != *gitlab.cern.ch/dss/eos.git ]]; \
        then git clone https://gitlab.cern.ch/dss/eos.git . ; fi

RUN dnf install -y dnf-plugins-core epel-release \
    && dnf config-manager --set-enabled crb      \
    && echo -e "[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/${EOS_CODENAME}-depend/el-10/$(uname -m)/\ngpgcheck=0\nenabled=1\npriority=4" > /etc/yum.repos.d/eos-depend.repo

RUN dnf install --nogpg -y ccache cmake gcc-c++ git make rpm-build rpm-sign which moreutils \
    && git submodule update --init --recursive \
    && mkdir build && cd build     \
    && cmake .. -DPACKAGEONLY=1 && make srpm \
    && cd .. \
    && dnf builddep --nogpgcheck -y build/SRPMS/* \
    && dnf clean all

ENTRYPOINT /bin/bash

