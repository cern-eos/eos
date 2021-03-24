FROM gitlab-registry.cern.ch/linuxsupport/c8-base

LABEL maintainer="Fabio Luchetti, faluchet@cern.ch, CERN 2020"

ARG EOS_CODENAME
WORKDIR /builds/dss/eos/

# If the working directory is a not the top-level dir of a git repo OR git remote is not set to the EOS repo url.
# On Gitlab CI, the test won't (and don't have to) pass.
RUN dnf install --nogpg -y git && dnf clean all \
    && if [[ $(git rev-parse --git-dir) != .git ]] || [[ $(git config --get remote.origin.url) != *gitlab.cern.ch/dss/eos.git ]]; \
        then git clone https://gitlab.cern.ch/dss/eos.git . ; fi

RUN dnf install -y epel-release \
    && dnf install --nogpg -y ccache dnf-plugins-core gcc-c++ git make python2 python3 python3-setuptools rpm-build rpm-sign tar which \
    && git submodule update --init --recursive \
    && ./misc/cmake/cmake-3.15.5-Linux-x86_64.sh --prefix=/usr/ --skip-license \
    && mkdir build \
    && cd build \
    && cmake ../ -DPACKAGEONLY=1 && make srpm \
    && cd ../ \
    && echo -e '[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/'${EOS_CODENAME}'-depend/el-8/x86_64/\ngpgcheck=0\nenabled=1\npriority=4\n' >> /etc/yum.repos.d/eos-depend.repo \
    && dnf builddep --nogpgcheck --allowerasing -y build/SRPMS/* \
    && dnf install -y moreutils \
    && dnf clean all
# install moreutils just for 'ts', nice to benchmark the build time.
# cleaning yum cache should reduce image size.

ENTRYPOINT /bin/bash
