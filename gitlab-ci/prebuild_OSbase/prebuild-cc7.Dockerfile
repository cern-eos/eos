FROM centos:7

LABEL maintainer="Fabio Luchetti, faluchet@cern.ch, CERN 2020"

ARG EOS_CODENAME
WORKDIR /builds/dss/eos/

# Fix CentOS7 repository after the EOL
RUN sed -i -e '/mirrorlist/d' -e 's/# \?baseurl=/baseurl=/g' -e 's/mirror.centos.org/vault.centos.org/g' -e 's/$releasever/7.9.2009/g' /etc/yum.repos.d/*.repo

# If the working directory is a not the top-level dir of a git repo OR git remote is not set to the EOS repo url.
# On Gitlab CI, the test won't (and don't have to) pass.
RUN yum install --nogpg -y epel-release git centos-release-scl && \
    yum clean all && \
    sed -i -e '/mirrorlist/d' -e 's/# \?baseurl=/baseurl=/g' -e 's/mirror.centos.org/vault.centos.org/g' -e 's/$releasever/7.9.2009/g' /etc/yum.repos.d/*.repo && \
    if [[ $(git rev-parse --git-dir) != .git ]] || [[ $(git config --get remote.origin.url) != *gitlab.cern.ch/dss/eos.git ]]; \
      then git clone https://gitlab.cern.ch/dss/eos.git . ; \
    fi

RUN yum install --nogpg -y ccache cmake3 gcc-c++ git make rpm-build rpm-sign tar which yum-plugin-priorities \
    && git submodule update --init --recursive \
    && mkdir build \
    && cd build/ \
    && cmake3 ../ -DPACKAGEONLY=1 && make srpm \
    && cd ../ \
    && echo -e '[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/'${EOS_CODENAME}'-depend/el-7/x86_64/\ngpgcheck=0\nenabled=1\npriority=2\n' >> /etc/yum.repos.d/eos-depend.repo \
    && yum-builddep --nogpgcheck --setopt="cern*.exclude=xrootd*" -y build/SRPMS/* \
    && yum install -y moreutils \
    && yum clean all
# install moreutils just for 'ts', nice to benchmark the build time.
# cleaning yum cache should reduce image size.

ENTRYPOINT /bin/bash
