FROM gitlab-registry.cern.ch/linuxsupport/slc6-base

LABEL maintainer="Fabio Luchetti, faluchet@cern.ch, CERN 2020"

WORKDIR /builds/dss/eos/

# If the working directory is a not the top-level dir of a git repo OR git remote is not set to the EOS repo url.
# On Gitlab CI, the test won't (and don't have to) pass.
RUN yum install --nogpg -y git && yum clean all \
    && if [[ $(git rev-parse --git-dir) != .git ]] || [[ $(git config --get remote.origin.url) != *gitlab.cern.ch/dss/eos.git ]]; \
        then git clone https://gitlab.cern.ch/dss/eos.git . ; fi

RUN yum install --nogpg -y ccache gcc-c++ git make rpm-build sl-release-scl tar which yum-plugin-priorities \
	&& source gitlab-ci/export_branch.sh \
    && echo "Exporting BRANCH=${BRANCH}" \
    && git submodule update --init --recursive \
    && ./misc/cmake/cmake-3.15.5-Linux-x86_64.sh --prefix=/usr/ --skip-license \
    && mkdir build \
    && cd build/ \
    && cmake ../ -DPACKAGEONLY=1 && make srpm \
    && cd ../ \
    && echo -e '[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/'${BRANCH}'-depend/el-6/x86_64/\ngpgcheck=0\nenabled=1\npriority=1\n' >> /etc/yum.repos.d/eos-depend.repo \
    && yum-builddep --nogpgcheck --setopt="cern*.exclude=xrootd*" -y build/SRPMS/* \
    && yum install -y moreutils \
    && yum clean all
# install moreutils just for 'ts', nice to benchmark the build time.
# cleaning yum cache should reduce image size.

ENTRYPOINT /bin/bash
