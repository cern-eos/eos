FROM gitlab-registry.cern.ch/linuxsupport/cc7-base

LABEL maintainer="Fabio Luchetti, faluchet@cern.ch, CERN 2020"

ARG PREBUILD_NAME
ARG CMAKE_OPTIONS
ARG CXXFLAGS

WORKDIR /builds/dss/eos/

RUN if [[ $PREBUILD_NAME == "cc7_xrd_testing" ]]; then \
      echo -e '[xrootd-testing]\nname=XRootD Testing repository\nbaseurl=http://xrootd.org/binaries/testing/slc/7/$basearch http://xrootd.cern.ch/sw/repos/testing/slc/7/$basearch\ngpgcheck=0\nenabled=1\npriority=1\nprotect=0\ngpgkey=http://xrootd.cern.ch/sw/releases/RPM-GPG-KEY.txt\nexclude=xrootd-*5.0.0*\n' >> /etc/yum.repos.d/xrootd-testing.repo; \
    fi

RUN yum install --nogpg -y gcc-c++ cmake3 make rpm-build which git yum-plugin-priorities tar ccache sl-release-scl rpm-sign \
    && source gitlab-ci/export_branch.sh \
    && echo "Exporting BRANCH=${BRANCH}" \
    && git submodule update --init --recursive \
    && mkdir build \
    && cd build/ \
    && cmake3 ../ -DPACKAGEONLY=1 ${CMAKE_OPTIONS} && make srpm \
    && cd ../ \
    && echo -e '[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/'${BRANCH}'-depend/el-7/x86_64/\ngpgcheck=0\nenabled=1\npriority=2\n' >> /etc/yum.repos.d/eos-depend.repo \
    && yum-builddep --nogpgcheck --setopt="cern*.exclude=xrootd*" -y build/SRPMS/* \
    && yum install -y moreutils \
    && yum clean all
# install moreutils just for 'ts', nice to benchmark the build time.
# cleaning yum cache should reduce image size.

ENTRYPOINT /bin/bash
