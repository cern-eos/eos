FROM --platform=linux/arm64 gitlab-registry.cern.ch/linuxsupport/cs8-base

LABEL maintainer="Abhishek Lekshmanan, abhishek.l@cern.ch"

ARG EOS_CODENAME
WORKDIR /builds/dss/eos/

# If the working directory is a not the top-level dir of a git repo OR git remote is not set to the EOS repo url.
# On Gitlab CI, the test won't (and don't have to) pass.
RUN dnf install --nogpg -y git && dnf clean all \
    && if [[ $(git rev-parse --git-dir) != .git ]] || [[ $(git config --get remote.origin.url) != *gitlab.cern.ch/dss/eos.git ]]; \
        then git clone https://gitlab.cern.ch/dss/eos.git . ; fi

RUN dnf install -y epel-release \
    && dnf install --nogpg -y ccache dnf-plugins-core gcc-c++ git make python2 python3 python3-setuptools rpm-build rpm-sign tar which cmake \
    && git submodule update --init --recursive \
    && mkdir build \
    && cd build \
    && cmake ../ -DPACKAGEONLY=1 -DCLIENT=1 -Wno-dev && make srpm \
    && cd ../

RUN echo -e '[eos-depend]\nname=EOS dependencies\nbaseurl=https://linuxsoft.cern.ch/internal/repos/eos8s-testing/aarch64/os/\ngpgcheck=0\nenabled=1\npriority=4\n' >> /etc/yum.repos.d/eos-depend.repo


# Currently using --skip-unavailable as the one dependency eos-folly is not
# built from koji, also this is not necessary for client builds, but dnf/yum
# builddep has no understanding of rpm conditionals, so other than extracting
# and manually filtering, there seems to be no easy way to teach builddep to
# partially install the dependencies.. since these are build dependencies we'll
# fail to build; so it is easily caught if something goes wrong, so this is safe
RUN dnf builddep --nogpgcheck --allowerasing --skip-unavailable -y build/SRPMS/* \
   && dnf install -y moreutils \
   && dnf clean all
#install moreutils just for 'ts', nice to benchmark the build time.
#cleaning yum cache should reduce image size.

ENTRYPOINT /bin/bash
