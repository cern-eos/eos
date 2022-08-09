ARG REPO_LOCATION=gitlab-registry.cern.ch

ARG CODENAME=diopside
ARG IMAGE_BUILDER=${REPO_LOCATION}/linuxsupport/cs9-base
ARG IMAGE_RUNNER=${REPO_LOCATION}/linuxsupport/cs9-base

########################################################
# Build stage 1: EOS dependencies (eos-folly)
########################################################

FROM ${IMAGE_BUILDER}:latest AS folly-deps-builder

ADD eos-deps /eos-deps-ci
WORKDIR /eos-deps-ci

# step 1: dependencies for eos-folly
RUN dnf install -y epel-release rpmdevtools python3-devel python3-setuptools sudo tar yum-utils
RUN dnf update libarchive
RUN dnf builddep -y eos-folly-deps.spec
RUN mkdir -p /root/rpmbuild/SOURCES &&\
    cp *.patch SConstruct.double-conversion /root/rpmbuild/SOURCES
RUN rpmbuild -ba --undefine=_disable_source_fetch eos-folly-deps.spec
RUN mkdir /folly-deps &&\
    cp -r /root/rpmbuild/RPMS/ /root/rpmbuild/SRPMS /folly-deps &&\
    dnf install -y /folly-deps/RPMS/$(uname -m)/*

# step 2: eos-folly
RUN rm -rf /root/rpmbuild/SOURCES/* /root/rpmbuild/RPMS /root/rpmbuild/SRPMS &&\
    cp *.patch /root/rpmbuild/SOURCES
RUN rpmbuild -ba --undefine=_disable_source_fetch eos-folly.spec
RUN mkdir /folly &&\
    cp -r /root/rpmbuild/RPMS/ /root/rpmbuild/SRPMS /folly

########################################################
# Build stage 2: EOS
########################################################

FROM ${IMAGE_BUILDER}:latest AS eos-builder

COPY --from=folly-deps-builder /folly-deps /eos-folly-deps
COPY --from=folly-deps-builder /folly /eos-folly

RUN dnf install -y epel-release &&\
    dnf install --nogpg -y dnf-plugins-core gcc-c++ git cmake make python3 python3-setuptools rpm-build rpm-sign tar which
RUN dnf install -y /eos-folly-deps/RPMS/$(uname -m)/* /eos-folly/RPMS/$(uname -m)/*
RUN rm -rf /eos-folly /eos-folly-deps

ADD . /eos-src
WORKDIR /eos-src
RUN rm -rf ./eos-deps

RUN git submodule update --init --recursive
RUN mkdir build
WORKDIR build

RUN cmake ../ -Wno-dev -DPACKAGEONLY=1
RUN make srpm VERBOSE=1
WORKDIR /eos-src

RUN echo -e "[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/${CODENAME}-depend/el-9s/$(uname -m)/\ngpgcheck=0\nenabled=1\npriority=4\n" >> /etc/yum.repos.d/eos-depend.repo
RUN dnf builddep --nogpgcheck --allowerasing -y build/SRPMS/*
RUN rpmbuild --rebuild --define "_rpmdir build/RPMS/" --define "_build_name_fmt %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm" build/SRPMS/*
#| ts disable timestamp on CentOS Stream 9, as moreutils is not available
RUN mkdir /eos &&\
    cp -r build/RPMS/ build/SRPMS/ /eos

########################################################
# Build stage 3: assemble EOS runner
########################################################

FROM ${IMAGE_RUNNER}:latest AS eos-runner

# For the following sections, refer to https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.authors="https://eos-community.web.cern.ch/"
LABEL org.opencontainers.image.url='https://eos-web.web.cern.ch/eos-web/'
LABEL org.opencontainers.image.documentation='http://eos-docs.web.cern.ch/eos-docs/'
LABEL org.opencontainers.image.source='https://github.com/cern-eos/eos'
LABEL org.opencontainers.image.vendor='European Centre for Nuclear Research (CERN)'
# For the license format, refer to the SPDX format: https://spdx.org/licenses/
LABEL org.opencontainers.image.licenses='GPL-3.0-only'

COPY --from=folly-deps-builder /folly-deps /_temp/eos-folly-deps
COPY --from=folly-deps-builder /folly /_temp/eos-folly
COPY --from=eos-builder /eos /_temp/eos

RUN ls -l /_temp/eos-folly-deps/RPMS &&\
    echo "--------------------------" &&\
    ls -l /_temp/eos-folly/RPMS &&\
    echo "--------------------------" &&\
    ls -l /_temp/eos/RPMS
#RUN echo -e "[eos-depend]\nname=EOS dependencies\nbaseurl=http://storage-ci.web.cern.ch/storage-ci/eos/${CODENAME}-depend/el-9s/$(uname -m)/\ngpgcheck=0\nenabled=1\npriority=4\n" >> /etc/yum.repos.d/eos-depend.repo
#RUN dnf install -y /_temp/eos-folly-deps/RPMS/$(uname -m)/* /_temp/eos-folly/RPMS/$(uname -m)/* /_temp/eos/RPMS/$(uname -m)/*
#RUN rm -rf /_temp
