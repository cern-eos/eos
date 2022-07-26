ARG REPO_LOCATION=gitlab-registry.cern.ch

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
RUN mkdir deps-folly &&\
    cp -r /root/rpmbuild/RPMS/ /root/rpmbuild/SRPMS/ deps-folly

# step 2: eos-folly
RUN dnf install -y deps-folly/RPMS/$(uname -m)
RUN rm -rf /root/rpmbuild/SOURCES/* /root/rpmbuild/RPMS /root/rpmbuild/SRPMS &&\
    cp *.patch /root/rpmbuild/SOURCES
RUN rpmbuild -ba --undefine=_disable_source_fetch eos-folly.spec
RUN mkdir /folly &&\
    cp -r /root/rpmbuild/RPMS/ /root/rpmbuild/SRPMS /folly

########################################################
# Build stage 2: EOS
########################################################

FROM ${IMAGE_BUILDER}:latest AS eos-builder

ADD . /eos-src
WORKDIR /eos-src
COPY --from=folly-deps-builder /folly /eos-folly

RUN dnf install -y epel-release &&\
    dnf install --nogpg -y dnf-plugins-core gcc-c++ git cmake make python3 python3-setuptools rpm-build rpm-sign tar which
RUN dnf install -y /eos-folly/RPMS/$(uname -m)

RUN git submodule update --init --recursive
RUN mkdir build
WORKDIR build

RUN cmake ../ -DPACKAGEONLY=1 -DCLIENT=1
RUN make srpm
WORKDIR /eos

RUN dnf builddep --nogpgcheck --allowerasing -y build/SRPMS/*
RUN rpmbuild --rebuild --define "_rpmdir build/RPMS/" --define "_build_name_fmt %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm" build/SRPMS/*  #| ts disable timestamp on CentOS Stream 9, as moreutils is not available
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

COPY --from=eos-builder /eos-folly /temp/eos-folly
COPY --from=eos-builder /eos /temp/eos

RUN ls -l /temp/eos-folly/RPMS &&\
    echo "--------------------------" &&\
    ls -l /temp/eos/RPMS
#RUN dnf install -y /temp/eos-folly/RPMS/$(uname -m) /temp/eos/RPMS/$(uname -m)
