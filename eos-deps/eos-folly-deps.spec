%define distribution %(/usr/lib/rpm/redhat/dist.sh --distnum)
%define scons_package scons
%define scons scons

%if 0%{distribution} >= 8
%define scons_package python3-scons
%define scons scons-3
%endif

#-------------------------------------------------------------------------------
# Custom strip command for SLC6, CC7
#-------------------------------------------------------------------------------
%if 0%{distribution} == 6 || 0%{distribution} == 7
%global __strip /opt/rh/devtoolset-8/root/usr/bin/strip
%endif

%define boost_ver 1_79_0
%define boost_ver_dots 1.79.0
%define fmt_ver_dots 9.0.0

Name:           eos-folly-deps
Summary:        Boost library, packaged as EOS dependency

Version:        2022.07.04.00

Release:        1%{dist}%{?_with_tsan:.tsan}
License:        Apache
URL:            https://github.com/boostorg/boost
Source0:        https://boostorg.jfrog.io/artifactory/main/release/%{boost_ver_dots}/source/boost_%{boost_ver}.tar.gz
Source1:        https://github.com/google/glog/archive/v0.6.0.tar.gz
Source2:        https://github.com/gflags/gflags/archive/v2.2.2.tar.gz
Source3:        https://github.com/google/double-conversion/archive/v1.1.6.tar.gz
Source4:        SConstruct.double-conversion
Source5:	https://github.com/fmtlib/fmt/archive/%{fmt_ver_dots}.tar.gz

# don't leak buildroot paths to boost cmake files: https://lists.boost.org/Archives/boost/2020/04/248812.php
Patch0:         boost-remove-cmakedir.patch
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: which
BuildRequires: zlib-static
BuildRequires: zlib-devel
BuildRequires: m4
BuildRequires: automake
BuildRequires: libtool
BuildRequires: %{scons_package}
BuildRequires: openssl
BuildRequires: openssl-devel
BuildRequires: libevent
BuildRequires: libevent-devel
BuildRequires: cmake
BuildRequires: cmake3

%if 0%{distribution} == 6 || 0%{distribution} == 7
BuildRequires: devtoolset-8
%else
BuildRequires: perl-Data-Dumper
%endif
%if %{?_with_tsan:1}%{!?_with_tsan:0}
BuildRequires: libtsan
Requires: libtsan
%endif

%description
Boost used as EOS build dependency.

%global debug_package %{nil}

%package devel
Summary: eos-folly-deps development files
Group: Development/Libraries

%description devel
This package provides headers and libraries for eos-folly-deps.

%prep
%setup -q -c -n eos-folly-deps -a 0 -a 1 -a 2 -a 3 -a 5
%patch0 -p1

%build
%if 0%{distribution} == 6 || 0%{distribution} == 7
source /opt/rh/devtoolset-8/enable
%endif

%if %{?_with_tsan:1}%{!?_with_tsan:0}
export CXXFLAGS='-fsanitize=thread -g3 -fPIC'
%else
export CXXFLAGS='-g3 -fPIC'
%endif

mkdir TEMPROOT
TEMP_ROOT=$PWD/TEMPROOT

#-------------------------------------------------------------------------------
# Compile boost
#-------------------------------------------------------------------------------
pushd boost_%{boost_ver}
./bootstrap.sh --prefix=%{buildroot}/opt/eos-folly --with-libraries=context,thread,program_options,regex,system,chrono,filesystem,date_time
./b2 %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Compile gflags
#-------------------------------------------------------------------------------
pushd gflags-2.2.2
mkdir build && cd build
CXXFLAGS="-fPIC"
%if 0%{?fedora} >= 35
CXXFLAGS="-fPIC -Wno-deprecated-declarations"
%endif
cmake3 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=/opt/eos-folly -DCMAKE_INSTALL_LIBDIR=lib .. 
make %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Compile glog
#-------------------------------------------------------------------------------
pushd glog-0.6.0
mkdir build && cd build
CXXFLAGS="-fPIC" cmake3 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=/opt/eos-folly -DCMAKE_INSTALL_LIBDIR=lib ..
make %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Compile double-conversion
#-------------------------------------------------------------------------------
pushd double-conversion-1.1.6
%{scons} -f %{SOURCE4}

%{__install} -D -m 755 ./libdouble_conversion.a ${TEMP_ROOT}/lib/libdouble-conversion.a
%{__install} -D -m 755 ./libdouble_conversion_pic.a ${TEMP_ROOT}/lib/libdouble-conversion_pic.a

%{__install} -D -m 755 ./src/double-conversion.h ${TEMP_ROOT}/include/double-conversion/double-conversion.h
%{__install} -D -m 755 ./src/bignum.h  ${TEMP_ROOT}/include/double-conversion/bignum.h
%{__install} -D -m 755 ./src/bignum-dtoa.h ${TEMP_ROOT}/include/double-conversion/bignum-dtoa.h
%{__install} -D -m 755 ./src/cached-powers.h ${TEMP_ROOT}/include/double-conversion/cached-powers.h
%{__install} -D -m 755 ./src/diy-fp.h ${TEMP_ROOT}/include/double-conversion/diy-fp.h
%{__install} -D -m 755 ./src/fast-dtoa.h ${TEMP_ROOT}/include/double-conversion/fast-dtoa.h
%{__install} -D -m 755 ./src/fixed-dtoa.h ${TEMP_ROOT}/include/double-conversion/fixed-dtoa.h
%{__install} -D -m 755 ./src/ieee.h ${TEMP_ROOT}/include/double-conversion/ieee.h
%{__install} -D -m 755 ./src/strtod.h ${TEMP_ROOT}/include/double-conversion/strtod.h
%{__install} -D -m 755 ./src/utils.h ${TEMP_ROOT}/include/double-conversion/utils.h

popd

#-------------------------------------------------------------------------------
# Compile fmtlib/fmt (static)
#-------------------------------------------------------------------------------
pushd fmt-%{fmt_ver_dots}
mkdir build && cd build
CXXFLAGS="-fPIC" cmake3 -DBUILD_SHARED_LIBS=TRUE -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=/opt/eos-folly -DCMAKE_INSTALL_LIBDIR=lib ..
make %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------

%install

%if 0%{distribution} == 6 || 0%{distribution} == 7
source /opt/rh/devtoolset-8/enable
%endif

%if %{?_with_tsan:1}%{!?_with_tsan:0}
export CXXFLAGS='-fsanitize=thread -g3'
%else
export CXXFLAGS='-g3'
%endif

#-------------------------------------------------------------------------------
# Install boost
#-------------------------------------------------------------------------------
pushd boost_%{boost_ver}
./b2 --prefix=%{buildroot}/opt/eos-folly install %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Install gflags
#-------------------------------------------------------------------------------
pushd gflags-2.2.2
cd build
make DESTDIR=%{buildroot} install %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Install glog
#-------------------------------------------------------------------------------
pushd glog-0.6.0
cd build
make DESTDIR=%{buildroot} install %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Install fmtlib/fmt
#-------------------------------------------------------------------------------
pushd fmt-%{fmt_ver_dots}
cd build
make DESTDIR=%{buildroot} install %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Clean up /root/.cmake
#-------------------------------------------------------------------------------
rm -rf %{buildroot}/root/

#-------------------------------------------------------------------------------
# Install double-conversion
#-------------------------------------------------------------------------------
pushd double-conversion-1.1.6
%{__install} -D -m 755 ./libdouble_conversion_pic.a ${RPM_BUILD_ROOT}/opt/eos-folly/lib/libdouble-conversion.a

%{__install} -D -m 755 ./src/double-conversion.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/double-conversion.h
%{__install} -D -m 755 ./src/bignum.h  ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/bignum.h
%{__install} -D -m 755 ./src/bignum-dtoa.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/bignum-dtoa.h
%{__install} -D -m 755 ./src/cached-powers.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/cached-powers.h
%{__install} -D -m 755 ./src/diy-fp.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/diy-fp.h
%{__install} -D -m 755 ./src/fast-dtoa.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/fast-dtoa.h
%{__install} -D -m 755 ./src/fixed-dtoa.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/fixed-dtoa.h
%{__install} -D -m 755 ./src/ieee.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/ieee.h
%{__install} -D -m 755 ./src/strtod.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/strtod.h
%{__install} -D -m 755 ./src/utils.h ${RPM_BUILD_ROOT}/opt/eos-folly/include/double-conversion/utils.h
popd

%files
/opt/eos-folly/*

%changelog
* Fri Jul 08 2022 Kane Bruce <kane.bruce@cern.ch> - 0.1.0
- Advance boost to 1.79.0, accommodate Folly ver push to 2022.07.04.00
- [Abhishek] Add boost 1.79 patch to fix buildpath bug
* Wed Jan 15 2020 Mihai Patrascoiu <mihai.patrascoiu@cern.ch> - 0.0.2
- Accommodate CentOS 8 build
* Wed Nov 27 2019 Georgios Bitzes <georgios.bitzes@cern.ch> - 0.0.1
- Initial package
