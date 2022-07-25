%define distribution %(/usr/lib/rpm/redhat/dist.sh --distnum)
%define cmake cmake3

#-------------------------------------------------------------------------------
# Custom strip command for SLC6, CC7
#-------------------------------------------------------------------------------
%if 0%{distribution} == 6 || 0%{distribution} == 7
%global __strip /opt/rh/devtoolset-8/root/usr/bin/strip
%endif

Name:           eos-folly
Summary:        Facebook Folly library, packaged as EOS dependency

Version:        2022.07.04.00

Release:        1%{dist}%{?_with_tsan:.tsan}
License:        Apache
URL:            https://github.com/facebook/folly.git
Source0:        https://github.com/facebook/folly/archive/v%{version}.tar.gz

BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: which
BuildRequires: zlib-static
BuildRequires: zlib-devel
BuildRequires: m4
BuildRequires: automake
BuildRequires: libtool
BuildRequires: openssl
BuildRequires: openssl-devel
BuildRequires: libevent
BuildRequires: libevent-devel
BuildRequires: cmake3
BuildRequires: eos-folly-deps = %{version}
Requires: eos-folly-deps = %{version}

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
Facebook Folly, used as EOS build dependency.

%global debug_package %{nil}

%package devel
Summary: eos-folly development files
Group: Development/Libraries

%description devel
This package provides the headers and static library for eos-folly.

%prep
%setup -q -c -n eos-folly -a 0
 
%build
%if 0%{distribution} == 6 || 0%{distribution} == 7
source /opt/rh/devtoolset-8/enable
%endif

%if %{?_with_tsan:1}%{!?_with_tsan:0}
export CXXFLAGS='-fsanitize=thread -g3 -fPIC'
%else
export CXXFLAGS='-g3 -fPIC'
%endif

# @note EOS-4450
%if 0%{?fedora} >= 33 || 0%{distribution} == 8
export CMAKE_VERSION="3.22.5"
export CPU_ARCH="$(uname -m)"
export CMAKE_SH_SCRIPT="cmake-${CMAKE_VERSION}-linux-${CPU_ARCH}.sh"
export TMP="/tmp/"
echo "Installing and using cmake ${CMAKE_VERSION}"
curl --location https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/${CMAKE_SH_SCRIPT} --output $TMP/${CMAKE_SH_SCRIPT}
chmod +x $TMP/${CMAKE_SH_SCRIPT}
mkdir -p $TMP/cmake_install/ && $TMP/${CMAKE_SH_SCRIPT} --prefix="$TMP/cmake_install/" --skip-license
%define cmake $TMP/cmake_install/bin/cmake
%endif

mkdir TEMPROOT
TEMP_ROOT=$PWD/TEMPROOT

#-------------------------------------------------------------------------------
# Compile folly
#-------------------------------------------------------------------------------
pushd folly-%{version}
mkdir builddir && cd builddir
%{cmake} .. -DCMAKE_PREFIX_PATH=/opt/eos-folly -DCMAKE_INSTALL_PREFIX=/opt/eos-folly
make %{?_smp_mflags}
popd

%install

%if 0%{distribution} == 6 || 0%{distribution} == 7
source /opt/rh/devtoolset-8/enable
%endif

%if %{?_with_tsan:1}%{!?_with_tsan:0}
export CXXFLAGS='-fsanitize=thread -g3 -fPIC'
%else
export CXXFLAGS='-g3 -fPIC'
%endif

#-------------------------------------------------------------------------------
# Install folly
#-------------------------------------------------------------------------------
pushd folly-%{version}/
cd builddir
make DESTDIR=%{buildroot} install %{?_smp_mflags}
popd

#-------------------------------------------------------------------------------
# Make folly shared library
#-------------------------------------------------------------------------------
%if %{?_with_tsan:1}%{!?_with_tsan:0}
export LIBTSAN='-ltsan'
%else
export LIBTSAN=''
%endif

pushd ${RPM_BUILD_ROOT}/opt/eos-folly/lib
g++ -shared ${CXXFLAGS} -o libfolly.so -Wl,--whole-archive libfolly.a /opt/eos-folly/lib/libdouble-conversion.a /opt/eos-folly/lib/libglog.so -Wl,-no-whole-archive -Wl,-rpath,/opt/eos-folly/lib -L/opt/eos-folly/lib -lboost_context -lboost_program_options -lssl -levent -lboost_filesystem -lboost_regex -lgflags ${LIBTSAN}
popd

%files
/opt/eos-folly/*

%changelog
* Fri Jul 08 2022 Kane Bruce <kane.bruce@cern.ch> - 0.2.0
- Improve spec build process with multi-arch targeting support, use cmake 3.22.5
- Roll forward folly version by 3 years to v2022.07.04.00
* Tue Oct 13 2020 Fabio Luchetti <fabio.luchetti@cern.ch> - 0.1.0
- Update to v2020.10.05.00
* Thu Apr 12 2018 Georgios Bitzes <georgios.bitzes@cern.ch> - 0.0.1
- Initial package
