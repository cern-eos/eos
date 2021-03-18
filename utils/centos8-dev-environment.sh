#!/bin/bash

PATH_CMAKE_DIR='/opt/eos/cmake/bin'

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &>/dev/null
  exit 1
}

## Usage: hasMinRequiredVersion currentVersion requiredVersion
hasMinRequiredVersion() {
  currentver=$1
  requiredver=$2
  if [ "$(printf '%s\n' "$requiredver" "$currentver" | sort -V | head -n1)" = "$requiredver" ]; then
    # 0 = true
    return 0
  else
    # 1 = false
    return 1
  fi
}

addEOSCmakeDirToPATHBashrc() {
  pathCMakeDir=$PATH_CMAKE_DIR
  pathCopy=$PATH
  echo $pathCopy | grep -q "$pathCMakeDir"
  if [[ $? -ne 0 ]]; then
    # The path of cmake is not in $PATH, we add it
    pathCopy="$pathCopy:$pathCMakeDir"
  fi
  #Check if there is a PATH configuration with the pathCMakeDir setup in the .bashrc file
  cat ~/.bashrc | egrep -q 'PATH=.*'"$pathCMakeDir"'.*'
  if [[ $? -ne 0 ]]; then
    # No PATH configuration, add it to persist the PATH change
    echo "Adding CMake from the eos-cmake package to the PATH in the .bashrc file"
    echo 'export PATH=$PATH:'"$pathCMakeDir" >> ~/.bashrc
  fi
  export PATH=$pathCopy
}

installEosCMake() {
  echo "Installing EOS CMake"
  sudo yum install -y eos-cmake --disablerepo=* --enablerepo=eos-citrine-dep
  sudo ln -fs $PATH_CMAKE_DIR/cmake $PATH_CMAKE_DIR/cmake3
}

installCMakeIfNecessary() {
  # We will install cmake if it does not exist or if the installed
  # version is not above the 3.14 (see CMakeLists.txt)
  hash -r
  necessaryCMakeVersion=3.14
  cmakeExists=$(command -v cmake)
  if [ ! -z $cmakeExists ]; then
    # CMake exists, check its version
    cmakeVersion=$(cmake --version | awk 'NR==1{print $3}')
    if hasMinRequiredVersion $cmakeVersion $necessaryCMakeVersion; then
      echo "CMake has the good version, no need to install another one"
    else
      sudo yum remove -y cmake
      installEosCMake || die "Unable to install CMake"
    fi
  else
    installEosCMake || die "Unable to install CMake"
  fi
}

createXrootdRepo() {
  echo "Creating the xrootd.repo and adding it to the yum repository directory"
  sudo cat >/etc/yum.repos.d/xrootd.repo <<'EOF'
[xrootd-stable]
name=XRootD Stable repository
baseurl=http://xrootd.org/binaries/stable/slc/8/$basearch http://xrootd.cern.ch/sw/repos/stable/slc/8/$basearch
gpgcheck=1
enabled=1
protect=0
gpgkey=http://xrootd.cern.ch/sw/releases/RPM-GPG-KEY.txt

EOF
}

createEosAndEosCitrineRepo() {
  echo "Creating the eos.repo and adding it to the yum repository directory"
  sudo cat >/etc/yum.repos.d/eos.repo <<'EOF'
[eos-citrine]
name=EOS 4.0 Version
baseurl=https://storage-ci.web.cern.ch/storage-ci/eos/citrine/tag/el-8/x86_64/
gpgcheck=0

[eos-citrine-dep]
name=EOS 4.0 Dependencies
baseurl=https://storage-ci.web.cern.ch/storage-ci/eos/citrine-depend/el-8/x86_64/
gpgcheck=0

EOF
}

createQuarkdbRepo() {
  echo "Creating the quarkdb.repo and adding it to the yum repository directory"
  sudo cat >/etc/yum.repos.d/quarkdb.repo <<'EOF'
[quarkdb-stable]
name=QuarkDB repository [stable]
baseurl=http://storage-ci.web.cern.ch/storage-ci/quarkdb/tag/el8/x86_64/
enabled=1
gpgcheck=False

EOF
}

echo "Installing necessary utilities"
sudo yum install --nogpg -y python3 wget ccache gcc-c++ gdb make rpm-build rpm-sign gnutls && yum clean all || die "Error while installing necessary utilities"

createXrootdRepo

createEosAndEosCitrineRepo

createQuarkdbRepo

installCMakeIfNecessary || die "Unable to install CMake"
addEOSCmakeDirToPATHBashrc

EOS_PROJECT_ROOT_DIR="$(git rev-parse --show-toplevel)"
cd $EOS_PROJECT_ROOT_DIR
mkdir -p build
cd build
cmake ../ -DPACKAGEONLY=1 -Wno-dev && make srpm || die "Unable to create the SRPMS."
cd $EOS_PROJECT_ROOT_DIR

sudo yum clean all

echo "Installing libmicrohttpd-devel"
sudo yum install -y libmicrohttpd-devel || die 'ERROR while installing libmicrohttp packages'
echo "Running yum-builddep to build the EOS dependencies"
sudo yum-builddep --nogpgcheck --allowerasing --enablerepo=eos-citrine-dep -y ./build/SRPMS/* || die 'ERROR while building the dependencies'

sudo yum install -y quarkdb quarkdb-debuginfo redis || die 'ERROR while installing quarkdb packages'
hash -r
