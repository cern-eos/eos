#!/bin/bash

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

createXrootdRepo() {
echo "Creating the xrootd.repo and adding it to the yum repository directory"
sudo cat > /etc/yum.repos.d/xrootd.repo <<'EOF'
[xrootd-stable]
name=XRootD Stable repository
baseurl=http://xrootd.org/binaries/stable/slc/7/$basearch http://xrootd.cern.ch/sw/repos/stable/slc/7/$basearch
gpgcheck=1
enabled=1
protect=0
gpgkey=http://xrootd.cern.ch/sw/releases/RPM-GPG-KEY.txt

EOF
}

createEosAndEosCitrinRepo() {
echo "Creating the eos.repo and adding it to the yum repository directory"
sudo cat > /etc/yum.repos.d/eos.repo <<'EOF'
[eos-citrine]
name=EOS 4.0 Version
baseurl=https://storage-ci.web.cern.ch/storage-ci/eos/citrine/tag/el-7/x86_64/
gpgcheck=0

[eos-citrine-dep]
name=EOS 4.0 Dependencies
baseurl=https://storage-ci.web.cern.ch/storage-ci/eos/citrine-depend/el-7/x86_64/
gpgcheck=0

EOF
}

createQuarkDbRepo(){
echo "Creating the quarkdb.repo and adding it to the yum repository directory"
sudo cat > /etc/yum.repos.d/quarkdb.repo <<'EOF'
[quarkdb-stable]
name=QuarkDB repository [stable]
baseurl=http://storage-ci.web.cern.ch/storage-ci/quarkdb/tag/el7/x86_64/
enabled=1
gpgcheck=False

EOF
}

enableDevToolSetIfNecessary() {
  devtoolSet8EnableCommand='source /opt/rh/devtoolset-8/enable'
  if ! grep -Fxq "$devtoolSet8EnableCommand" ~/.bashrc
  then
    echo "Adding the line '$devtoolSet8EnableCommand' in ~/.bashrc"
    echo $devtoolSet8EnableCommand >> ~/.bashrc
    echo "Rebooting the VM"
    sudo reboot -h now
  fi
}

echo "Installing necessary utilities"
sudo yum install --nogpg -y ccache centos-release-scl-rh cmake3 gcc-c++ gdb make rpm-build rpm-sign yum-plugin-priorities gnutls && yum clean all || die "Error while installing necessary utilities"

EOS_PROJECT_ROOT_DIR="$(git rev-parse --show-toplevel)"
cd $EOS_PROJECT_ROOT_DIR
mkdir -p build
cd build
cmake3 ../ -DPACKAGEONLY=1 && make srpm || die "Unable to make the srpm"

createXrootdRepo

createEosAndEosCitrinRepo

createQuarkDbRepo

sudo yum clean all

echo "Installing libmicrohttpd-devel"
sudo yum install -y libmicrohttpd-devel --disablerepo="*" --enablerepo=eos-citrine-dep || die 'ERROR while installing libmicrohttp packages'
echo "Running yum-builddep to build the dependencies of EOS"
sudo yum-builddep --nogpgcheck --setopt="cern*.exclude=xrootd*,libmicrohttp*" -y SRPMS/* || die 'ERROR while building the dependencies'

echo "Removing the xrootd-5 installed packages"
sudo yum remove -y xrootd-*

echo "Installing the xrootd-4 packages"
sudo yum install -y xrootd xrootd-client xrootd-server-devel xrootd-private-devel --disablerepo="*" --enablerepo=eos-citrine-dep || die "ERROR while trying to install xrootd xrootd-client xrootd-server-devel xrootd-private-devel"

echo "Installing quarkdb"
sudo yum install -y quarkdb quarkdb-debuginfo redis || die 'ERROR while installing quarkdb packages'

enableDevToolSetIfNecessary