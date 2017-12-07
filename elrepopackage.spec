Name:           eos-citrine-repo
Version:        1
Release:        1
Url:            http://cern.ch/eos/
Summary:        EOS YUM repository definition for the 'citrine' releases
License:        none
Group:          Applications/System
BuildArch:      noarch
%description
This package contains the YUM configuration for the EOS 'citrine'
releases and their third-party dependencies.  RPMs are signed with
    http://storage-ci.web.cern.ch/storage-ci/storageci.key

%install
mkdir -p ${RPM_BUILD_ROOT}%{_sysconfdir}/yum.repos.d/
cat <<EOFrepo >${RPM_BUILD_ROOT}%{_sysconfdir}/yum.repos.d/eos-citrine.repo
[eos-citrine]
name=EOS binaries from EOS project, citrine branch
baseurl=http://cern.ch/storage-ci/eos/citrine/tag/el-\$releasever/\$basearch/
enabled=1
gpgcheck=1
priority=10 

[eos-citrine-deps]
name=EOS dependencies from EOS project, citrine branch
baseurl=http://cern.ch/storage-ci/eos/citrine-depend/el-\$releasever/\$basearch/
enabled=1
gpgcheck=0
priority=10 

EOFrepo



%files
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/yum.repos.d/eos-citrine.repo
