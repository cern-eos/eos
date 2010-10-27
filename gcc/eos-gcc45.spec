Summary: EOS gcc 4.5 shared libraries
Name: eos-gcc45lib
Version: 4.5
Release: 0
License: None
Group: Applications/File
#URL: 
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
AutoReqProv: no

%description
Contains stdc++ & gcc_s library from 4.5. compiler
%prep
%build
%install
rm -rf $RPM_BUILD_ROOT/
mkdir -p $RPM_BUILD_ROOT/opt/eos/lib
cp /afs/cern.ch/sw/lcg/contrib/gcc/4.5.0/x86_64-slc5-gcc43-opt/lib/libstdc++.so.6.0.14 $RPM_BUILD_ROOT/opt/eos/lib
cp /afs/cern.ch/sw/lcg/contrib/gcc/4.5.0/x86_64-slc5-gcc43-opt/lib/libgcc_s.so.1       $RPM_BUILD_ROOT/opt/eos/lib
ln -s libstdc++.so.6.0.14 $RPM_BUILD_ROOT/opt/eos/lib/libstdc++.so.6
%clean
rm -rf $RPM_BUILD_ROOT


%files
/opt/eos/lib

%defattr(-,root,root,-)
%doc


%changelog
* Wed Oct 27 2010 Andreas Joachim Peters <andreas.joachim.peters@cern.ch> - eos-gcc45lib
- Initial build.

%pre

%post
%preun
%postun
echo
