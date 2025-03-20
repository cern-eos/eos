SPECFILE = $(shell find . -maxdepth 1 -type f -name '*.spec' )
DIST    ?= $(shell rpm --eval %{dist})
RPMBUILD = $(shell pwd)/rpmbuild

PACKAGE  = $(shell awk '$$1 == "Name:"     { print $$2 }' $(SPECFILE) )
VERSION  = $(shell awk '$$1 == "Version:"  { print $$2 }' $(SPECFILE) )

PERLDIR  = $(shell perl -V:installsitearch | cut -d "'" -f 2)
INSTALL ?= install
DESTDIR ?= $(RPMBUILD)/BUILDROOT

clean:
	rm -rf $(PACKAGE)-$(VERSION)
	rm -rf eos-apmon-*.tar.gz
	rm -rf $(RPMBUILD)

dist: clean
	mkdir -p $(PACKAGE)-$(VERSION)
	rsync -aC --exclude '.__afs*' --exclude $(PACKAGE)-$(VERSION) . $(PACKAGE)-$(VERSION)
	tar cpfz ./$(PACKAGE)-$(VERSION).tar.gz $(PACKAGE)-$(VERSION)

install:
	mkdir -p $(DESTDIR)/perl/ApMon/ApMon/
	mkdir -p $(DESTDIR)/etc/logrotate.d/
	mkdir -p $(DESTDIR)/opt/eos/apmon
	mkdir -p $(DESTDIR)/etc/sysconfig/
	mkdir -p $(DESTDIR)/var/log/eos
	mkdir -p $(DESTDIR)/$(PERLDIR)/ApMon/ApMon
	mkdir -p $(DESTDIR)/usr/sbin/
	mkdir -p $(DESTDIR)/usr/lib/systemd/system/
	mkdir -p $(DESTDIR)/var/log/eos/apmon
	cd perl; for name in `find . -type f | grep -v svn`; do $(INSTALL) -m 755 $$name $(DESTDIR)/$(PERLDIR)/$$name; done
	$(INSTALL) -m 644 usr/lib/systemd/system/eosapmond.service $(DESTDIR)/usr/lib/systemd/system/
	$(INSTALL) -m 755 opt/eos/apmon/eosapmond $(DESTDIR)/opt/eos/apmon/eosapmond
	$(INSTALL) -m 644 etc/logrotate.d/eosapmond $(DESTDIR)/etc/logrotate.d/eosapmond
	$(INSTALL) -m 755 run.sh $(DESTDIR)/opt/eos/apmon/run.sh

prepare: dist
	mkdir -p $(RPMBUILD)/RPMS/$(DIST)
	mkdir -p $(RPMBUILD)/SRPMS/
	mkdir -p $(RPMBUILD)/SPECS/
	mkdir -p $(RPMBUILD)/SOURCES/
	mkdir -p $(RPMBUILD)/BUILD/
	cp eos-apmon-*.tar.gz $(RPMBUILD)/SOURCES 
	cp $(SPECFILE) $(RPMBUILD)/SOURCES 

srpm: prepare $(SPECFILE)
	rpmbuild --define "_source_filedigest_algorithm md5" --define "_binary_filedigest_algorithm md5" \
		--define "_topdir $(RPMBUILD)" -ts $(RPMBUILD)/SOURCES/eos-apmon-*.tar.gz

rpm: srpm
	rpmbuild --rebuild --define "_rpmdir $(RPMBUILD)/RPMS/" \
		--define "_build_name_fmt %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm" rpmbuild/SRPMS/eos-apmon-*.src.rpm
