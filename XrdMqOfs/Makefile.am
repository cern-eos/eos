#######################################################################
## Makefile.am for libXrdMqOfs
##
## 
## Version info: $Id: Makefile.am,v 1.5 2006/07/24 07:38:57 dfeich Exp $
## Checked in by $Author: apeters $
#######################################################################

lib_LTLIBRARIES = libXrdMqOfs.la libXrdMqClient.la

INCLUDES = -I$(XROOTD_INCDIR) ${OPENSSL_INCDIR} -I../

bin_PROGRAMS = xrdmqclienttest xrdmqclientmaster xrdmqclientworker xrdmqcryptotest xrdmqdumper xrdmqfeeder xrdmqsharedobjectclient xrdmqsharedobjectbroadcastclient

xrdmqclienttest_SOURCES = XrdMqClientTest.cc
xrdmqclienttest_LDADD   = libXrdMqClient.la

xrdmqdumper_SOURCES = XrdMqQueueDumper.cc
xrdmqdumper_LDADD   = libXrdMqClient.la
xrdmqdumper_LDFLAGS = -static -export-dynamic

xrdmqfeeder_SOURCES = XrdMqQueueFeeder.cc
xrdmqfeeder_LDADD   = libXrdMqClient.la
xrdmqfeeder_LDFLAGS = -static -export-dynamic

xrdmqclientmaster_SOURCES = XrdMqClientMaster.cc
xrdmqclientmaster_LDADD   = libXrdMqClient.la
xrdmqclientmaster_LDFLAGS = -static -export-dynamic

xrdmqclientworker_SOURCES = XrdMqClientWorker.cc
xrdmqclientworker_LDADD   = libXrdMqClient.la
xrdmqclientworker_LDFLAGS = -static -export-dynamic

xrdmqcryptotest_SOURCES = XrdMqCryptoTest.cc
xrdmqcryptotest_LDADD   = libXrdMqClient.la -lXrdNet
xrdmqcryptotest_LDFLAGS = -static -export-dynamic -g 

xrdmqsharedobjectclient_SOURCES = XrdMqSharedObjectClient.cc
xrdmqsharedobjectclient_LDADD   = libXrdMqClient.la
xrdmqsharedobjectclient_LDFLAGS = -static -export-dynamic -lrt

xrdmqsharedobjectbroadcastclient_SOURCES = XrdMqSharedObjectBroadCastClient.cc
xrdmqsharedobjectbroadcastclient_LDADD   = libXrdMqClient.la
xrdmqsharedobjectbroadcastclient_LDFLAGS = -static -export-dynamic -lrt

libXrdMqOfs_la_SOURCES = \
	XrdMqOfs.cc \
	XrdMqOfsFSctl.cc \
	XrdMqOfs.hh \
	XrdMqMessage.cc \
	XrdMqMessage.hh 


libXrdMqOfs_la_LIBADD = -L$(XROOTD_LIBDIR)/ -lXrdSys -lXrdOuc -lXrdClient -lXrdOfs ${OPENSSL_LIBDIR} -lcrypto -luuid -lcurses 
libXrdMqOfs_la_LDFLAGS = -module

EXTRA_DIST = 

libXrdMqClient_la_SOURCES = \
	XrdMqClient.cc \
	XrdMqClient.hh \
	XrdMqMessage.cc \
	XrdMqMessage.hh \
	XrdMqTiming.hh \
	XrdMqMessaging.cc \
	XrdMqMessaging.hh \
	XrdMqRWMutex.hh \
	XrdMqSharedObject.cc \
	XrdMqSharedObject.hh

libXrdMqClient_la_LIBADD = -L$(XROOTD_LIBDIR)/ -lXrdSys -lXrdNet -lXrdOuc -lXrdClient ${OPENSSL_LIBDIR} -lcrypto -luuid -lcurses

rpm:	dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb xrootd-mq.spec

srpm:	
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bs xrootd-mq.spec
