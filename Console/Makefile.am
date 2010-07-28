INCLUDES = -I$(XROOTD_INCDIR) ${OPENSSL_INCDIR} -I../

bin_PROGRAMS = eos

eos_SOURCES = \
                ConsoleMain.cc

eos_LDFLAGS = -static -export-dynamic 
eos_LDADD = -ldl -L${XROOTD_LIBDIR}/ -lXrdClient $(top_srcdir)/XrdCommon/libXrdCommon.la -ltermcap

