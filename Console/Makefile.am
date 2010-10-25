INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) ${OPENSSL_INCDIR} -I../

SUBDIRS = Commands

bin_PROGRAMS = eos

eos_SOURCES = \
                ConsoleMain.cc

eos_LDFLAGS = -static -export-dynamic 
eos_LDADD = ./Commands/libEosConsole.la -ldl -L${XROOTD_LIBDIR}/ -lXrdClient $(top_srcdir)/XrdCommon/libXrdCommon.la -ltermcap 

