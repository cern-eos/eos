INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) -I../

bin_PROGRAMS = eosfilesync eosdirsync

eosfilesync_SOURCES = eosfilesync.cc 
eosdirsync_SOURCES  = eosdirsync.cc

eosfilesync_LDFLAGS = -static -export-dynamic 
eosdirsync_LDFLAGS  = -static -export-dynamic 

eosfilesync_LDADD   = -L${XROOTD_LIBDIR}/ -lXrdClient -lXrdOuc ../XrdCommon/libXrdCommon.la -ldl -lXrdOuc
eosdirsync_LDADD    = -L${XROOTD_LIBDIR}/ -lXrdClient -lXrdOuc ../XrdCommon/libXrdCommon.la -ldl -lXrdOuc
