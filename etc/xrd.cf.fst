###########################################################
xrootd.fslib libXrdFstOfs.so
xrootd.async off nosf
xrd.network keepalive
###########################################################
xrootd.seclib libXrdSec.so
sec.protocol  unix
###########################################################
all.export / nolock
all.trace none
all.manager localhost 2131
#ofs.trace open
###########################################################
xrd.port 1095
ofs.authlib libXrdCapability.so
ofs.authorize
###########################################################
fstofs.symkey AAAAAAAAAAAAAAAAAAAAAAAAAAA=
fstofs.broker root://lxbra0301.cern.ch:1097//eos/
#fstofs.trace client
fstofs.autoboot true
fstofs.quotainterval 10

