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
#ofs.trace open
###########################################################
xrd.port 1095
ofs.authlib libXrdCapability.so
ofs.authorize
###########################################################
fstofs.symkey MTIzNDU2Nzg5MDEyMzQ1Njc4OTA=
fstofs.broker root://localhost:1097//eos/
#fstofs.trace client
fstofs.autoboot true
fstofs.quotainterval 10

