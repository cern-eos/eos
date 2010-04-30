###########################################################
xrootd.fslib /opt/eos/lib/libXrdFstOfs.so
xrootd.async off nosf
xrd.network keepalive
###########################################################
xrootd.seclib /opt/eos/lib/libXrdSec.so
sec.protocol /opt/eos/lib unix
###########################################################
all.export / nolock
all.trace none
#ofs.trace open
###########################################################
xrd.port 1095
ofs.authlib /opt/eos/lib/libXrdCapability.so
ofs.authorize
###########################################################
fstofs.symkey MTIzNDU2Nzg5MDEyMzQ1Njc4OTAK
fstofs.broker root://localhost:1097//eos/
#fstofs.trace client
fstofs.autoboot true

