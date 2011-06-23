###########################################################
xrootd.fslib libXrdEosFst.so
xrootd.async off nosf
xrd.network keepalive
###########################################################
xrootd.seclib libXrdSec.so
sec.protocol unix
sec.protocol sss -c /etc/eos.keytab -s /etc/eos.keytab
sec.protbind * only unix sss
###########################################################
all.export / nolock
all.trace none
all.manager localhost 2131
#ofs.trace open
###########################################################
xrd.port 1095
ofs.authlib libXrdEosAuth.so
ofs.authorize
###########################################################
# this can be overwritten by EOS_SYM_KEY defined in /etc/sysconfig/xrd
fstofs.symkey AAAAAAAAAAAAAAAAAAAAAAAAAAA=
# this URL can be overwritten by EOS_BROKER_URL defined /etc/sysconfig/xrd
fstofs.broker root://localhost:1097//eos/
fstofs.autoboot true
fstofs.quotainterval 10
fstofs.metalog /var/eos/md/
#fstofs.trace client
###########################################################
