	###########################################################
set MGM=$EOS_MGM_ALIAS
###########################################################

xrootd.fslib -2 libXrdEosFst.so
xrootd.async off nosf
xrd.network keepalive
xrootd.redirect $(MGM):1094 chksum

###########################################################
xrootd.seclib libXrdSec.so
sec.protocol unix
sec.protocol sss -c /etc/eos.client.keytab -s /etc/eos.keytab
sec.protbind * only unix sss
###########################################################
all.export / nolock
all.trace none
all.manager localhost 2131
#ofs.trace open
###########################################################
xrd.port 1095
ofs.persist off
ofs.osslib libEosFstOss.so
ofs.tpc pgm /usr/bin/xrdcp
###########################################################
# this URL can be overwritten by EOS_BROKER_URL defined /etc/sysconfig/xrd
fstofs.broker root://localhost:1097//eos/
fstofs.autoboot true
fstofs.quotainterval 10
fstofs.metalog /var/eos/md/
#fstofs.authdir /var/eos/auth/
#fstofs.trace client
###########################################################
# QuarkDB cluster info needed by FSCK to perform the namespace scan
#fstofs.qdbcluster localhost:777
#fstofs.qdbpassword_file /etc/eos.keytab

#-------------------------------------------------------------------------------
# Configuration for XrdHttp http(s) service on port 11000
#-------------------------------------------------------------------------------
#if exec xrootd
#   xrd.protocol XrdHttp:11000 /usr/lib64/libXrdHttp-4.so
#   http.exthandler EosFstHttp /usr/lib64/libEosFstHttp.so none
#   http.cert /etc/grid-security/daemon/host.cert
#   http.key /etc/grid-security/daemon/privkey.pem
#   http.cafile /etc/grid-security/daemon/ca.cert
#fi
