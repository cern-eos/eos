XrdHttp
-------

HTTP(S) using the XRootD thread-pool and XrdHttp can be enabled in ```/etc/xrd.cf.mgm``` like:


```
if exec xrootd
   xrd.protocol XrdHttp:9000 /usr/lib64/libXrdHttp-4.so
   http.exthandler EosMgmHttp /usr/lib64/libEosMgmHttp.so eos::mgm::http::redirect-to-https=1
   http.cert /etc/grid-security/daemon/host.cert
   http.key /etc/grid-security/daemon/privkey.pem
   http.cafile /etc/grid-security/daemon/ca.cert
fi
```

To disabel HTTPS you can remove the cert/key/cafile directives. If you want to redirect the data transfer from HTTPS to HTTP you can change the configuration key to ```eos::mgm::http::redirect-to-https=0``` or just put ```none``` at this place. 

The targetport in redirection is currently taken from the sysconfig file and uses ```EOS_FST_HTTP_PORT+1000```.



