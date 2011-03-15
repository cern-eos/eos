#ifndef ___XRDMGMOFS_SECURITY_H__
#define ___XRDMGMOFS_SECURITY_H__

#include "XrdAcc/XrdAccAuthorize.hh"

#define AUTHORIZE(usr, env, optype, action, pathp, edata) \
    if (usr && gOFS->Authorization \
    &&  !gOFS->Authorization->Access(usr, pathp, optype, env)) \
       {gOFS->Emsg(epname, edata, EACCES, action, pathp); return SFS_ERROR;}

#define AUTHORIZE2(usr,edata,opt1,act1,path1,env1,opt2,act2,path2,env2) \
       {AUTHORIZE(usr, env1, opt1, act1, path1, edata); \
        AUTHORIZE(usr, env2, opt2, act2, path2, edata); \
       }

#define OOIDENTENV(usr, env) \
    if (usr) {if (usr->name) env.Put(SEC_USER, usr->name); \
              if (usr->host) env.Put(SEC_HOST, usr->host);}
#endif
