#ifndef __XRDFSTOFS_CLIENTADMIN_HH__
#define __XRDFSTOFS_CLIENTADMIN_HH__

#include "XrdClient/XrdClientAdmin.hh"


class XrdFstOfsClientAdmin {
  XrdSysMutex clock;
  XrdClientAdmin* Admin;
  
public:
  void Lock() {clock.Lock();}
  void UnLock() {clock.UnLock();}
  XrdClientAdmin* GetAdmin() { return Admin;}

  XrdFstOfsClientAdmin(const char* url) { Admin = new XrdClientAdmin(url);};
  ~XrdFstOfsClientAdmin() { if (Admin) delete Admin;}
};

class XrdFstOfsClientAdminManager {
private:
  XrdSysMutex Mutex;
  XrdOucHash<XrdFstOfsClientAdmin> admins;
public:
  XrdFstOfsClientAdmin* GetAdmin(const char* hostport) {
    XrdFstOfsClientAdmin* myadmin=0;
    Mutex.Lock();
    if ((myadmin = admins.Find(hostport))) {
      Mutex.UnLock();
      return myadmin;
    } else {
      XrdOucString url = "root://"; url += hostport; url += "//dummy";
      myadmin = new XrdFstOfsClientAdmin(url.c_str());
      admins.Add(hostport,myadmin);
      Mutex.UnLock();
      return myadmin;
    }
  }
  
  XrdFstOfsClientAdminManager() {};
  ~XrdFstOfsClientAdminManager() {};
  
};

#endif
