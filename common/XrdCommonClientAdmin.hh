#ifndef __XRDCOMMON_CLIENTADMIN_HH__
#define __XRDCOMMON_CLIENTADMIN_HH__

#include "XrdClient/XrdClientAdmin.hh"


class XrdCommonClientAdmin {
  XrdSysMutex clock;
  XrdClientAdmin* Admin;
  
public:
  void Lock() {clock.Lock();}
  void UnLock() {clock.UnLock();}
  XrdClientAdmin* GetAdmin() { return Admin;}

  XrdCommonClientAdmin(const char* url) { Admin = new XrdClientAdmin(url);};
  ~XrdCommonClientAdmin() { if (Admin) delete Admin;}
};

class XrdCommonClientAdminManager {
private:
  XrdSysMutex Mutex;
  XrdOucHash<XrdCommonClientAdmin> admins;
public:
  XrdCommonClientAdmin* GetAdmin(const char* hostport) {
    XrdCommonClientAdmin* myadmin=0;
    Mutex.Lock();
    if ((myadmin = admins.Find(hostport))) {
      Mutex.UnLock();
      return myadmin;
    } else {
      XrdOucString url = "root://"; url += hostport; url += "//dummy";
      myadmin = new XrdCommonClientAdmin(url.c_str());
      admins.Add(hostport,myadmin);
      Mutex.UnLock();
      return myadmin;
    }
  }
  
  XrdCommonClientAdminManager() {};
  ~XrdCommonClientAdminManager() {};
  
};

#endif
