#ifndef __EOSCOMMON_CLIENTADMIN_HH__
#define __EOSCOMMON_CLIENTADMIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClientAdmin.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class ClientAdmin {
  XrdSysMutex clock;
  XrdClientAdmin* Admin;
  
public:
  void Lock() {clock.Lock();}
  void UnLock() {clock.UnLock();}
  XrdClientAdmin* GetAdmin() { return Admin;}

  ClientAdmin(const char* url) { Admin = new XrdClientAdmin(url);};
  ~ClientAdmin() { if (Admin) delete Admin;}
};

class ClientAdminManager {
private:
  XrdSysMutex Mutex;
  XrdOucHash<ClientAdmin> admins;
public:
  ClientAdmin* GetAdmin(const char* hostport) {
    ClientAdmin* myadmin=0;
    Mutex.Lock();
    if ((myadmin = admins.Find(hostport))) {
      Mutex.UnLock();
      return myadmin;
    } else {
      XrdOucString url = "root://"; url += hostport; url += "//dummy";
      myadmin = new ClientAdmin(url.c_str());
      admins.Add(hostport,myadmin);
      Mutex.UnLock();
      return myadmin;
    }
  }
  
  ClientAdminManager() {};
  ~ClientAdminManager() {};
  
};

EOSCOMMONNAMESPACE_END
#endif
