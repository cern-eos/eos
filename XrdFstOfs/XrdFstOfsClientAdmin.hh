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

#endif
