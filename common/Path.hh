#ifndef __EOSCOMMON_PATH__
#define __EOSCOMMON_PATH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Path {
private:
  XrdOucString fullPath;
  XrdOucString parentPath;
  XrdOucString lastPath;
  std::vector<std::string> subPath;

public:
  const char* GetName()                  { return lastPath.c_str();}
  const char* GetPath()                  { return fullPath.c_str();}
  const char* GetParentPath()            { return parentPath.c_str();    }
  const char* GetSubPath(unsigned int i) { if (i<subPath.size()) return subPath[i].c_str(); else return 0; }
  unsigned int GetSubPathSize()          { return subPath.size();  }

  Path(const char* path){
    fullPath = path;

    if (fullPath.endswith('/')) 
      fullPath.erase(fullPath.length()-1);
    
    // remove  /.$
    if (fullPath.endswith("/.")) {
      fullPath.erase(fullPath.length()-2);
    }

    // reassign /..
    if (fullPath.endswith("/..")) {
      fullPath.erase(fullPath.length()-3);
      int spos = fullPath.rfind("/");
      if (spos != STR_NPOS) {
        fullPath.erase(spos);
      }
    }

    int lastpos=0;
    int pos=0;
    do {
      pos = fullPath.find("/",pos);
      std::string subpath;
      if (pos!=STR_NPOS) {
	subpath.assign(fullPath.c_str(),pos+1);
	subPath.push_back(subpath);
	lastpos = pos;
	pos++;
      }
    } while (pos!=STR_NPOS);
    parentPath.assign(fullPath,0,lastpos);
    lastPath.assign(fullPath,lastpos+1);
  }
  
  ~Path(){};
};

EOSCOMMONNAMESPACE_END

#endif

