#ifndef __XRDFSTOFS_DELETION_HH__
#define __XRDFSTOFS_DELETION_HH__
/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstDeletion {
public:
  std::vector<unsigned long long> fIdVector;
  unsigned long fsId;
  XrdOucString localPrefix;
  XrdOucString managerId;


  XrdFstDeletion(std::vector<unsigned long long> &idvector, unsigned long fsid, const char* localprefix, const char* managerid) {
    fIdVector = idvector;  fsId = fsid; localPrefix = localprefix; managerId = managerid;
  }

  static XrdFstDeletion* Create(XrdOucEnv* capOpaque) {
    // decode the opaque tags
    const char* localprefix=0;
    XrdOucString hexfids="";
    XrdOucString hexfid="";
    XrdOucString access="";
    const char* sfsid=0;
    const char* smanager=0;
    std::vector <unsigned long long> idvector;

    unsigned long long fileid=0;
    unsigned long fsid=0;

    localprefix = capOpaque->Get("mgm.localprefix");
    hexfids     = capOpaque->Get("mgm.fids");
    sfsid       = capOpaque->Get("mgm.fsid");
    smanager    = capOpaque->Get("mgm.manager");
    access      = capOpaque->Get("mgm.access");

    // permission check
    if (access != "delete") 
      return 0;

    if (!localprefix || !hexfids.length() || !sfsid || !smanager) {
      return 0;
    }

    while(hexfids.replace(","," ")) {};
    XrdOucTokenizer subtokenizer((char*)hexfids.c_str());
    subtokenizer.GetLine();
    while (1) {
      hexfid = subtokenizer.GetToken();
      if (hexfid.length()) {
	fileid = XrdCommonFileId::Hex2Fid(hexfid.c_str());	
	idvector.push_back(fileid);
      } else {
	break;
      }
    }
    
    fsid   = atoi(sfsid);
    return new XrdFstDeletion(idvector, fsid, localprefix, smanager);
  };

  ~XrdFstDeletion() {};
};


#endif
