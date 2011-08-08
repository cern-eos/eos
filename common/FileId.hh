#ifndef __EOSCOMMON_FILEID__HH__
#define __EOSCOMMON_FILEID__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class FileId {
public:
  FileId();
  ~FileId();

  // convert a fid into a hex decimal string
  static void Fid2Hex(unsigned long long fid, XrdOucString &hexstring) {
    char hexbuffer[128];
    sprintf(hexbuffer,"%08llx", fid);
    hexstring = hexbuffer;
  }

  // convert a hex decimal string into a fid
  static unsigned long long Hex2Fid(const char* hexstring) {
    if (hexstring)
      return strtoll(hexstring, 0, 16);
    else
      return 0;
  }

  // compute a path from a fid and localprefix
  static void FidPrefix2FullPath(const char* hexstring, const char* localprefix,  XrdOucString &fullpath, unsigned int subindex = 0) {
    unsigned long long fid = Hex2Fid(hexstring);
    char sfullpath[16384];
    if (subindex) {
      sprintf(sfullpath,"%s/%08llx/%s.%u",localprefix,fid/10000, hexstring,subindex);
    } else {
      sprintf(sfullpath,"%s/%08llx/%s",localprefix,fid/10000, hexstring);
    }
    fullpath = sfullpath;
    while (fullpath.replace("//","/")) {}
  }

  // compute a fid from a prefix path
  static unsigned long long PathToFid(const char* path) {
    XrdOucString hexfid="";
    hexfid = path;
    int rpos = hexfid.rfind("/");
    if(rpos>0) 
      hexfid.erase(0,rpos+1);
    return Hex2Fid(hexfid.c_str());
  }
};

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

#endif

  

