#ifndef __XRDCOMMON_FILEID__HH__
#define __XRDCOMMON_FILEID__HH__

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

class XrdCommonFileId {
public:
  XrdCommonFileId();
  ~XrdCommonFileId();

  // convert a fid into a hex decimal string
  static void Fid2Hex(unsigned long fid, XrdOucString &hexstring) {
    char hexbuffer[128];
    sprintf(hexbuffer,"%08x", fid);
    hexstring = hexbuffer;
  }

  // convert a hex decimal string into a fid
  static unsigned long Hex2Fid(const char* hexstring) {
    return strtol(hexstring, NULL, 16);
  }

  // compute a path from a fid and localprefix
  static void FidPrefix2FullPath(const char* hexstring, const char* localprefix,  XrdOucString &fullpath, unsigned int subindex = 0) {
    unsigned int fid = Hex2Fid(hexstring);
    char sfullpath[16384];
    if (subindex) {
      sprintf(sfullpath,"%s/%08x/%s.%u",localprefix,fid/10000, hexstring,subindex);
    } else {
      sprintf(sfullpath,"%s/%08x/%s",localprefix,fid/10000, hexstring);
    }
    fullpath = sfullpath;
    while (fullpath.replace("//","/")) {}
  }
};

/*----------------------------------------------------------------------------*/
#endif

  

