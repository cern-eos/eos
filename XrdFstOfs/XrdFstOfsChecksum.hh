#ifndef __XRDFSTOFS_CHECKSUM_HH__
#define __XRDFSTOFS_CHECKSUM_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

class XrdFstOfsChecksum {
protected:
  XrdOucString Name;
  XrdOucString Checksum;
  bool needsRecalculation;

public:
  XrdFstOfsChecksum(){Name = "";}
  XrdFstOfsChecksum(const char* name){Name = name;needsRecalculation = false;}

  virtual bool Add(const char* buffer, size_t length, off_t offset) = 0;
  virtual void Finalize() {};
  virtual void Reset() = 0;
  virtual const char* GetHexChecksum() = 0;
  virtual const char* GetBinChecksum(int &len) = 0;
  const char* GetName() {return Name.c_str();}
  bool NeedsRecalculation() {return needsRecalculation;}

  virtual bool ScanFile(const char* path);
  virtual ~XrdFstOfsChecksum(){};

  
};

#endif
