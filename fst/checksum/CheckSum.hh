#ifndef __EOSFST_CHECKSUM_HH__
#define __EOSFST_CHECKSUM_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class CheckSum {
protected:
  XrdOucString Name;
  XrdOucString Checksum;
  bool needsRecalculation;

public:
  CheckSum(){Name = "";}
  CheckSum(const char* name){Name = name;needsRecalculation = false;}

  virtual bool Add(const char* buffer, size_t length, off_t offset) = 0;
  virtual void Finalize() {};
  virtual void Reset() = 0;
  virtual const char* GetHexChecksum() = 0;
  virtual const char* GetBinChecksum(int &len) = 0;
  const char* GetName() {return Name.c_str();}
  bool NeedsRecalculation() {return needsRecalculation;}

  virtual bool ScanFile(const char* path, unsigned long long &scansize, float &scantime, int rate=0);
  virtual ~CheckSum(){};

  
};

EOSFSTNAMESPACE_END

#endif
