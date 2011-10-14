// ----------------------------------------------------------------------
// File: CheckSum.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSFST_CHECKSUM_HH__
#define __EOSFST_CHECKSUM_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/Load.hh"
#include "common/LayoutId.hh"
#include "common/Attr.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class CheckSum {
protected:
  XrdOucString Name;
  XrdOucString Checksum;
  bool needsRecalculation;
  char* ChecksumMap;
  size_t ChecksumMapSize;
  int   ChecksumMapFd;
  size_t BlockSize;

  XrdOucString BlockXSPath;
  unsigned long long nXSBlocksChecked;
  unsigned long long nXSBlocksWritten;
  unsigned long long nXSBlocksWrittenHoles;

public:
  CheckSum(){Name = "";ChecksumMap = 0;}
  CheckSum(const char* name){Name = name;needsRecalculation = false;ChecksumMap=0; ChecksumMapSize=0;BlockSize=0;nXSBlocksChecked=0; nXSBlocksWritten=0; nXSBlocksWrittenHoles=0;BlockXSPath="";}

  virtual bool Add(const char* buffer, size_t length, off_t offset) = 0;
  virtual void Finalize() {};
  virtual void Reset() = 0;
  virtual void SetDirty() {
    needsRecalculation = true;
  }

  virtual const char* GetHexChecksum() = 0;
  virtual const char* GetBinChecksum(int &len) = 0;
  virtual bool Compare(const char* refchecksum);
  virtual off_t GetLastOffset() = 0;
  
  virtual int GetCheckSumLen() = 0;
  const char* GetName() {return Name.c_str();}
  bool NeedsRecalculation() {return needsRecalculation;}

  virtual bool ScanFile(const char* path, unsigned long long &scansize, float &scantime, int rate=0);
  virtual bool SetXSMap(off_t offset);
  virtual bool VerifyXSMap(off_t offset);

  virtual bool OpenMap(const char* mapfilepath, size_t maxfilesize, size_t blocksize, bool isRW);
  virtual bool ChangeMap(size_t newsize, bool shrink=true);
  virtual bool SyncMap();
  virtual bool CloseMap();

  virtual void AlignBlockExpand(off_t offset, size_t len, off_t &aligned_offset, size_t &aligend_len);
  virtual void AlignBlockShrink(off_t offset, size_t len, off_t &aligned_offset, size_t &aligend_len);
  virtual bool AddBlockSum(off_t offset, const char* buffer, size_t buffersize); // this only calculates the checksum on full blocks, not matching edge is not calculated
  virtual bool CheckBlockSum(off_t offset, const char* buffer, size_t buffersizem); // this only verifies the checksum on full blocks, not matching edge is not calculated
  virtual bool AddBlockSumHoles(int fd);
  virtual const char* MakeBlockXSPath(const char *filepath) {
    if ((!filepath))
      return 0;
    BlockXSPath = filepath;
    BlockXSPath += ".xsmap";
    return BlockXSPath.c_str();
  }
  
  virtual bool UnlinkXSPath() {
    // return 0 if success
    if (BlockXSPath.length()) {
      return ::unlink(BlockXSPath.c_str());
    }
    return true;
  }

  virtual unsigned long long GetXSBlocksChecked() { return nXSBlocksChecked;}
  virtual unsigned long long GetXSBlocksWritten() { return nXSBlocksWritten;}
  virtual unsigned long long GetXSBlocksWrittenHoles() { return nXSBlocksWrittenHoles;}

  virtual ~CheckSum(){};

  virtual void Print() {
    fprintf(stderr,"%s\n",GetHexChecksum());
  }


  std::string CheckSumMapFile; 
};

EOSFSTNAMESPACE_END

#endif
