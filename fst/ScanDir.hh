//------------------------------------------------------------------------------
// File: ScanDir.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#ifndef __EOSFST_SCANDIR_HH__
#define __EOSFST_SCANDIR_HH__

#include <pthread.h>
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "XrdOuc/XrdOucString.hh"

#include <sys/syscall.h>
#ifndef __APPLE__
#include <asm/unistd.h>
#endif

EOSFSTNAMESPACE_BEGIN

class Load;
class FileIo;
class CheckSum;

class ScanDir : eos::common::LogId
{
  //----------------------------------------------------------------------------
  //! This class scan's a directory tree and checks checksums (and
  //! blockchecksums if present) in a defined interval with limited bandwidth.
  //----------------------------------------------------------------------------
private:
  eos::fst::Load* fstLoad;
  eos::common::FileSystem::fsid_t fsId;
  XrdOucString dirPath;
  long int testInterval; // in seconds

  // Statistics
  long int noScanFiles;
  long int noCorruptFiles;
  long int noHWCorruptFiles;
  float durationScan;
  long long int totalScanSize;
  long long int bufferSize;
  long int noNoChecksumFiles;
  long int noTotalFiles;
  long int SkippedFiles;

  bool setChecksum;
  int rateBandwidth; // MB/s
  long alignment;
  char* buffer;
  pthread_t thread;
  bool bgThread;
  bool forcedScan;

public:

  ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
          eos::fst::Load* fstload, bool bgthread = true, long int testinterval = 10,
          int ratebandwidth = 100, bool setchecksum = false);

  void ScanFiles();

  void CheckFile(const char*);
  eos::fst::CheckSum* GetBlockXS(const char*, unsigned long long maxfilesize);
  bool ScanFileLoadAware(const std::unique_ptr<eos::fst::FileIo>&,
                         unsigned long long&, float&, const char*, unsigned long, const char* lfn,
                         bool& filecxerror, bool& blockxserror);

  std::string GetTimestamp();
  std::string GetTimestampSmeared();
  bool RescanFile(std::string);

  static void* StaticThreadProc(void*);
  void* ThreadProc();

  virtual ~ScanDir();

};

EOSFSTNAMESPACE_END

#endif
