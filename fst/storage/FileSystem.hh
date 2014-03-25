// ----------------------------------------------------------------------
// File: FileSystem.hh
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

#ifndef __EOSFST_FILESYSTEM_HH__
#define __EOSFST_FILESYSTEM_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/ScanDir.hh"
#include "fst/txqueue/TransferQueue.hh"
#include "fst/txqueue/TransferMultiplexer.hh"
#include "common/Logging.hh"
#include "common/Statfs.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"

/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <list>
#include <queue>
#include <map>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class FileSystem : public eos::common::FileSystem, eos::common::LogId
{
private:
  XrdOucString transactionDirectory;

  eos::common::Statfs* statFs; // the owner of the object is a global hash in eos::common::Statfs - this are just references
  eos::fst::ScanDir* scanDir; // the class scanning checksum on a filesystem
  unsigned long last_blocks_free;
  time_t last_status_broadcast;
  eos::common::FileSystem::fsstatus_t mLocalBootStatus; // the internal boot state not stored in the shared hash
  
  TransferQueue* mTxDrainQueue;
  TransferQueue* mTxBalanceQueue;
  TransferQueue* mTxExternQueue;

  TransferMultiplexer mTxMultiplexer;

  std::map<std::string, size_t> inconsistency_stats;
  std::map<std::string, std::set<eos::common::FileId::fileid_t> > inconsistency_sets;

  long long seqBandwidth; // measurement of sequential bandwidth
  int IOPS; // measurement of IOPS

public:
  FileSystem (const char* queuepath, const char* queue, XrdMqSharedObjectManager* som);

  ~FileSystem ();

  void
  SetTransactionDirectory (const char* tx)
  {
    transactionDirectory = tx;
  }
  void CleanTransactions ();
  void SyncTransactions ();

  void RunScanner (Load* fstLoad, time_t interval);

  std::string
  GetPath ()
  {
    return GetString("path");
  }

  const char*
  GetTransactionDirectory ()
  {
    return transactionDirectory.c_str();
  }

  TransferQueue*
  GetDrainQueue ()
  {
    return mTxDrainQueue;
  }

  TransferQueue*
  GetBalanceQueue ()
  {
    return mTxBalanceQueue;
  }

  TransferQueue*
  GetExternQueue ()
  {
    return mTxExternQueue;
  }

  XrdSysMutex InconsistencyStatsMutex; // mutex protecting inconsistency_stats

  std::map<std::string, size_t> *
  GetInconsistencyStats ()
  {
    return &inconsistency_stats;
  }

  std::map<std::string, std::set<eos::common::FileId::fileid_t> > *
  GetInconsistencySets ()
  {
    return &inconsistency_sets;
  }

  void
  SetStatus(eos::common::FileSystem::fsstatus_t status)
  {
    mLocalBootStatus = status;
    eos::common::FileSystem::SetStatus(status);
  }
  
  eos::common::FileSystem::fsstatus_t
  GetStatus()
  {
    // we patch this function because we don't want to see the shared information
    // but the 'true' information created locally
    return mLocalBootStatus;  
  }
  
  void BroadcastError (const char* msg);
  void BroadcastError (int errc, const char* errmsg);
  void BroadcastStatus ();

  bool OpenTransaction (unsigned long long fid);
  bool CloseTransaction (unsigned long long fid);

  void
  SetError (int errc, const char* errmsg)
  {
    if (errc)
    {
      eos_static_err("setting errc=%d errmsg=%s", errc, errmsg ? errmsg : "");
    }
    if (!SetLongLong("stat.errc", errc))
    {
      eos_static_err("cannot set errcode for filesystem %s", GetQueuePath().c_str());
    }
    if (!SetString("stat.errmsg", errmsg))
    {
      eos_static_err("cannot set errmsg for filesystem %s", GetQueuePath().c_str());
    }
  }

  eos::common::Statfs* GetStatfs ();

  void IoPing();

  long long getSeqBandwidth() {return seqBandwidth;}
  int getIOPS() {return IOPS;}
};

EOSFSTNAMESPACE_END

#endif
