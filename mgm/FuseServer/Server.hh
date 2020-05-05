// ----------------------------------------------------------------------
// File: FuseServer/Server.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#pragma once

#include "mgm/Namespace.hh"
#include <thread>

#include <map>
#include <atomic>

#include "mgm/fusex.pb.h"
#include "mgm/fuse-locks/LockTracker.hh"
#include "mgm/FuseServer/Caps.hh"
#include "mgm/FuseServer/Clients.hh"
#include "mgm/FuseServer/Flush.hh"
#include "mgm/FuseServer/Locks.hh"

#include "namespace/interface/IFileMD.hh"

EOSFUSESERVERNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FuseServer
//------------------------------------------------------------------------------
class Server : public eos::common::LogId

{
public:
  Server();

  ~Server();

  void start();
  void shutdown();

  std::string dump_message(const google::protobuf::Message& message);

  Clients& Client()
  {
    return mClients;
  }

  Caps& Cap()
  {
    return mCaps;
  }

  Lock& Locks()
  {
    return mLocks;
  }

  Flush& Flushs()
  {
    return mFlushs;
  }

  void Print(std::string& out, std::string options = "");

  int FillContainerMD(uint64_t id, eos::fusex::md& dir,
                      eos::common::VirtualIdentity& vid);
  bool FillFileMD(uint64_t id, eos::fusex::md& file,
                  eos::common::VirtualIdentity& vid);
  bool FillContainerCAP(uint64_t id, eos::fusex::md& md,
                        eos::common::VirtualIdentity& vid,
                        std::string reuse_uuid = "",
                        bool issue_only_one = false);

  Caps::shared_cap ValidateCAP(const eos::fusex::md& md, mode_t mode,
                               eos::common::VirtualIdentity& vid);
  bool ValidatePERM(const eos::fusex::md& md, const std::string& mode,
                    eos::common::VirtualIdentity& vid,
                    bool lock = true);

  uint64_t InodeFromCAP(const eos::fusex::md&);

  std::string Header(const std::string& response); // reply a sync-response header


  int OpBeginFlush(const std::string& identity,
                   const eos::fusex::md& md,
                   eos::common::VirtualIdentity& vid,
                   std::string* response = 0,
                   uint64_t* clock = 0);


  int OpEndFlush(const std::string& identity,
                 const eos::fusex::md& md,
                 eos::common::VirtualIdentity& vid,
                 std::string* response = 0,
                 uint64_t* clock = 0);

  int OpGetLs(const std::string& identity,
              const eos::fusex::md& md,
              eos::common::VirtualIdentity& vid,
              std::string* response = 0,
              uint64_t* clock = 0);

  int OpSet(const std::string& identity,
            const eos::fusex::md& md,
            eos::common::VirtualIdentity& vid,
            std::string* response = 0,
            uint64_t* clock = 0);

  int OpSetLink(const std::string& identity,
                const eos::fusex::md& md,
                eos::common::VirtualIdentity& vid,
                std::string* response = 0,
                uint64_t* clock = 0);

  int OpSetFile(const std::string& identity,
                const eos::fusex::md& md,
                eos::common::VirtualIdentity& vid,
                std::string* response = 0,
                uint64_t* clock = 0);

  int OpSetDirectory(const std::string& identity,
                     const eos::fusex::md& md,
                     eos::common::VirtualIdentity& vid,
                     std::string* response = 0,
                     uint64_t* clock = 0);



  int OpGetCap(const std::string& identity,
               const eos::fusex::md& md,
               eos::common::VirtualIdentity& vid,
               std::string* response = 0,
               uint64_t* clock = 0);

  int OpDelete(const std::string& identity,
               const eos::fusex::md& md,
               eos::common::VirtualIdentity& vid,
               std::string* response = 0,
               uint64_t* clock = 0);

  int OpDeleteFile(const std::string& identity,
                   const eos::fusex::md& md,
                   eos::common::VirtualIdentity& vid,
                   std::string* response = 0,
                   uint64_t* clock = 0);

  int OpDeleteDirectory(const std::string& identity,
                        const eos::fusex::md& md,
                        eos::common::VirtualIdentity& vid,
                        std::string* response = 0,
                        uint64_t* clock = 0);

  int OpDeleteLink(const std::string& identity,
                   const eos::fusex::md& md,
                   eos::common::VirtualIdentity& vid,
                   std::string* response = 0,
                   uint64_t* clock = 0);

  int OpGetLock(const std::string& identity,
                const eos::fusex::md& md,
                eos::common::VirtualIdentity& vid,
                std::string* response = 0,
                uint64_t* clock = 0);


  int OpSetLock(const std::string& identity,
                const eos::fusex::md& md,
                eos::common::VirtualIdentity& vid,
                std::string* response = 0,
                uint64_t* clock = 0);

  int HandleMD(const std::string& identity,
               const eos::fusex::md& md,
               eos::common::VirtualIdentity& vid,
               std::string* response = 0,
               uint64_t* clock = 0);

  void prefetchMD(const eos::fusex::md& md);

  bool CheckRecycleBinOrVersion(std::shared_ptr<eos::IFileMD> fmd);

  void
  MonitorCaps() noexcept;

  bool should_terminate()
  {
    return terminate_.load();
  } // check if threads should terminate

  void terminate()
  {
    terminate_.store(true, std::memory_order_seq_cst);
  } // indicate to terminate

  static const char* cident;

protected:
  Clients mClients;
  Caps mCaps;
  Lock mLocks;
  Flush mFlushs;

private:
  std::atomic<bool> terminate_;
  uint64_t c_max_children;

  //----------------------------------------------------------------------------
  //! Replaces the file's non-system attributes with client-supplied ones.
  //!
  //! @param fmd file metadata object
  //! @param md client supplied metadata
  //----------------------------------------------------------------------------
  void replaceNonSysAttributes(const std::shared_ptr<eos::IFileMD>& fmd,
                               const eos::fusex::md& md);
};


EOSFUSESERVERNAMESPACE_END
