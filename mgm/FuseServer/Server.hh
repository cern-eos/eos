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
#include <string>
#include <unordered_map>

#include "common/RWMutex.hh"
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
                      eos::common::VirtualIdentity& vid, bool lock=true);

  bool FillFileMD(uint64_t id, eos::fusex::md& file,
                  eos::common::VirtualIdentity& vid, bool lock=true);

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

  //----------------------------------------------------------------------------
  //! Trust-on-first-use binding between the wire (clientid, clientuuid) pair
  //! and the authenticated principal (vid.uid + normalized vid.host).
  //!
  //! On first observation the principal is registered for both keys.
  //! Subsequent calls verify the principal matches; mismatches return
  //! false. Empty wire keys are skipped.
  //!
  //! @param clientid   wire md.clientid()
  //! @param clientuuid wire md.clientuuid()
  //! @param vid        authenticated identity
  //! @return true if the bindings are fresh or match, false on collision
  //----------------------------------------------------------------------------
  bool VerifyOrBindClient(const std::string& clientid,
                          const std::string& clientuuid,
                          const eos::common::VirtualIdentity& vid) noexcept;

  //----------------------------------------------------------------------------
  //! Release the (clientid, clientuuid) bindings registered by
  //! VerifyOrBindClient. Called from Clients::MonitorHeartBeat when the
  //! corresponding mount session is evicted, so a subsequent reconnect
  //! from a fresh principal can re-register cleanly.
  //----------------------------------------------------------------------------
  void DropClientBinding(const std::string& clientid,
                         const std::string& clientuuid) noexcept;

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
  //! Wire-identity to authenticated-principal binding maps. See
  //! VerifyOrBindClient / DropClientBinding for rationale.
  //----------------------------------------------------------------------------
  struct ClientPrincipal {
    uid_t uid = (uid_t) -1;
    std::string host;

    bool operator==(const ClientPrincipal& o) const noexcept
    {
      return uid == o.uid && host == o.host;
    }
    bool operator!=(const ClientPrincipal& o) const noexcept
    {
      return !(*this == o);
    }
  };

  mutable eos::common::RWMutex mClientBindMutex;
  std::unordered_map<std::string, ClientPrincipal> mClientIdBind;
  std::unordered_map<std::string, ClientPrincipal> mClientUuidBind;

  static ClientPrincipal MakePrincipal(
    const eos::common::VirtualIdentity& vid) noexcept;

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
