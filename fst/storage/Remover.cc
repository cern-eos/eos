// ----------------------------------------------------------------------
// File: Remover.cc
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

#include "common/SymKeys.hh"
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Deletion.hh"
#include "fst/Config.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Thead requesting deletions from the MGM and unlinking the physical files
//------------------------------------------------------------------------------
void
Storage::Remover()
{
  using namespace std::chrono;
  static auto last_request_ts = system_clock::now();
  static std::chrono::seconds request_interval(300);
  // Used as barrier for FSTs proper config
  (void)gConfig.getFstNodeConfigQueue("Remover").c_str();
  (void)gConfig.WaitManager();
  uint64_t num_deleted = 0ull;
  const char* ptr = getenv("EOS_FST_DELETE_QUERY_INTERVAL");

  if (ptr) {
    try {
      request_interval = std::chrono::seconds(std::stoi(std::string(ptr)));
      eos_static_info("msg=\"update deletions request interval\" val=%llu",
                      request_interval.count());
    } catch (...) {}
  }

  // Check for deletions when starting
  (void) gOFS.Query2Delete();

  // Thread that unlinks stored files
  while (true) {
    num_deleted = 0ull;
    std::unique_ptr<Deletion> to_del;

    while ((to_del = GetDeletion())) {
      for (unsigned int i = 0; i < to_del->mFidVect.size(); ++i) {
        eos_static_debug("msg=\"delete file\" fxid=%08llx fsid=%u",
                         to_del->mFidVect[i], to_del->mFsid);
        ++num_deleted;
        const std::string hex_fid = eos::common::FileId::Fid2Hex(to_del->mFidVect[i]);
        XrdOucErrInfo error;
        XrdOucString capOpaqueString = "/?mgm.pcmd=drop";
        XrdOucString OpaqueString = "";
        OpaqueString += "&mgm.fsid=";
        OpaqueString += (int) to_del->mFsid;
        OpaqueString += "&mgm.fid=";
        OpaqueString += hex_fid.c_str();
        XrdOucEnv Opaque(OpaqueString.c_str());
        capOpaqueString += OpaqueString;
        // Delete local file
        std::string deletionreport;
        std::string deletionreport64;

        if ((gOFS._rem("/DELETION", error, (const XrdSecEntity*) 0, &Opaque,
                       0, 0, 0, true, &deletionreport) != SFS_OK)) {
          eos_static_warning("msg=\"unable to remove local file\" fxid=%s "
                             "fsid=%lu", hex_fid.c_str(), to_del->mFsid);
        } else {
          // Encode the deletion report only if deletion is successful
          eos::common::SymKey::ZBase64(deletionreport, deletionreport64);
          capOpaqueString += "&mgm.report=";
          capOpaqueString += deletionreport64.c_str();
        }

        // Update the manager
        if (gOFS.CallManager(&error, 0, 0, capOpaqueString)) {
          eos_static_err("msg=\"unable to drop file\" fxid=\"%s\" fsid=\"%u\"",
                         hex_fid.c_str(), to_del->mFsid);
        }
      }
    }

    auto now_ts = std::chrono::system_clock::now();
    bool request_del = (std::chrono::duration_cast<std::chrono::seconds>
                        (now_ts - last_request_ts) > request_interval);

    // Ask for more deletions if deleted something in last round or request
    // interval expired
    if (num_deleted || request_del) {
      eos_static_debug("%s", "msg=\"query manager for deletions\"");
      last_request_ts = now_ts;
      (void) gOFS.Query2Delete();
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  }
}

EOSFSTNAMESPACE_END
