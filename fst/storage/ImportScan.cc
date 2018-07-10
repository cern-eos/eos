// ----------------------------------------------------------------------
// File: ImportScan.cc
// Author: Mihai Patrascoiu - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#define __STDC_FORMAT_MACROS
#include <cinttypes>
#include <fst/FmdDbMap.hh>

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/ImportScan.hh"
#include "fst/io/FileIoPlugin.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::ImportScan()
{

  // Thread that performs import scans
  while (1) {
    mImportScanMutex.Lock();

    if (mImportScans.empty()) {
      mImportScanMutex.UnLock();
      sleep(1);
      continue;
    }

    eos::fst::ImportScan* scan = mImportScans.front();
    mImportScans.pop();

    if (!scan) {
      eos_static_debug("retrieved empty object from ImportScan queue");
      mImportScanMutex.UnLock();
      continue;
    }
    
    mImportScanMutex.UnLock();
    eos_static_debug("starting importScan fsid=%u extPath=%s lclPath=%s",
                     scan->fsId, scan->extPath.c_str(),
                     scan->lclPath.c_str());

    // Construct helper objects
    std::unique_ptr<FileIo> io(
        FileIoPlugin::GetIoObject(scan->extPath.c_str()));
    if (!io) {
      eos_static_err("unable to retrieve IO object for %s",
                     scan->extPath.c_str());
      continue;
    }

    FileIo::FtsHandle* handle = io->ftsOpen();
    if (!handle) {
      eos_static_err("fts_open failed for %s", scan->extPath.c_str());
      continue;
    }

    std::string filePath, lFilePath, pathSuffix;

    // Scan the directory found at extPath
    while ((filePath = io->ftsRead(handle)) != "") {
      eos_static_info("ImportScan -- processing file %s", filePath.c_str());
      lFilePath = filePath;

      // Remove opaque from file path
      size_t qpos = lFilePath.rfind("?");
      if (qpos != std::string::npos) {
        lFilePath.erase(qpos);
      }

      // Gather the data needed for file import
      struct stat buf;
      std::unique_ptr<FileIo> fIo(FileIoPlugin::GetIoObject(filePath));
      if (fIo->fileStat(&buf)) {
        eos_static_err("could not stat file %s", lFilePath.c_str());
        continue;
      }

      // Construct the path suffix from the file path
      int pos = scan->extPath.length();
      if (scan->extPath.rfind("?") != std::string::npos) {
        pos = scan->extPath.rfind("?");
      }
      pathSuffix = lFilePath.substr((size_t) pos);
      if (pathSuffix[0] == '/') {
        pathSuffix.erase(0, 1);
      }

      // Construct command message
      XrdOucErrInfo error;
      XrdOucString capOpaqueFile = "";
      capOpaqueFile += "/?mgm.pcmd=import";
      capOpaqueFile += "&mgm.import.fsid=";
      capOpaqueFile += (int) scan->fsId;
      capOpaqueFile += "&mgm.import.extpath=";
      capOpaqueFile += lFilePath.c_str();
      capOpaqueFile += "&mgm.import.lclpath=";
      capOpaqueFile += scan->lclPath;
      if (!scan->lclPath.endswith("/")) {
        capOpaqueFile += "/";
      }
      capOpaqueFile += pathSuffix.c_str();
      capOpaqueFile += "&mgm.import.size=";
      char filesize[256];
      sprintf(filesize, "%" PRIu64 "", buf.st_size);
      capOpaqueFile += filesize;

      // Send command and process Mgm file metadata response
      XrdOucString response;
      int rc = gOFS.CallManager(&error,  lFilePath.c_str(), 0,
                                capOpaqueFile, &response);
      if (rc) {
        eos_static_err("unable to import file name=%s fs=%u at manager %s",
                       lFilePath.c_str(), scan->fsId,
                       scan->managerId.c_str());
      } else if (!response.length()) {
        eos_static_err("file imported in namespace. Mgm file metadata expected "
                       "but response is empty name=%s fs=%u at manager %s",
                       lFilePath.c_str(), scan->fsId,
                       scan->managerId.c_str());
      } else {
        XrdOucEnv fMdEnv(response.c_str());
        int envlen;
        struct Fmd fMd;
        FmdHelper::Reset(fMd);

        // Reconstruct Mgm fmd entry
        if (gFmdDbMapHandler.EnvMgmToFmd(fMdEnv, fMd)) {
          // Create local fmd entry
          FmdHelper* localFmd =
              gFmdDbMapHandler.LocalGetFmd(fMd.fid(), scan->fsId,
                                fMd.uid(), fMd.gid(), fMd.lid(), true, false);
          fMd.set_layouterror(FmdHelper::LayoutError(fMd, scan->fsId));

          if (localFmd) {
            // Update from Mgm
            if (!gFmdDbMapHandler.UpdateFromMgm(
                              scan->fsId, fMd.fid(), fMd.cid(), fMd.lid(),
                              fMd.mgmsize(), fMd.mgmchecksum(), fMd.uid(),
                              fMd.gid(), fMd.ctime(), fMd.ctime_ns(),
                              fMd.mtime(), fMd.mtime_ns(), fMd.layouterror(),
                              fMd.locations())) {
              eos_static_err("unable to update local fmd entry from Mgm "
                             "name=%s metadata=%s", lFilePath.c_str(),
                             fMdEnv.Env(envlen));
            }

            delete localFmd;
          } else {
            eos_static_err("unable to create local fmd entry name=%s fs=%u",
                           lFilePath.c_str(), scan->fsId);
          }
        } else {
          eos_static_err("unable to parse Mgm file metadata. "
                         "No local fmd entry created name=%s metadata=%s",
                         lFilePath.c_str(), fMdEnv.Env(envlen));
        }
      }
    }

    if (io->ftsClose(handle)) {
      eos_static_err("fts_close failed for %s", scan->extPath.c_str());
    }

    delete handle;
  }
}

EOSFSTNAMESPACE_END
