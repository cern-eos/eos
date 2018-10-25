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
    eos_static_debug("ImportScan[id=%s] starting fsid=%u extPath=%s lclPath=%s",
                     scan->id.c_str(), scan->fsId, scan->extPath.c_str(),
                     scan->lclPath.c_str());

    // Construct helper objects
    std::unique_ptr<FileIo> io(
        FileIoPlugin::GetIoObject(scan->extPath.c_str()));
    if (!io) {
      eos_static_err("unable to retrieve IO object for %s",
                     scan->id.c_str(), scan->extPath.c_str());
      continue;
    }

    FileIo::FtsHandle* handle = io->ftsOpen();
    if (!handle) {
      eos_static_err("fts_open failed for %s", scan->extPath.c_str());
      continue;
    }

    // To be able to provide progress status on the importation procedure,
    // files will be processed in batches of 1k

    XrdOucErrInfo error;
    XrdOucString capOpaqueFile;
    long long totalFiles, batches;
    bool doImport = true;
    int rc;

    totalFiles = 0;
    batches = 0;

    while (doImport) {
      std::vector<std::string> files;
      std::vector<long long> filesizes;
      std::string file;
      int count = 0;

      files.clear();
      filesizes.clear();
      batches++;

      // Fetch batch of files
      while ((count < 1000) && ((file = io->ftsRead(handle)) != "")) {
        // Obtain file size from stat call
        struct stat buf;
        std::unique_ptr<FileIo> fIo(FileIoPlugin::GetIoObject(file));
        if (fIo->fileStat(&buf)) {
          eos_static_err("could not stat file %s", file.c_str());
          continue;
        }

        // Remove opaque info from file path
        size_t qpos = file.rfind("?");
        if (qpos != std::string::npos) {
          file.erase(qpos);
        }

        files.push_back(file);
        filesizes.push_back(buf.st_size);

        totalFiles++;
        count++;
      }

      // Send import start signal to MGM
      capOpaqueFile = "/?mgm.pcmd=import";
      capOpaqueFile += "&mgm.import.id=";
      capOpaqueFile += scan->id.c_str();
      capOpaqueFile += "&mgm.import.status=start";
      capOpaqueFile += "&mgm.import.status.batch=";
      capOpaqueFile += std::to_string(batches).c_str();
      capOpaqueFile += "&mgm.import.status.total=";
      capOpaqueFile += std::to_string(files.size()).c_str();

      rc = gOFS.CallManager(&error, 0, 0, capOpaqueFile);

      if (rc) {
        eos_static_err("ImportScan[id=%s] failed to send import start signal "
                       "at manager %s reason=\"%s\". Aborting batch=%ld.",
                       scan->id.c_str(), scan->managerId.c_str(),
                       error.getErrText(), batches);
        continue;
      } else {
        eos_static_info("ImportScan[id=%s] starting import of "
                        "batch=%ld files=%ld",
                        scan->id.c_str(), batches, files.size());
      }

      // Process each file for import
      for (size_t i = 0; i < files.size(); i++) {
        file = files[i];
        eos_static_info("ImportScan[id=%s] -- processing file %s",
                         scan->id.c_str(), file.c_str());

        // Construct the path suffix from the file path
        int pos = scan->extPath.length();
        if (scan->extPath.rfind("?") != STR_NPOS) {
          pos = scan->extPath.rfind("?");
        }

        std::string pathSuffix = file.substr((size_t) pos);
        if (pathSuffix[0] == '/') {
          pathSuffix.erase(0, 1);
        }

        // Prepare command arguments
        char filesize[256];
        sprintf(filesize, "%lld", filesizes[i]);
        XrdOucString destPath = scan->lclPath.c_str();
        if (!destPath.endswith("/")) {
          destPath += "/";
        }
        destPath += pathSuffix.c_str();

        // Construct command message
        capOpaqueFile = "/?mgm.pcmd=import";
        capOpaqueFile += "&mgm.import.id=";
        capOpaqueFile += scan->id.c_str();
        capOpaqueFile += "&mgm.import.fsid=";
        capOpaqueFile += (int) scan->fsId;
        capOpaqueFile += "&mgm.import.extpath=";
        capOpaqueFile += file.c_str();
        capOpaqueFile += "&mgm.import.lclpath=";
        capOpaqueFile += destPath.c_str();
        capOpaqueFile += "&mgm.import.size=";
        capOpaqueFile += filesize;

        // Send command and process Mgm file metadata response
        XrdOucString response;
        rc = gOFS.CallManager(&error, 0, 0, capOpaqueFile, &response);

        if (rc) {
          eos_static_err("ImportScan[id=%s] unable to import file "
                         "fs=%u name=%s dest=%s at manager %s reason=\"%s\"",
                         scan->id.c_str(), scan->fsId, file.c_str(),
                         destPath.c_str(), scan->managerId.c_str(),
                         error.getErrText());
        } else if (!response.length()) {
          eos_static_err("ImportScan[id=%s] file imported in namespace. "
                         "Mgm file metadata expected but response is empty "
                         "fs=%u name=%s dest=%s at manager %s",
                         scan->id.c_str(), scan->fsId, file.c_str(),
                         destPath.c_str(), scan->managerId.c_str());
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
                                             fMd.uid(), fMd.gid(), fMd.lid(),
                                             true, false);
            fMd.set_layouterror(FmdHelper::LayoutError(fMd, scan->fsId));

            if (localFmd) {
              // Update from Mgm
              if (!gFmdDbMapHandler.UpdateFromMgm(
                  scan->fsId, fMd.fid(), fMd.cid(), fMd.lid(),
                  fMd.mgmsize(), fMd.mgmchecksum(), fMd.uid(),
                  fMd.gid(), fMd.ctime(), fMd.ctime_ns(),
                  fMd.mtime(), fMd.mtime_ns(), fMd.layouterror(),
                  fMd.locations())) {
                eos_static_err("ImportScan[id=%s] unable to update local "
                               "fmd entry from Mgm fs=%u name=%s metadata=%s",
                               scan->id.c_str(), scan->fsId, file.c_str(),
                               fMdEnv.Env(envlen));
              }

              delete localFmd;
            } else {
              eos_static_err("ImportScan[id=%s] unable to create "
                             "local fmd entry fs=%u name=%s",
                             scan->id.c_str(), scan->fsId, file.c_str());
            }
          } else {
            eos_static_err("ImportScan[id=%s] unable to parse Mgm file metadata. "
                           "No local fmd entry created fs=%u name=%s metadata=%s",
                           scan->id.c_str(), scan->fsId, file.c_str(),
                           fMdEnv.Env(envlen));
          }
        }
      }

      doImport = (count == 1000);
    }

    // Send import end signal to MGM
    capOpaqueFile = "/?mgm.pcmd=import";
    capOpaqueFile += "&mgm.import.id=";
    capOpaqueFile += scan->id.c_str();
    capOpaqueFile += "&mgm.import.status=end";
    capOpaqueFile += "&mgm.import.status.batch=";
    capOpaqueFile += std::to_string(batches).c_str();
    capOpaqueFile += "&mgm.import.status.total=";
    capOpaqueFile += std::to_string(totalFiles).c_str();

    rc = gOFS.CallManager(&error, 0, 0, capOpaqueFile);

    if (rc) {
      eos_static_warning("ImportScan[id=%s] failed to send import end signal "
                         "at manager %s reason=\"%s\"",
                         scan->id.c_str(), scan->managerId.c_str(),
                         error.getErrText());
    } else {
      eos_static_info("ImportScan[id=%s] finished successfully batches=%d "
                      "total_files=%ld",
                      scan->id.c_str(), batches, totalFiles);
    }

    if (io->ftsClose(handle)) {
      eos_static_err("fts_close failed for %s", scan->extPath.c_str());
    }

    delete handle;
  }
}

EOSFSTNAMESPACE_END
