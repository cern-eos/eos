// ----------------------------------------------------------------------
// File: InjectionScan.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/InjectionScan.hh"
#include "fst/io/FileIoPlugin.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::InjectionScan()
{

  // Thread that performs injection scans
  while (1) {
    mInjectionScanMutex.Lock();

    if (mInjectionScans.empty()) {
      mInjectionScanMutex.UnLock();
      sleep(1);
      continue;
    }

    eos::fst::InjectionScan* inScan = mInjectionScans.front();
    mInjectionScans.pop();

    if (!inScan) {
      eos_static_debug("retrieved empty object from InjectionScan queue");
      mInjectionScanMutex.UnLock();
      continue;
    }
    
    mInjectionScanMutex.UnLock();
    eos_static_debug("Starting injectionScan fsid=%u extPath=%s lclPath=%s",
                     inScan->fsId, inScan->extPath.c_str(),
                     inScan->lclPath.c_str());

    // Construct helper objects
    std::unique_ptr<FileIo> io(
        FileIoPlugin::GetIoObject(inScan->extPath.c_str()));
    if (!io) {
      eos_static_err("unable to retrieve IO object for %s",
                     inScan->extPath.c_str());
      continue;
    }

    FileIo::FtsHandle* handle = io->ftsOpen();
    if (!handle) {
      eos_static_err("fts_open failed for %s", inScan->extPath.c_str());
      continue;
    }

    std::string filePath, lFilePath;

    // Scan the directory found at extPath
    while ((filePath = io->ftsRead(handle)) != "") {
      eos_static_info("[InjectionScan] processing file %s", filePath.c_str());
      lFilePath = filePath;

      // Remove opaque from file path
      size_t qpos = lFilePath.rfind("?");
      if (qpos != std::string::npos) {
        lFilePath.erase(qpos);
      }

      // Gather the data needed for file injection
      struct stat buf;
      std::unique_ptr<FileIo> fIo(FileIoPlugin::GetIoObject(filePath));
      if (fIo->fileStat(&buf)) {
        eos_static_err("could not stat file %s", lFilePath.c_str());
        continue;
      }

      // Construct command message
      XrdOucErrInfo error;
      XrdOucString capOpaqueFile = "";
      capOpaqueFile += "/?mgm.pcmd=inject";
      capOpaqueFile += "&mgm.inject.fsid=";
      capOpaqueFile += (int) inScan->fsId;
      capOpaqueFile += "&mgm.inject.extpath=";
      capOpaqueFile += lFilePath.c_str();
      capOpaqueFile += "&mgm.inject.lclpath=";
      capOpaqueFile += inScan->lclPath;
      capOpaqueFile += "&mgm.inject.size=";
      char filesize[256];
      sprintf(filesize, "%" PRIu64 "", buf.st_size);
      capOpaqueFile += filesize;

      int rc = gOFS.CallManager(&error,  lFilePath.c_str(), 0, capOpaqueFile);
      if (rc) {
        eos_static_err("unable to inject file name=%s fs=%u at manager %s",
                       lFilePath.c_str(), inScan->fsId, "manager_goes_here");
      }
    }

    if (io->ftsClose(handle)) {
      eos_static_err("fts_close failed for %s", inScan->extPath.c_str());
    }

    delete handle;
  }
}

EOSFSTNAMESPACE_END
