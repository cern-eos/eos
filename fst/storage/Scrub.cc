// ----------------------------------------------------------------------
// File: Scrub.cc
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

#include "fst/storage/Storage.hh"
#include "fst/storage/FileSystem.hh"
#include <fcntl.h>

#ifdef __APPLE__
#define O_DIRECT 0
#endif

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Scrub()
{
  // create a 1M pattern
  eos_info("%s", "msg=\"create scrubbing pattern ...\"");

  for (int i = 0; i < 1024 * 1024 / 8; i += 2) {
    mScrubPattern[0][i] = 0xaaaa5555aaaa5555ULL;
    mScrubPattern[0][i + 1] = 0x5555aaaa5555aaaaULL;
    mScrubPattern[1][i] = 0x5555aaaa5555aaaaULL;
    mScrubPattern[1][i + 1] = 0xaaaa5555aaaa5555ULL;
  }

  eos_info("%s", "msg=\"start scrubbing\"");

  // this thread reads the oldest files and checks their integrity
  while (true) {
    time_t start = time(0);
    std::set<eos::common::FileSystem::fsid_t> fsids;
    // Collect all file system ids registered
    {
      eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);

      for (const auto& elem : mFsMap) {
        fsids.insert(elem.first);
      }
    }
    eos_debug("msg=\"running on %lu file systems\"", fsids.size());
    std::string path {""};
    uint64_t free {0ull};
    uint64_t blocks {0ull};
    bool direct_io = false;
    eos::common::BootStatus boot_st;
    eos::common::ConfigStatus config_st;

    for (auto fsid : fsids) {
      {
        eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
        auto it = mFsMap.find(fsid);

        if (it == mFsMap.end()) {
          eos_warning("msg=\"skip removed file system\" fsid=%lu", fsid);
          continue;
        }

        auto fs = it->second;
        path = fs->GetPath();

        if (fs->GetStatfs() == nullptr) {
          eos_notice("msg=\"statfs failed on file system\" fsid=%lu path=\"%s\"",
                     fsid, path.c_str());
          continue;
        }

        free = fs->GetStatfs()->GetStatfs()->f_bfree;
        blocks = fs->GetStatfs()->GetStatfs()->f_blocks;
        // Disable direct IO for ZFS
        direct_io = (fs->GetStatfs()->GetStatfs()->f_type != 0x2fc12fc1);
        boot_st = fs->GetStatus();
        config_st = fs->GetConfigStatus();
      }

      // Skip scrubbing file systems for which either of the following
      // conditions hold:
      // - not a local file system (i.e. remote)
      // - not in writable mode
      // - not booted
      if (path.empty() || (path[0] != '/') ||
          (config_st < eos::common::ConfigStatus::kWO) ||
          (boot_st != eos::common::BootStatus::kBooted)) {
        continue;
      }

      struct stat buf;

      std::string no_scrub = path + "/" + ".eosnoscrub";

      if (!::stat(no_scrub.c_str(), &buf)) {
        eos_debug("msg=\"scrub is disabled, remove %s to activate\"",
                  no_scrub.c_str());
        continue;
      }

      if (ScrubFs(path.c_str(), free, blocks, fsid, direct_io)) {
        // Filesystem has errors
        eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
        auto it = mFsMap.find(fsid);

        if (it == mFsMap.end()) {
          eos_warning("msg=\"skip removed file system\" fsid=%lu", fsid);
          continue;
        }

        it->second->BroadcastError(EIO, "filesystem probe error detected");
      }
    }

    time_t stop = time(0);
    int nsleep = ((300) - (stop - start));

    if (nsleep > 0) {
      eos_debug("msg=\"scrubber will pause for %u seconds\"", nsleep);
      std::this_thread::sleep_for(std::chrono::seconds(nsleep));
    }
  }
}

//------------------------------------------------------------------------------
// Scrub filesystem
//------------------------------------------------------------------------------
int
Storage::ScrubFs(const char* path, unsigned long long free,
                 unsigned long long blocks, unsigned long id, bool direct_io)
{
  int MB = 1; // the test files have 1 MB
  int index = 10 - (int)(10.0 * free / blocks);
  eos_static_debug("Running Scrubber on filesystem path=%s id=%u free=%llu blocks=%llu index=%d",
                   path, id, free, blocks, index);
  int fserrors = 0;

  for (int fs = 1; fs <= index; fs++) {
    // check if test file exists, if not, write it
    XrdOucString scrubfile[2];
    scrubfile[0] = path;
    scrubfile[1] = path;
    scrubfile[0] += "/scrub.write-once.";
    scrubfile[0] += fs;
    scrubfile[1] += "/scrub.re-write.";
    scrubfile[1] += fs;
    struct stat buf;
    int dflags = 0;

    if (direct_io) {
      dflags = O_DIRECT;
    }

    for (int k = 0; k < 2; k++) {
      eos_static_debug("Scrubbing file %s", scrubfile[k].c_str());

      if (((k == 0) && stat(scrubfile[k].c_str(), &buf)) || ((k == 0) &&
          (buf.st_size != (MB * 1024 * 1024))) || ((k == 1))) {
        // ok, create this file once
        int ff = 0;

        if (k == 0) {
          ff = open(scrubfile[k].c_str(), O_CREAT | O_TRUNC | O_WRONLY | dflags,
                    S_IRWXU);
        } else {
          ff = open(scrubfile[k].c_str(), O_CREAT | O_WRONLY | dflags, S_IRWXU);
        }

        if (ff < 0) {
          eos_static_crit("Unable to create/wopen scrubfile %s errno=%d",
                          scrubfile[k].c_str(), errno);
          fserrors = 1;
          break;
        }

        // select the pattern randomly
        int rshift = (int)((1.0 * rand() / RAND_MAX) + 0.5);
        eos_static_debug("rshift is %d", rshift);

        for (int i = 0; i < MB; i++) {
          int nwrite = write(ff, mScrubPattern[rshift], 1024 * 1024);

          if (nwrite != (1024 * 1024)) {
            eos_static_crit("Unable to write all needed bytes for scrubfile %s errno=%d",
                            scrubfile[k].c_str(), errno);
            fserrors = 1;
            break;
          }

          if (k != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
          }
        }

        close(ff);
      }

      // do a read verify
      int ff = open(scrubfile[k].c_str(), dflags | O_RDONLY);

      if (ff < 0) {
        eos_static_crit("Unable to open static scrubfile %s, errno=%d",
                        scrubfile[k].c_str(), errno);
        return 1;
      }

      int eberrors = 0;

      for (int i = 0; i < MB; i++) {
        int nread = read(ff, mScrubPatternVerify, 1024 * 1024);

        if (nread != (1024 * 1024)) {
          eos_static_crit("Unable to read all needed bytes from scrubfile %s errno=%d",
                          scrubfile[k].c_str(), errno);
          fserrors = 1;
          break;
        }

        unsigned long long* ref = (unsigned long long*) mScrubPattern[0];
        unsigned long long* cmp = (unsigned long long*) mScrubPatternVerify;

        // do a quick check
        for (int b = 0; b < MB * 1024 / 8; b++) {
          if ((*ref != *cmp)) {
            ref = (unsigned long long*) mScrubPattern[1];

            if (*(ref) == *cmp) {
              // ok - pattern shifted
            } else {
              // this is real fatal error
              eberrors++;
            }
          }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }

      if (eberrors) {
        eos_static_alert("%d block errors on filesystem %lu scrubfile %s", eberrors, id,
                         scrubfile[k].c_str());
        fserrors++;
      }

      close(ff);
    }
  }

  if (fserrors) {
    return 1;
  }

  return 0;
}

EOSFSTNAMESPACE_END
