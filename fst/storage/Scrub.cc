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

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__
#define O_DIRECT 0
#endif

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Scrub ()
{
  // create a 1M pattern
  eos_static_info("Creating Scrubbing pattern ...");
  for (int i = 0; i < 1024 * 1024 / 8; i += 2)
  {
    scrubPattern[0][i] = 0xaaaa5555aaaa5555ULL;
    scrubPattern[0][i + 1] = 0x5555aaaa5555aaaaULL;
    scrubPattern[1][i] = 0x5555aaaa5555aaaaULL;
    scrubPattern[1][i + 1] = 0xaaaa5555aaaa5555ULL;
  }


  eos_static_info("Start Scrubbing ...");

  // this thread reads the oldest files and checks their integrity
  while (1)
  {
    time_t start = time(0);
    unsigned int nfs = 0;
    {
      eos::common::RWMutexReadLock lock(fsMutex);
      nfs = fileSystemsVector.size();
      eos_static_debug("FileSystem Vector %u", nfs);
    }
    for (unsigned int i = 0; i < nfs; i++)
    {
      fsMutex.LockRead();

      if (i < fileSystemsVector.size())
      {
        std::string path = fileSystemsVector[i]->GetPath();

        if (!fileSystemsVector[i]->GetStatfs())
        {
          eos_static_info("GetStatfs failed");
          fsMutex.UnLockRead();
          continue;
        }


        unsigned long long free = fileSystemsVector[i]->GetStatfs()->GetStatfs()->f_bfree;
        unsigned long long blocks = fileSystemsVector[i]->GetStatfs()->GetStatfs()->f_blocks;
        unsigned long id = fileSystemsVector[i]->GetId();
        eos::common::FileSystem::fsstatus_t bootstatus = fileSystemsVector[i]->GetStatus();
        eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[i]->GetConfigStatus();

        fsMutex.UnLockRead();

        if (!id)
          continue;

        // check if there is a lable on the disk and if the configuration shows the same fsid
        if ((bootstatus == eos::common::FileSystem::kBooted) &&
            (configstatus >= eos::common::FileSystem::kRO) &&
            (!CheckLabel(fileSystemsVector[i]->GetPath(), fileSystemsVector[i]->GetId(), fileSystemsVector[i]->GetString("uuid"), true)))
        {
          fileSystemsVector[i]->BroadcastError(EIO, "filesystem seems to be not mounted anymore");
          continue;
        }


        // don't scrub on filesystems which are not in writable mode!
        if (configstatus < eos::common::FileSystem::kWO)
          continue;

        // don't scrub on filesystems which are not booted
        if (bootstatus != eos::common::FileSystem::kBooted)
          continue;


        // don't scrub filesystems which are 'remote'
        if (path[0] != '/')
          continue;

        if (ScrubFs(path.c_str(), free, blocks, id))
        {
          // filesystem has errors!
          fsMutex.LockRead();
          if ((i < fileSystemsVector.size()) && fileSystemsVector[i])
          {
            fileSystemsVector[i]->BroadcastError(EIO, "filesystem probe error detected");
          }
          fsMutex.UnLockRead();
        }
      }
      else
      {
        fsMutex.UnLockRead();
      }
    }
    time_t stop = time(0);

    int nsleep = ((300)-(stop - start));
    if (nsleep > 0)
    {
      eos_static_debug("Scrubber will pause for %u seconds", nsleep);
      XrdSysTimer sleeper;
      sleeper.Snooze(nsleep);
    }
  }
}

/*----------------------------------------------------------------------------*/
int
Storage::ScrubFs (const char* path, unsigned long long free, unsigned long long blocks, unsigned long id)
{
  int MB = 1; // the test files have 1 MB

  int index = 10 - (int) (10.0 * free / blocks);

  eos_static_debug("Running Scrubber on filesystem path=%s id=%u free=%llu blocks=%llu index=%d", path, id, free, blocks, index);

  int fserrors = 0;

  for (int fs = 1; fs <= index; fs++)
  {
    // check if test file exists, if not, write it
    XrdOucString scrubfile[2];
    scrubfile[0] = path;
    scrubfile[1] = path;
    scrubfile[0] += "/scrub.write-once.";
    scrubfile[0] += fs;
    scrubfile[1] += "/scrub.re-write.";
    scrubfile[1] += fs;
    struct stat buf;

    for (int k = 0; k < 2; k++)
    {
      eos_static_debug("Scrubbing file %s", scrubfile[k].c_str());
      if (((k == 0) && stat(scrubfile[k].c_str(), &buf)) || ((k == 0) && (buf.st_size != (MB * 1024 * 1024))) || ((k == 1)))
      {
        // ok, create this file once
        int ff = 0;
        if (k == 0)
          ff = open(scrubfile[k].c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_DIRECT, S_IRWXU);
        else
          ff = open(scrubfile[k].c_str(), O_CREAT | O_WRONLY | O_DIRECT, S_IRWXU);

        if (ff < 0)
        {
          eos_static_crit("Unable to create/wopen scrubfile %s", scrubfile[k].c_str());
          fserrors = 1;
          break;
        }
        // select the pattern randomly
        int rshift = (int) ((1.0 * rand() / RAND_MAX) + 0.5);
        eos_static_debug("rshift is %d", rshift);
        for (int i = 0; i < MB; i++)
        {
          int nwrite = write(ff, scrubPattern[rshift], 1024 * 1024);
          if (nwrite != (1024 * 1024))
          {
            eos_static_crit("Unable to write all needed bytes for scrubfile %s", scrubfile[k].c_str());
            fserrors = 1;
            break;
          }
          if (k != 0)
          {
            XrdSysTimer msSleep;
            msSleep.Wait(100);
          }
        }
        close(ff);
      }

      // do a read verify
      int ff = open(scrubfile[k].c_str(), O_DIRECT | O_RDONLY);
      if (ff < 0)
      {
        eos_static_crit("Unable to open static scrubfile %s", scrubfile[k].c_str());
        return 1;
      }

      int eberrors = 0;

      for (int i = 0; i < MB; i++)
      {
        int nread = read(ff, scrubPatternVerify, 1024 * 1024);
        if (nread != (1024 * 1024))
        {
          eos_static_crit("Unable to read all needed bytes from scrubfile %s", scrubfile[k].c_str());
          fserrors = 1;
          break;
        }
        unsigned long long* ref = (unsigned long long*) scrubPattern[0];
        unsigned long long* cmp = (unsigned long long*) scrubPatternVerify;
        // do a quick check
        for (int b = 0; b < MB * 1024 / 8; b++)
        {
          if ((*ref != *cmp))
          {
            ref = (unsigned long long*) scrubPattern[1];
            if (*(ref) == *cmp)
            {
              // ok - pattern shifted
            }
            else
            {
              // this is real fatal error
              eberrors++;
            }
          }
        }
        XrdSysTimer msSleep;
        msSleep.Wait(100);
      }
      if (eberrors)
      {
        eos_static_alert("%d block errors on filesystem %lu scrubfile %s", eberrors, id, scrubfile[k].c_str());
        fserrors++;
      }
      close(ff);
    }
  }

  if (fserrors)
  {
    return 1;
  }

  return 0;
}

EOSFSTNAMESPACE_END


