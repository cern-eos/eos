// ----------------------------------------------------------------------
// File: Verify.cc
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
#include "fst/XrdFstOss.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/

extern eos::fst::XrdFstOss *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Verify ()
{
  // this thread unlinks stored files
  while (1)
  {
    verificationsMutex.Lock();
    if (!verifications.size())
    {
      verificationsMutex.UnLock();
      sleep(1);
      continue;
    }

    eos::fst::Verify* verifyfile = verifications.front();
    if (verifyfile)
    {
      eos_static_debug("got %llu\n", (unsigned long long) verifyfile);
      verifications.pop();
      runningVerify = verifyfile;

      {
        XrdSysMutexHelper wLock(gOFS.OpenFidMutex);
        if (gOFS.WOpenFid[verifyfile->fsId].count(verifyfile->fId))
        {
          if (gOFS.WOpenFid[verifyfile->fsId][verifyfile->fId] > 0)
          {
            eos_static_warning("file is currently opened for writing id=%x on fs=%u - skipping verification", verifyfile->fId, verifyfile->fsId);
            verifications.push(verifyfile);
            verificationsMutex.UnLock();
            continue;
          }
        }
      }
    }
    else
    {
      eos_static_debug("got nothing");
      verificationsMutex.UnLock();
      runningVerify = 0;
      continue;
    }
    verificationsMutex.UnLock();

    eos_static_debug("verifying File Id=%x on Fs=%u", verifyfile->fId, verifyfile->fsId);
    // verify the file
    XrdOucString hexfid = "";
    eos::common::FileId::Fid2Hex(verifyfile->fId, hexfid);
    XrdOucErrInfo error;

    XrdOucString fstPath = "";

    eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(), verifyfile->localPrefix.c_str(), fstPath);

    {
      FmdSqlite* fMd = 0;
      fMd = gFmdSqliteHandler.GetFmd(verifyfile->fId, verifyfile->fsId, 0, 0, 0, 0, true);
      if (fMd)
      {
        // force a resync of meta data from the MGM
        // e.g. store in the WrittenFilesQueue to have it done asynchronous
        gOFS.WrittenFilesQueueMutex.Lock();
        gOFS.WrittenFilesQueue.push(fMd->fMd);
        gOFS.WrittenFilesQueueMutex.UnLock();
        delete fMd;
      }
    }

    // get current size on disk
    struct stat statinfo;
    if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo)))
    {
      eos_static_err("unable to verify file id=%x on fs=%u path=%s - stat on local disk failed", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
      // if there is no file, we should not commit anything to the MGM
      verifyfile->commitSize = 0;
      verifyfile->commitChecksum = 0;
      statinfo.st_size = 0; // indicates the missing file - not perfect though
    }

    // even if the stat failed, we run this code to tag the file as is ...
    // attach meta data
    FmdSqlite* fMd = 0;
    fMd = gFmdSqliteHandler.GetFmd(verifyfile->fId, verifyfile->fsId, 0, 0, 0, verifyfile->commitFmd, true);
    bool localUpdate = false;
    if (!fMd)
    {
      eos_static_err("unable to verify id=%x on fs=%u path=%s - no local MD stored", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
    }
    else
    {
      if (fMd->fMd.size != (unsigned long long) statinfo.st_size)
      {
        eos_static_err("updating file size: path=%s fid=%s fs value %llu - changelog value %llu", verifyfile->path.c_str(), hexfid.c_str(), statinfo.st_size, fMd->fMd.size);
        localUpdate = true;
      }

      if (fMd->fMd.lid != verifyfile->lId)
      {
        eos_static_err("updating layout id: path=%s fid=%s central value %u - changelog value %u", verifyfile->path.c_str(), hexfid.c_str(), verifyfile->lId, fMd->fMd.lid);
        localUpdate = true;
      }

      if (fMd->fMd.cid != verifyfile->cId)
      {
        eos_static_err("updating container: path=%s fid=%s central value %llu - changelog value %llu", verifyfile->path.c_str(), hexfid.c_str(), verifyfile->cId, fMd->fMd.cid);
        localUpdate = true;
      }

      // update size
      fMd->fMd.size = statinfo.st_size;
      fMd->fMd.lid = verifyfile->lId;
      fMd->fMd.cid = verifyfile->cId;

      // if set recalculate the checksum
      CheckSum* checksummer = ChecksumPlugins::GetChecksumObject(fMd->fMd.lid);

      unsigned long long scansize = 0;
      float scantime = 0; // is ms

      if ((checksummer) && verifyfile->computeChecksum && (!checksummer->ScanFile(fstPath.c_str(), scansize, scantime, verifyfile->verifyRate)))
      {
        eos_static_crit("cannot scan file to recalculate the checksum id=%llu on fs=%u path=%s", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
      }
      else
      {
        XrdOucString sizestring;
        if (checksummer && verifyfile->computeChecksum)
          eos_static_info("rescanned checksum - size=%s time=%.02fms rate=%.02f MB/s limit=%d MB/s", eos::common::StringConversion::GetReadableSizeString(sizestring, scansize, "B"), scantime, 1.0 * scansize / 1000 / (scantime ? scantime : 99999999999999LL), verifyfile->verifyRate);

        if (checksummer && verifyfile->computeChecksum)
        {
          int checksumlen = 0;
          checksummer->GetBinChecksum(checksumlen);

          // check if the computed checksum differs from the one in the change log
          bool cxError = false;
          std::string computedchecksum = checksummer->GetHexChecksum();

          if (fMd->fMd.checksum != computedchecksum)
            cxError = true;

          // commit the disk checksum in case of differences between the in-memory value
          if (fMd->fMd.diskchecksum != computedchecksum)
            localUpdate = true;

          if (cxError)
          {
            eos_static_err("checksum invalid   : path=%s fid=%s checksum=%s stored-checksum=%s", verifyfile->path.c_str(), hexfid.c_str(), checksummer->GetHexChecksum(), fMd->fMd.checksum.c_str());
            fMd->fMd.checksum = computedchecksum;
            fMd->fMd.diskchecksum = computedchecksum;
            fMd->fMd.disksize = fMd->fMd.size;
            if (verifyfile->commitSize)
            {
              fMd->fMd.mgmsize = fMd->fMd.size;
            }

            if (verifyfile->commitChecksum)
            {
              fMd->fMd.mgmchecksum = computedchecksum;
            }
            localUpdate = true;
          }
          else
          {
            eos_static_info("checksum OK        : path=%s fid=%s checksum=%s", verifyfile->path.c_str(), hexfid.c_str(), checksummer->GetHexChecksum());
          }
          eos::common::Attr *attr = eos::common::Attr::OpenAttr(fstPath.c_str());
          if (attr)
          {
            // update the extended attributes
            attr->Set("user.eos.checksum", checksummer->GetBinChecksum(checksumlen), checksumlen);
            attr->Set(std::string("user.eos.checksumtype"), std::string(checksummer->GetName()));
            attr->Set("user.eos.filecxerror", "0");
            delete attr;
          }
        }

        eos::common::Path cPath(verifyfile->path.c_str());

        // commit local
        if (localUpdate && (!gFmdSqliteHandler.Commit(fMd)))
        {
          eos_static_err("unable to verify file id=%llu on fs=%u path=%s - commit to local MD storage failed", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
        }
        else
        {
          if (localUpdate) eos_static_info("commited verified meta data locally id=%llu on fs=%u path=%s", verifyfile->fId, verifyfile->fsId, fstPath.c_str());

          // commit to central mgm cache, only if commitSize or commitChecksum is set
          XrdOucString capOpaqueFile = "";
          XrdOucString mTimeString = "";
          capOpaqueFile += "/?";
          capOpaqueFile += "&mgm.pcmd=commit";
          capOpaqueFile += "&mgm.verify.checksum=1";
          capOpaqueFile += "&mgm.size=";
          char filesize[1024];
          sprintf(filesize, "%llu", fMd->fMd.size);
          capOpaqueFile += filesize;
          capOpaqueFile += "&mgm.fid=";
          capOpaqueFile += hexfid;
          capOpaqueFile += "&mgm.path=";
          capOpaqueFile += verifyfile->path.c_str();

          if (checksummer && verifyfile->computeChecksum)
          {
            capOpaqueFile += "&mgm.checksum=";
            capOpaqueFile += checksummer->GetHexChecksum();
            if (verifyfile->commitChecksum)
            {
              capOpaqueFile += "&mgm.commit.checksum=1";
            }
          }

          if (verifyfile->commitSize)
          {
            capOpaqueFile += "&mgm.commit.size=1";
          }

          capOpaqueFile += "&mgm.mtime=";
          capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long) fMd->fMd.mtime);
          capOpaqueFile += "&mgm.mtime_ns=";
          capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long) fMd->fMd.mtime_ns);

          capOpaqueFile += "&mgm.add.fsid=";
          capOpaqueFile += (int) fMd->fMd.fsid;

          if (verifyfile->commitSize || verifyfile->commitChecksum)
          {
            if (localUpdate) eos_static_info("commited verified meta data centrally id=%llu on fs=%u path=%s", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
            int rc = gOFS.CallManager(&error, verifyfile->path.c_str(), verifyfile->managerId.c_str(), capOpaqueFile);
            if (rc)
            {
              eos_static_err("unable to verify file id=%s fs=%u at manager %s", hexfid.c_str(), verifyfile->fsId, verifyfile->managerId.c_str());
            }
          }
        }
      }
      if (checksummer)
      {
        delete checksummer;
      }
      if (fMd)
      {
        delete fMd;
      }
    }
    runningVerify = 0;
    if (verifyfile) delete verifyfile;
  }
}

EOSFSTNAMESPACE_END


