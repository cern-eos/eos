// ----------------------------------------------------------------------
// File: FileSystem.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                
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
#include "fst/storage/FileSystem.hh"
#include "fst/XrdFstOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

#ifdef __APPLE__ 
#define O_DIRECT 0
#endif

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
FileSystem::FileSystem (const char* queuepath,
                        const char* queue, XrdMqSharedObjectManager* som
                        ) : eos::common::FileSystem (queuepath, queue, som, true)
{
  last_blocks_free = 0;
  last_status_broadcast = 0;
  seqBandwidth = 0;
  IOPS = 0;

  transactionDirectory = "";
  statFs = 0;
  scanDir = 0;
  std::string n1 = queuepath;
  n1 += "/drain";
  std::string n2 = queuepath;
  n2 += "/balance";
  std::string n3 = queuepath;
  n3 += "/extern";
  
  mLocalBootStatus = eos::common::FileSystem::kDown;
  
  mTxDrainQueue = new TransferQueue(&mDrainQueue, n1.c_str());
  mTxBalanceQueue = new TransferQueue(&mBalanceQueue, n2.c_str());
  mTxExternQueue = new TransferQueue(&mExternQueue, n3.c_str());

  mTxMultiplexer.Add(mTxDrainQueue);
  mTxMultiplexer.Add(mTxBalanceQueue);
  mTxMultiplexer.Add(mTxExternQueue);
  mTxMultiplexer.Run();

}

/*----------------------------------------------------------------------------*/
FileSystem::~FileSystem ()
{
  if (scanDir)
  {
    delete scanDir;
  }

  // ----------------------------------------------------------------------------
  // we call the FmdSqliteHandler shutdown function for this filesystem
  // ----------------------------------------------------------------------------

  gFmdSqliteHandler.ShutdownDB(GetId());

  // ----------------------------------------------------------------------------
  // @todo we accept this tiny memory leak to be able to let running 
  // transfers callback their queue
  // -> we don't delete them here!
  //  if (mTxDrainQueue) {
  //    delete mTxDrainQueue;
  //  }
  //  if (mTxBalanceQueue) {
  //    delete mTxBalanceQueue;
  //  }
  //  if (mTxExternQueue) {
  //    delete mTxExternQueue;
  //  }
  // ----------------------------------------------------------------------------
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastError (const char* msg)
{
  bool shutdown=false;
  {
    XrdSysMutexHelper sLock(gOFS.ShutdownMutex);
    if (gOFS.Shutdown)
      shutdown = true;
  }

  if(!shutdown) 
  {
    SetStatus(eos::common::FileSystem::kOpsError);
    SetError(errno ? errno : EIO, msg);
  }
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastError (int errc, const char* errmsg)
{
  bool shutdown = false;
  {
    XrdSysMutexHelper sLock(gOFS.ShutdownMutex);
    if (gOFS.Shutdown)
      shutdown = true;
  }

  if (!shutdown) 
  {
    SetStatus(eos::common::FileSystem::kOpsError);
    SetError(errno ? errno : EIO, errmsg);
  }
}

/*----------------------------------------------------------------------------*/
eos::common::Statfs*
FileSystem::GetStatfs ()
{
  statFs = eos::common::Statfs::DoStatfs(GetPath().c_str());
  if ((!statFs) && GetPath().length())
  {
    eos_err("cannot statfs");
    BroadcastError("cannot statfs");
    return 0;
  }

  return statFs;
}

/*----------------------------------------------------------------------------*/
void
FileSystem::CleanTransactions ()
{
  DIR* tdir = opendir(GetTransactionDirectory());
  if (tdir)
  {
    struct dirent* name;
    while ((name = readdir(tdir)))
    {
      XrdOucString sname = name->d_name;
      // skipp . & ..
      if (sname.beginswith("."))
        continue;
      XrdOucString fulltransactionpath = GetTransactionDirectory();
      fulltransactionpath += "/";
      fulltransactionpath += name->d_name;
      struct stat buf;
      if (!stat(fulltransactionpath.c_str(), &buf))
      {
        XrdOucString hexfid = name->d_name;
        XrdOucString localprefix = GetPath().c_str();
        XrdOucString fstPath;
        eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(), localprefix.c_str(), fstPath);
        unsigned long long fileid = eos::common::FileId::Hex2Fid(hexfid.c_str());

        // we allow to keep files open for 1 week
        bool isOpen = false;
        {
          XrdSysMutexHelper wLock(gOFS.OpenFidMutex);
          if (gOFS.WOpenFid[GetId()].count(fileid))
          {
            if (gOFS.WOpenFid[GetId()][fileid] > 0)
            {
              isOpen = true;
            }
          }
        }

	if ((buf.st_mtime < (time(NULL) - (7 * 86400))) && (!isOpen))
        {

	  FmdSqlite* fMd = 0;
	  fMd = gFmdSqliteHandler.GetFmd(fileid, GetId(), 0, 0, 0, 0, true);

	  if (fMd) 
	  {
	    std::set<eos::common::FileSystem::fsid_t> location_set = FmdSqlite::GetLocations(fMd->fMd);
	    if (location_set.count(GetId()))
	    {
	      // close that transaction and keep the file
	      gOFS.Storage->CloseTransaction(GetId(), fileid);
	      delete fMd;
	      continue;
	    }
	    delete fMd;
	  }

          eos_static_info("action=delete transaction=%llx fstpath=%s",
                          sname.c_str(),
                          fulltransactionpath.c_str());

          // -------------------------------------------------------------------------------------------------------
          // clean-up this file locally
          // -------------------------------------------------------------------------------------------------------

          XrdOucErrInfo error;
          int retc = gOFS._rem("/CLEANTRANSACTIONS",
                               error, 0, 0, fstPath.c_str(),
                               fileid, GetId(), true);
          if (retc)
          {
            eos_static_debug("deletion failed for %s", fstPath.c_str());
          }
        }
        else
        {
          eos_static_info("action=keep transaction=%llx fstpath=%s isopen=%d",
                          sname.c_str(), fulltransactionpath.c_str(), isOpen);
        }
      }
    }
    closedir(tdir);
  }
  else
  {
    eos_static_err("Unable to open transactiondirectory %s",
                   GetTransactionDirectory());
  }
}

/*----------------------------------------------------------------------------*/
void
FileSystem::SyncTransactions (const char* manager)
{
  DIR* tdir = opendir(GetTransactionDirectory());
  if (tdir)
  {
    struct dirent* name;
    while ((name = readdir(tdir)))
    {
      XrdOucString sname = name->d_name;
      // skipp . & ..
      if (sname.beginswith("."))
        continue;
      XrdOucString fulltransactionpath = GetTransactionDirectory();
      fulltransactionpath += "/";
      fulltransactionpath += name->d_name;
      struct stat buf;
      if (!stat(fulltransactionpath.c_str(), &buf))
      {
        XrdOucString hexfid = name->d_name;
        const char* localprefix = GetPath().c_str();
        XrdOucString fstPath;
        eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(),
                                                localprefix, fstPath);
        unsigned long long fid = eos::common::FileId::Hex2Fid(hexfid.c_str());

	// try to sync this file from the MGM
	if (gFmdSqliteHandler.ResyncMgm(GetId(), fid, manager))
	{
	  eos_static_info("msg=\"resync ok\" fsid=%lu fid=%llx", (unsigned long) GetId(), fid);
	}
	else
	{
	  eos_static_err("msg=\"resync failed\" fsid=%lu fid=%llx", (unsigned long) GetId(), fid);
	  continue;
	}
      }
    }
    closedir(tdir);
  }
  else
  {
    eos_static_err("Unable to open transactiondirectory %s",
                   GetTransactionDirectory());
  }
}

/*----------------------------------------------------------------------------*/
void
FileSystem::RunScanner (Load* fstLoad, time_t interval)
{
  if (scanDir)
  {
    delete scanDir;
  }
  
  // create the object running the scanner thread
  scanDir = new ScanDir(GetPath().c_str(), GetId(), fstLoad, true, interval);
  eos_info("Started 'ScanDir' thread with interval time of %u seconds",
           (unsigned long) interval);
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::OpenTransaction (unsigned long long fid)
{
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  XrdOucString hexstring = "";
  eos::common::FileId::Fid2Hex(fid, hexstring);
  tagfile += hexstring;
  int fd = open(tagfile.c_str(),
                O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH | S_IRGRP);
  if (fd > 0)
  {
    close(fd);
    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::CloseTransaction (unsigned long long fid)
{
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  XrdOucString hexstring = "";
  eos::common::FileId::Fid2Hex(fid, hexstring);
  tagfile += hexstring;
  if (unlink(tagfile.c_str()))
    return false;
  return true;
}


/*----------------------------------------------------------------------------*/
void
FileSystem::IoPing ()
{
  std::string cmdbw="eos-iobw "; cmdbw += GetPath().c_str();
  std::string cmdiops="eos-iops "; cmdiops += GetPath().c_str();

  eos_info("\"%s\" \"%s\"", cmdbw.c_str(), cmdiops.c_str());

  std::string bwMeasurement = eos::common::StringConversion::StringFromShellCmd (cmdbw.c_str());
  std::string iopsMeasurement = eos::common::StringConversion::StringFromShellCmd (cmdiops.c_str());

  if ( 
      bwMeasurement.length() &&
      iopsMeasurement.length() )
  {
    seqBandwidth = strtol(bwMeasurement.c_str(),0,10);
    IOPS = atoi(iopsMeasurement.c_str());
  }
  else
  {
    seqBandwidth = 0;
    IOPS = 0;
  }
  eos_info("bw=%lld iops=%d",seqBandwidth,IOPS);
}


EOSFSTNAMESPACE_END
