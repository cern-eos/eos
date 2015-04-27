// ----------------------------------------------------------------------
// File: Converter.cc
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
#include "mgm/Converter.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"
/*----------------------------------------------------------------------------*/
extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

XrdSysMutex eos::mgm::Converter::gSchedulerMutex;
XrdScheduler* eos::mgm::Converter::gScheduler;
XrdSysMutex eos::mgm::Converter::gConverterMapMutex;
std::map<std::string, Converter*> eos::mgm::Converter::gConverterMap;

/*----------------------------------------------------------------------------*/
ConverterJob::ConverterJob (eos::common::FileId::fileid_t fid,
                            const char* conversionlayout,
                            std::string &convertername)
: mFid (fid),
mConversionLayout (conversionlayout),
mConverterName (convertername)
/*----------------------------------------------------------------------------*/
/*
 * @brief Constructor of a conversion job
 * 
 * @param fid file id of the file to convert
 * @param conversionlayout string describing the conversion layout to use
 * @param convertername to be used
 */
/*----------------------------------------------------------------------------*/
{
  mProcPath = gOFS->MgmProcConversionPath.c_str();
  mProcPath += "/";
  char xfid[20];
  snprintf(xfid, sizeof (xfid), "%016llx", (long long) mFid);
  mProcPath += "/";
  mProcPath += xfid;
  mProcPath += ":";
  mProcPath += conversionlayout;
}

/*----------------------------------------------------------------------------*/
void
ConverterJob::DoIt ()
/*----------------------------------------------------------------------------*/
/*
 * @brief run a third-party conversion transfer
 * 
 */
/*----------------------------------------------------------------------------*/
{
  eos_static_info("msg=\"start tpc job\" fxid=%016x layout=%s proc_path=%s",
                  mFid, mConversionLayout.c_str(), mProcPath.c_str());
  XrdSysTimer sleeper;

  eos::FileMD* fmd = 0;
  eos::IContainerMD* cmd = 0;
  uid_t owner_uid = 0;
  gid_t owner_gid = 0;
  unsigned long long size = 0;
  eos::IContainerMD::XAttrMap attrmap;

  XrdOucString sourceChecksum;
  XrdOucString sourceAfterChecksum;
  XrdOucString sourceSize;
  Converter* startConverter = 0;
  Converter* stopConverter = 0;
  {
    XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
    startConverter = Converter::gConverterMap[mConverterName];
  }

  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
    try
    {
      fmd = gOFS->eosFileService->getFileMD(mFid);
      owner_uid = fmd->getCUid();
      owner_gid = fmd->getCGid();
      size = fmd->getSize();
      mSourcePath = gOFS->eosView->getUri(fmd);
      eos::common::Path cPath(mSourcePath.c_str());
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      // load the extended attributes
      eos::IContainerMD::XAttrMap::const_iterator it;
      for (it = cmd->attributesBegin(); it != cmd->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }

      // get the checksum string if defined
      for (unsigned int i = 0;
        i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++)
      {
        char hb[3];
        sprintf(hb, "%02x", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
        sourceChecksum += hb;
      }

      // get the size string
      eos::common::StringConversion::GetSizeString(sourceSize,
                                                   (unsigned long long) fmd->getSize());

      std::string conversionattribute = "sys.conversion.";
      conversionattribute += mConversionLayout.c_str();
      XrdOucString lEnv;
      const char* val = 0;
      if (attrmap.count(mConversionLayout.c_str()))
      {
        // conversion layout can either point to a conversion attribute definition in the parent directory
        val =
          eos::common::LayoutId::GetEnvFromConversionIdString(lEnv, attrmap[mConversionLayout.c_str()].c_str());
      }
      else
      {
        // or can be directly a hexadecimal layout representation or env representation
        val =
          eos::common::LayoutId::GetEnvFromConversionIdString(lEnv, mConversionLayout.c_str());
      }

      if (val)
      {
        mTargetCGI = val;
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_static_err("fid=%016x errno=%d msg=\"%s\"\n",
                     mFid, e.getErrno(), e.getMessage().str().c_str());
    }
  }

  bool success = false;
  if (mTargetCGI.length())
  {
    // -------------------------------------------------------------------------
    // this is properly defined job
    // -------------------------------------------------------------------------
    eos_static_info("msg=\"conversion layout correct\" fxid=%016x cgi=\"%s\"",
                    mFid, mTargetCGI.c_str());

    // -------------------------------------------------------------------------
    // prepare the TPC copy job
    // -------------------------------------------------------------------------

    if (!size)
    {
      // empty files don't work/need with TPC
      mTPCJob.thirdParty = false;
      mTPCJob.thirdPartyFallBack = false;
    }
    else
    {
      // non-empty files run with TPC
      mTPCJob.thirdParty = true;
      mTPCJob.thirdPartyFallBack = false;
    }
    mTPCJob.force = true;
    mTPCJob.posc = false;
    mTPCJob.coerce = false;

    std::string source = mSourcePath.c_str();
    std::string target = mProcPath.c_str();

    std::string cgi = "eos.ruid=2&eos.rgid=2&";
    cgi += mTargetCGI.c_str();
    cgi += "&eos.app=converter";
    cgi += "&eos.targetsize=";
    cgi += sourceSize.c_str();
    if (sourceChecksum.length())
    {
      cgi += "&eos.checksum=";
      cgi += sourceChecksum.c_str();
    }
    mTPCJob.source.SetProtocol("root");
    mTPCJob.source.SetHostName("localhost");
    mTPCJob.source.SetUserName("root");
    mTPCJob.source.SetParams("eos.app=converter");
    mTPCJob.target.SetProtocol("root");
    mTPCJob.target.SetHostName("localhost");
    mTPCJob.target.SetUserName("root");
    mTPCJob.target.SetParams(cgi);
    mTPCJob.source.SetPath(source);
    mTPCJob.source.SetParams("eos.ruid=0&eos.rgid=0");
    mTPCJob.target.SetPath(target);
    mTPCJob.sourceLimit = 1;
    mTPCJob.checkSumPrint = false;
    mTPCJob.chunkSize = 4 * 1024 * 1024;
    mTPCJob.parallelChunks = 1;

    XrdCl::CopyProcess lCopyProcess;
    lCopyProcess.AddJob(&mTPCJob);

    XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();

    eos_static_info("[tpc]: %s=>%s %s",
                    mTPCJob.source.GetURL().c_str(),
                    mTPCJob.target.GetURL().c_str(),
                    lTpcPrepareStatus.ToStr().c_str());

    if (lTpcPrepareStatus.IsOK())
    {
      XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
      eos_static_info("[tpc]: %s %d", lTpcStatus.ToStr().c_str(), lTpcStatus.IsOK());
      if (lTpcStatus.IsOK())
      {
        success = true;
      }
      else
      {
        success = false;
      }
    }
    else
    {
      success = false;
    }
  }
  else
  {
    // -------------------------------------------------------------------------
    // this is a crappy defined job
    // -------------------------------------------------------------------------
    eos_static_err("msg=\"conversion layout definition wrong\" fxid=%016x layout=%s",
                   mFid, mConversionLayout.c_str());
    success = false;
  }

  // ---------------------------------------------------------------------------
  // check if the file is still the same on source side
  // ---------------------------------------------------------------------------
  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
    try
    {
      fmd = gOFS->eosFileService->getFileMD(mFid);

      // get the checksum string if defined
      for (unsigned int i = 0;
        i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++)
      {
        char hb[3];
        sprintf(hb, "%02x", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
        sourceAfterChecksum += hb;
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_static_err("fid=%016x errno=%d msg=\"%s\"\n",
                     mFid, e.getErrno(), e.getMessage().str().c_str());
    }

    if (sourceChecksum != sourceAfterChecksum)
    {
      success = false;
      eos_static_err("fid=%016x conversion failed since file was modified",
                     mFid);
    }
  }

  eos_static_info("msg=\"stop tpc job\" fxid=%016x layout=%s",
                  mFid, mConversionLayout.c_str());

  {
    // -------------------------------------------------------------------------
    // we can only call-back to the Converter object if it wasn't 
    // destroyed/recreated in the mean-while
    // -------------------------------------------------------------------------

    XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
    stopConverter = Converter::gConverterMap[mConverterName];
    if (startConverter && (startConverter == stopConverter))
    {
      stopConverter->GetSignal()->Signal();
      stopConverter->DecActiveJobs();
    }
  }

  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo error;

  if (success)
  {
    // -------------------------------------------------------------------------
    // we merge the conversion entry
    // -------------------------------------------------------------------------

    if (!gOFS->merge(mProcPath.c_str(),
                     mSourcePath.c_str(),
                     error,
                     rootvid))
    {
      eos_static_info("msg=\"deleted processed conversion job entry\" name=\"%s\"",
                      mConversionLayout.c_str());
      gOFS->MgmStats.Add("ConversionDone", owner_uid, owner_gid, 1);
    }
    else
    {
      eos_static_err("msg=\"failed to remove failed conversion job entry\" name=\"%s\"",
                     mConversionLayout.c_str());
      gOFS->MgmStats.Add("ConversionFailed", owner_uid, owner_gid, 1);
    }
  }
  else
  {
    // -------------------------------------------------------------------------
    // we set owner nobody to indicate that this is a failed/faulty entry
    // -------------------------------------------------------------------------
    if (!gOFS->_rem(mProcPath.c_str(),
                    error,
                    rootvid,
                    (const char*) 0))
    {
      eos_static_info("msg=\"removed failed conversion entry\" name=\"%s\"",
                      mConversionLayout.c_str());
    }
    else
    {

      eos_static_err("msg=\"failed to removefailed conversion job entry\" name=\"%s\"",
                     mConversionLayout.c_str());
    }
    gOFS->MgmStats.Add("ConversionFailed", owner_uid, owner_gid, 1);
  }


  delete this;
}

/*----------------------------------------------------------------------------*/
Converter::Converter (const char* spacename)
/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor by space name
 * 
 * @param spacename name of the associated space
 */
/*----------------------------------------------------------------------------*/
{
  mSpaceName = spacename;
  XrdSysMutexHelper sLock(gSchedulerMutex);
  if (!gScheduler)
  {
    gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 128, 64);
    gScheduler->Start();
  }

  {

    XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
    // store this object in the converter map for callbask
    gConverterMap[spacename] = this;
  }

  mActiveJobs = 0;

  XrdSysThread::Run(&mThread,
                    Converter::StaticConverter,
                    static_cast<void *> (this),
                    XRDSYSTHREAD_HOLD,
                    "Converter Thread");
}

/*----------------------------------------------------------------------------*/
void
Converter::Stop ()
/*----------------------------------------------------------------------------*/
/**
 * @brief thread stop function
 */
/*---------------------- ------------------------------------------------------*/
{
  XrdSysThread::Cancel(mThread);
}

/*----------------------------------------------------------------------------*/
Converter::~Converter ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/
{
  Stop();

  {
    XrdSysThread::Join(mThread, NULL);
  }

  {
    XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
    gConverterMap[mSpaceName] = 0;
  }
}

/*----------------------------------------------------------------------------*/
void*
Converter::StaticConverter (void* arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Static thread startup function calling Convert
 */
/*----------------------------------------------------------------------------*/
{

  return reinterpret_cast<Converter*> (arg)->Convert();
}

/*----------------------------------------------------------------------------*/
void*
Converter::Convert (void)
/*----------------------------------------------------------------------------*/
/**
 * @brief eternal loop trying to run conversion jobs
 *
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo error;
  XrdSysThread::SetCancelOn();
  // ---------------------------------------------------------------------------
  // wait that the namespace is initialized
  // ---------------------------------------------------------------------------
  bool go = false;
  do
  {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);
      if (gOFS->Initialized == gOFS->kBooted)
      {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }
  while (!go);

  XrdSysTimer sleeper;
  sleeper.Snooze(10);

  // ---------------------------------------------------------------------------
  // Reset old jobs pending from service restart/crash
  // ---------------------------------------------------------------------------
  ResetJobs();

  // ---------------------------------------------------------------------------
  // loop forever until cancelled
  // ---------------------------------------------------------------------------

  // the conversion fid set points from file id to conversion attribute name in 
  // the parent container of the fid

  std::map<eos::common::FileId::fileid_t, std::string> lConversionFidMap;

  while (1)
  {
    bool IsSpaceConverter = true;
    bool IsMaster = true;

    int lSpaceTransfers = 0;
    //    int lSpaceTransferRate = 0; => currently not used

    XrdSysThread::SetCancelOff();
    {
      // -----------------------------------------------------------------------
      // extract the current settings if conversion enabled and how many 
      // conversion jobs should run
      // -----------------------------------------------------------------------
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      std::set<FsGroup*>::const_iterator git;
      if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str()))
        break;

      if (FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
 GetConfigMember("converter") == "on")
        IsSpaceConverter = true;
      else
        IsSpaceConverter = false;

      lSpaceTransfers =
        atoi(FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("converter.ntx").c_str());
      // lSpaceTransferRate = atoi(FsView::gFsView.mSpaceView[mSpaceName.c_str()]->
      // GetConfigMember("converter.rate").c_str());
    }

    IsMaster = gOFS->MgmMaster.IsMaster();

    if (IsMaster && IsSpaceConverter)
    {
      if (!lConversionFidMap.size())
      {
        XrdMgmOfsDirectory dir;
        int listrc = 0;
        // fill the conversion queue with the existing entries
        listrc = dir.open(gOFS->MgmProcConversionPath.c_str(),
                          rootvid,
                          (const char*) 0);
        if (listrc == SFS_OK)
        {
          const char* val;
          while ((val = dir.nextEntry()))
          {
            XrdOucString sfxid = val;
            if ((sfxid == ".") ||
                (sfxid == ".."))
            {
              continue;
            }
            XrdOucString fxid;
            XrdOucString conversionattribute;

            eos_static_info("name=\"%s\"", sfxid.c_str());

            std::string lFullConversionFilePath =
              gOFS->MgmProcConversionPath.c_str();
            lFullConversionFilePath += "/";
            lFullConversionFilePath += val;
            struct stat buf;

            if (gOFS->_stat(lFullConversionFilePath.c_str(), &buf, error, rootvid, ""))
            {
              continue;
            }

            if ((buf.st_uid != 0) /* this is a failed or scheduled entry */)
            {
              continue;
            }


            if (eos::common::StringConversion::SplitKeyValue(sfxid, fxid, conversionattribute) &&
                (eos::common::FileId::Hex2Fid(fxid.c_str())) &&
                (fxid.length() == 16))
            {
              if (conversionattribute.beginswith(mSpaceName.c_str()))
              {
                // -----------------------------------------------------------
                // this is a valid entry like <fxid>:<attribute>
                // we add it to the set
                // if <attribute> starts with our space name!
                // -----------------------------------------------------------
                lConversionFidMap[eos::common::FileId::Hex2Fid(fxid.c_str())] =
                  conversionattribute.c_str();

                // -------------------------------------------------------------------------
                // we set owner admin to indicate that this is a scheduled entry
                // -------------------------------------------------------------------------
                if (!gOFS->_chown(lFullConversionFilePath.c_str(),
                                  3,
                                  4,
                                  error,
                                  rootvid,
                                  (const char*) 0))
                {
                  eos_static_info("msg=\"tagged scheduled conversion entry with owner admin\" name=\"%s\"",
                                  conversionattribute.c_str());
                }
                else
                {

                  eos_static_err("msg=\"failed to tag with owner admin scheduled conversion job entry\" name=\"%s\"",
                                 conversionattribute.c_str());
                }
              }
            }
            else
            {
              eos_static_warning("split=%d fxid=%llu fxid=|%s|length=%u", eos::common::StringConversion::SplitKeyValue(sfxid, fxid, conversionattribute), eos::common::FileId::Hex2Fid(fxid.c_str()), fxid.c_str(), fxid.length());

              // this is an invalid entry not following the <key(016x)>:<value> syntax

              // this is an invalid entry we just remove it
              if (!gOFS->_rem(lFullConversionFilePath.c_str(),
                              error,
                              rootvid,
                              (const char*) 0))
              {
                eos_static_warning("msg=\"deleted invalid conversion entry\" name=\"%s\"", val);
              }
            }
          }
          dir.close();

        }
        else
        {
          eos_static_err("msg=\"failed to list conversion directory\" path=\"%s\"",
                         gOFS->MgmProcConversionPath.c_str());
        }
      }
      eos_static_info("converter is enabled ntx=%d nqueued=%d",
                      lSpaceTransfers,
                      lConversionFidMap.size());
    }
    else
    {
      lConversionFidMap.clear();
      if (IsMaster)
        eos_static_debug("converter is disabled");
      else
        eos_static_debug("converter is in slave mode");
    }

    // -------------------------------------------------------------------------
    // Schedule some conversion jobs if any 
    // -------------------------------------------------------------------------
    int nschedule = lSpaceTransfers - mActiveJobs;
    for (int i = 0; i < nschedule; i++)
    {

      if (lConversionFidMap.size())
      {
        auto it = lConversionFidMap.begin();
        ConverterJob* job = new ConverterJob(it->first,
                                             it->second.c_str(),
                                             mSpaceName);
        // use the global shared scheduler
        XrdSysMutexHelper sLock(gSchedulerMutex);
        gScheduler->Schedule((XrdJob*) job);
        IncActiveJobs();
        // remove the entry from the conversion map
        lConversionFidMap.erase(lConversionFidMap.begin());
      }
      else
      {
        break;
      }
    }

    XrdSysThread::SetCancelOn();
    // -------------------------------------------------------------------------
    // Let some time pass or wait for a notification
    // -------------------------------------------------------------------------
    mDoneSignal.Wait(10);

    XrdSysThread::CancelPoint();
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
void
Converter::PublishActiveJobs ()
/*----------------------------------------------------------------------------*/
/**
 * @brief publish the active job number in the space view
 *
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  char sactive[256];
  snprintf(sactive, sizeof (sactive) - 1, "%lu", mActiveJobs);
  FsView::gFsView.mSpaceView[mSpaceName.c_str()]->SetConfigMember
    ("stat.converter.active",
     sactive,
     true,
     "/eos/*/mgm",
     true);
}

/*----------------------------------------------------------------------------*/
void
Converter::ResetJobs ()
/*----------------------------------------------------------------------------*/
/**
 * @brief publish the active job number in the space view
 *
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo error;

  XrdMgmOfsDirectory dir;
  int listrc = dir.open(gOFS->MgmProcConversionPath.c_str(),
                        rootvid,
                        (const char*) 0);
  if (listrc == SFS_OK)
  {
    const char* val;
    while ((val = dir.nextEntry()))
    {
      XrdOucString sfxid = val;
      if ((sfxid == ".") ||
          (sfxid == ".."))
      {
        continue;
      }

      std::string lFullConversionFilePath =
        gOFS->MgmProcConversionPath.c_str();
      lFullConversionFilePath += "/";
      lFullConversionFilePath += val;

      if (!gOFS->_chown(lFullConversionFilePath.c_str(),
                        0,
                        0,
                        error,
                        rootvid,
                        (const char*) 0))
      {
        eos_static_info("msg=\"reset scheduled conversion entry with owner root\" name=\"%s\"",
                        lFullConversionFilePath.c_str());
      }
      else
      {

        eos_static_err("msg=\"failed to reset with owner root scheduled old job entry\" name=\"%s\"",
                       lFullConversionFilePath.c_str());
      }
    }
  }
  dir.close();
}
EOSMGMNAMESPACE_END
