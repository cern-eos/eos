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
#include "XrdCl/XrdClCopyProcess.hh"
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
  mSourcePath = gOFS->MgmProcConversionPath.c_str();
  mSourcePath += "/";
  char xfid[20];
  snprintf(xfid, sizeof (xfid), "%016llx", (long long) mFid);
  mSourcePath += "/";
  mSourcePath += xfid;
  mSourcePath += ":";
  mSourcePath += conversionlayout;
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
  eos_static_info("msg=\"start tpc job\" fxid=%016x layout=%s source_path=%s",
                  mFid, mConversionLayout.c_str(), mSourcePath.c_str());
  XrdSysTimer sleeper;
  //XrdCl::JobDescriptor lJob; //< the local tpc job description

  eos::FileMD* fmd = 0;
  eos::ContainerMD* cmd = 0;

  eos::ContainerMD::XAttrMap attrmap;


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
      mSourcePath = gOFS->eosView->getUri(fmd);
      eos::common::Path cPath(mSourcePath.c_str());
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      // load the extended attributes
      eos::ContainerMD::XAttrMap::const_iterator it;
      for (it = cmd->attributesBegin(); it != cmd->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }

      std::string conversionattribute = "sys.conversion.";
      conversionattribute += mConversionLayout.c_str();
      XrdOucString lEnv;
      if (attrmap.count(mConversionLayout.c_str()))
      {
        // conversion layout can either point to a conversion attribute definition in the parent directory
        mTargetCGI =
          eos::common::LayoutId::GetEnvFromHexLayoutIdString(lEnv, attrmap[mConversionLayout.c_str()].c_str());
      }
      else
      {
        // or can be directly a hexadecimal layout representation
        mTargetCGI =
          eos::common::LayoutId::GetEnvFromHexLayoutIdString(lEnv, mConversionLayout.c_str());
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_static_err("fid=%016x errno=%d msg=\"%s\"\n",
                     mFid, e.getErrno(), e.getMessage().str().c_str());
    }
  }

  if (mTargetCGI.length())
  {
    // this is properly defined job
    eos_static_info("msg=\"conversion layout correct\" fxid=%016x cgi=\"%s\"",
                    mFid, mTargetCGI.c_str());
  }
  else
  {
    // this is a crappy defined job
    eos_static_err("msg=\"conversion layout definition wrong\" fxid=%016x layout=%s",
                   mFid, mConversionLayout.c_str());
  }
  eos_static_info("msg=\"stop  tpc job\" fxid=%016x layout=%s",
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
Converter::~Converter ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysThread::Cancel(mThread);
  if (!gOFS->Shutdown)
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
            if (!(sfxid.endswith("run")))
            {
              if (eos::common::StringConversion::SplitKeyValue(sfxid, fxid, conversionattribute) &&
                  (eos::common::FileId::Hex2Fid(fxid.c_str())) &&
                  (fxid.length() == 16))
              {
                // this is a valid entry like <fxid>:<attribute> - we add it to the set
                lConversionFidMap[eos::common::FileId::Hex2Fid(fxid.c_str())] =
                  conversionattribute.c_str();
              }
              else
              {
                // this is an invalid entry not following the <key(016x)>:<value> syntax
                std::string lFullConversionFilePath =
                  gOFS->MgmProcConversionPath.c_str();
                lFullConversionFilePath += "/";
                lFullConversionFilePath += val;
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
            else
            {
              eos_static_info("no run");
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
        eos_static_info("converter is disabled");
      else
        eos_static_info("converter is in slave mode");
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
        mActiveJobs++;
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

EOSMGMNAMESPACE_END
