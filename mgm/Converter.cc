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
#include "common/StringConversion.hh"
#include "common/FileId.hh"
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

/*----------------------------------------------------------------------------*/
Converter::Converter (const char* spacename)
/*----------------------------------------------------------------------------*/
/*
 * @brief Constructor by space name
 * 
 * @param spacename name of the associated space
 */
/*----------------------------------------------------------------------------*/
{
  mSpaceName = spacename;
  mScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 128, 64);
  mScheduler->Start();
  XrdSysThread::Run(&mThread, Converter::StaticConverter, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Converter Thread");
}

/*----------------------------------------------------------------------------*/
Converter::~Converter ()
/*----------------------------------------------------------------------------*/
/*
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysThread::Cancel(mThread);
  if (!gOFS->Shutdown)
  {
    XrdSysThread::Join(mThread, NULL);
  }
  if (mScheduler)
  {
    delete mScheduler;
    mScheduler = 0;
  }
}

/*----------------------------------------------------------------------------*/
void*
Converter::StaticConverter (void* arg)
/*----------------------------------------------------------------------------*/
/*
 * @brief Static thread startup functino calling Convert
 */
/*----------------------------------------------------------------------------*/
{
  return reinterpret_cast<Converter*> (arg)->Convert();
}

/*----------------------------------------------------------------------------*/
void*
Converter::Convert (void)
/*----------------------------------------------------------------------------*/
/*
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

  // ---------------------------------------------------------------------------
  // loop forever until cancelled
  // ---------------------------------------------------------------------------

  // the conversion fid set points from file id to conversion attribute name in 
  // the parent container of the fid

  std::map<eos::common::FileId::fileid_t, std::string> lConversionFidMap;

  while (1)
  {
    bool IsSpaceConverter = true;
    int lSpaceTransfers = 0;
    int lSpaceTransferRate = 0;

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

      if (FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("converter") == "on")
        IsSpaceConverter = true;
      else
        IsSpaceConverter = false;

      lSpaceTransfers = atoi(FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("converter.ntx").c_str());
      lSpaceTransferRate = atoi(FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("converter.rate").c_str());
    }

    if (IsSpaceConverter)
    {
      if (!lConversionFidMap.size())
      {
        XrdMgmOfsDirectory dir;
        int listrc = 0;
        // fill the conversion queue with the existing entries
        listrc = dir.open(gOFS->MgmProcConversionPath.c_str(), rootvid, (const char*) 0);
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
              if (eos::common::StringConversion::SplitKeyValue(sfxid, fxid, conversionattribute) && (eos::common::FileId::Hex2Fid(fxid.c_str())))
              {
                // this is a valid entry like <fxid>:<attribute> - we add it to the set
                lConversionFidMap[eos::common::FileId::Hex2Fid(fxid.c_str())] = conversionattribute.c_str();
              }
              else
              {
                // this is an invalid entry not following the <key>:<value> syntax
                std::string lFullConversionFilePath = gOFS->MgmProcConversionPath.c_str();
                lFullConversionFilePath += "/";
                lFullConversionFilePath += val;
                // this is an invalid entry we just remove it
                if (!gOFS->_rem(lFullConversionFilePath.c_str(), error, rootvid, (const char*) 0))
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
          eos_static_err("msg=\"failed to list conversion directory\" path=\"%s\"", gOFS->MgmProcConversionPath.c_str());
        }
      }
      eos_static_info("converter is enabled ntx=%d nqueued=%d", lSpaceTransfers, lConversionFidMap.size());
    }
    else
    {
      eos_static_info("converter is disabled");
    }

    XrdSysThread::SetCancelOn();
    // -------------------------------------------------------------------------
    // Let some time pass ..
    // -------------------------------------------------------------------------
    for (size_t i = 0; i < 10; i++)
    {
      XrdSysTimer sleeper;
      sleeper.Snooze(1);
      XrdSysThread::CancelPoint();
    }
  }
  return 0;
}

EOSMGMNAMESPACE_END
