// ----------------------------------------------------------------------
// File: Workflow.cc
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
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "mgm/Workflow.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/WFE.hh"
#include "mgm/FsView.hh"
#include "common/Constants.hh"
#include "namespace/Prefetcher.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/

int
Workflow::Trigger(const std::string& event, std::string workflow,
                  eos::common::VirtualIdentity& vid,
                  const char * const ininfo, std::string& errorMessage)
{
  errno = 0;

  if (workflow == "none" && vid.sudoer) {
    eos_static_info("\"none\" workflow has been called by sudoer, ignoring the event");
    return 0;
  }

  if ((workflow == eos::common::RETRIEVE_WRITTEN_WORKFLOW_NAME &&
       vid.prot != "sss")
      || (workflow == "none" && !vid.sudoer)) {
    workflow = "default";
  }

  if ((event == "open")) {
    std::string key = "sys.workflow.";
    key += event;
    key += ".";
    key += workflow;

    if (mAttr && (*mAttr).count(key)) {
      eos_static_info("key=%s %d %d", key.c_str(), mAttr, (*mAttr).count(key));
      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      int retc = Create(vid, ininfo, errorMessage);

      if (!retc) {
        if ((workflow == "enonet")) {
          std::string stallkey = key + ".stall";

          if ((*mAttr).count(stallkey)) {
            int stalltime = eos::common::StringConversion::GetSizeFromString((
                              *mAttr)[stallkey]);
            return stalltime;
          }
        }

        return 0;
      }

      errno = retc;
      return retc;
    } else {
      errno = ENOKEY;
    }
  } else {
    std::string key = "sys.workflow." + event + "." + workflow;

    if (mAttr && (*mAttr).count(key)) {
      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      int retc = Create(vid, ininfo, errorMessage);

      if (retc != 0) {
        return retc;
      }

      return 0;
    } else {
      errno = ENOKEY;
    }
  }

  // not defined
  return -1;
}

/*----------------------------------------------------------------------------*/
std::string
Workflow::getCGICloseW(std::string workflow,
                       const eos::common::VirtualIdentity& vid)
{
  std::string cgi;
  std::string key = "sys.workflow.closew." + workflow;
  std::string syncKey = "sys.workflow.sync::closew." + workflow;

  // synchronous closew has priority
  if (mAttr && (*mAttr).count(syncKey)) {
    std::string fullPath;
    decltype(gOFS->eosFileService->getFileMD(mFid)->getCUid()) cuid = 99;
    decltype(gOFS->eosFileService->getFileMD(mFid)->getCGid()) cgid = 99;

    try {
      eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
      eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
      auto fmd = gOFS->eosFileService->getFileMD(mFid);
      fullPath = gOFS->eosView->getUri(fmd.get());
      cuid = fmd->getCUid();
      cgid = fmd->getCGid();
    } catch (eos::MDException& e) {
      eos_static_err("Not creating workflow URL because cannot get meta data. Reason: %s",
                     e.what());
      return "";
    }

    std::ostringstream attrStream;
    std::string separator;

    for (const auto& attribute : WFE::CollectAttributes(fullPath)) {
      attrStream << separator << attribute.first <<
                 eos::common::WF_CUSTOM_ATTRIBUTES_TO_FST_EQUALS << attribute.second;
      separator = eos::common::WF_CUSTOM_ATTRIBUTES_TO_FST_SEPARATOR;
    }

    auto attrStr = attrStream.str();
    std::string attrEncoded;
    eos::common::SymKey::Base64Encode(attrStr.c_str(), attrStr.length(), attrEncoded);
    cgi = "&mgm.event=sync::closew&mgm.workflow=";
    cgi += workflow;
    cgi += "&mgm.instance=";
    cgi += gOFS->MgmOfsInstanceName.c_str();
    cgi += "&mgm.owner_uid=";
    cgi += std::to_string(cuid);
    cgi += "&mgm.owner_gid=";
    cgi += std::to_string(cgid);
    cgi += "&mgm.requestor=";
    cgi += WFE::GetUserName(vid.uid);
    cgi += "&mgm.requestorgroup=";
    cgi += WFE::GetGroupName(vid.gid);
    cgi += "&mgm.attributes=";
    cgi += attrEncoded;
  } else if (mAttr && (*mAttr).count(key)) {
    cgi = "&mgm.event=closew&mgm.workflow=";
    cgi += workflow;
  }

  return cgi;
}

/*----------------------------------------------------------------------------*/
std::string
Workflow::getCGICloseR(std::string workflow)
{
  std::string cgi;
  std::string key = "sys.workflow.closer." + workflow;
  std::string syncKey = "sys.workflow.sync::closer." + workflow;

  // synchronous closer has priority
  if (mAttr && (*mAttr).count(syncKey)) {
    cgi = "&mgm.event=sync::close&mgm.workflow=";
    cgi += workflow;
  } else if (mAttr && (*mAttr).count(key)) {
    cgi = "&mgm.event=close&mgm.workflow=";
    cgi += workflow;
  }

  return cgi;
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Attach(const char* path)
{
  return false;
}

/*----------------------------------------------------------------------------*/
int
Workflow::Create(eos::common::VirtualIdentity& vid,
                 const char * const ininfo, std::string& errorMessage)
{
  try {
    return ExceptionThrowingCreate(vid, ininfo, errorMessage);
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching here means that an exception was thrown
  eos_static_err("msg =\"Caught an unexpected exception: %s\"", errorMessage.c_str());
  return ECANCELED;
}

/*----------------------------------------------------------------------------*/
int
Workflow::ExceptionThrowingCreate(eos::common::VirtualIdentity& vid,
  const char * const ininfo, std::string& errorMessage)
{
  int retc = 0;
  WFE::Job job(mFid, vid, errorMessage);
  time_t t = time(nullptr);

  if (job.IsSync(mEvent)) {
    if (WfeEnabled()) {
      job.AddAction(mAction, mEvent, t, mWorkflow, "r");
      return job.DoIt(true, errorMessage, ininfo);
    }
  } else {
    if (WfeRecordingEnabled()) {
      job.AddAction(mAction, mEvent, t, mWorkflow, "q");
      retc = job.Save("q", t);

      if (retc) {
        eos_static_err("failed to save");
        return retc;
      }
    }
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Delete()
{
  return false;
}

bool
Workflow::WfeRecordingEnabled()
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  return FsView::gFsView.mSpaceView.count("default") &&
         (FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe") != "off");
}

bool
Workflow::WfeEnabled()
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  return FsView::gFsView.mSpaceView.count("default") &&
         (FsView::gFsView.mSpaceView["default"]->GetConfigMember("wfe") == "on");
}

EOSMGMNAMESPACE_END
