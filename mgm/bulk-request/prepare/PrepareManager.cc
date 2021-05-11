//------------------------------------------------------------------------------
//! @file PrepareManager.cc
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "PrepareManager.hh"
#include "mgm/Stat.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include <XrdOuc/XrdOucTList.hh>
#include <XrdVersion.hh>
#include <common/Path.hh>
#include <common/SecEntity.hh>
#include <common/utils/XrdUtils.hh>
#include <mgm/Acl.hh>
#include <mgm/Macros.hh>
#include <mgm/XrdMgmOfs.hh>
#include <mgm/bulk-request/exception/PersistencyException.hh>

EOSMGMNAMESPACE_BEGIN

PrepareManager::PrepareManager()
{
}

int PrepareManager::prepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client) {
  return doPrepare(pargs,error,client);
}

int PrepareManager::doPrepare(XrdSfsPrep& pargs, XrdOucErrInfo& error, const XrdSecEntity* client) {
  EXEC_TIMING_BEGIN("Prepare");
  eos_info("prepareOpts=\"%s\"", prepareOptsToString(pargs.opts).c_str());
  static const char* epname = mEpname.c_str();
  const char* tident = error.getErrUser();
  eos::common::VirtualIdentity vid;
  XrdOucTList* pptr = pargs.paths;
  XrdOucTList* optr = pargs.oinfo;
  std::string info;
  info = (optr ? (optr->text ? optr->text : "") : "");
  eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }
  {
    const int nbFilesInPrepareRequest = eos::common::XrdUtils::countNbElementsInXrdOucTList(pargs.paths);
    gOFS->MgmStats.Add("Prepare", vid.uid, vid.gid, nbFilesInPrepareRequest);
  }
  std::string cmd = "mgm.pcmd=event";
  std::list<std::pair<char**, char**>> pathsWithPrepare;
  // Initialise the request ID for the Prepare request to the one provided by XRootD
  XrdOucString reqid(pargs.reqid);
  // Validate the event type
  std::string event;
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5
  // Strip "quality of service" bits from pargs.opts so that only the action to
  // be taken is left
  const int pargsOptsAction = getPrepareActionsFromOpts(pargs.opts);

  // The XRootD prepare actions are mutually exclusive
  switch (pargsOptsAction) {
  case 0:
    if(gOFS->mTapeEnabled) {
      gOFS->Emsg(epname, error, EINVAL, "prepare with empty pargs.opts on tape-enabled back-end");
      return SFS_ERROR;
    }
    break;

  case Prep_STAGE:
    event = "sync::prepare";
    mBulkRequest.reset(BulkRequestFactory::createStageBulkRequest(vid));
    reqid = mBulkRequest->getId().c_str();
    break;

  case Prep_CANCEL:
    event = "sync::abort_prepare";
    break;

  case Prep_EVICT:
    event = "sync::evict_prepare";
    break;

  default:
    // More than one flag was set or there is an unknown flag
    gOFS->Emsg(epname, error, EINVAL, "prepare - invalid value for pargs.opts =",
               std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#else

  // The XRootD prepare flags are mutually exclusive
  switch (pargs.opts) {
  case 0:
    if(mTapeEnabled) {
      gOFS->Emsg(epname, error, EINVAL, "prepare with empty pargs.opts on tape-enabled back-end");
      return SFS_ERROR;
    }
    break;

  case Prep_STAGE:
    event = "sync::prepare";
    break;

  case Prep_FRESH:
    event = "sync::abort_prepare";
    break;

  default:
    // More than one flag was set or there is an unknown flag
    gOFS->Emsg(epname, error, EINVAL, "prepare - invalid value for pargs.opts =",
         std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#endif

  // check that all files exist
  while (pptr) {
    XrdOucString prep_path = (pptr->text ? pptr->text : "");
    std::string orig_path = prep_path.c_str();
    eos_info("msg =\"checking file exists\" path=\"%s\"", prep_path.c_str());
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      prep_path = path;
    }
    {
      const char* path = prep_path.c_str();
      const char* ininfo = "";
      MAYREDIRECT;
    }
    XrdSfsFileExistence check;

    if (prep_path.length() == 0) {
      gOFS->Emsg(epname, error, ENOENT,
                 "prepare - path empty or uses forbidden characters:",
                 orig_path.c_str());
      return SFS_ERROR;
    }

    if (gOFS->_exists(prep_path.c_str(), check, error, client, "") ||
        (check != XrdSfsFileExistIsFile)) {
      if (check != XrdSfsFileExistIsFile) {
        gOFS->Emsg(epname, error, ENOENT,
                   "prepare - file does not exist or is not accessible to you",
                   prep_path.c_str());
      }

      return SFS_ERROR;
    }

    eos::IContainerMD::XAttrMap attributes;

    if (!event.empty() &&
        gOFS->_attr_ls(eos::common::Path(prep_path.c_str()).GetParentPath(), error, vid,
                       nullptr, attributes) == 0) {
      bool foundPrepareTag = false;
      std::string eventAttr = "sys.workflow." + event;

      for (const auto& attrEntry : attributes) {
        foundPrepareTag |= attrEntry.first.find(eventAttr) == 0;
      }

      if (foundPrepareTag) {
        pathsWithPrepare.emplace_back(&(pptr->text),
                                      optr != nullptr ? & (optr->text) : nullptr);
        if(isStagePrepare()) {
          //For now, only a stage prepare will create a bulk-request
          mBulkRequest->addPath(pptr->text);
        }
      } else {
        // don't do workflow if no such tag
        pptr = pptr->next;

        if (optr) {
          optr = optr->next;
        }

        continue;
      }
    } else {
      // don't do workflow if event not set or we can't check attributes
      pptr = pptr->next;

      if (optr) {
        optr = optr->next;
      }

      continue;
    }

    // check that we have write permission on path
    if (gOFS->_access(prep_path.c_str(), P_OK, error, vid, "")) {
      return gOFS->Emsg(epname, error, EPERM,
                        "prepare - you don't have workflow permission",
                        prep_path.c_str());
    }

    pptr = pptr->next;

    if (optr) {
      optr = optr->next;
    }
  }
  if(!pathsWithPrepare.empty() && isStagePrepare()) {
    try {
      saveBulkRequest();
    } catch(const PersistencyException &ex){
      eos_err("msg=Unable to persist the bulk request %s, ErrorMsg=%s",mBulkRequest->getId().c_str(),ex.what());
      return ex.fillXrdErrInfo(error, EIO);
    }
  }
  //Trigger the prepare workflow
  triggerPrepareWorkflow(pathsWithPrepare,cmd,event,reqid,error,vid);

  int retc = SFS_OK;
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5

  // If we generated our own request ID, return it to the client
  if (isStagePrepare()) {
    // If we return SFS_DATA, the first parameter is the length of the buffer, not the error code
    error.setErrInfo(reqid.length() + 1, reqid.c_str());
    retc = SFS_DATA;
  }

#endif
  EXEC_TIMING_END("Prepare");
  return retc;
}

void PrepareManager::saveBulkRequest() {
  if(mBulkRequestBusiness != nullptr){
    mBulkRequestBusiness->saveBulkRequest(mBulkRequest);
  }
}

void PrepareManager::setBulkRequestBusiness(std::shared_ptr<BulkRequestBusiness> bulkRequestBusiness) {
  mBulkRequestBusiness = bulkRequestBusiness;
}

std::string PrepareManager::prepareOptsToString(const int opts) const{
  std::ostringstream result;
  const int priority = opts & Prep_PMASK;

  switch (priority) {
  case Prep_PRTY0:
    result << "PRTY0";
    break;

  case Prep_PRTY1:
    result << "PRTY1";
    break;

  case Prep_PRTY2:
    result << "PRTY2";
    break;

  case Prep_PRTY3:
    result << "PRTY3";
    break;

  default:
    result << "PRTYUNKNOWN";
  }

  const int send_mask = 12;
  const int send = opts & send_mask;

  switch (send) {
  case 0:
    break;

  case Prep_SENDAOK:
    result << ",SENDAOK";
    break;

  case Prep_SENDERR:
    result << ",SENDERR";
    break;

  case Prep_SENDACK:
    result << ",SENDACK";
    break;

  default:
    result << ",SENDUNKNOWN";
  }

  if (opts & Prep_WMODE) {
    result << ",WMODE";
  }

  if (opts & Prep_STAGE) {
    result << ",STAGE";
  }

  if (opts & Prep_COLOC) {
    result << ",COLOC";
  }

  if (opts & Prep_FRESH) {
    result << ",FRESH";
  }

#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5

  if (opts & Prep_CANCEL) {
    result << ",CANCEL";
  }

  if (opts & Prep_QUERY) {
    result << ",QUERY";
  }

  if (opts & Prep_EVICT) {
    result << ",EVICT";
  }

#endif
  return result.str();
}

const int PrepareManager::getPrepareActionsFromOpts(const int pargsOpts) const {
  const int pargsOptsQoS = Prep_PMASK | Prep_SENDAOK | Prep_SENDERR | Prep_SENDACK
                           | Prep_WMODE | Prep_COLOC | Prep_FRESH;
  return pargsOpts & ~pargsOptsQoS;
}

const bool PrepareManager::isStagePrepare() const {
  return mBulkRequest != nullptr &&
         mBulkRequest->getType() == eos::mgm::BulkRequest::PREPARE_STAGE;
}

void PrepareManager::triggerPrepareWorkflow(const std::list<std::pair<char**, char**>> & pathsToPrepare, const std::string & cmd, const std::string &event, const XrdOucString & reqid, XrdOucErrInfo & error, const eos::common::VirtualIdentity& vid) {
  for (auto& pathPair : pathsToPrepare) {
    XrdOucString prep_path = (*pathPair.first ? *pathPair.first : "");
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      prep_path = path;
    }
    XrdOucString prep_info = pathPair.second != nullptr ? (*pathPair.second ?
                                                           *pathPair.second : "") : "";
    eos_info("msg=\"about to trigger WFE\" path=\"%s\" info=\"%s\"",
             prep_path.c_str(), prep_info.c_str());
    XrdOucEnv prep_env(prep_info.c_str());
    prep_info = cmd.c_str();
    prep_info += "&mgm.event=";
    prep_info += event.c_str();
    prep_info += "&mgm.workflow=";

    if (prep_env.Get("eos.workflow")) {
      prep_info += prep_env.Get("eos.workflow");
    } else {
      prep_info += "default";
    }

    prep_info += "&mgm.fid=0&mgm.path=";
    prep_info += prep_path.c_str();
    prep_info += "&mgm.logid=";
    prep_info += this->logId;
    prep_info += "&mgm.ruid=";
    prep_info += (int)vid.uid;
    prep_info += "&mgm.rgid=";
    prep_info += (int)vid.gid;
    prep_info += "&mgm.reqid=";
    prep_info += reqid.c_str();

    if (prep_env.Get("activity")) {
      prep_info += "&activity=";
      prep_info += prep_env.Get("activity");
    }

    XrdSecEntity lClient(vid.prot.c_str());
    lClient.name = (char*) vid.name.c_str();
    lClient.tident = (char*) vid.tident.c_str();
    lClient.host = (char*) vid.host.c_str();
    XrdOucString lSec = "&mgm.sec=";
    lSec += eos::common::SecEntity::ToKey(&lClient,
                                          "eos").c_str();
    prep_info += lSec;
    XrdSfsFSctl args;
    args.Arg1 = prep_path.c_str();
    args.Arg1Len = prep_path.length();
    args.Arg2 = prep_info.c_str();
    args.Arg2Len = prep_info.length();
    auto ret_wfe = gOFS->FSctl(SFS_FSCTL_PLUGIN, args, error, &lClient);

    // Log errors but continue to process the rest of the files in the list
    if (ret_wfe != SFS_DATA) {
      eos_err("Unable to prepare - synchronous prepare workflow error %s; %s",
              prep_path.c_str(), error.getErrText());
    }
  }
}

std::shared_ptr<BulkRequest> PrepareManager::getBulkRequest() const {
  return mBulkRequest;
}


EOSMGMNAMESPACE_END
