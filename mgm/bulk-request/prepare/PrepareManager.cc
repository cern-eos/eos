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
#include <XrdOuc/XrdOucTList.hh>
#include <XrdVersion.hh>
#include <common/Path.hh>
#include <common/SecEntity.hh>
#include <common/utils/XrdUtils.hh>
#include <mgm/Acl.hh>
#include <mgm/Macros.hh>
#include <mgm/XrdMgmOfs.hh>
#include <mgm/bulk-request/exception/PersistencyException.hh>
#include <mgm/bulk-request/prepare/PrepareUtils.hh>

EOSBULKNAMESPACE_BEGIN

PrepareManager::PrepareManager(IMgmFileSystemInterface & mgmFsInterface):mMgmFsInterface(mgmFsInterface)
{
}

int PrepareManager::prepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client) {
  return doPrepare(pargs,error,client);
}

void PrepareManager::initializeStagePrepareRequest(XrdOucString& reqid) {
  reqid = eos::common::StringConversion::timebased_uuidstring().c_str();
}

void PrepareManager::initializeEvictPrepareRequest(XrdOucString& reqid) {

}

int PrepareManager::doPrepare(XrdSfsPrep& pargs, XrdOucErrInfo& error, const XrdSecEntity* client) {
  EXEC_TIMING_BEGIN("Prepare");
  eos_info("prepareOpts=\"%s\"", PrepareUtils::prepareOptsToString(pargs.opts).c_str());
  static const char* epname = mEpname.c_str();
  const char* tident = error.getErrUser();
  eos::common::VirtualIdentity vid;
  XrdOucTList* pptr = pargs.paths;
  XrdOucTList* optr = pargs.oinfo;
  std::string info;
  info = (optr ? (optr->text ? optr->text : "") : "");
  eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
  mMgmFsInterface.addStats("IdMap", vid.uid, vid.gid, 1);
  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }
  {
    const int nbFilesInPrepareRequest = eos::common::XrdUtils::countNbElementsInXrdOucTList(pargs.paths);
    mMgmFsInterface.addStats("Prepare",vid.uid, vid.gid, nbFilesInPrepareRequest);
  }
  std::string cmd = "mgm.pcmd=event";
  std::list<std::pair<char**, char**>> pathsToPrepare;
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
    if(mMgmFsInterface.isTapeEnabled()) {
      mMgmFsInterface.Emsg(epname, error, EINVAL, "prepare with empty pargs.opts on tape-enabled back-end");
      return SFS_ERROR;
    }
    break;

  case Prep_STAGE:
    event = "sync::prepare";
    mPrepareAction = PrepareAction::STAGE;
    initializeStagePrepareRequest(reqid);
    break;

  case Prep_CANCEL:
    mPrepareAction = PrepareAction::ABORT;
    event = "sync::abort_prepare";
    break;

  case Prep_EVICT:
    mPrepareAction = PrepareAction::EVICT;
    event = "sync::evict_prepare";
    initializeEvictPrepareRequest(reqid);
    break;

  default:
    // More than one flag was set or there is an unknown flag
    mMgmFsInterface.Emsg(epname, error, EINVAL, "prepare - invalid value for pargs.opts =",
                         std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#else

  // The XRootD prepare flags are mutually exclusive
  switch (pargs.opts) {
  case 0:
    if(mMgmFsInterface.isTapeEnabled()) {
      mMgmFsInterface.Emsg(epname, error, EINVAL, "prepare with empty pargs.opts on tape-enabled back-end");
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
   mMgmFsInterface.Emsg(epname, error, EINVAL, "prepare - invalid value for pargs.opts =",
         std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#endif

  // check that all files exist
  while (pptr) {
    //Extended attributes for the current file's parent directory
    eos::IContainerMD::XAttrMap attributes;

    XrdOucString prep_path = (pptr->text ? pptr->text : "");
    std::string orig_path = prep_path.c_str();
    eos_info("msg=\"checking file exists\" path=\"%s\"", prep_path.c_str());
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      //Valgrind Source and destination overlap in strncpy(...)
      if(prep_path.c_str() != path) {
        prep_path = path;
      }
    }
    {
      const char* path = prep_path.c_str();
      const char* ininfo = "";
      MAYREDIRECT;
    }
    XrdSfsFileExistence check;

    if (prep_path.length() == 0) {
      eos_info("msg=\"Ignoring empty path or path formed with forbidden characters\" path=\"%s\")",orig_path.c_str());
      goto nextPath;
    }

    addPathToBulkRequest(prep_path.c_str());

    if (mMgmFsInterface._exists(prep_path.c_str(), check, error, client, "") ||
        (check != XrdSfsFileExistIsFile)) {
      //https://its.cern.ch/jira/browse/EOS-4739
      //For every prepare scenario, we continue to process the files even if they do not exist or are not correct
      //The user will then have to query prepare to figure out that the files do not exist
      eos_info("msg=\"prepare - file does not exist or is not accessible to you\" path=\"%s\"",prep_path.c_str());
      goto nextPath;
    }

    if (!event.empty() &&
        mMgmFsInterface._attr_ls(eos::common::Path(prep_path.c_str()).GetParentPath(), error, vid,
                       nullptr, attributes) == 0) {
      bool foundPrepareTag = false;
      std::string eventAttr = "sys.workflow." + event;

      for (const auto& attrEntry : attributes) {
        foundPrepareTag |= attrEntry.first.find(eventAttr) == 0;
      }

      if (foundPrepareTag) {
        pathsToPrepare.emplace_back(&(pptr->text),
                                      optr != nullptr ? & (optr->text) : nullptr);
      } else {
        // don't do workflow if no such tag
        goto nextPath;
      }
    } else {
      // don't do workflow if event not set or we can't check attributes
      goto nextPath;
    }

    // check that we have write permission on path
    if (mMgmFsInterface._access(prep_path.c_str(), P_OK, error, vid, "")) {
      //https://its.cern.ch/jira/browse/EOS-4739
      //For every prepare scenario, we continue to process the files even if they do not exist or are not correct
      //The user will then have to query prepare to figure out that the directory where the files are located has
      //no workflow permission
      eos_info("msg=\"Ignoring file because there is no workflow permission\" path=\"%s\"",prep_path.c_str());
      pathsToPrepare.pop_back();
      goto nextPath;
    }

    nextPath:
      pptr = pptr->next;

      if (optr) {
        optr = optr->next;
      }
  }

  try {
    saveBulkRequest();
  } catch(const PersistencyException &ex){
    return ex.fillXrdErrInfo(error, EIO);
  }

  //Trigger the prepare workflow
  triggerPrepareWorkflow(pathsToPrepare,cmd,event,reqid,error,vid);

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

}

void PrepareManager::addPathToBulkRequest(const std::string& path){
  //The normal PrepareManager does not have any bulk-request, do nothing
}

const int PrepareManager::getPrepareActionsFromOpts(const int pargsOpts) const {
  const int pargsOptsQoS = Prep_PMASK | Prep_SENDAOK | Prep_SENDERR | Prep_SENDACK
                           | Prep_WMODE | Prep_COLOC | Prep_FRESH;
  return pargsOpts & ~pargsOptsQoS;
}

const bool PrepareManager::isStagePrepare() const {
  return mPrepareAction == PrepareAction::STAGE;
}

void PrepareManager::triggerPrepareWorkflow(const std::list<std::pair<char**, char**>> & pathsToPrepare, const std::string & cmd, const std::string &event, const XrdOucString & reqid, XrdOucErrInfo & error, const eos::common::VirtualIdentity& vid) {
  for (auto& pathPair : pathsToPrepare) {
    XrdOucString prep_path = (*pathPair.first ? *pathPair.first : "");
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      //Valgrind Source and destination overlap in strncpy(...)
      if(prep_path.c_str() != path) {
        prep_path = path;
      }
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
    auto ret_wfe = mMgmFsInterface.FSctl(SFS_FSCTL_PLUGIN, args, error, &lClient);

    // Log errors but continue to process the rest of the files in the list
    if (ret_wfe != SFS_DATA) {
      eos_err("Unable to prepare - synchronous prepare workflow error %s; %s",
              prep_path.c_str(), error.getErrText());
    }
  }
}

EOSBULKNAMESPACE_END
