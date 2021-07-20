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
#include "common/Constants.hh"
#include "mgm/Stat.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/bulk-request/utils/json/JSONCppJsonifier.hh"
#include <XrdOuc/XrdOucTList.hh>
#include <XrdVersion.hh>
#include <common/Path.hh>
#include <common/SecEntity.hh>
#include <common/utils/XrdUtils.hh>
#include <mgm/Acl.hh>
#include <mgm/Macros.hh>
#include <mgm/XrdMgmOfs.hh>
#include <mgm/bulk-request/File.hh>

#include <mgm/bulk-request/exception/PersistencyException.hh>
#include <mgm/bulk-request/prepare/PrepareUtils.hh>
#include <xrootd/XrdSfs/XrdSfsFlags.hh>

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
      std::ostringstream oss;
      std::string errorMsg = "prepare - file does not exist or is not accessible to you";
      oss << "msg=\"" << errorMsg << "\" path=\"" << prep_path.c_str() << "\"";
      eos_info(oss.str().c_str());
      setErrorToBulkRequest(prep_path.c_str(),errorMsg);
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
        std::ostringstream oss;
        oss << "No prepare workflow set on the directory " << eos::common::Path(prep_path.c_str()).GetParentPath();
        setErrorToBulkRequest(prep_path.c_str(),oss.str());
        goto nextPath;
      }
    } else {
      // don't do workflow if event not set or we can't check attributes
      if(!event.empty()) {
        std::ostringstream oss;
        oss << "Unable to check the extended attributes of the directory "
            << eos::common::Path(prep_path.c_str()).GetParentPath();
        setErrorToBulkRequest(prep_path.c_str(), oss.str());
      }
      goto nextPath;
    }

    // check that we have write permission on path
    if (mMgmFsInterface._access(prep_path.c_str(), P_OK, error, vid, "")) {
      //https://its.cern.ch/jira/browse/EOS-4739
      //For every prepare scenario, we continue to process the files even if they do not exist or are not correct
      //The user will then have to query prepare to figure out that the directory where the files are located has
      //no workflow permission
      std::ostringstream oss;
      std::string errorMsg = "Ignoring file because there is no workflow permission";
      oss << "msg=\"" << errorMsg << "\" path=\"" << prep_path.c_str() << "\"";
      eos_info(oss.str().c_str());
      setErrorToBulkRequest(prep_path.c_str(),errorMsg);
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

std::unique_ptr<QueryPrepareResult> PrepareManager::queryPrepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client) {
  std::unique_ptr<QueryPrepareResult> queryPrepareResult(new QueryPrepareResult());
  int retCode = doQueryPrepare(pargs, error, client,*queryPrepareResult);
  queryPrepareResult->setReturnCode(retCode);
  return queryPrepareResult;
}

int PrepareManager::doQueryPrepare(XrdSfsPrep &pargs, XrdOucErrInfo & error, const XrdSecEntity* client, QueryPrepareResult & result) {
  EXEC_TIMING_BEGIN("QueryPrepare");
  ACCESSMODE_R;
  eos_info("cmd=\"_prepare_query\"");
  eos::common::VirtualIdentity vid;
  {
    const char* tident = error.getErrUser();
    XrdOucTList* optr = pargs.oinfo;
    std::string info(optr && optr->text ? optr->text : "");
    eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
  }
  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }

  // ID of the original prepare request. We don't need this to look up the list of files in
  // the request, as they are provided in the arguments. Anyway we return it in the reply
  // as a convenience for the client to track which prepare request the query applies to.
  XrdOucString reqid(pargs.reqid);

  int path_cnt = 0;
  std::vector<std::string> pathsToQuery;
  for (XrdOucTList* pptr = pargs.paths; pptr; pptr = pptr->next) {
    if (!pptr->text) {
      continue;
    }
    pathsToQuery.push_back(pptr->text);
    ++path_cnt;
  }

  mMgmFsInterface.addStats("QueryPrepare", vid.uid, vid.gid, path_cnt);
  std::shared_ptr<FileCollection::Files> fileCollection;
  if(reqid.length()){
    //Look in the persistency layer for informations about files submitted previously for staging
    fileCollection = getFileCollectionFromPersistency(reqid.c_str());
  }

  std::shared_ptr<QueryPrepareResponse> response = result.getResponse();

  // Set the queryPrepareFileResponses for each file in the list
  for (auto & queriedPath: pathsToQuery) {
    response->responses.push_back(QueryPrepareFileResponse(queriedPath));
    auto& rsp = response->responses.back();
    // check if the file exists
    XrdOucString prep_path;
    {
      const char* inpath = rsp.path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;
      prep_path = path;
    }
    {
      const char* path = rsp.path.c_str();
      const char* ininfo = "";
      MAYREDIRECT;
    }

    if (prep_path.length() == 0) {
      rsp.error_text = "path empty or uses forbidden characters";
      continue;
    }

    XrdSfsFileExistence check;

    if (mMgmFsInterface._exists(prep_path.c_str(), check, error, client, "") ||
        check != XrdSfsFileExistIsFile) {
      rsp.error_text = "file does not exist or is not accessible to you";
      continue;
    }

    rsp.is_exists = true;

    // Check file state (online/offline)
    XrdOucErrInfo xrd_error;
    struct stat buf;

    if (mMgmFsInterface._stat(rsp.path.c_str(), &buf, xrd_error, vid, nullptr, nullptr, false)) {
      rsp.error_text = xrd_error.getErrText();
      continue;
    }

    mMgmFsInterface._stat_set_flags(&buf);
    rsp.is_on_tape = buf.st_rdev & XRDSFS_HASBKUP;
    rsp.is_online  = !(buf.st_rdev & XRDSFS_OFFLINE);
    // Check file status in the extended attributes
    eos::IFileMD::XAttrMap xattrs;

    if (mMgmFsInterface._attr_ls(eos::common::Path(prep_path.c_str()).GetPath(), xrd_error, vid,
                 nullptr, xattrs) == 0) {
      auto xattr_it = xattrs.find(eos::common::RETRIEVE_REQID_ATTR_NAME);

      if (xattr_it != xattrs.end()) {
        // has file been requested? (not necessarily with this request ID)
        rsp.is_requested = !xattr_it->second.empty();
        // and is this specific request ID present in the request?
        rsp.is_reqid_present = (xattr_it->second.find(reqid.c_str()) != string::npos);
      }

      xattr_it = xattrs.find(eos::common::RETRIEVE_REQTIME_ATTR_NAME);

      if (xattr_it != xattrs.end()) {
        rsp.request_time = xattr_it->second;
      }

      xattr_it = xattrs.find(eos::common::RETRIEVE_ERROR_ATTR_NAME);

      if (xattr_it == xattrs.end()) {
        // If there is no retrieve error, check for an archive error
        xattr_it = xattrs.find(eos::common::ARCHIVE_ERROR_ATTR_NAME);
      }

      if (xattr_it != xattrs.end()) {
        rsp.error_text = xattr_it->second;
      }
    } else {
      // failed to read extended attributes
      rsp.error_text = xrd_error.getErrText();
      continue;
    }
  }

  response->request_id = reqid.c_str();

  /*
  json_ss << "{"
          << "\"request_id\":\"" << reqid << "\","
          << "\"responses\":[";
  bool is_first(true);

  for (auto& r : response) {
    if (is_first) {
      is_first = false;
    } else {
      json_ss << ",";
    }

    json_ss << r;
  }

  json_ss << "]"
          << "}";
          */

  result.setQueryPrepareFinished();

  EXEC_TIMING_END("QueryPrepare");
  return SFS_DATA;
}

const std::shared_ptr<FileCollection::Files> PrepareManager::getFileCollectionFromPersistency(const std::string& reqid) {
  return std::shared_ptr<FileCollection::Files>(new FileCollection::Files());
}

EOSBULKNAMESPACE_END
