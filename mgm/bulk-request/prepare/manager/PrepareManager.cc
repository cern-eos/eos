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

#include <XrdVersion.hh>
#include <XrdOuc/XrdOucTList.hh>
#include <XrdSfs/XrdSfsFlags.hh>
#include "PrepareManager.hh"
#include "common/Constants.hh"
#include "mgm/Stat.hh"
#include "mgm/EosCtaReporter.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "common/SecEntity.hh"
#include "common/utils/XrdUtils.hh"
#include "common/Definitions.hh"
#include "mgm/Macros.hh"
#include "mgm/XrdMgmOfs.hh"
#include <mgm/XattrSet.hh>
#include "mgm/bulk-request/File.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include "mgm/bulk-request/prepare/PrepareUtils.hh"

EOSBULKNAMESPACE_BEGIN

PrepareManager::PrepareManager(std::unique_ptr<IMgmFileSystemInterface>&&
                               mgmFsInterface): mMgmFsInterface(std::move(mgmFsInterface))
{
}

int PrepareManager::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                            const XrdSecEntity* client) noexcept
{
  return doPrepare(pargs, error, client);
}

int PrepareManager::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                            const common::VirtualIdentity* vid) noexcept
{
  XrdSecEntity client;
  return doPrepare(pargs, error, &client, vid);
}

int PrepareManager::prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                            const std::string& authz) noexcept
{
  XrdSecEntity client;
  return doPrepare(pargs, error, &client, nullptr, authz);
}

void PrepareManager::initializeStagePrepareRequest(XrdOucString& reqid,
    const common::VirtualIdentity& vid)
{
  // Override the XRootD-supplied request ID. The request ID can be any arbitrary string, so long as
  // it is guaranteed to be unique for each request.
  //
  // Note: To use the default request ID supplied in pargs.reqid, return SFS_OK instead of SFS_DATA.
  //       Overriding is only possible in the case of PREPARE. In the case of ABORT and QUERY requests,
  //       pargs.reqid should contain the request ID that was returned by the corresponding PREPARE.
  // Request ID = XRootD-generated request ID + timestamp
  std::ostringstream ss;
  ss << ':' << time(0);
  reqid.append(ss.str().c_str());
}

void PrepareManager::initializeCancelPrepareRequest(XrdOucString& reqid)
{
  //Nothing to do as cancellation does not require the creation of an ID
}

int PrepareManager::doPrepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                              const XrdSecEntity* client, const common::VirtualIdentity* vidClient, const std::string& authz) noexcept
{
  EXEC_TIMING_BEGIN("Prepare");
  eos_info("prepareOpts=\"%s\"",
           PrepareUtils::prepareOptsToString(pargs.opts).c_str());
  static const char* epname = mEpname.c_str();
  XrdOucTList* pptr = pargs.paths;
  XrdOucTList* optr = pargs.oinfo;
  std::string info;
  info = (optr ? (optr->text ? optr->text : "") : "");
  eos::common::VirtualIdentity vid;

  // Map each individual path to a VID
  std::map<std::string, eos::common::VirtualIdentity> fileToVidMap;
  {
    XrdOucTList* pptr_aux = pargs.paths;
    for (; pptr_aux; pptr_aux = pptr_aux->next) {
      if (pptr->text) {
        XrdOucString prep_path = pptr->text;
        fileToVidMap[prep_path.c_str()] = eos::common::VirtualIdentity::Nobody();
      }
    }
  }

  if (vidClient != nullptr) {
    vid = *vidClient;
    for (auto & [file_path, file_vid] : fileToVidMap) {
      file_vid = vid;
    }
  } else if (!authz.empty()) {
    vid = eos::common::VirtualIdentity::Nobody();
    mMgmFsInterface->addStats("IdMap", vid.uid, vid.gid, 1);
    std::string env = "authz=" + eos::common::StringConversion::curl_default_escaped(authz);
    for (auto & [file_path, file_vid] : fileToVidMap) {
      // TODO: Replace 'AOP_Update' by 'AOP_Stage' once this is implemented in XRootD
      eos::common::Mapping::IdMap(client, env.c_str(), client->tident, file_vid, mMgmFsInterface->getTokenHandler(), AOP_Update, file_path);
    }
  } else {
    const char* tident = error.getErrUser();
    eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
    mMgmFsInterface->addStats("IdMap", vid.uid, vid.gid, 1);
    for (auto & [file_path, file_vid] : fileToVidMap) {
      file_vid = vid;
    }
  }

  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }
  const int nbFilesProvidedByUser =
    eos::common::XrdUtils::countNbElementsInXrdOucTList(pargs.paths);
  {
    mMgmFsInterface->addStats("Prepare", vid.uid, vid.gid,
                              nbFilesProvidedByUser);
  }
  std::string cmd = "mgm.pcmd=event";
  std::list<std::tuple<char**, char**, EosCtaReporterPrepareReq>> pathsToPrepare;
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
    if (mMgmFsInterface->isTapeEnabled()) {
      mMgmFsInterface->Emsg(epname, error, EINVAL,
                            "prepare with empty pargs.opts on tape-enabled back-end");
      return SFS_ERROR;
    }

    break;

  case Prep_STAGE:
    event = "sync::prepare";
    mPrepareAction = PrepareAction::STAGE;
    initializeStagePrepareRequest(reqid, vid);
    break;

  case Prep_CANCEL:
    mPrepareAction = PrepareAction::ABORT;
    initializeCancelPrepareRequest(reqid);
    event = "sync::abort_prepare";
    break;

  case Prep_EVICT:
    mPrepareAction = PrepareAction::EVICT;
    event = "sync::evict_prepare";
    break;

  default:
    // More than one flag was set or there is an unknown flag
    mMgmFsInterface->Emsg(epname, error, EINVAL,
                          "prepare - invalid value for pargs.opts =",
                          std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#else

  // The XRootD prepare flags are mutually exclusive
  switch (pargs.opts) {
  case 0:
    if (mMgmFsInterface.isTapeEnabled()) {
      mMgmFsInterface.Emsg(epname, error, EINVAL,
                           "prepare with empty pargs.opts on tape-enabled back-end");
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
    mMgmFsInterface.Emsg(epname, error, EINVAL,
                         "prepare - invalid value for pargs.opts =",
                         std::to_string(pargs.opts).c_str());
    return SFS_ERROR;
  }

#endif
  int error_counter = 0;
  XrdOucErrInfo first_error;
  struct timespec ts_now;
  eos::common::Timing::GetTimeSpec(ts_now);

  // check that all files exist
  for (
    ; pptr
    ; pptr = pptr->next, optr = optr ? optr->next : optr) {
    XrdOucString prep_path = (pptr->text ? pptr->text : "");
    std::string orig_path = prep_path.c_str();
    std::unique_ptr<bulk::File> currentFile = nullptr;
    auto & path_vid = fileToVidMap[orig_path];
    EosCtaReporterPrepareReq eosLog([&](const std::string & in) {
      mMgmFsInterface->writeEosReportRecord(in);
    });
    eosLog
    .addParam(EosCtaReportParam::SEC_APP, "tape_prepare")
    .addParam(EosCtaReportParam::LOG, std::string(mMgmFsInterface->get_logId()))
    .addParam(EosCtaReportParam::PATH, orig_path)
    .addParam(EosCtaReportParam::RUID, path_vid.uid)
    .addParam(EosCtaReportParam::RGID, path_vid.gid)
    .addParam(EosCtaReportParam::TD, path_vid.tident.c_str())
    .addParam(EosCtaReportParam::HOST, mMgmFsInterface->get_host())
    .addParam(EosCtaReportParam::PREP_REQ_REQID, reqid.c_str())
    .addParam(EosCtaReportParam::TS, ts_now.tv_sec)
    .addParam(EosCtaReportParam::TNS, ts_now.tv_nsec);
    eos_info("msg=\"checking file exists\" path=\"%s\"", prep_path.c_str());
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;

      //Valgrind Source and destination overlap in strncpy(...)
      if (prep_path.c_str() != path) {
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
      std::string errorMsg = "prepare - path empty or uses forbidden characters";
      mMgmFsInterface->Emsg(epname, error, ENOENT,
                            errorMsg.append(":").c_str(),
                            orig_path.c_str());

      if (error_counter == 0) {
        first_error = error;
      }

      error_counter++;
      eosLog
      .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
      .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, false)
      .addParam(EosCtaReportParam::PREP_REQ_ERROR, errorMsg);
      continue;
    }

    currentFile = std::make_unique<File>(prep_path.c_str());

    if (mMgmFsInterface->_exists(prep_path.c_str(), check, error, path_vid, "") ||
        (check != XrdSfsFileExistIsFile)) {
      std::string errorMsg =
        "prepare - file does not exist or is not accessible to you";
      mMgmFsInterface->Emsg(epname, error, ENOENT,
                            errorMsg.append(":").c_str(),
                            prep_path.c_str());
      currentFile->setError(errorMsg);

      if (error_counter == 0) {
        first_error = error;
      }

      error_counter++;
      addFileToBulkRequest(std::move(currentFile));
      eosLog
      .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
      .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, false)
      .addParam(EosCtaReportParam::PREP_REQ_ERROR, errorMsg);
      continue;
    }

    //Extended attributes for the current file's parent directory
    eos::IContainerMD::XAttrMap attributes;

    if (!event.empty() &&
        mMgmFsInterface->_attr_ls(eos::common::Path(prep_path.c_str()).GetParentPath(),
                                  error, path_vid,
                                  nullptr, attributes) == 0) {
      bool foundPrepareTag = false;
      std::string eventAttr = "sys.workflow." + event;
      eosLog.addParam(EosCtaReportParam::PREP_REQ_EVENT, event);

      for (const auto& attrEntry : attributes) {
        foundPrepareTag |= attrEntry.first.find(eventAttr) == 0;
      }

      if (foundPrepareTag) {
        pathsToPrepare.emplace_back(&(pptr->text),
                                    optr != nullptr ? & (optr->text) : nullptr,
                                    std::move(eosLog));
      } else {
        // don't do workflow if no such tag
        std::ostringstream oss;
        oss << "No prepare workflow set on the directory " << eos::common::Path(
              prep_path.c_str()).GetParentPath();
        currentFile->setError(oss.str());
        addFileToBulkRequest(std::move(currentFile));
        eosLog
        .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
        .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, true);
        continue;
      }
    } else {
      // don't do workflow if event not set or we can't check attributes
      if (!event.empty()) {
        std::ostringstream oss;
        oss << "Unable to check the extended attributes of the directory "
            << eos::common::Path(prep_path.c_str()).GetParentPath();
        currentFile->setError(oss.str());
        eosLog
        .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
        .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, false)
        .addParam(EosCtaReportParam::PREP_REQ_ERROR, oss.str());
      }

      addFileToBulkRequest(std::move(currentFile));
      continue;
    }

    // check that we have write permission on path
    // This can only be done after we confirm that there the directory contains a prepare workflow attribute
    if (mMgmFsInterface->_access(prep_path.c_str(), P_OK, error, path_vid, "")) {
      std::string errorMsg = "prepare - you don't have prepare permission";
      mMgmFsInterface->Emsg(epname, error, EPERM,
                            errorMsg.append(":").c_str(),
                            prep_path.c_str());
      currentFile->setError(errorMsg);
      pathsToPrepare.pop_back();

      if (error_counter == 0) {
        first_error = error;
      }

      error_counter++;
      addFileToBulkRequest(std::move(currentFile));
      eosLog
      .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, true)
      .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, true)
      .addParam(EosCtaReportParam::PREP_REQ_ERROR, errorMsg);
      continue;
    }

    eos::IFileMD::XAttrMap xattrs;

    if (isStagePrepare()) {
      // Check file status in the extended attributes
      if (mMgmFsInterface->_attr_ls(
              eos::common::Path(prep_path.c_str()).GetPath(), error, path_vid,
              nullptr, xattrs) == 0) {
        XattrSet prepareReqIds;
        auto xattr_it = xattrs.find(eos::common::RETRIEVE_REQID_ATTR_NAME);
        if (xattr_it != xattrs.end() && !xattr_it->second.empty()) {
          prepareReqIds.deserialize(xattr_it->second);
        }

        if (prepareReqIds.values.size() >=
            mMgmFsInterface->getReqIdMaxCount()) {
          std::ostringstream oss;
          oss << "prepare - reached maximum number of retrieve requests on file "
              << " (" << mMgmFsInterface->getReqIdMaxCount() << ")";
          std::string errorMsg = oss.str();
          mMgmFsInterface->Emsg(epname, error, EUSERS,
                                errorMsg.append(":").c_str(),
                                orig_path.c_str());

          currentFile->setError(errorMsg);
          pathsToPrepare.pop_back();

          if (error_counter == 0) {
            first_error = error;
          }
          error_counter++;

          eosLog.addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
              .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, false)
              .addParam(EosCtaReportParam::PREP_REQ_ERROR, errorMsg);
          addFileToBulkRequest(std::move(currentFile));
          continue;
        }
      } else {
        // failed to read extended attributes
        std::ostringstream oss;
        oss << "Unable to check the extended attributes of the file "
            << prep_path;
        currentFile->setError(oss.str());
        pathsToPrepare.pop_back();
        eosLog.addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
            .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, false)
            .addParam(EosCtaReportParam::PREP_REQ_ERROR, oss.str());
        addFileToBulkRequest(std::move(currentFile));
        continue;
      }
    }

    if (currentFile != nullptr) {
      addFileToBulkRequest(std::move(currentFile));
    }
  }

  try {
    saveBulkRequest();
  } catch (const PersistencyException& ex) {
    return ex.fillXrdErrInfo(error, EIO);
  }

  if (isStagePrepare() && nbFilesProvidedByUser == error_counter) {
    //All stage request failed
    eos_err("Unable to prepare - failed to prepare all files with reqID %s",
            reqid.c_str());

    if (error_counter > 0) {
      int err_code;
      std::stringstream err_message;
      err_message << first_error.getErrText(err_code);

      if (error_counter > 1) {
        err_message << " (all " << (error_counter - 1) <<
                    " other files also failed with errors)";
      }

      error.setErrInfo(err_code, err_message.str().c_str());
    }

    if (!ignorePrepareFailures()) {
      return SFS_ERROR;
    }
  }

  //Trigger the prepare workflow
  triggerPrepareWorkflow(pathsToPrepare, cmd, event, reqid, error, vid);
  int retc = SFS_OK;
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5

  // If we generated our own request ID, return it to the client
  if (isStagePrepare()) {
    // If we return SFS_DATA, the first parameter is the length of the buffer, not the error code
    error.setErrInfo(reqid.length() + 1, reqid.c_str());
    retc = SFS_DATA;
  } else {
    if (error_counter > 0) {
      if (!ignorePrepareFailures()) {
        int err_code;
        std::stringstream err_message;
        err_message << first_error.getErrText(err_code);

        if (error_counter > 1) {
          err_message << " (" << (error_counter - 1) <<
                      " other files also failed with errors)";
        }

        error.setErrInfo(err_code, err_message.str().c_str());
        retc = SFS_ERROR;
      }
    }
  }

#endif
  EXEC_TIMING_END("Prepare");
  return retc;
}

void PrepareManager::saveBulkRequest()
{
}

bool PrepareManager::ignorePrepareFailures()
{
  return false;
}

void PrepareManager::addFileToBulkRequest(std::unique_ptr<File>&& file)
{
  //The normal PrepareManager does not have any bulk-request, do nothing
  // Sub-classes may decide to implement this member function
}

int PrepareManager::getPrepareActionsFromOpts(const int pargsOpts) const
{
  const int pargsOptsQoS = Prep_PMASK | Prep_SENDAOK | Prep_SENDERR | Prep_SENDACK
                           | Prep_WMODE | Prep_COLOC | Prep_FRESH;
  return pargsOpts & ~pargsOptsQoS;
}

bool PrepareManager::isStagePrepare() const
{
  return mPrepareAction == PrepareAction::STAGE;
}

void PrepareManager::triggerPrepareWorkflow(
  std::list<std::tuple<char**, char**, EosCtaReporterPrepareReq>>& pathsToPrepare,
  const std::string& cmd, const std::string& event, const XrdOucString& reqid,
  XrdOucErrInfo& error, const eos::common::VirtualIdentity& vid)
{
  for (auto& pathTuple : pathsToPrepare) {
    EosCtaReporterPrepareReq eosLog = std::move(std::get<2>(pathTuple));
    XrdOucString prep_path = (*std::get<0>(pathTuple) ? *std::get<0>
                              (pathTuple) : "");
    {
      const char* inpath = prep_path.c_str();
      const char* ininfo = "";
      NAMESPACEMAP;

      //Valgrind Source and destination overlap in strncpy(...)
      if (prep_path.c_str() != path) {
        prep_path = path;
      }
    }
    XrdOucString prep_info = std::get<1>(pathTuple) != nullptr ?
                             (*std::get<1>(pathTuple) ? *std::get<1>(pathTuple) : "") : "";
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
    auto ret_wfe = mMgmFsInterface->FSctl(SFS_FSCTL_PLUGIN, args, error, &lClient);

    // Log errors but continue to process the rest of the files in the list
    if (ret_wfe != SFS_DATA) {
      std::ostringstream oss;
      oss << "Unable to prepare - synchronous prepare workflow error " <<
          prep_path.c_str() << "; " << error.getErrText();
      eos_err(oss.str().c_str());
      eosLog
      .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, false)
      .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, false)
      .addParam(EosCtaReportParam::PREP_REQ_ERROR, oss.str());
    } else {
      eosLog
      .addParam(EosCtaReportParam::PREP_REQ_SENTTOWFE, true)
      .addParam(EosCtaReportParam::PREP_REQ_SUCCESSFUL, true);
    }
  }
}

std::unique_ptr<QueryPrepareResult> PrepareManager::queryPrepare(
  XrdSfsPrep& pargs, XrdOucErrInfo& error, const XrdSecEntity* client)
{
  std::unique_ptr<QueryPrepareResult> queryPrepareResult(new
      QueryPrepareResult());
  int retCode = doQueryPrepare(pargs, error, client, *queryPrepareResult);
  queryPrepareResult->setReturnCode(retCode);
  return queryPrepareResult;
}

std::unique_ptr<QueryPrepareResult> PrepareManager::queryPrepare(
  XrdSfsPrep& pargs, XrdOucErrInfo& error,
  const common::VirtualIdentity* vidClient)
{
  std::unique_ptr<QueryPrepareResult> queryPrepareResult(new
      QueryPrepareResult());
  int retCode = doQueryPrepare(pargs, error, nullptr, *queryPrepareResult,
                               vidClient);
  queryPrepareResult->setReturnCode(retCode);
  return queryPrepareResult;
}

std::unique_ptr<QueryPrepareResult> PrepareManager::queryPrepare(
  XrdSfsPrep& pargs, XrdOucErrInfo& error,
  const std::string& authz)
{
  std::unique_ptr<QueryPrepareResult> queryPrepareResult(new
      QueryPrepareResult());
  int retCode = doQueryPrepare(pargs, error, nullptr, *queryPrepareResult,
                               nullptr, authz);
  queryPrepareResult->setReturnCode(retCode);
  return queryPrepareResult;
}

int PrepareManager::doQueryPrepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
                                   const XrdSecEntity* client, QueryPrepareResult& result,
                                   const common::VirtualIdentity* vidClient,
                                   const std::string& authz)
{
  EXEC_TIMING_BEGIN("QueryPrepare");
  ACCESSMODE_R;
  eos_info("cmd=\"_prepare_query\"");
  eos::common::VirtualIdentity vid;

  // Map each individual path to a VID
  std::map<std::string, eos::common::VirtualIdentity> fileToVidMap;

  // ID of the original prepare request. We don't need this to look up the list of files in
  // the request, as they are provided in the arguments. Anyway we return it in the reply
  // as a convenience for the client to track which prepare request the query applies to.
  XrdOucString reqid(pargs.reqid);
  int path_cnt = 0;
  FileCollection filesToQueryCollection;

  for (XrdOucTList* pptr = pargs.paths; pptr; pptr = pptr->next) {
    if (!pptr->text) {
      continue;
    }

    fileToVidMap[pptr->text] = eos::common::VirtualIdentity::Nobody();
    filesToQueryCollection.addFile(std::make_unique<File>(pptr->text));
    ++path_cnt;
  }

  if (vidClient != nullptr) {
    vid = *vidClient;
    for (auto & [file_path, file_vid] : fileToVidMap) {
      file_vid = vid;
    }
  } else if (!authz.empty()) {
    vid = eos::common::VirtualIdentity::Nobody();
    mMgmFsInterface->addStats("IdMap", vid.uid, vid.gid, 1);
    std::string env = "authz=" + eos::common::StringConversion::curl_default_escaped(authz);
    for (auto & [file_path, file_vid] : fileToVidMap) {
      // TODO: Replace 'AOP_Stat' by 'AOP_Stage' once this is implemented in XRootD
      eos::common::Mapping::IdMap(client, env.c_str(), client->tident, file_vid, mMgmFsInterface->getTokenHandler(), AOP_Stat, file_path);
    }
  } else  {
    const char* tident = error.getErrUser();
    XrdOucTList* optr = pargs.oinfo;
    std::string info(optr && optr->text ? optr->text : "");
    eos::common::Mapping::IdMap(client, info.c_str(), tident, vid);
    for (auto & [file_path, file_vid] : fileToVidMap) {
      file_vid = vid;
    }
  }

  MAYSTALL;
  {
    const char* path = "/";
    const char* ininfo = "";
    MAYREDIRECT;
  }

  mMgmFsInterface->addStats("QueryPrepare", vid.uid, vid.gid, path_cnt);
  auto filesToQuery = filesToQueryCollection.getAllFiles();
  std::shared_ptr<QueryPrepareResponse> response = result.getResponse();

  // Set the queryPrepareFileResponses for each file in the list
  for (auto& file : *filesToQuery) {
    response->responses.push_back(QueryPrepareFileResponse(file->getPath()));
    auto& rsp = response->responses.back();
    auto currentFile = file;
    auto & file_vid = fileToVidMap[file->getPath()];
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
    //Initialization of variables
    XrdOucErrInfo xrd_error;
    struct stat buf;
    eos::IFileMD::XAttrMap xattrs;
    XrdSfsFileExistence check;

    if (prep_path.length() == 0) {
      currentFile->setErrorIfNotAlreadySet("USER ERROR: path empty or uses forbidden characters");
      goto logErrorAndContinue;
    }

    if (mMgmFsInterface->_exists(prep_path.c_str(), check, error, file_vid, "") ||
        check != XrdSfsFileExistIsFile) {
      currentFile->setErrorIfNotAlreadySet("USER ERROR: file does not exist or is not accessible to you");
      goto logErrorAndContinue;
    }

    rsp.is_exists = true;

    // Check file state (online/offline)
    if (mMgmFsInterface->_stat(rsp.path.c_str(), &buf, xrd_error, file_vid, nullptr,
                               nullptr, false)) {
      currentFile->setErrorIfNotAlreadySet(xrd_error.getErrText());
      goto logErrorAndContinue;
    }

    mMgmFsInterface->_stat_set_flags(&buf);
    rsp.is_on_tape = buf.st_rdev & XRDSFS_HASBKUP;
    rsp.is_online  = !(buf.st_rdev & XRDSFS_OFFLINE);

    // Check file status in the extended attributes
    if (mMgmFsInterface->_attr_ls(eos::common::Path(prep_path.c_str()).GetPath(),
                                  xrd_error, file_vid,
                                  nullptr, xattrs) == 0) {
      auto xattr_it = xattrs.find(eos::common::RETRIEVE_REQID_ATTR_NAME);

      if (xattr_it != xattrs.end()) {
        // has file been requested? (not necessarily with this request ID)
        rsp.is_requested = !xattr_it->second.empty();
        // and is this specific request ID present in the request?
        rsp.is_reqid_present = (xattr_it->second.find(reqid.c_str()) != std::string::npos);
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
        currentFile->setErrorIfNotAlreadySet(xattr_it->second);
      }
    } else {
      // failed to read extended attributes
      currentFile->setErrorIfNotAlreadySet(xrd_error.getErrText());
      goto logErrorAndContinue;
    }

    if (mMgmFsInterface->_access(prep_path.c_str(), P_OK, error, file_vid, "")) {
      currentFile->setError(
        std::string("USER ERROR: you don't have prepare permission"));
      goto logErrorAndContinue;
    }

logErrorAndContinue:

    if (currentFile->getError()) {
      rsp.error_text = currentFile->getError().value();
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

EOSBULKNAMESPACE_END
