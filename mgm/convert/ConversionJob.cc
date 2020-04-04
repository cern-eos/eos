//------------------------------------------------------------------------------
// File: ConversionJob.cc
// Author: Mihai Patrascoiu - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/Stat.hh"
#include "mgm/convert/ConversionJob.hh"
#include "common/Logging.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <XrdCl/XrdClCopyProcess.hh>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Utility functions to help with file conversion
//------------------------------------------------------------------------------
namespace
{
//------------------------------------------------------------------------------
// Generate default MGM URL
//------------------------------------------------------------------------------
XrdCl::URL NewUrl() {
  XrdCl::URL url;
  url.SetProtocol("root");
  url.SetUserName("root");
  url.SetHostPort(gOFS->MgmOfsAlias.c_str(), gOFS->ManagerPort);
  return url;
}


//------------------------------------------------------------------------------
// Generate default TPC properties
//------------------------------------------------------------------------------
XrdCl::PropertyList TpcProperties(uint64_t size) {
  using eos::common::FileId;
  XrdCl::PropertyList properties;
  properties.Set("force", true);
  properties.Set("posc", false);
  properties.Set("coerce", false);
  properties.Set("sourceLimit", (uint16_t) 1);
  properties.Set("chunkSize", (uint32_t) (4 * 1024 * 1024));
  properties.Set("parallelChunks", (uint16_t) 1);
  properties.Set("tpcTimeout", FileId::EstimateTpcTimeout(size).count());

  if (size) {
    properties.Set("thirdParty", "only");
  }

  return properties;
}
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConversionJob::ConversionJob(const eos::IFileMD::id_t fid,
                             const ConversionInfo& conversion_info) :
  mFid(fid), mConversionInfo(conversion_info), mStatus(Status::PENDING)
{
  mConversionPath =
    SSTR(gOFS->MgmProcConversionPath << "/" << mConversionInfo.ToString());
}

//------------------------------------------------------------------------------
// Execute a third-party copy
//------------------------------------------------------------------------------
void ConversionJob::DoIt() noexcept
{
  using eos::common::FileId;
  using eos::common::LayoutId;
  std::string source_path;
  std::string source_xs;
  std::string source_xs_postconversion;
  bool overwrite_checksum;
  uint64_t source_size;

  gOFS->MgmStats.Add("ConversionJobStarted", 0, 0, 1);
  eos_debug("msg=\"starting conversion job\" conversion_id=%s",
            mConversionInfo.ToString().c_str());

  // Avoid running cancelled jobs
  if (mProgressHandler.ShouldCancel(0)) {
    HandleError("conversion job cancelled before start");
    return;
  }

  mStatus = Status::RUNNING;

  // Retrieve file metadata
  try {
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mConversionInfo.fid);
    source_path = gOFS->eosView->getUri(fmd.get());
    source_size = fmd->getSize();
    eos::appendChecksumOnStringAsHex(fmd.get(), source_xs);

    // Check if conversion requests a checksum rewrite
    std::string file_checksum = LayoutId::GetChecksumString(fmd->getLayoutId());
    std::string conversion_checksum =
      LayoutId::GetChecksumString(mConversionInfo.lid);
    overwrite_checksum = (file_checksum != conversion_checksum);
  } catch (eos::MDException& e) {
    HandleError("failed to retrieve file metadata",
                SSTR("fxid=" << FileId::Fid2Hex(mConversionInfo.fid)
                     << " ec=" << e.getErrno()
                     << " emsg=\"" << e.getMessage().str() << "\""));
    return;
  }

  // Construct destination CGI
  std::ostringstream dst_cgi;
  dst_cgi << "&eos.ruid=" << DAEMONUID << "&eos.rgid=" << DAEMONGID
          << "&" << ConversionCGI(mConversionInfo)
          << "&eos.app=eos/converter"
          << "&eos.targetsize=" << source_size;

  if (source_xs.size() && !overwrite_checksum) {
    dst_cgi << "&eos.checksum=" << source_xs;
  }

  // Prepare the TPC job
  XrdCl::URL url_src = NewUrl();
  url_src.SetParams("eos.ruid=0&eos.rgid=0&eos.app=eos/converter");
  url_src.SetPath(source_path);

  XrdCl::URL url_dst = NewUrl();
  url_dst.SetParams(dst_cgi.str());
  url_dst.SetPath(mConversionPath);

  eos::common::XrdConnIdHelper src_id_helper(gOFS->mXrdConnPool, url_src);
  eos::common::XrdConnIdHelper dst_id_helper(gOFS->mXrdConnPool, url_dst);
  XrdCl::PropertyList properties = TpcProperties(source_size);
  properties.Set("source", url_src);
  properties.Set("target", url_dst);

  // Create the TPC job
  XrdCl::PropertyList result;
  XrdCl::CopyProcess copy;
  copy.AddJob(properties, &result);
  XrdCl::XRootDStatus prepare_status = copy.Prepare();
  eos_info("[tpc]: %s@%s => %s@%s prepare_msg=%s",
           url_src.GetHostId().c_str(), url_src.GetLocation().c_str(),
           url_dst.GetHostId().c_str(), url_dst.GetLocation().c_str(),
           prepare_status.ToStr().c_str());

  // Check the TPC prepare status
  if (!prepare_status.IsOK()) {
    HandleError("prepare conversion failed");
    return;
  }

  // Trigger the TPC job
  XrdCl::XRootDStatus tpc_status = copy.Run(&mProgressHandler);

  if (!tpc_status.IsOK()) {
    HandleError(tpc_status.ToStr(),
                SSTR("tpc_src=" << url_src.GetLocation()
                     << " tpc_dst=" << url_dst.GetLocation()));
    return;
  }

  eos_info("[tpc]: %s => %s status=success tpc_msg=%s",
           url_src.GetLocation().c_str(), url_dst.GetLocation().c_str(),
           tpc_status.ToStr().c_str());

  // -------------------------------------------------------------------------
  // TPC job succeeded:
  //  - Verify new file has all fragments according to layout
  //  - Verify initial file hasn't changed
  //  - Merge the conversion entry
  // -------------------------------------------------------------------------

  // Verify new file has all fragments according to layout
  try {
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosView->getFile(mConversionPath);
    size_t expected =
      eos::common::LayoutId::GetStripeNumber(mConversionInfo.lid) + 1;
    size_t actual = fmd->getNumLocation();

    if (expected != actual) {
      HandleError("converted file replica number mismatch",
                  SSTR("expected=" << expected << " actual=" << actual));
      return;
    }
  } catch (eos::MDException& e) {
    HandleError("failed to retrieve converted file metadata",
                SSTR("path=" << mConversionPath << " ec=" << e.getErrno()
                     << " emsg=\"" << e.getMessage().str() << "\""));
    return;
  }

  // Verify initial file hasn't changed
  try {
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mConversionInfo.fid);
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
    auto fmd = gOFS->eosFileService->getFileMD(mConversionInfo.fid);
    eos::appendChecksumOnStringAsHex(fmd.get(), source_xs_postconversion);
  } catch (eos::MDException& e) {
    eos_debug("msg=\"failed to retrieve file metadata\" fxid=%08llx "
              "ec=%d emsg=\"%s\" conversion_id=%s", mConversionInfo.fid,
              e.getErrno(), e.getMessage().str().c_str(),
              mConversionInfo.ToString().c_str());
  }

  if (source_xs != source_xs_postconversion) {
    HandleError("file checksum changed during conversion",
                SSTR("fxid=" << FileId::Fid2Hex(mConversionInfo.fid)
                     << " initial_xs=" << source_xs << " final_xs="
                     << source_xs_postconversion));
    return;
  }

  XrdOucErrInfo error;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  // Merge the conversion entry
  if (gOFS->merge(mConversionPath.c_str(), source_path.c_str(), error, rootvid)) {
    HandleError("failed to merge conversion entry", SSTR("path="
                << source_path << " converted_path=" << mConversionPath));
    return;
  }

  // Finalize  QoS transition
  XrdOucString target_qos;
  XrdOucString current_qos;

  if (gOFS->_qos_get(source_path.c_str(), error, rootvid,
                     "target_qos", target_qos)) {
    HandleError("error retrieving target_qos", SSTR("path=" << source_path
                << " emsg=\"" << error.getErrText() << "\""));
    return;
  }

  if (target_qos != "null") {
    if (gOFS->_qos_get(source_path.c_str(), error, rootvid,
                       "current_qos", current_qos)) {
      HandleError("error retrieving current_qos", SSTR("path=" << source_path
                  << " emsg=\"" << error.getErrText() << "\""));
      return;
    }

    if (target_qos == current_qos) {
      if (gOFS->_attr_rem(source_path.c_str(), error, rootvid,
                          (const char*) 0, "user.eos.qos.target")) {
        HandleError("error removing target_qos", SSTR("path=" << source_path
                    << " emsg=\"" << error.getErrText() << "\""));
        return;
      }
    }
  }

  gOFS->MgmStats.Add("ConversionJobSuccessful", 0, 0, 1);
  eos_info("msg=\"conversion successful\" conversion_id=%s",
           mConversionInfo.ToString().c_str());
  mStatus = Status::DONE;

  return;
}

//------------------------------------------------------------------------------
// Log the error message, store it and set the job as failed
//------------------------------------------------------------------------------
void ConversionJob::HandleError(const std::string& emsg,
                                const std::string& details)
{
  gOFS->MgmStats.Add("ConversionJobFailed", 0, 0, 1);
  eos_err("msg=\"%s\" %s conversion_id=%s", emsg.c_str(), details.c_str(),
          mConversionInfo.ToString().c_str());
  mErrorString = (details.empty()) ? emsg : (emsg + " -- " + details);
  mStatus = Status::FAILED;
}

//------------------------------------------------------------------------------
// Construct CGI from conversion info
//------------------------------------------------------------------------------
std::string ConversionJob::ConversionCGI(const ConversionInfo& info) const
{
  using eos::common::LayoutId;
  std::ostringstream cgi;

  cgi << "eos.layout.type=" << LayoutId::GetLayoutTypeString(info.lid)
      << "&eos.layout.nstripes=" << LayoutId::GetStripeNumberString(info.lid)
      << "&eos.layout.blockchecksum=" << LayoutId::GetBlockChecksumString(info.lid)
      << "&eos.layout.checksum=" << LayoutId::GetChecksumString(info.lid)
      << "&eos.layout.blocksize=" << LayoutId::GetBlockSizeString(info.lid);

  cgi << "&eos.space=" << info.location.getSpace()
      << "&eos.group=" << info.location.getIndex();

  if (!info.plct_policy.empty()) {
    cgi << "&eos.placementpolicy=" << info.plct_policy;
  }

  return cgi.str();
}

EOSMGMNAMESPACE_END
