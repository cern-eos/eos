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

#include "mgm/convert/ConversionJob.hh"
#include "mgm/Stat.hh"
#include "mgm/FsView.hh"
#include "common/Constants.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMDSvc.hh"

//------------------------------------------------------------------------------
// Utility functions to help with file conversion
//------------------------------------------------------------------------------
namespace
{
//------------------------------------------------------------------------------
// Generate default MGM URL
//------------------------------------------------------------------------------
XrdCl::URL NewUrl()
{
  XrdCl::URL url;
  url.SetProtocol("root");
  url.SetUserName("root");
  url.SetHostPort(gOFS->MgmOfsAlias.c_str(), gOFS->ManagerPort);
  return url;
}

//------------------------------------------------------------------------------
// Generate default TPC properties
//------------------------------------------------------------------------------
XrdCl::PropertyList TpcProperties(uint64_t size)
{
  using eos::common::FileId;
  XrdCl::PropertyList properties;
  properties.Set("force", true);
  properties.Set("posc", false);
  properties.Set("coerce", false);
  properties.Set("sourceLimit", (uint16_t) 1);
  properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
  properties.Set("parallelChunks", (uint16_t) 1);
  properties.Set("tpcTimeout", FileId::EstimateTpcTimeout(size).count());

  if (size) {
    properties.Set("thirdParty", "only");
  }

  return properties;
}
}

EOSMGMNAMESPACE_BEGIN

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
// Destructor
//------------------------------------------------------------------------------
ConversionJob::~ConversionJob()
{
  XrdOucErrInfo error;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  (void) gOFS->_rem(mConversionPath.c_str(), error, rootvid, (const char*) 0);
  gOFS->mFidTracker.RemoveEntry(mFid);
}

//------------------------------------------------------------------------------
// Execute a third-party copy
//------------------------------------------------------------------------------
void ConversionJob::DoIt() noexcept
{
  using eos::common::FileId;
  using eos::common::LayoutId;
  std::string source_xs;
  std::string source_xs_postconversion;
  bool overwrite_checksum;
  uint64_t source_size;
  eos::IFileMD::LocationVector src_locations;
  eos::IFileMD::LocationVector src_unlink_loc;
  gOFS->MgmStats.Add("ConversionJobStarted", 0, 0, 1);
  eos_static_debug("msg=\"starting conversion job\" conversion_id=%s",
                   mConversionInfo.ToString().c_str());

  // Avoid running cancelled jobs
  if (mProgressHandler.ShouldCancel(0)) {
    HandleError("conversion job cancelled before start");
    return;
  }

  mStatus = Status::RUNNING;

  // Retrieve file metadata
  try {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    auto fmd = gOFS->eosFileService->getFileMD(mConversionInfo.mFid);
    mSourcePath = gOFS->eosView->getUri(fmd.get());
    source_size = fmd->getSize();
    src_locations = fmd->getLocations();
    src_unlink_loc = fmd->getUnlinkedLocations();
    eos::appendChecksumOnStringAsHex(fmd.get(), source_xs);
    // Check if conversion requests a checksum rewrite
    std::string file_checksum = LayoutId::GetChecksumString(fmd->getLayoutId());
    std::string conversion_checksum =
      LayoutId::GetChecksumString(mConversionInfo.mLid);
    overwrite_checksum = (file_checksum != conversion_checksum);
  } catch (eos::MDException& e) {
    HandleError("failed to retrieve file metadata",
                SSTR("fxid=" << FileId::Fid2Hex(mConversionInfo.mFid)
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

  // Add the list of file systems to exclude for the new entry
  std::string exclude_fsids = "&eos.excludefsid=";

  for (const auto& fsid : src_locations) {
    exclude_fsids += std::to_string(fsid);
    exclude_fsids += ",";
  }

  for (const auto& fsid : src_unlink_loc) {
    exclude_fsids += std::to_string(fsid);
    exclude_fsids += ",";
  }

  if (*exclude_fsids.rbegin() == ',') {
    exclude_fsids.pop_back();
  }

  dst_cgi << exclude_fsids;
  // Prepare the TPC job
  XrdCl::URL url_src = NewUrl();
  url_src.SetParams("eos.ruid=0&eos.rgid=0&eos.app=eos/converter");
  url_src.SetPath(mSourcePath);
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
  eos_static_info("[tpc]: %s@%s => %s@%s prepare_msg=%s",
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

  eos_static_info("[tpc]: %s => %s status=success tpc_msg=%s",
                  url_src.GetLocation().c_str(), url_dst.GetLocation().c_str(),
                  tpc_status.ToStr().c_str());

  // TPC job succeeded:
  //  - Verify new file has all fragments according to layout
  //  - Verify initial file hasn't changed
  //  - Merge the conversion entry
  // Verify new file has all fragments according to layout
  try {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    auto fmd = gOFS->eosView->getFile(mConversionPath);
    size_t expected = LayoutId::GetStripeNumber(mConversionInfo.mLid) + 1;
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
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mConversionInfo.mFid);
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    auto fmd = gOFS->eosFileService->getFileMD(mConversionInfo.mFid);
    eos::appendChecksumOnStringAsHex(fmd.get(), source_xs_postconversion);
  } catch (eos::MDException& e) {
    eos_static_debug("msg=\"failed to retrieve file metadata\" fxid=%08llx "
                     "ec=%d emsg=\"%s\" conversion_id=%s", mConversionInfo.mFid,
                     e.getErrno(), e.getMessage().str().c_str(),
                     mConversionInfo.ToString().c_str());
  }

  if (source_xs != source_xs_postconversion) {
    HandleError("file checksum changed during conversion",
                SSTR("fxid=" << FileId::Fid2Hex(mConversionInfo.mFid)
                     << " initial_xs=" << source_xs << " final_xs="
                     << source_xs_postconversion));
    return;
  }

  XrdOucErrInfo error;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  // Merge the conversion entry
  if (!Merge()) {
    HandleError("failed to merge conversion entry",
                SSTR("path=" << mSourcePath << " converted_path="
                     << mConversionPath));
    return;
  }

  // Finalize  QoS transition
  XrdOucString target_qos;
  XrdOucString current_qos;

  if (gOFS->_qos_get(mSourcePath.c_str(), error, rootvid,
                     "target_qos", target_qos)) {
    HandleError("error retrieving target_qos", SSTR("path=" << mSourcePath
                << " emsg=\"" << error.getErrText() << "\""));
    return;
  }

  if (target_qos != "null") {
    if (gOFS->_qos_get(mSourcePath.c_str(), error, rootvid,
                       "current_qos", current_qos)) {
      HandleError("error retrieving current_qos", SSTR("path=" << mSourcePath
                  << " emsg=\"" << error.getErrText() << "\""));
      return;
    }

    if (target_qos == current_qos) {
      if (gOFS->_attr_rem(mSourcePath.c_str(), error, rootvid,
                          (const char*) 0, "user.eos.qos.target")) {
        HandleError("error removing target_qos", SSTR("path=" << mSourcePath
                    << " emsg=\"" << error.getErrText() << "\""));
        return;
      }
    }
  }

  gOFS->MgmStats.Add("ConversionJobSuccessful", 0, 0, 1);
  eos_static_info("msg=\"conversion successful\" conversion_id=%s",
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
  eos_static_err("msg=\"%s\" %s conversion_id=%s", emsg.c_str(), details.c_str(),
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
  cgi << "eos.layout.type=" << LayoutId::GetLayoutTypeString(info.mLid)
      << "&eos.layout.nstripes=" << LayoutId::GetStripeNumberString(info.mLid)
      << "&eos.layout.blockchecksum=" << LayoutId::GetBlockChecksumString(info.mLid)
      << "&eos.layout.checksum=" << LayoutId::GetChecksumString(info.mLid)
      << "&eos.layout.blocksize=" << LayoutId::GetBlockSizeString(info.mLid);
  cgi << "&eos.space=" << info.mLocation.getSpace()
      << "&eos.group=" << info.mLocation.getIndex();

  if (!info.mPlctPolicy.empty()) {
    cgi << "&eos.placementpolicy=" << info.mPlctPolicy;
  }

  return cgi.str();
}

//------------------------------------------------------------------------------
// Merge original and the newly converted one so that the initial file
// identifier and all the rest of the metadata information is preserved.
// Steps for a successful conversion
//   1. Update the new locations for original fid
//   2. Trigger FST rename of the physical files from conv_fid to fid
//   3. Unlink the locations for original fid
//   4. Update the layout information for original fid
//   5. Remove the conv_fid and FST local info
//   6. Trigger an MGM resync for the new location of fid
//------------------------------------------------------------------------------
bool
ConversionJob::Merge()
{
  std::list<eos::IFileMD::location_t> conv_locations;
  eos::IFileMD::id_t orig_fid {0ull}, conv_fid {0ull};
  std::shared_ptr<eos::IFileMD> orig_fmd, conv_fmd;
  unsigned long conv_lid = mConversionInfo.mLid;
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);

    try {
      orig_fmd = gOFS->eosFileService->getFileMD(mFid);
      conv_fmd = gOFS->eosView->getFile(mConversionPath);
    } catch (const eos::MDException& e) {
      eos_static_err("msg=\"failed to retrieve file metadata\" msg=\"%s\"",
                     e.what());
      return false;
    }

    orig_fid = orig_fmd->getId();
    conv_fid = conv_fmd->getId();

    // Add the new locations
    for (const auto& loc : conv_fmd->getLocations()) {
      orig_fmd->addLocation(loc);
      conv_locations.push_back(loc);
    }

    gOFS->eosView->updateFileStore(orig_fmd.get());
  }
  // For each location get the FST information and trigger a physical file
  // rename from the conv_fmd(fid) to the orig_fmd(fid)
  bool failed_rename = false;
  std::string fst_host;
  int fst_port;

  for (const auto& loc : conv_locations) {
    {
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      FileSystem* fs = FsView::gFsView.mIdView.lookupByID(loc);

      if ((fs == nullptr) ||
          (fs->GetStatus() != eos::common::BootStatus::kBooted) ||
          (fs->GetConfigStatus() != eos::common::ConfigStatus::kRW)) {
        eos_static_err("msg=\"file system config cannot accept conversion\" "
                       "fsid=%u", loc);
        failed_rename = true;
        break;
      }

      fst_host = fs->GetHost();
      fst_port = fs->getCoreParams().getLocator().getPort();
    }
    std::ostringstream oss;
    oss << "root://" << fst_host << ":" << fst_port << "/?xrd.wantprot=sss";
    XrdCl::URL url(oss.str());

    if (!url.IsValid()) {
      eos_static_err("msg=\"invalid FST url\" url=\"%s\"", oss.str().c_str());
      failed_rename = true;
      break;
    }

    oss.str("");
    // Build up the actual query string
    oss << "/?fst.pcmd=local_rename"
        << "&fst.rename.ofid=" << eos::common::FileId::Fid2Hex(conv_fid)
        << "&fst.rename.nfid=" << eos::common::FileId::Fid2Hex(orig_fid)
        << "&fst.rename.fsid=" << loc
        << "&fst.nspath=" << mSourcePath;
    uint16_t timeout = 10;
    XrdCl::Buffer arg;
    XrdCl::Buffer* response {nullptr};
    XrdCl::FileSystem fs {url};
    arg.FromString(oss.str());
    XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                          response, timeout);

    if (!status.IsOK() || (response->ToString() != "OK")) {
      eos_static_err("msg=\"failed local rename on file system\" fsid=%u status=%d",
                     loc, status.IsOK());
      failed_rename = true;
      delete response;
      break;
    }

    delete response;
    eos_static_debug("msg=\"successful rename on file system\" orig_fxid=%08llx "
                     "conv_fxid=%08llx fsid=%u", orig_fid, conv_fid, loc);
  }

  // Do cleanup in case of failures
  if (failed_rename) {
    // Update locations and clean up conversion file object
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);

    try {
      orig_fmd = gOFS->eosFileService->getFileMD(orig_fid);
    } catch (const eos::MDException& e) {
      eos_static_err("msg=\"failed to retrieve file metadata\" msg=\"%s\"",
                     e.what());
      return false;
    }

    // Unlink all the newly added locations
    for (const auto& loc : orig_fmd->getLocations()) {
      if (std::find(conv_locations.begin(), conv_locations.end(), loc) !=
          conv_locations.end()) {
        orig_fmd->unlinkLocation(loc);
      }
    }

    gOFS->eosView->updateFileStore(orig_fmd.get());
    return false;
  }

  {
    // Update locations and clean up conversion file object
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);

    try {
      orig_fmd = gOFS->eosFileService->getFileMD(orig_fid);
      conv_fmd = gOFS->eosFileService->getFileMD(conv_fid);
    } catch (const eos::MDException& e) {
      eos_static_err("msg=\"failed to retrieve file metadata\" msg=\"%s\"",
                     e.what());
      return false;
    }

    // Unlink the old locations from the original file object
    for (const auto& loc : orig_fmd->getLocations()) {
      if (loc == eos::common::TAPE_FS_ID) {
        continue;
      }

      if (std::find(conv_locations.begin(), conv_locations.end(), loc) ==
          conv_locations.end()) {
        orig_fmd->unlinkLocation(loc);
      }
    }

    // Update the new layout id
    orig_fmd->setLayoutId(conv_lid);
    gOFS->eosView->updateFileStore(orig_fmd.get());
  }

  // Trigger a resync of the local information for the new locations
  for (const auto& loc : conv_locations) {
    if (gOFS->QueryResync(orig_fid, loc, true)) {
      eos_static_err("msg=\"failed to send resync\" fxid=%08llx fsid=%u",
                     orig_fid, loc);
    }
  }

  return true;
}

EOSMGMNAMESPACE_END
