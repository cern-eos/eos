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

#include "mgm/Converter.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Master.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/utils/Checksum.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"
#include "XrdCl/XrdClCopyProcess.hh"

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define _STR(x) #x
#define STR(x) _STR(x)
#define SDUID STR(DAEMONUID)
#define SDGID STR(DAEMONGID)

EOSMGMNAMESPACE_BEGIN

XrdSysMutex eos::mgm::Converter::gSchedulerMutex;
XrdScheduler* eos::mgm::Converter::gScheduler;
XrdSysMutex eos::mgm::Converter::gConverterMapMutex;
std::map<std::string, Converter*> eos::mgm::Converter::gConverterMap;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConverterJob::ConverterJob(eos::common::FileId::fileid_t fid,
                           const char* conversionlayout,
                           std::string& convertername):
  mFid(fid),
  mConversionLayout(conversionlayout),
  mConverterName(convertername)
{
  mProcPath = gOFS->MgmProcConversionPath.c_str();
  mProcPath += "/";
  char xfid[20];
  snprintf(xfid, sizeof(xfid), "%016llx", (long long) mFid);
  mProcPath += xfid;
  mProcPath += ":";
  mProcPath += conversionlayout;
}

//------------------------------------------------------------------------------
// Run a third-party conversion transfer
//------------------------------------------------------------------------------
void
ConverterJob::DoIt()

{
  using eos::common::StringConversion;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;
  eos_static_info("msg=\"start tpc job\" fxid=%08llx layout=%s proc_path=%s",
                  mFid, mConversionLayout.c_str(), mProcPath.c_str());
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> cmd;
  uid_t owner_uid = 0;
  gid_t owner_gid = 0;
  unsigned long long size = 0;
  eos::IFileMD::LocationVector src_locations;
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
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosFileService->getFileMD(mFid);
      owner_uid = fmd->getCUid();
      owner_gid = fmd->getCGid();
      size = fmd->getSize();
      src_locations = fmd->getLocations();
      mSourcePath = gOFS->eosView->getUri(fmd.get());
      eos::common::Path cPath(mSourcePath.c_str());
      cmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      cmd = gOFS->eosView->getContainer(gOFS->eosView->getUri(cmd.get()));
      XrdOucErrInfo error;
      // Load the attributes
      gOFS->_attr_ls(gOFS->eosView->getUri(cmd.get()).c_str(), error, rootvid, 0,
                     attrmap, false, true);
      // Get checksum as string
      eos::appendChecksumOnStringAsHex(fmd.get(), sourceChecksum);
      // Get size
      StringConversion::GetSizeString(sourceSize,
                                      (unsigned long long) fmd->getSize());
      std::string conversionattribute = "sys.conversion.";
      conversionattribute += mConversionLayout.c_str();
      XrdOucString lEnv;
      const char* val = 0;

      if (attrmap.count(mConversionLayout.c_str())) {
        // Conversion layout can either point to a conversion attribute
        // definition in the parent directory.
        val = eos::common::LayoutId::GetEnvFromConversionIdString(
                lEnv, attrmap[mConversionLayout.c_str()].c_str());
      } else {
        // or can be directly a hexadecimal layout representation or env representation
        val =
          eos::common::LayoutId::GetEnvFromConversionIdString(lEnv,
              mConversionLayout.c_str());
      }

      if (val) {
        mTargetCGI = val;
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_err("fxid=%08llx errno=%d msg=\"%s\"\n",
                     mFid, e.getErrno(), e.getMessage().str().c_str());
    }
  }
  bool success = false;

  if (mTargetCGI.length()) {
    // This is a properly defined job
    eos_static_info("msg=\"conversion layout correct\" fxid=%08llx cgi=\"%s\"",
                    mFid, mTargetCGI.c_str());
    // Prepare the TPC copy job
    XrdCl::PropertyList properties;
    XrdCl::PropertyList result;

    if (size) {
      properties.Set("thirdParty", "only");
    }

    properties.Set("force", true);
    properties.Set("posc", false);
    properties.Set("coerce", false);
    std::string exclude_fsids = "&eos.excludefsid=";

    for (const auto& fsid : src_locations) {
      exclude_fsids += std::to_string(fsid);
      exclude_fsids += ",";
    }

    if (*exclude_fsids.rbegin() == ',') {
      exclude_fsids.pop_back();
    }

    std::string source = mSourcePath.c_str();
    std::string target = mProcPath.c_str();
    std::string cgi = "eos.ruid=" SDUID "&eos.rgid=" SDGID "&";
    cgi += mTargetCGI.c_str();
    cgi += exclude_fsids;
    cgi += "&eos.app=eos/converter";
    cgi += "&eos.targetsize=";
    cgi += sourceSize.c_str();

    if (sourceChecksum.length()) {
      cgi += "&eos.checksum=";
      cgi += sourceChecksum.c_str();
    }

    XrdCl::URL url_src;
    url_src.SetProtocol("root");
    url_src.SetHostName("localhost");
    url_src.SetUserName("root");
    url_src.SetParams("eos.ruid=0&eos.rgid=0&eos.app=eos/converter");
    url_src.SetPath(source);
    XrdCl::URL url_dst;
    url_dst.SetProtocol("root");
    url_dst.SetHostName("localhost");
    url_dst.SetUserName("root");
    url_dst.SetParams(cgi);
    url_dst.SetPath(target);
    eos::common::XrdConnIdHelper src_id_helper(gOFS->mXrdConnPool, url_src);
    eos::common::XrdConnIdHelper dst_id_helper(gOFS->mXrdConnPool, url_dst);
    properties.Set("source", url_src);
    properties.Set("target", url_dst);
    properties.Set("sourceLimit", (uint16_t) 1);
    properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
    properties.Set("parallelChunks", (uint8_t) 1);
    XrdCl::CopyProcess lCopyProcess;
    lCopyProcess.AddJob(properties, &result);
    XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();
    eos_static_info("[tpc]: %s=>%s %s",
                    url_src.GetURL().c_str(),
                    url_dst.GetURL().c_str(),
                    lTpcPrepareStatus.ToStr().c_str());

    if (lTpcPrepareStatus.IsOK()) {
      XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
      eos_static_info("[tpc]: %s %d", lTpcStatus.ToStr().c_str(), lTpcStatus.IsOK());
      success = lTpcStatus.IsOK();
    } else {
      success = false;
    }
  } else {
    // -------------------------------------------------------------------------
    // this is a crappy defined job
    // -------------------------------------------------------------------------
    eos_static_err("msg=\"conversion layout definition wrong\" fxid=%08llx "
                   "layout=%s", mFid, mConversionLayout.c_str());
    success = false;
  }

  // Check if the file is still the same on source side
  {
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mFid);
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosFileService->getFileMD(mFid);
      // get the checksum string if defined
      eos::appendChecksumOnStringAsHex(fmd.get(), sourceAfterChecksum);
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_err("fxid=%08llx errno=%d msg=\"%s\"\n",
                     mFid, e.getErrno(), e.getMessage().str().c_str());
    }

    if (sourceChecksum != sourceAfterChecksum) {
      eos_static_err("fxid=%08llx conversion failed since file was modified", mFid);
      success = false;
    }
  }
  // Check if the new file has all fragments according to the layout
  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosView->getFile(mProcPath);

      if ((eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1) !=
          fmd->getNumLocation()) {
        eos_static_err("[tpc] failing conversion for wrong stripe number : "
                       "path=%s n-layout-stripes=%u n-stripes=%u",
                       mProcPath.c_str(),
                       eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1,
                       fmd->getNumLocation());
        success = false;
      }
    } catch (eos::MDException& e) {
      eos_static_err("path=%s errno=%d msg=\"%s\"\n",
                     mProcPath.c_str(), e.getErrno(), e.getMessage().str().c_str());
      errno = e.getErrno();
      success = false;
    }
  }
  eos_static_info("msg=\"stop tpc job\" fxid=%08llx layout=%s success=%d",
                  mFid, mConversionLayout.c_str(), success);
  {
    // We can only call-back to the Converter object if it wasn't destroyed/
    // recreated in the mean-while.
    XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
    stopConverter = Converter::gConverterMap[mConverterName];

    if (startConverter && (startConverter == stopConverter)) {
      stopConverter->GetSignal()->Signal();
      stopConverter->DecActiveJobs();
    }
  }

  if (Merge()) {
    eos_static_info("msg=\"successful conversion\" name=\"%s\"",
                    mConversionLayout.c_str());
    gOFS->MgmStats.Add("ConversionDone", owner_uid, owner_gid, 1);
  } else {
    eos_static_err("msg=\"failed conversion\" name=\"%s\"",
                   mConversionLayout.c_str());
    gOFS->MgmStats.Add("ConversionFailed", owner_uid, owner_gid, 1);
  }

  // Delete conversion entry
  (void) gOFS->_rem(mProcPath.c_str(), error, rootvid, (const char*) 0);
  delete this;
}

//------------------------------------------------------------------------------
// Merge origial and the newly converted one so that the initial file
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
ConverterJob::Merge()
{
  std::list<eos::IFileMD::location_t> conv_locations;
  eos::IFileMD::id_t orig_fid {0ull}, conv_fid {0ull};
  std::shared_ptr<eos::IFileMD> orig_fmd, conv_fmd;
  unsigned long conv_lid = eos::common::LayoutId::GetLidFromConversionId
                           (mConversionLayout.c_str());
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    try {
      orig_fmd = gOFS->eosFileService->getFileMD(mFid);
      conv_fmd = gOFS->eosView->getFile(mProcPath);
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
    delete response;

    if (!status.IsOK()) {
      eos_static_err("msg=\"failed local rename on file system\" fsid=%u", loc);
      failed_rename = true;
      break;
    }

    eos_static_debug("msg=\"successful rename on file system\" orig_fid=%08llx "
                     "conv_fid=%08llx fsid=%u", orig_fid, conv_fid, loc);
  }

  // Do cleanup in case of failures
  if (failed_rename) {
    // Update locations and clean up conversion file object
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

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
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

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
    if (gOFS->SendResync(orig_fid, loc, true)) {
      eos_static_err("msg=\"failed to send resync\" fid=%08llx fsid=%u",
                     orig_fid, loc);
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Constructor by space name
//------------------------------------------------------------------------------
Converter::Converter(const char* spacename)
{
  mSpaceName = spacename;
  XrdSysMutexHelper sLock(gSchedulerMutex);

  if (!gScheduler) {
    gScheduler = new XrdScheduler(&gMgmOfsEroute, &gMgmOfsTrace, 2, 128, 64);
    gScheduler->Start();
  }

  {
    XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
    // store this object in the converter map for callback
    gConverterMap[spacename] = this;
  }

  mActiveJobs = 0;
  mThread.reset(&Converter::Convert, this);
}

//------------------------------------------------------------------------------
// Stop convertor thread
//------------------------------------------------------------------------------
void
Converter::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Converter::~Converter()
{
  Stop();
  XrdSysMutexHelper cLock(Converter::gConverterMapMutex);
  gConverterMap[mSpaceName] = 0;
}

//------------------------------------------------------------------------------
// Eternal loop trying to run conversion jobs
//------------------------------------------------------------------------------
void
Converter::Convert(ThreadAssistant& assistant) noexcept
{
  using eos::common::StringConversion;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(10));

  // Reset old jobs pending from service restart/crash
  if (gOFS->mMaster->IsMaster()) {
    ResetJobs();
  }

  // loop forever until cancelled
  // the conversion fid set points from file id to conversion attribute name in
  // the parent container of the fid
  std::map<eos::common::FileId::fileid_t, std::string> lConversionFidMap;

  while (!assistant.terminationRequested()) {
    bool IsSpaceConverter = true;
    bool IsMaster = true;
    int lSpaceTransfers = 0;
    {
      // Extract the current settings if conversion enabled and how many
      // conversion jobs should run.
      uint64_t timeout_ns = 100 * 1e6; // 100ms

      // Try to read lock the mutex
      while (!FsView::gFsView.ViewMutex.TimedRdLock(timeout_ns)) {
        if (assistant.terminationRequested()) {
          return;
        }
      }

      if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
        FsView::gFsView.ViewMutex.UnLockRead();
        IsSpaceConverter = false;
        break;
      }

      if (FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("converter")
          == "on") {
        IsSpaceConverter = true;
      } else {
        IsSpaceConverter = false;
      }

      lSpaceTransfers = atoi(FsView::gFsView.mSpaceView[mSpaceName.c_str()]-> \
                             GetConfigMember("converter.ntx").c_str());
      FsView::gFsView.ViewMutex.UnLockRead();
    }
    IsMaster = gOFS->mMaster->IsMaster();

    if (IsMaster && IsSpaceConverter) {
      if (lConversionFidMap.size() == 0) {
        XrdMgmOfsDirectory dir;
        int listrc = 0;
        // fill the conversion queue with the existing entries
        listrc = dir.open(gOFS->MgmProcConversionPath.c_str(), rootvid,
                          (const char*) 0);

        if (listrc == SFS_OK) {
          const char* val;

          while ((val = dir.nextEntry())) {
            XrdOucString sfxid = val;

            if ((sfxid == ".") || (sfxid == "..")) {
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

            if (gOFS->_stat(lFullConversionFilePath.c_str(), &buf, error, rootvid, "")) {
              continue;
            }

            if ((buf.st_uid != 0) /* this is a failed or scheduled entry */) {
              continue;
            }

            if (StringConversion::SplitKeyValue(sfxid, fxid,
                                                conversionattribute) &&
                (eos::common::FileId::Hex2Fid(fxid.c_str())) &&
                (fxid.length() == 16)) {
              if (conversionattribute.beginswith(mSpaceName.c_str())) {
                // This is a valid entry like <fxid>:<attribute> and we add it
                // to the set if <attribute> starts with our space name!
                lConversionFidMap[eos::common::FileId::Hex2Fid(fxid.c_str())] =
                  conversionattribute.c_str();

                // We set owner admin to indicate that this is a scheduled entry
                if (!gOFS->_chown(lFullConversionFilePath.c_str(), 3, 4, error,
                                  rootvid, (const char*) 0)) {
                  eos_static_info("msg=\"tagged scheduled conversion entry with"
                                  " owner admin\" name=\"%s\"",
                                  conversionattribute.c_str());
                } else {
                  eos_static_err("msg=\"failed to tag with owner admin scheduled"
                                 " conversion job entry\" name=\"%s\"",
                                 conversionattribute.c_str());
                }
              }
            } else {
              // fxid is something else here
              eos_static_warning("msg='invalid key:value format' split=%d fxid=%08llx fxid=|%s|length=%u",
                                 StringConversion::SplitKeyValue(sfxid, fxid,
                                     conversionattribute),
                                 eos::common::FileId::Hex2Fid(fxid.c_str()),
                                 fxid.c_str(), fxid.length());

              // This is an invalid entry not following the <key(016x)>:<value>
              // syntax - just remove it
              if (!gOFS->_rem(lFullConversionFilePath.c_str(), error, rootvid,
                              (const char*) 0)) {
                eos_static_warning("msg=\"deleted invalid conversion entry\" "
                                   "name=\"%s\"", val);
              }
            }
          }

          dir.close();
        } else {
          eos_static_err("msg=\"failed to list conversion directory\" path=\"%s\"",
                         gOFS->MgmProcConversionPath.c_str());
        }
      }

      eos_static_info("converter is enabled ntx=%d nqueued=%d",
                      lSpaceTransfers,
                      lConversionFidMap.size());
    } else {
      lConversionFidMap.clear();

      if (IsMaster) {
        eos_static_debug("converter is disabled");
      } else {
        eos_static_debug("converter is in slave mode");
      }
    }

    // Schedule some conversion jobs if any
    int nschedule = lSpaceTransfers - mActiveJobs;

    for (int i = 0; i < nschedule; i++) {
      if (lConversionFidMap.size()) {
        auto it = lConversionFidMap.begin();
        ConverterJob* job = new ConverterJob(it->first,
                                             it->second.c_str(),
                                             mSpaceName);
        // use the global shared scheduler
        XrdSysMutexHelper sLock(gSchedulerMutex);
        gScheduler->Schedule((XrdJob*) job);
        IncActiveJobs();
        // Remove the entry from the conversion map
        lConversionFidMap.erase(lConversionFidMap.begin());
      } else {
        break;
      }
    }

    // Let some time pass or wait for a notification
    for (int i = 0; i < 10; i++) {
      mDoneSignal.Wait(1);

      if (assistant.terminationRequested()) {
        return;
      }
    }
  }
}

//------------------------------------------------------------------------------
// Publish the active job number in the space view
//------------------------------------------------------------------------------
void
Converter::PublishActiveJobs()
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  char sactive[256];
  snprintf(sactive, sizeof(sactive) - 1, "%lu", mActiveJobs);
  FsView::gFsView.mSpaceView[mSpaceName.c_str()]->SetConfigMember("stat.converter.active",
      sactive, true);
}

//------------------------------------------------------------------------------
// Reset pending conversion entries
//------------------------------------------------------------------------------
void
Converter::ResetJobs()
{
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;
  XrdMgmOfsDirectory dir;
  int listrc = dir.open(gOFS->MgmProcConversionPath.c_str(), rootvid,
                        (const char*) 0);

  if (listrc == SFS_OK) {
    const char* val;

    while ((val = dir.nextEntry())) {
      XrdOucString sfxid = val;

      if ((sfxid == ".") || (sfxid == "..")) {
        continue;
      }

      std::string lFullConversionFilePath =
        gOFS->MgmProcConversionPath.c_str();
      lFullConversionFilePath += "/";
      lFullConversionFilePath += val;

      if (!gOFS->_chown(lFullConversionFilePath.c_str(), 0, 0, error,
                        rootvid, (const char*) 0)) {
        eos_static_info("msg=\"reset scheduled conversion entry with owner "
                        "root\" name=\"%s\"", lFullConversionFilePath.c_str());
      } else {
        eos_static_err("msg=\"failed to reset with owner root scheduled old "
                       "job entry\" name=\"%s\"", lFullConversionFilePath.c_str());
      }
    }
  }

  dir.close();
}

EOSMGMNAMESPACE_END
