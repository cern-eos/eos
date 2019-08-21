// ----------------------------------------------------------------------
// File: QoS.cc
// Author: Mihai Patrascoiu - CERN
// ----------------------------------------------------------------------

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

//------------------------------------------------------------------------------
// This file is included in the source code of XrdMgmOfs.cc
//------------------------------------------------------------------------------

namespace
{
  //----------------------------------------------------------------------------
  //! Helper class for retrieving QoS properties.
  //!
  //! The class takes as input a file metadata pointer,
  //! which it will use to query for properties.
  //!
  //! The class should be called under lock to ensure thread safety.
  //----------------------------------------------------------------------------
  class QoSGetter
  {
  public:
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    QoSGetter(std::shared_ptr<eos::IFileMD> _fmd) : fmd(_fmd) {}

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~QoSGetter() = default;

    //--------------------------------------------------------------------------
    //! Retrieve all QoS properties
    //--------------------------------------------------------------------------
    eos::IFileMD::QoSAttrMap All();

    //--------------------------------------------------------------------------
    //! Retrieve CDMI-specific QoS properties
    //--------------------------------------------------------------------------
    eos::IFileMD::QoSAttrMap CDMI();

    //--------------------------------------------------------------------------
    //! Retrieve QoS property by key
    //--------------------------------------------------------------------------
    std::string Get(const std::string& key) const;

  private:
    //--------------------------------------------------------------------------
    // Methods retrieving a particular QoS property
    //--------------------------------------------------------------------------
    std::string Attr(const char* key) const;
    std::string ChecksumType() const;
    std::string DiskSize() const;
    std::string LayoutType() const;
    std::string Id() const;
    std::string Path() const;
    std::string Placement() const;
    std::string Redundancy() const;
    std::string Size() const;

    std::shared_ptr<eos::IFileMD> fmd; ///< pointer to file metadata
    ///< dispatch table based on QoS key word
    std::map<std::string, std::function<std::string()>> dispatch {
      { "checksum",   [this](){ return QoSGetter::ChecksumType(); } },
      { "disksize",   [this](){ return QoSGetter::DiskSize();     } },
      { "layout",     [this](){ return QoSGetter::LayoutType();   } },
      { "id",         [this](){ return QoSGetter::Id();           } },
      { "path",       [this](){ return QoSGetter::Path();         } },
      { "placement",  [this](){ return QoSGetter::Placement();    } },
      { "redundancy", [this](){ return QoSGetter::Redundancy();   } },
      { "size",       [this](){ return QoSGetter::Size();         } }
    };
  };

  //----------------------------------------------------------------------------
  // Retrieve all QoS properties
  //----------------------------------------------------------------------------
  eos::IFileMD::QoSAttrMap QoSGetter::All() {
    eos::IFileMD::QoSAttrMap qosMap = CDMI();

    for (auto& it: dispatch) {
      qosMap.emplace(it.first, it.second());
    }

    return qosMap;
  }

  //----------------------------------------------------------------------------
  // Retrieve CDMI-specific QoS properties
  //----------------------------------------------------------------------------
  eos::IFileMD::QoSAttrMap QoSGetter::CDMI() {
    eos::IFileMD::QoSAttrMap cdmiMap;
    std::string sgeotags = "";
    size_t count = 0;

    cdmiMap["cdmi_data_redundancy_provided"] =
        std::to_string(eos::common::LayoutId::GetRedundancyStripeNumber(
            fmd->getLayoutId()));
    cdmiMap["cdmi_latency_provided"] = "100";

    for (auto& location: fmd->getLocations()) {
      std::string geotag = "null";

      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
      eos::common::FileSystem* filesystem =
          FsView::gFsView.mIdView.lookupByID(location);

      if (filesystem) {
        geotag = filesystem->GetString("stat.geotag");
      }

      sgeotags += geotag;

      if (++count < fmd->getNumLocation()) {
        sgeotags += " ";
      }
    }

    cdmiMap["cdmi_geographic_placement_provided"] = sgeotags;

    return cdmiMap;
  }

  //----------------------------------------------------------------------------
  // Retrieve QoS property by key
  //----------------------------------------------------------------------------
  std::string QoSGetter::Get(const std::string& key) const {
    std::string value = "";
    auto it = dispatch.find(key);

    if (it != dispatch.end()) {
      value = it->second();
    }

    return value;
  }

  //----------------------------------------------------------------------------
  // Methods retrieving a particular QoS property
  //----------------------------------------------------------------------------

  std::string QoSGetter::Attr(const char* key) const {
    std::string value = "null";
    const auto& attrMap = fmd->getAttributes();

    if (attrMap.count(key)) {
      value = attrMap.at(key);
    }

    return value;
  }

  std::string QoSGetter::ChecksumType() const {
    return eos::common::LayoutId::GetChecksumStringReal(fmd->getLayoutId());
  }

  std::string QoSGetter::DiskSize() const {
    uint64_t physicalSize = fmd->getSize() *
        eos::common::LayoutId::GetSizeFactor(fmd->getLayoutId());
    return std::to_string(physicalSize);
  }

  std::string QoSGetter::LayoutType() const {
    return eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
  }

  std::string QoSGetter::Id() const {
    return std::to_string(fmd->getId());
  }

  std::string QoSGetter::Path() const {
    std::string path = "null";

    try {
      path = gOFS->eosView->getUri(fmd.get());
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception retrieving path\" fxid=%08llx "
                       "ec=%d emsg=\"%s\"", fmd->getId(),
                       e.getErrno(), e.getMessage().str().c_str());
    }

    return path;
  }

  std::string QoSGetter::Placement() const {
    std::string placement = "null";

    try {
      std::string path = gOFS->eosView->getUri(fmd.get());
      eos::common::Path cPath(path);

      eos::mgm::Scheduler::tPlctPolicy plctplcy;
      eos::common::VirtualIdentity vid;
      std::string targetgeotag;
      XrdOucErrInfo error;
      XrdOucEnv env;

      eos::IContainerMD::XAttrMap attrmap;
      gOFS->_attr_ls(cPath.GetParentPath(), error, vid, 0, attrmap, false);

      Policy::GetPlctPolicy(cPath.GetParentPath(), attrmap, vid, env, plctplcy,
                            targetgeotag);
      placement = Scheduler::PlctPolicyString(plctplcy);
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception retrieving path\" fxid=%08llx "
                       "ec=%d emsg=\"%s\"", fmd->getId(),
                       e.getErrno(), e.getMessage().str().c_str());
    }

    return placement;
  }

  std::string QoSGetter::Redundancy() const {
    return eos::common::LayoutId::GetStripeNumberString(fmd->getLayoutId());
  }

  std::string QoSGetter::Size()  const {
    return std::to_string(fmd->getSize());
  }
}

//------------------------------------------------------------------------------
// List QoS properties for a given entry - low-level API
//------------------------------------------------------------------------------
int
XrdMgmOfs::_qos_ls(const char* path, XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   eos::IFileMD::QoSAttrMap& map,
                   bool only_cdmi)
{
  static const char* epname = "qos_ls";
  EXEC_TIMING_BEGIN("QoSLs");
  gOFS->MgmStats.Add("QoSLs", vid.uid, vid.gid, 1);
  errno = 0;

  eos_info("msg=\"list QoS values\" path=%s only_cdmi=%d", path, only_cdmi);

  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
  eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);

  try {
    std::shared_ptr<eos::IFileMD> fmd = gOFS->eosView->getFile(path);
    map = (only_cdmi) ? QoSGetter{fmd}.CDMI()
                      : QoSGetter{fmd}.All();
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception retrieving file metadata\" path=%s "
              "ec=%d emsg=\"%s\"", path, e.getErrno(),
              e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("QoSLs");

  if (errno) {
    std::string keys = (only_cdmi) ? "cdmi" : "all";
    return Emsg(epname, error, errno, "list QoS values",
                SSTR("keys=" << keys << " path=" << path).c_str());
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Get QoS property for a given entry by key - low-level API
//------------------------------------------------------------------------------
int
XrdMgmOfs::_qos_get(const char* path, XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    const char* key,
                    XrdOucString& value)
{
  static const char* epname = "qos_get";
  EXEC_TIMING_BEGIN("QoSGet");
  gOFS->MgmStats.Add("QoSGet", vid.uid, vid.gid, 1);
  errno = 0;

  eos_info("msg=\"get QoS value\" path=%s key=%s",
           path, (key ? key : "(null)"));

  if (!key) {
    return Emsg(epname, error, EINVAL, "get QoS value - empty key");
  }

  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
  eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);

  try {
    std::shared_ptr<eos::IFileMD> fmd = gOFS->eosView->getFile(path);
    value = QoSGetter{fmd}.Get(key).c_str();
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception retrieving file metadata\" path=%s "
              "ec=%d emsg=\"%s\"", path, e.getErrno(),
              e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("QoSGet");

  if (!value.length()) {
    return Emsg(epname, error, EINVAL, "get QoS value - invalid key",
                SSTR(key << " path=" << path).c_str());
  }

  if (errno) {
    return Emsg(epname, error, errno, "get QoS value",
                SSTR(key << " path=" << path).c_str());
  }

  return SFS_OK;
}

//----------------------------------------------------------------------------
// Schedule QoS properties for a given entry - low-level API
// If no value is provided for a QoS property, it will be left unchanged.
//----------------------------------------------------------------------------
int
XrdMgmOfs::_qos_set(const char* path, XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    std::string& conversion_id,
                    int layout, int nstripes,
                    int checksum, std::string policy)
{
  using eos::common::LayoutId;
  static const char* epname = "qos_set";
  EXEC_TIMING_BEGIN("QoSSet");
  gOFS->MgmStats.Add("QoSSet", vid.uid, vid.gid, 1);
  errno = 0;

  eos_info("msg=\"set QoS - initial values\" path=%s layout=%d nstripes=%d "
           "checksum=%d policy=%s", path, layout, nstripes,
           checksum, policy.c_str());

  std::shared_ptr<eos::IFileMD> fmd = 0;
  eos::common::FileId::fileid_t fileid = 0;
  eos::IFileMD::location_t fsid = 0;
  LayoutId::layoutid_t layoutid = 0;
  unsigned long current_layout = 0;
  unsigned long current_checksumid = 0;
  unsigned long current_nstripes = 0;

  {
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
    eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosView->getFile(path);
      fileid = fmd->getId();
      layoutid = fmd->getLayoutId();

      // Extract current QoS properties
      current_layout = LayoutId::GetLayoutType(layoutid);
      current_checksumid = LayoutId::GetChecksum(layoutid);
      current_nstripes = LayoutId::GetStripeNumber(layoutid) + 1;

      if (fmd->getNumLocation()) {
        const auto& locations_vect = fmd->getLocations();
        fsid = *(locations_vect.begin());
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception retrieving file metadata\" path=%s "
                "ec=%d emsg=\"%s\"", path, e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  if (!fmd) {
    return Emsg(epname, error, errno, "retrieve file metadata", path);
  }

  // Extract current scheduling space
  std::string space = "";

  {
    eos::common::RWMutexReadLock rlock(FsView::gFsView.ViewMutex);
    auto filesystem = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!filesystem) {
      return Emsg(epname, error, errno, "retrieve filesystem location", path);
    }

    space = filesystem->GetString("schedgroup");
    size_t ppos = space.find(".");
    if (ppos != std::string::npos) {
      space.erase(ppos);
    }
  }

  layout = (layout != -1) ? layout : current_layout;
  nstripes = (nstripes != -1) ? nstripes : current_nstripes;
  checksum = (checksum != -1) ? checksum : current_checksumid;

  // Generate layout id
  layoutid = LayoutId::GetId(layout, checksum, nstripes,
                             LayoutId::k4M, LayoutId::kCRC32C,
                             LayoutId::GetRedundancyStripeNumber(layoutid));

  // Generate conversion id
  conversion_id = SSTR(
    std::hex << std::setw(16) << std::setfill('0') << fileid
    << ":" << space
    << "#" << std::setw(8) << std::setfill('0') << layoutid
    << (policy.length() ? ("~" + policy) : ""));

  eos_info("msg=\"set QoS - final values\" path=%s layout=%d nstripes=%d "
           "checksum=%d policy=%s space=%s conversion_file=%s",
           path, layout, nstripes, checksum, policy.c_str(),
           space.c_str(), conversion_id.c_str());

  // Create conversion job
  std::string conversion_file = SSTR(
    gOFS->MgmProcConversionPath.c_str() << "/" << conversion_id);
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

  if (gOFS->_touch(conversion_file.c_str(), error, rootvid, 0)) {
    return Emsg(epname, error, errno, "create QoS conversion job",
                conversion_id.c_str());
  }

  EXEC_TIMING_END("QoSGet");

  if (errno) {
    return Emsg(epname, error, errno, "set QoS properties", path);
  }

  return SFS_OK;
}
