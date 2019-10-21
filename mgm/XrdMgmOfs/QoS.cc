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
  //! The class takes as input an entry metadata pointer,
  //! which it will use to query for properties.
  //!
  //! The "qos_class" property retrieval mechanism:
  //!
  //! Initially, an attempt is made to retrieve it from extended attributes.
  //! If that fails, an attempt is made to match the list of attributes
  //! against a defined QoS class. If no match is found, "null" is returned.
  //!
  //! The class should be called under lock to ensure thread safety.
  //----------------------------------------------------------------------------
  template<typename T>
  class QoSGetter
  {
  public:
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    QoSGetter(T _md) : md(_md) {}

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
    std::string Class() const;
    std::string DiskSize() const;
    std::string LayoutType() const;
    std::string Id() const;
    std::string Path() const;
    std::string Placement() const;
    std::string Replica() const;
    std::string Size() const;

    T md; ///< pointer to entry metadata
    ///< dispatch table based on QoS key word
    std::map<std::string, std::function<std::string()>> dispatch {
      { "checksum",    [this](){ return QoSGetter::ChecksumType(); } },
      { "current_qos", [this](){ return QoSGetter::Class();        } },
      { "disksize",    [this](){ return QoSGetter::DiskSize();     } },
      { "layout",      [this](){ return QoSGetter::LayoutType();   } },
      { "id",          [this](){ return QoSGetter::Id();           } },
      { "path",        [this](){ return QoSGetter::Path();         } },
      { "placement",   [this](){ return QoSGetter::Placement();    } },
      { "replica",     [this](){ return QoSGetter::Replica();      } },
      { "size",        [this](){ return QoSGetter::Size();         } },
      { "target_qos",  [this](){ return QoSGetter::Attr("user.eos.qos.target"); } },
    };
  };

  //----------------------------------------------------------------------------
  // Retrieve all QoS properties
  //----------------------------------------------------------------------------
  template<typename T>
  eos::IFileMD::QoSAttrMap QoSGetter<T>::All() {
    eos::IFileMD::QoSAttrMap qosMap = CDMI();

    for (auto& it: dispatch) {
      qosMap.emplace(it.first, it.second());
    }

    return qosMap;
  }

  //----------------------------------------------------------------------------
  // Retrieve CDMI-specific QoS properties
  //----------------------------------------------------------------------------
  template<typename T>
  eos::IFileMD::QoSAttrMap QoSGetter<T>::CDMI() {
    eos::IFileMD::QoSAttrMap cdmiMap;
    std::string qos_name = QoSGetter::Get("current_qos");

    if (gOFS->mQoSClassMap.count(qos_name)) {
      const QoSClass qos_class = gOFS->mQoSClassMap.at(qos_name);
      std::ostringstream splacement;
      size_t count = 0;

      splacement << "[";
      for (auto& location: qos_class.locations) {
        splacement << " " << location;

        if (++count < qos_class.locations.size()) {
          splacement << ",";
        }
      }

      splacement << " ]";

      cdmiMap[CDMI_REDUNDANCY_TAG] = std::to_string(qos_class.cdmi_redundancy);
      cdmiMap[CDMI_LATENCY_TAG] = std::to_string(qos_class.cdmi_latency);
      cdmiMap[CDMI_PLACEMENT_TAG] = splacement.str();
    }

    return cdmiMap;
  }

  //----------------------------------------------------------------------------
  // Retrieve QoS property by key
  //----------------------------------------------------------------------------
  template<typename T>
  std::string QoSGetter<T>::Get(const std::string& key) const {
    std::string value = "";
    auto it = dispatch.find(key);

    if (it != dispatch.end()) {
      value = it->second();
    }

    return value;
  }

  //----------------------------------------------------------------------------
  // Methods retrieving a particular QoS property
  //
  // Depending on the property, they are implemened via templated
  // or specialized templated functions.
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  // Retrieve extended attributes
  //----------------------------------------------------------------------------
  template<typename T>
  std::string QoSGetter<T>::Attr(const char* key) const {
    std::string value = "null";

    if (md->hasAttribute(key)) {
      value = md->getAttribute(key);
    }

    return value;
  }

  //----------------------------------------------------------------------------
  // Retrieve checksum type
  //----------------------------------------------------------------------------
  template<>
  std::string QoSGetter<eos::IFileMDPtr>::ChecksumType() const {
    return eos::common::LayoutId::GetChecksumStringReal(md->getLayoutId());
  }

  template<>
  std::string QoSGetter<eos::IContainerMDPtr>::ChecksumType() const {
    std::string value = QoSGetter::Attr("sys.forced.checksum");
    int checksum_id = eos::common::LayoutId::GetChecksumFromString(value);
    return eos::common::LayoutId::GetChecksumStringReal(checksum_id);
  }

  //----------------------------------------------------------------------------
  // Retrieve size
  //----------------------------------------------------------------------------
  template<>
  std::string QoSGetter<eos::IFileMDPtr>::Size()  const {
    return std::to_string(md->getSize());
  }

  template<>
  std::string QoSGetter<eos::IContainerMDPtr>::Size() const {
    return std::to_string(md->getTreeSize());
  }

  //----------------------------------------------------------------------------
  // Retrieve disk size
  //----------------------------------------------------------------------------
  template<>
  std::string QoSGetter<eos::IFileMDPtr>::DiskSize() const {
    uint64_t physicalSize = md->getSize() *
        eos::common::LayoutId::GetSizeFactor(md->getLayoutId());
    return std::to_string(physicalSize);
  }

  template<>
  std::string QoSGetter<eos::IContainerMDPtr>::DiskSize() const {
    return QoSGetter::Size();
  }

  //----------------------------------------------------------------------------
  // Retrieve layout type
  //----------------------------------------------------------------------------
  template<>
  std::string QoSGetter<eos::IFileMDPtr>::LayoutType() const {
    return eos::common::LayoutId::GetLayoutTypeString(md->getLayoutId());
  }

  template<>
  std::string QoSGetter<eos::IContainerMDPtr>::LayoutType() const {
    return QoSGetter::Attr("sys.forced.layout");
  }

  //----------------------------------------------------------------------------
  // Retrieve id
  //----------------------------------------------------------------------------
  template<typename T>
  std::string QoSGetter<T>::Id() const {
    return std::to_string(md->getId());
  }

  //----------------------------------------------------------------------------
  // Retrieve path
  //----------------------------------------------------------------------------
  template<typename T>
  std::string QoSGetter<T>::Path() const {
    std::string path = "null";

    try {
      path = gOFS->eosView->getUri(md.get());
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception retrieving path\" fxid=%08llx "
                       "ec=%d emsg=\"%s\"", md->getId(),
                       e.getErrno(), e.getMessage().str().c_str());
    }

    return path;
  }

  //----------------------------------------------------------------------------
  // Retrieve placement
  //----------------------------------------------------------------------------
  template<typename T>
  std::string QoSGetter<T>::Placement() const {
    std::string placement = "null";

    try {
      std::string path = gOFS->eosView->getUri(md.get());

      // Ugly type check
      // (avoids specialized template helper function for path deduction)
      if (std::is_same<T, eos::IFileMDPtr>::value) {
        path = eos::common::Path(path).GetParentPath();
      }

      eos::mgm::Scheduler::tPlctPolicy plctplcy;
      eos::common::VirtualIdentity vid;
      std::string targetgeotag;
      XrdOucErrInfo error;
      XrdOucEnv env;

      eos::IContainerMD::XAttrMap attrmap;
      gOFS->_attr_ls(path.c_str(), error, vid, 0, attrmap, false);

      Policy::GetPlctPolicy(path.c_str(), attrmap, vid, env, plctplcy,
                            targetgeotag);
      placement = Scheduler::PlctPolicyString(plctplcy);
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception retrieving path\" fxid=%08llx "
                       "ec=%d emsg=\"%s\"", md->getId(),
                       e.getErrno(), e.getMessage().str().c_str());
    }

    return placement;
  }

  //----------------------------------------------------------------------------
  // Retrieve replica
  //----------------------------------------------------------------------------
  template<>
  std::string QoSGetter<eos::IFileMDPtr>::Replica() const {
    return std::to_string(md->getNumLocation());
  }

  template<>
  std::string QoSGetter<eos::IContainerMDPtr>::Replica() const {
    return QoSGetter::Attr("sys.forced.nstripes");
  }

  //----------------------------------------------------------------------------
  // Retrieve QoS class
  //----------------------------------------------------------------------------
  template<typename T>
  std::string QoSGetter<T>::Class() const {
    std::string qos_class = QoSGetter::Attr("user.eos.qos.class");

    if (qos_class == "null") {
      eos::IFileMD::QoSAttrMap attributes;

      attributes["checksum"] = QoSGetter::ChecksumType();
      attributes["layout"] = QoSGetter::LayoutType();
      attributes["placement"] = QoSGetter::Placement();
      attributes["replica"] = QoSGetter::Replica();

      for (const auto& it_class: gOFS->mQoSClassMap) {
        const auto& class_attributes = it_class.second.attributes;
        bool match = true;

        for (auto it = attributes.begin();
             it != attributes.end() && match; it++) {
          if ((!class_attributes.count(it->first)) ||
              (class_attributes.at(it->first) != it->second)) {
            match = false;
          }
        }

        if (match) {
          qos_class = it_class.second.name;
          break;
        }
      }
    }

    return qos_class;
  }

  //----------------------------------------------------------------------------
  //! Helper function to check the given <key>=<value> is a valid QoS property
  //!
  //! @param key QoS key
  //! @param value QoS value
  //!
  //! @return true if pair is valid, false otherwise
  //----------------------------------------------------------------------------
  bool IsValidQoSProperty(const std::string& key, const std::string& value)
  {
    if (key == "placement") {
      return Scheduler::PlctPolicyFromString(value) != -1;
    } else if (key == "layout") {
      return eos::common::LayoutId::GetLayoutFromString(value) != -1;
    } else if (key == "checksum") {
      return eos::common::LayoutId::GetChecksumFromString(value) != -1;
    } else if (key == "replica") {
      int number = std::stoi(value);
      return (number >= 1 && number <= 16);
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Helper function to extract a QoS property,
  //! given the entry metadata object and the key
  //!
  //! @param md the ile or container metadata object
  //! @param key the QoS key
  //!
  //! @return the request QoS property
  //----------------------------------------------------------------------------
  std::string QoSValueFromMd(const eos::FileOrContainerMD& md,
                             const char* key)
  {
    if (md.file) {
      return QoSGetter<eos::IFileMDPtr>{md.file}.Get(key);
    }

    return QoSGetter<eos::IContainerMDPtr>{md.container}.Get(key);
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

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);
  std::string cmd_retrieved_qos;

  try {
    eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
    eos::FileOrContainerMD md = gOFS->eosView->getItem(path).get();

    if (md.file) {
      map = (only_cdmi) ? QoSGetter<eos::IFileMDPtr>{md.file}.CDMI()
                        : QoSGetter<eos::IFileMDPtr>{md.file}.All();
    } else {
      map = (only_cdmi) ? QoSGetter<eos::IContainerMDPtr>{md.container}.CDMI()
                        : QoSGetter<eos::IContainerMDPtr>{md.container}.All();

      if (map.count("current_qos") && map["current_qos"] != "null") {
        cmd_retrieved_qos = map["current_qos"];
      }
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception retrieving item metadata\" path=%s "
              "ec=%d emsg=\"%s\"", path, e.getErrno(),
              e.getMessage().str().c_str());
  }

  // Check if identified QoS class needs to be updated
  // Note: applies only to containers
  if (!errno && cmd_retrieved_qos.length()) {
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex);

    try {
      eos::IContainerMDPtr cmd = gOFS->eosView->getContainer(path);
      std::string attr_qos;

      if (cmd->hasAttribute("user.eos.qos.class")) {
        attr_qos = cmd->getAttribute("user.eos.qos.class");
      }

      if (cmd_retrieved_qos != attr_qos) {
        eos_info("msg=\"setting QoS class match in extended attributes\" "
                 "path=%s qos_class=%s", path, cmd_retrieved_qos.c_str());

        cmd->setAttribute("user.eos.qos.class", cmd_retrieved_qos);
        eosView->updateContainerStore(cmd.get());
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception setting extended attributes\" path=%s "
                "ec=%d emsg=\"%s\"", path, e.getErrno(),
                e.getMessage().str().c_str());
    }
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

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);
  std::string cmd_retrieved_qos;

  try {
    eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
    eos::FileOrContainerMD md = gOFS->eosView->getItem(path).get();
    value = QoSValueFromMd(md, key).c_str();

    if (md.container && !strcmp(key, "current_qos") && value != "null") {
      cmd_retrieved_qos = value.c_str();
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception retrieving item metadata\" path=%s "
              "ec=%d emsg=\"%s\"", path, e.getErrno(),
              e.getMessage().str().c_str());
  }

  // Check if identified QoS class needs to be updated
  // Note: applies only to containers
  if (!errno && cmd_retrieved_qos.length()) {
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex);

    try {
      eos::IContainerMDPtr cmd = gOFS->eosView->getContainer(path);
      std::string attr_qos;

      if (cmd->hasAttribute("user.eos.qos.class")) {
        attr_qos = cmd->getAttribute("user.eos.qos.class").c_str();
      }

      if (cmd_retrieved_qos != attr_qos) {
        eos_info("msg=\"setting QoS class match in extended attributes\" "
                 "path=%s qos_class=%s", path, value.c_str());

        cmd->setAttribute("user.eos.qos.class", value.c_str());
        eosView->updateContainerStore(cmd.get());
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception setting extended attributes\" path=%s "
                "ec=%d emsg=\"%s\"", path, e.getErrno(),
                e.getMessage().str().c_str());
    }
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
                    const eos::mgm::QoSClass& qos,
                    std::string& conversion_id)
{
  using eos::common::LayoutId;
  static const char* epname = "qos_set";
  EXEC_TIMING_BEGIN("QoSSet");
  gOFS->MgmStats.Add("QoSSet", vid.uid, vid.gid, 1);
  errno = 0;

  eos_info("msg=\"set QoS class\" path=%s qos_class=%s",
           path, qos.name.c_str());

  // Validate QoS class properties
  for (const auto& it: qos.attributes) {
    if (!IsValidQoSProperty(it.first, it.second)) {
      eos_static_err("msg=\"invalid QoS property %s=%s\"",
                     it.first.c_str(), it.second.c_str());
      return Emsg(epname, error, EINVAL,
                  "set QoS class due to invalid fields", it.first.c_str());
    }
  }

  eos::FileOrContainerMD md;
  std::string current_qos;

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
    md = gOFS->eosView->getItem(path).get();
    current_qos = QoSValueFromMd(md, "current_qos");
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception retrieving file metadata\" path=%s "
              "ec=%d emsg=\"%s\"", path, e.getErrno(),
              e.getMessage().str().c_str());
  }

  if (!md.file && !md.container) {
    return Emsg(epname, error, errno, "retrieve item metadata", path);
  }

  // Abort if current QoS is same as target QoS
  if (current_qos == qos.name) {
    return Emsg(epname, error, EINVAL,
                "set QoS class identical with current class", path);
  }

  if (md.file) {
    // For files:
    //   - create a new conversion job,
    //   - store the QoS target extended attributes

    eos::IFileMDPtr fmd = 0;
    eos::common::FileId::fileid_t fileid = 0;
    eos::IFileMD::location_t fsid = 0;
    LayoutId::layoutid_t layoutid = 0;
    unsigned long current_layout = 0;
    unsigned long current_checksumid = 0;
    unsigned long current_nstripes = 0;

    try {
      eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex);
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
      return Emsg(epname, error, e.getErrno(), "retrieve file metadata", path);
    }

    // Extract current scheduling space
    std::string space = "";
    {
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
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

    // Extract new layout components from QoS class
    int layout = -1;
    int checksumid = -1;
    int nstripes = -1;
    std::string policy = "";

    for (const auto& it: qos.attributes) {
      if (it.first == "layout") {
        layout = LayoutId::GetLayoutFromString(it.second);
      } else if (it.first == "replica") {
        nstripes = std::stoi(it.second);
      } else if (it.first == "checksum") {
        checksumid = LayoutId::GetChecksumFromString(it.second);
      } else if (it.first == "placement") {
        policy = it.second;
      }
    }

    // Generate layout id
    layout = (layout != -1) ? layout : current_layout;
    nstripes = (nstripes != -1) ? nstripes : current_nstripes;
    checksumid = (checksumid != -1) ? checksumid : current_checksumid;
    layoutid = LayoutId::GetId(layout, checksumid, nstripes,
                               LayoutId::k4M, LayoutId::kCRC32C,
                               LayoutId::GetRedundancyStripeNumber(layoutid));

    // Generate conversion id
    conversion_id = SSTR(
      std::hex << std::setw(16) << std::setfill('0') << fileid
               << ":" << space
               << "#" << std::setw(8) << std::setfill('0') << layoutid
               << (policy.length() ? ("~" + policy) : ""));

    eos_info("msg=\"set QoS class - scheduling conversion job\" path=%s "
             "layout=%d nstripes=%d checksum=%d policy=%s space=%s "
             "conversion_file=%s", path, layout, nstripes, checksumid,
             policy.c_str(), space.c_str(), conversion_id.c_str());

    // Create conversion job
    std::string conversion_file = SSTR(
      gOFS->MgmProcConversionPath.c_str() << "/" << conversion_id);
    eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

    if (gOFS->_touch(conversion_file.c_str(), error, rootvid, 0)) {
      return Emsg(epname, error, errno, "create QoS conversion job",
                  conversion_id.c_str());
    }

    // Add the target QoS attribute
    try {
      eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex);
      fmd = gOFS->eosView->getFile(path);
      fmd->setAttribute("user.eos.qos.target", qos.name);
      eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception setting extended attributes\" path=%s "
                "ec=%d emsg=\"%s\"", path, e.getErrno(),
                e.getMessage().str().c_str());
    }
  } else {
    // For containers, only set the QoS target extended attribute
    try {
      eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex);
      eos::IContainerMDPtr cmd = gOFS->eosView->getContainer(path);
      cmd->setAttribute("user.eos.qos.target", qos.name);
      eosView->updateContainerStore(cmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception setting extended attributes\" path=%s "
                "ec=%d emsg=\"%s\"", path, e.getErrno(),
                e.getMessage().str().c_str());
      return Emsg(epname, error, e.getErrno(), "set extended attributes", path);
    }

    conversion_id = SSTR(path << "|" << qos.name);
  }

  EXEC_TIMING_END("QoSSet");

  if (errno) {
    return Emsg(epname, error, errno, "set QoS properties", path);
  }

  return SFS_OK;
}
