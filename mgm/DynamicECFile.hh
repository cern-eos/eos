#ifndef __EOS_NS_FILE_MD_HH__
#define __EOS_NS_FILE_MD_HH__

#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "common/LayoutId.hh"
#include "mgm/Namespace.hh"

#include <stdint.h>
#include <cstring>
#include <string>
#include <vector>
#include <sys/time.h>
#include <cmath>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class IFileMDSvc;
class IContainerMD;

//------------------------------------------------------------------------------
//! Class holding the metadata information concerning a single file
//------------------------------------------------------------------------------
class DynamicECFile: public IFileMD
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DynamicECFile(IFileMD::id_t id);
  /*
    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    virtual ~DynamicECFile() {};
  */
  //----------------------------------------------------------------------------
  //! Virtual copy constructor
  //----------------------------------------------------------------------------
  virtual DynamicECFile* clone() const override;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  DynamicECFile(const DynamicECFile& other);

  //----------------------------------------------------------------------------
  //! Assignment operator
  //----------------------------------------------------------------------------
  DynamicECFile& operator = (const DynamicECFile& other);

  //----------------------------------------------------------------------------
  //! Get file id
  //----------------------------------------------------------------------------
  IFileMD::id_t getId() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pId;
  }

  //----------------------------------------------------------------------------
  //! Get file identifier
  //----------------------------------------------------------------------------
  FileIdentifier getIdentifier() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return FileIdentifier(pId);
  }

  //----------------------------------------------------------------------------
  //! Get creation time
  //----------------------------------------------------------------------------
  void getCTime(ctime_t& ctime) const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    ctime.tv_sec = pCTime.tv_sec;
    ctime.tv_nsec = pCTime.tv_nsec;
  }

  //----------------------------------------------------------------------------
  //! Set creation time
  //----------------------------------------------------------------------------
  void setCTime(ctime_t ctime) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pCTime.tv_sec = ctime.tv_sec;
    pCTime.tv_nsec = ctime.tv_nsec;
  }

  //----------------------------------------------------------------------------
  //! Set creation time to now
  //----------------------------------------------------------------------------
  void setCTimeNow() override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
#ifdef __APPLE__
    struct timeval tv;
    gettimeofday(&tv, 0);
    pCTime.tv_sec = tv.tv_sec;
    pCTime.tv_nsec = tv.tv_usec * 1000;
#else
    clock_gettime(CLOCK_REALTIME, &pCTime);
#endif
  }

  //----------------------------------------------------------------------------
  //! Get modification time
  //----------------------------------------------------------------------------
  void getMTime(ctime_t& mtime) const override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    mtime.tv_sec = pMTime.tv_sec;
    mtime.tv_nsec = pMTime.tv_nsec;
  }

  //----------------------------------------------------------------------------
  //! Set modification time
  //----------------------------------------------------------------------------
  void setMTime(ctime_t mtime) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pMTime.tv_sec = mtime.tv_sec;
    pMTime.tv_nsec = mtime.tv_nsec;
  }

  //----------------------------------------------------------------------------
  //! Set modification time to now
  //----------------------------------------------------------------------------
  void setMTimeNow() override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
#ifdef __APPLE__
    struct timeval tv;
    gettimeofday(&tv, 0);
    pMTime.tv_sec = tv.tv_sec;
    pMTime.tv_nsec = tv.tv_usec * 1000;
#else
    clock_gettime(CLOCK_REALTIME, &pMTime);
#endif
  }

  //----------------------------------------------------------------------------
  //! Get sync time
  //----------------------------------------------------------------------------
  void getSyncTime(ctime_t& mtime) const override
  {
    getMTime(mtime);
  }

  //----------------------------------------------------------------------------
  //! Set sync time
  //----------------------------------------------------------------------------
  void setSyncTime(ctime_t mtime) override
  {
  }

  //----------------------------------------------------------------------------
  //! Set sync time
  //----------------------------------------------------------------------------
  void setSyncTimeNow() override
  {
  }

  //----------------------------------------------------------------------------
  //! Get cloneId (dummy)
  //----------------------------------------------------------------------------
  uint64_t getCloneId() const override
  {
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Set cloneId (dummy)
  //----------------------------------------------------------------------------
  void setCloneId(uint64_t id) override
  {
  }

  //----------------------------------------------------------------------------
  //! Get cloneFST (dummy)
  //----------------------------------------------------------------------------
  const std::string getCloneFST() const override
  {
    return std::string("");
  }

  //----------------------------------------------------------------------------
  //! Set cloneFST (dummy)
  //----------------------------------------------------------------------------
  void setCloneFST(const std::string& data) override
  {
  }

  //----------------------------------------------------------------------------
  //! Get size
  //----------------------------------------------------------------------------
  uint64_t getSize() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pSize;
  }

  double getActualSizeFactor()
  {
    //return eos::common::LayoutId::GetStripeNumber(this->getLayoutId()) ;
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);

    if (eos::common::LayoutId::GetLayoutType(this->getLayoutId()) == 5)
      return ((1.0 * this-> getLocations().size()) /
              (eos::common::LayoutId::GetStripeNumber(this->getLayoutId()) + 1 -
               eos::common::LayoutId::GetRedundancyStripeNumber(this->getLayoutId())));

    //return pSize * ((1.0 * eos::common::LayoutId::GetStripeNumber(this->getLayoutId())) /
    //  (eos::common::LayoutId::GetStripeNumber(this->getLayoutId() + 1 -
    //    (eos::common::LayoutId::GetStripeNumber(this->getLayoutId())-this->getLocations().size()))));
    //eos::common::LayoutId::GetStripeNumber(this->getLayoutId()eos::common::LayoutId::GetStripeNumber(this->getLayeos::common::LayoutId::GetStripeNumber(this->getLayoutId()outId();
    return 1.0;
  }

  //----------------------------------------------------------------------------
  //! Set size - 48 bytes will be used
  //----------------------------------------------------------------------------
  void setSize(uint64_t size) override;

  //----------------------------------------------------------------------------
  //! Get tag
  //----------------------------------------------------------------------------
  IContainerMD::id_t getContainerId() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pContainerId;
  }

  //----------------------------------------------------------------------------
  //! Set tag
  //----------------------------------------------------------------------------
  void setContainerId(IContainerMD::id_t containerId) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pContainerId = containerId;
  }

  //----------------------------------------------------------------------------
  //! Get checksum
  //----------------------------------------------------------------------------
  const Buffer getChecksum() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pChecksum;
  }

  //----------------------------------------------------------------------------
  //! Set checksum
  //----------------------------------------------------------------------------
  void setChecksum(const Buffer& checksum) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pChecksum = checksum;
  }

  //----------------------------------------------------------------------------
  //! Clear checksum
  //----------------------------------------------------------------------------
  void clearChecksum(uint8_t size = 20) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    char zero = 0;

    for (uint8_t i = 0; i < size; i++) {
      pChecksum.putData(&zero, 1);
    }
  }

  //----------------------------------------------------------------------------
  //! Set checksum
  //!
  //! @param checksum address of a memory location string the checksum
  //! @param size     size of the checksum in bytes
  //----------------------------------------------------------------------------
  void setChecksum(const void* checksum, uint8_t size) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pChecksum.clear();
    pChecksum.putData(checksum, size);
  }

  //----------------------------------------------------------------------------
  //! Get name
  //----------------------------------------------------------------------------
  const std::string getName() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pName;
  }

  //----------------------------------------------------------------------------
  //! Set name
  //----------------------------------------------------------------------------
  void setName(const std::string& name) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pName = name;
  }

  //----------------------------------------------------------------------------
  //! Add location
  //----------------------------------------------------------------------------
  void addLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Get vector with all the locations
  //----------------------------------------------------------------------------
  LocationVector getLocations() const override;

  //----------------------------------------------------------------------------
  //! Get location
  //----------------------------------------------------------------------------
  location_t getLocation(unsigned int index) override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);

    if (index < pLocation.size()) {
      return pLocation[index];
    }

    return 0;
  }

  //----------------------------------------------------------------------------
  //! Remove location that was previously unlinked
  //----------------------------------------------------------------------------
  void removeLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Remove all locations that were previously unlinked
  //----------------------------------------------------------------------------
  void removeAllLocations() override;

  //----------------------------------------------------------------------------
  //! Get vector with all unlinked locations
  //----------------------------------------------------------------------------
  LocationVector getUnlinkedLocations() const override;

  //----------------------------------------------------------------------------
  //! Unlink location
  //----------------------------------------------------------------------------
  void unlinkLocation(location_t location) override;

  //----------------------------------------------------------------------------
  //! Unlink all locations
  //----------------------------------------------------------------------------
  void unlinkAllLocations() override;

  //----------------------------------------------------------------------------
  //! Clear unlinked locations without notifying the listeners
  //----------------------------------------------------------------------------
  void clearUnlinkedLocations() override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pUnlinkedLocation.clear();
  }

  //----------------------------------------------------------------------------
  //! Test the unlinkedlocation
  //----------------------------------------------------------------------------
  bool hasUnlinkedLocation(location_t location) override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return hasUnlinkedLocationLocked(location);
  }

  bool hasUnlinkedLocationLocked(location_t location)
  {
    for (unsigned int i = 0; i < pUnlinkedLocation.size(); i++) {
      if (pUnlinkedLocation[i] == location) {
        return true;
      }
    }

    return false;
  }


  //----------------------------------------------------------------------------
  //! Get number of unlinked locations
  //----------------------------------------------------------------------------
  size_t getNumUnlinkedLocation() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pUnlinkedLocation.size();
  }

  //----------------------------------------------------------------------------
  //! Clear locations without notifying the listeners
  //----------------------------------------------------------------------------
  void clearLocations() override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pLocation.clear();
  }

  //----------------------------------------------------------------------------
  //! Test the location
  //----------------------------------------------------------------------------
  bool hasLocation(location_t location) override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return hasLocationLocked(location);
  }

  bool hasLocationLocked(location_t location)
  {
    for (unsigned int i = 0; i < pLocation.size(); i++) {
      if (pLocation[i] == location) {
        return true;
      }
    }

    return false;
  }


  //----------------------------------------------------------------------------
  //! Get number of location
  //----------------------------------------------------------------------------
  size_t getNumLocation() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pLocation.size();
  }

  //----------------------------------------------------------------------------
  //! Get uid
  //----------------------------------------------------------------------------
  uid_t getCUid() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pCUid;
  }

  //----------------------------------------------------------------------------
  //! Set uid
  //----------------------------------------------------------------------------
  void setCUid(uid_t uid) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pCUid = uid;
  }

  //----------------------------------------------------------------------------
  //! Get gid
  //----------------------------------------------------------------------------
  gid_t getCGid() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pCGid;
  }

  //----------------------------------------------------------------------------
  //! Set gid
  //----------------------------------------------------------------------------
  void setCGid(gid_t gid) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pCGid = gid;
  }

  //----------------------------------------------------------------------------
  //! Get layout
  //----------------------------------------------------------------------------
  layoutId_t getLayoutId() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pLayoutId;
  }

  //----------------------------------------------------------------------------
  //! Set layout
  //----------------------------------------------------------------------------
  void setLayoutId(layoutId_t layoutId) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pLayoutId = layoutId;
  }

  //----------------------------------------------------------------------------
  //! Get flags
  //----------------------------------------------------------------------------
  uint16_t getFlags() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pFlags;
  }

  //----------------------------------------------------------------------------
  //! Get the n-th flag
  //----------------------------------------------------------------------------
  bool getFlag(uint8_t n) override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pFlags & (0x0001 << n);
  }

  //----------------------------------------------------------------------------
  //! Set flags
  //----------------------------------------------------------------------------
  void setFlags(uint16_t flags) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pFlags = flags;
  }

  //-------------------------------- --------------------------------------------
  //! Set the n-th flag
  //----------------------------------------------------------------------------
  void setFlag(uint8_t n, bool flag) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);

    if (flag) {
      pFlags |= (1 << n);
    } else {
      pFlags &= (~(1 << n));
    }
  }
//test
  //---------------- ------------------------------------------------------------
  //! Env Representation
  //----------------------------------------------------------------------------
  void getEnv(std::string& env, bool escapeAnd = false) override;


  // this is out while it is only for fileMD
  //----------------------------------------------------------------------------
  //! Set the FileMDSvc object
  //----------------------------------------------------------------------------
  void setFileMDSvc(IFileMDSvc* fileMDSvc) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pFileMDSvc = fileMDSvc;
  }

  //----------------------------------------------------------------------------
  //! Get the FileMDSvc object
  //----------------------------------------------------------------------------
  virtual IFileMDSvc* getFileMDSvc() override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pFileMDSvc;
  }



  //----------------------------------------------------------------------------
  //! Serialize the object to a buffer
  //----------------------------------------------------------------------------
  void serialize(Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Deserialize the class to a buffer
  //----------------------------------------------------------------------------
  void deserialize(const Buffer& buffer) override;

  //----------------------------------------------------------------------------
  //! Get symbolic link
  //----------------------------------------------------------------------------
  std::string getLink() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pLinkName;
  }

  //----------------------------------------------------------------------------
  //! Set symbolic link
  //----------------------------------------------------------------------------
  void setLink(std::string link_name) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pLinkName = link_name;
  }

  //----------------------------------------------------------------------------
  //! Check if symbolic link
  //----------------------------------------------------------------------------
  bool isLink() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pLinkName.length() ? true : false;
  }

  //----------------------------------------------------------------------------
  //! Add extended attribute
  //----------------------------------------------------------------------------
  void setAttribute(const std::string& name, const std::string& value) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pXAttrs[name] = value;
  }

  //----------------------------------------------------------------------------
  //! Remove attribute
  //----------------------------------------------------------------------------
  void removeAttribute(const std::string& name) override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    XAttrMap::iterator it = pXAttrs.find(name);

    if (it != pXAttrs.end()) {
      pXAttrs.erase(it);
    }
  }

  //----------------------------------------------------------------------------
  //! Remove all attributes
  //----------------------------------------------------------------------------
  void clearAttributes() override
  {
    std::unique_lock<std::shared_timed_mutex> lock(mMutex);
    pXAttrs.clear();
  }

  //----------------------------------------------------------------------------
  //! Check if the attribute exist
  //----------------------------------------------------------------------------
  bool hasAttribute(const std::string& name) const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pXAttrs.find(name) != pXAttrs.end();
  }

  //----------------------------------------------------------------------------
  //! Return number of attributes
  //----------------------------------------------------------------------------
  size_t numAttributes() const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    return pXAttrs.size();
  }

  //----------------------------------------------------------------------------
  //! Get the attribute
  //----------------------------------------------------------------------------
  std::string getAttribute(const std::string& name) const override
  {
    std::shared_lock<std::shared_timed_mutex> lock(mMutex);
    XAttrMap::const_iterator it = pXAttrs.find(name);

    if (it == pXAttrs.end()) {
      MDException e(ENOENT);
      e.getMessage() << "Attribute: " << name << " not found";
      throw e;
    }

    return it->second;
  }

  //----------------------------------------------------------------------------
  //! Get map copy of the extended attributes
  //!
  //! @return std::map containing all the extended attributes
  //----------------------------------------------------------------------------
  eos::IFileMD::XAttrMap getAttributes() const override;

protected:
  //----------------------------------------------------------------------------
  // Data members
  //----------------------------------------------------------------------------
  IFileMD::id_t       pId;
  ctime_t             pCTime;
  ctime_t             pMTime;
  uint64_t            pSize;
  IContainerMD::id_t  pContainerId;
  uid_t               pCUid;
  gid_t               pCGid;
  layoutId_t          pLayoutId;
  uint16_t            pFlags;
  std::string         pName;
  std::string         pLinkName;
  LocationVector      pLocation;
  LocationVector      pUnlinkedLocation;
  Buffer              pChecksum;
  XAttrMap            pXAttrs;
  IFileMDSvc*         pFileMDSvc;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_FILE_MD_HH__
