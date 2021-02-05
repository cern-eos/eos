///< This is a helper file for testing the DynamicEC in google test with files.

#include "mgm/dynamicec/DynamicECFile.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <sstream>

namespace eos
{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------



DynamicECFile::DynamicECFile(IFileMD::id_t id):
  IFileMD(),
  pId(id),
  pSize(0),
  pContainerId(0),
  pCUid(0),
  pCGid(0),
  pLayoutId(0),
  pFlags(0),
  pChecksum(0),
  pFileMDSvc()
{
  pCTime.tv_sec = pCTime.tv_nsec = 0;
  pMTime.tv_sec = pMTime.tv_nsec = 0;
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
DynamicECFile*
DynamicECFile::clone() const
{
  return new DynamicECFile(*this);
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
DynamicECFile::DynamicECFile(const DynamicECFile& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Assignment operator
//------------------------------------------------------------------------------
DynamicECFile&
DynamicECFile::operator = (const DynamicECFile& other)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  pName        = other.pName;
  pId          = other.pId;
  pSize        = other.pSize;
  pContainerId = other.pContainerId;
  pCUid        = other.pCUid;
  pCGid        = other.pCGid;
  pLayoutId    = other.pLayoutId;
  pFlags       = other.pFlags;
  pLinkName    = other.pLinkName;
  pLocation    = other.pLocation;
  pUnlinkedLocation = other.pUnlinkedLocation;
  pCTime       = other.pCTime;
  pMTime       = other.pMTime;
  pChecksum    = other.pChecksum;
  pXAttrs      = other.pXAttrs;
  return *this;
}

//------------------------------------------------------------------------------
// Add location
//------------------------------------------------------------------------------
void DynamicECFile::addLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  if (hasLocationLocked(location)) {
    return;
  }

  pLocation.push_back(location);
  lock.unlock();
}

//------------------------------------------------------------------------------
// Remove location
//------------------------------------------------------------------------------
void DynamicECFile::removeLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  std::vector<location_t>::iterator it;
  bool removed = false;

  for (it = pUnlinkedLocation.begin(); it < pUnlinkedLocation.end(); ++it) {
    if (*it == location) {
      pUnlinkedLocation.erase(it);
      removed = true;
      break;
    }
  }

  if (removed) {
    lock.unlock();
  }
}

//------------------------------------------------------------------------------
// Remove all locations that were previously unlinked
//------------------------------------------------------------------------------
void DynamicECFile::removeAllLocations()
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  std::vector<location_t>::reverse_iterator it;
  std::vector<location_t> remove_loc;

  while ((it = pUnlinkedLocation.rbegin()) != pUnlinkedLocation.rend()) {
    pUnlinkedLocation.pop_back();
    remove_loc.push_back(*it);
  }

  lock.unlock();
}

//------------------------------------------------------------------------------
// Unlink location
//------------------------------------------------------------------------------
void DynamicECFile::unlinkLocation(location_t location)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  std::vector<location_t>::iterator it;
  bool unlinked = false;

  for (it = pLocation.begin() ; it < pLocation.end(); it++) {
    if (*it == location) {
      pUnlinkedLocation.push_back(*it);
      pLocation.erase(it);
      unlinked = true;
      break;
    }
  }

  if (unlinked) {
    lock.unlock();
  }
}

//------------------------------------------------------------------------------
// Unlink all locations
//------------------------------------------------------------------------------
void DynamicECFile::unlinkAllLocations()
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  std::vector<location_t>::reverse_iterator it;
  std::vector<location_t> unlink_loc;

  while ((it = pLocation.rbegin()) != pLocation.rend()) {
    location_t loc = *it;

    if (!hasUnlinkedLocationLocked(loc)) {
      pUnlinkedLocation.push_back(loc);
    }

    pLocation.pop_back();
    unlink_loc.push_back(loc);
  }

  lock.unlock();
  std::shared_lock<std::shared_timed_mutex> slock(mMutex);
}

//------------------------------------------------------------------------
//  Env Representation
//------------------------------------------------------------------------
void DynamicECFile::getEnv(std::string& env, bool escapeAnd)
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  env = "";
  std::ostringstream o;
  std::string saveName = pName;

  if (escapeAnd) {
    if (!saveName.empty()) {
      std::string from = "&";
      std::string to = "#AND#";
      size_t start_pos = 0;

      while ((start_pos = saveName.find(from, start_pos)) != std::string::npos) {
        saveName.replace(start_pos, from.length(), to);
        start_pos += to.length();
      }
    }
  }

  o << "name=" << saveName << "&id=" << pId << "&ctime=" << pCTime.tv_sec;
  o << "&ctime_ns=" << pCTime.tv_nsec << "&mtime=" << pMTime.tv_sec;
  o << "&mtime_ns=" << pMTime.tv_nsec << "&size=" << pSize;
  o << "&cid=" << pContainerId << "&uid=" << pCUid << "&gid=" << pCGid;
  o << "&lid=" << pLayoutId;
  env += o.str();
  env += "&location=";
  LocationVector::iterator it;
  char locs[16];

  for (it = pLocation.begin(); it != pLocation.end(); ++it) {
    snprintf(locs, sizeof(locs), "%u", *it);
    env += locs;
    env += ",";
  }

  for (it = pUnlinkedLocation.begin(); it != pUnlinkedLocation.end(); ++it) {
    snprintf(locs, sizeof(locs), "!%u", *it);
    env += locs;
    env += ",";
  }

  env += "&checksum=";
  uint8_t size = pChecksum.getSize();

  for (uint8_t i = 0; i < size; i++) {
    char hx[3];
    hx[0] = 0;
    snprintf(hx, sizeof(hx), "%02x",
             *((unsigned char*)(pChecksum.getDataPtr() + i)));
    env += hx;
  }
}

//------------------------------------------------------------------------------
// Serialize the object to a buffer
//------------------------------------------------------------------------------
void DynamicECFile::serialize(Buffer& buffer)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  buffer.putData(&pId,          sizeof(pId));
  buffer.putData(&pCTime,       sizeof(pCTime));
  buffer.putData(&pMTime,       sizeof(pMTime));
  uint64_t tmp = pFlags;
  tmp <<= 48;
  tmp |= (pSize & 0x0000ffffffffffff);
  buffer.putData(&tmp,          sizeof(tmp));
  buffer.putData(&pContainerId, sizeof(pContainerId));
  std::string nameAndLink = pName;

  if (pLinkName.length()) {
    nameAndLink += "//";
    nameAndLink += pLinkName;
  }

  uint16_t len = nameAndLink.length() + 1;
  buffer.putData(&len,          sizeof(len));
  buffer.putData(nameAndLink.c_str(), len);
  len = pLocation.size();
  buffer.putData(&len, sizeof(len));
  LocationVector::iterator it;

  for (it = pLocation.begin(); it != pLocation.end(); ++it) {
    location_t location = *it;
    buffer.putData(&location, sizeof(location_t));
  }

  len = pUnlinkedLocation.size();
  buffer.putData(&len, sizeof(len));

  for (it = pUnlinkedLocation.begin(); it != pUnlinkedLocation.end(); ++it) {
    location_t location = *it;
    buffer.putData(&location, sizeof(location_t));
  }

  buffer.putData(&pCUid,      sizeof(pCUid));
  buffer.putData(&pCGid,      sizeof(pCGid));
  buffer.putData(&pLayoutId, sizeof(pLayoutId));
  uint8_t size = pChecksum.getSize();
  buffer.putData(&size, sizeof(size));
  buffer.putData(pChecksum.getDataPtr(), size);

  if (pXAttrs.size()) {
    uint16_t len = pXAttrs.size();
    buffer.putData(&len, sizeof(len));
    XAttrMap::iterator it;

    for (it = pXAttrs.begin(); it != pXAttrs.end(); ++it) {
      uint16_t strLen = it->first.length() + 1;
      buffer.putData(&strLen, sizeof(strLen));
      buffer.putData(it->first.c_str(), strLen);
      strLen = it->second.length() + 1;
      buffer.putData(&strLen, sizeof(strLen));
      buffer.putData(it->second.c_str(), strLen);
    }
  }
}

//------------------------------------------------------------------------------
// Deserialize the class to a buffer
//------------------------------------------------------------------------------
void DynamicECFile::deserialize(const Buffer& buffer)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  uint16_t offset = 0;
  offset = buffer.grabData(offset, &pId,          sizeof(pId));
  offset = buffer.grabData(offset, &pCTime,       sizeof(pCTime));
  offset = buffer.grabData(offset, &pMTime,       sizeof(pMTime));
  uint64_t tmp;
  offset = buffer.grabData(offset, &tmp,          sizeof(tmp));
  pSize = tmp & 0x0000ffffffffffff;
  tmp >>= 48;
  pFlags = tmp & 0x000000000000ffff;
  offset = buffer.grabData(offset, &pContainerId, sizeof(pContainerId));
  uint16_t len = 0;
  offset = buffer.grabData(offset, &len, 2);
  char strBuffer[len];
  offset = buffer.grabData(offset, strBuffer, len);
  pName = strBuffer;
  size_t link_pos = pName.find("//");

  if (link_pos != std::string::npos) {
    pLinkName = pName.substr(link_pos + 2);
    pName.erase(link_pos);
  }

  offset = buffer.grabData(offset, &len, 2);

  for (uint16_t i = 0; i < len; ++i) {
    location_t location;
    offset = buffer.grabData(offset, &location, sizeof(location_t));
    pLocation.push_back(location);
  }

  offset = buffer.grabData(offset, &len, 2);

  for (uint16_t i = 0; i < len; ++i) {
    location_t location;
    offset = buffer.grabData(offset, &location, sizeof(location_t));
    pUnlinkedLocation.push_back(location);
  }

  offset = buffer.grabData(offset, &pCUid,      sizeof(pCUid));
  offset = buffer.grabData(offset, &pCGid,      sizeof(pCGid));
  offset = buffer.grabData(offset, &pLayoutId, sizeof(pLayoutId));
  uint8_t size = 0;
  offset = buffer.grabData(offset, &size, sizeof(size));
  pChecksum.resize(size);
  offset = buffer.grabData(offset, pChecksum.getDataPtr(), size);

  if ((buffer.size() - offset) >= 4) {
    uint16_t len1 = 0;
    uint16_t len2 = 0;
    uint16_t len = 0;
    offset = buffer.grabData(offset, &len, sizeof(len));

    for (uint16_t i = 0; i < len; ++i) {
      offset = buffer.grabData(offset, &len1, sizeof(len1));
      char strBuffer1[len1];
      offset = buffer.grabData(offset, strBuffer1, len1);
      offset = buffer.grabData(offset, &len2, sizeof(len2));
      char strBuffer2[len2];
      offset = buffer.grabData(offset, strBuffer2, len2);
      pXAttrs.insert(std::make_pair <char*, char*>(strBuffer1, strBuffer2));
    }
  }
}

//------------------------------------------------------------------------------
// Get vector with all the locations
//------------------------------------------------------------------------------
IFileMD::LocationVector
DynamicECFile::getLocations() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return pLocation;
}

//------------------------------------------------------------------------------
// Get vector with all unlinked locations
//------------------------------------------------------------------------------
IFileMD::LocationVector
DynamicECFile::getUnlinkedLocations() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return pUnlinkedLocation;
}

//------------------------------------------------------------------------------
// Set size - 48 bytes will be used
//------------------------------------------------------------------------------
void
DynamicECFile::setSize(uint64_t size)
{
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  //int64_t sizeChange = (size & 0x0000ffffffffffff) - pSize;
  pSize = size & 0x0000ffffffffffff;
  lock.unlock();
}

//------------------------------------------------------------------------------
// Get map copy of the extended attributes
//------------------------------------------------------------------------------
eos::IFileMD::XAttrMap
DynamicECFile::getAttributes() const
{
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return pXAttrs;
}

}
