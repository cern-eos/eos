/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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

#include "namespace/ns_on_filesystem/FsFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <sstream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileMD::FileMD(id_t id, IFileMDSvc* fileMDSvc):
  IFileMD(),
  pId(id),
  pSize(0),
  pContainerId(0),
  pCUid(0),
  pCGid(0),
  pLayoutId(0),
  pFlags(0),
  pChecksum(0),
  pFileMDSvc(fileMDSvc)
{
  pCTime.tv_sec = pCTime.tv_nsec = 0;
  pMTime.tv_sec = pMTime.tv_nsec = 0;
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
FileMD*
FileMD::clone() const
{
  return new FileMD(*this);
}

//------------------------------------------------------------------------------
// Copy constructor
//------------------------------------------------------------------------------
FileMD::FileMD(const FileMD& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Asignment operator
//------------------------------------------------------------------------------
FileMD&
FileMD::operator = (const FileMD& other)
{
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
  pFileMDSvc   = 0;
  return *this;
}

//------------------------------------------------------------------------------
// Add location
//------------------------------------------------------------------------------
void FileMD::addLocation(location_t location)
{
  if (hasLocation(location))
    return;

  pLocation.push_back(location);
  IFileMDChangeListener::Event e(this,
                                 IFileMDChangeListener::LocationAdded,
                                 location);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Replace location by index
//------------------------------------------------------------------------------
void FileMD::replaceLocation(unsigned int index, location_t newlocation)
{
  location_t oldLocation = pLocation[index];
  pLocation[index] = newlocation;
  IFileMDChangeListener::Event e(this,
                                 IFileMDChangeListener::LocationReplaced,
                                 newlocation, oldLocation);
  pFileMDSvc->notifyListeners(&e);
}

//------------------------------------------------------------------------------
// Remove location
//------------------------------------------------------------------------------
void FileMD::removeLocation(location_t location)
{
  std::vector<location_t>::iterator it;

  for (it = pUnlinkedLocation.begin(); it < pUnlinkedLocation.end(); ++it)
  {
    if (*it == location)
    {
      pUnlinkedLocation.erase(it);
      IFileMDChangeListener::Event e(this,
                                     IFileMDChangeListener::LocationRemoved,
                                     location);
      pFileMDSvc->notifyListeners(&e);
      return;
    }
  }
}

//------------------------------------------------------------------------------
// Remove all locations that were previously unlinked
//------------------------------------------------------------------------------
void FileMD::removeAllLocations()
{
  std::vector<location_t>::reverse_iterator it;

  while ((it = pUnlinkedLocation.rbegin()) != pUnlinkedLocation.rend())
  {
    pUnlinkedLocation.pop_back();
    IFileMDChangeListener::Event e(this,
                                   IFileMDChangeListener::LocationRemoved,
                                   *it);
    pFileMDSvc->notifyListeners(&e);
  }
}

//------------------------------------------------------------------------------
// Unlink location
//------------------------------------------------------------------------------
void FileMD::unlinkLocation(location_t location)
{
  std::vector<location_t>::iterator it;

  for (it = pLocation.begin() ; it < pLocation.end(); it++)
  {
    if (*it == location)
    {
      pUnlinkedLocation.push_back(*it);
      pLocation.erase(it);
      IFileMDChangeListener::Event e(this,
                                     IFileMDChangeListener::LocationUnlinked,
                                     location);
      pFileMDSvc->notifyListeners(&e);
      return;
    }
  }
}

//------------------------------------------------------------------------------
// Unlink all locations
//------------------------------------------------------------------------------
void FileMD::unlinkAllLocations()
{
  std::vector<location_t>::reverse_iterator it;

  while ((it = pLocation.rbegin()) != pLocation.rend())
  {
    location_t loc = *it;
    pUnlinkedLocation.push_back(loc);
    pLocation.pop_back();
    IFileMDChangeListener::Event e(this,
                                   IFileMDChangeListener::LocationUnlinked,
                                   loc);
    pFileMDSvc->notifyListeners(&e);
  }
}

//------------------------------------------------------------------------
//  Env Representation
//------------------------------------------------------------------------
void FileMD::getEnv(std::string& env, bool escapeAnd)
{
  env = "";
  std::ostringstream o;
  std::string saveName = pName;

  if (escapeAnd)
  {
    if (!saveName.empty())
    {
      std::string from = "&";
      std::string to = "#AND#";
      size_t start_pos = 0;

      while ((start_pos = saveName.find(from, start_pos)) != std::string::npos)
      {
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

  for (it = pLocation.begin(); it != pLocation.end(); ++it)
  {
    snprintf(locs, sizeof(locs), "%u", *it);
    env += locs;
    env += ",";
  }

  for (it = pUnlinkedLocation.begin(); it != pUnlinkedLocation.end(); ++it)
  {
    snprintf(locs, sizeof(locs), "!%u", *it);
    env += locs;
    env += ",";
  }

  env += "&checksum=";
  uint8_t size = pChecksum.getSize();

  for (uint8_t i = 0; i < size; i++)
  {
    char hx[3];
    hx[0] = 0;
    snprintf(hx, sizeof(hx), "%02x",
             *((unsigned char*)(pChecksum.getDataPtr() + i)));
    env += hx;
  }
}


//------------------------------------------------------------------------------
// Get vector with all the locations
//------------------------------------------------------------------------------
IFileMD::LocationVector
FileMD::getLocations() const
{
  return pLocation;
}

//------------------------------------------------------------------------------
// Get vector with all unlinked locations
//------------------------------------------------------------------------------
IFileMD::LocationVector
FileMD::getUnlinkedLocations() const
{
  IFileMD::LocationVector result;
  result = pUnlinkedLocation;
  return std::move(result);
}

EOSNSNAMESPACE_END
