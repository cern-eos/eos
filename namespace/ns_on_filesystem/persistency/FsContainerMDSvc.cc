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

#include "namespace/ns_on_filesystem/FsContainerMDSvc.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsSContainerMDSvc::FsContainerMDSvc():
    mMountPath("")
{
  // empty
}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
FsContainerMDSvc::~FsContainerMDSvc()
{
  // empty
}

//------------------------------------------------------------------------------
// Initizlize the container service
//------------------------------------------------------------------------------
void
FsContainerMDSvc::initialize()
{
  std::cout << "FsContainerMDSvc::initialize" << std::endl;
  return;
}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void
FsContainerMDSvc::configure(std::map<std::string, std::string>& config)
{
  std::cout << "FsContainerMDSvc::configure" << std::endl;
  auto it = config.find("mount_point");

  if (it != config.end())
  {
    mMountPath = it->second;

    // Mount point must end with '/'
    if (mMountPoint.rbegin() != '/')
      mMountPoint += '/';

    // Make sure the mount point is accessible
    struct stat sinfo;
    int retc = stat(mMountPath.c_str(), &sinfo);

    if (retc)
    {
      MDException e(errno);
      e.getMessage() << "Mount point " << mMountPath << " unavailable";
      throw e;
    }
  }
}

//------------------------------------------------------------------------------
// Finalize the container service
//----------------------------------------------------------------------------
void
FsContainerMDSvc::finalize()
{
  std::cout << "FsContainerMDSvc::finalize" << std::endl;
  return;
}

//------------------------------------------------------------------------------
// Get the container metadata information for the given path
//------------------------------------------------------------------------------
IContainerMD*
FsContainerMDSvc::getContainerMD(const std::string& rel_path)
{
  if (rel_path.begin() == '/')
    rel_path.erase(1);

  std::string full_path = mMountPath + rel_path;
  return new FsContainerMD(full_path);
}

//------------------------------------------------------------------------------
// Create new container metadata object with an assigned id, the user has
// to fill all the remaining fields
//------------------------------------------------------------------------------
IContainerMD*
FsContainerMDSvc::createContainer()
{
  return new FsContainerMD("");
}

//----------------------------------------------------------------------------
// Remove object from the store
//----------------------------------------------------------------------------
void
FsContainerMDSvc::removeContainer(IContainerMD* obj)
{
  // TODO: this should do a recursive removal
  std::string full_path = obj->getName();
  int rc = rmdir(full_path.c_str());

  if (rc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << full_path << " failed to delete";
    throw e;
  }

  return;
}

EOSNSNAMESPACE_END
