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

#include "namespace/ns_on_filesystem/FsContainerMD.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>
#include <dirent>
#include <ctime>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FsContainerMD::FsContainerMD(const std::string& path):
  mFullPath(path)
{
  mSubContainers.set_deleted_key("");
  mFiles.set_deleted_key("");
  mSubContainers.set_empty_key("##_EMPTY_##");
  mFiles.set_empty_key("##_EMPTY_##");
}

//------------------------------------------------------------------------------
// Desstructor
//------------------------------------------------------------------------------
FsContainerMD::~FsContainerMD()
{
  // empty
}

//------------------------------------------------------------------------------
// Virtual copy constructor
//------------------------------------------------------------------------------
IContainerMD*
FsContainerMD::clone() const
{
  return new FsContainerMD(*this);
}

//------------------------------------------------------------------------------
//! Copy constructor
//------------------------------------------------------------------------------
FsContainer::FsContainerMD(const ContainerMD& other)
{
  *this = other;
}

//------------------------------------------------------------------------------
// Assignment operator
//------------------------------------------------------------------------------
FsContainerMD&
FsContainerMD::operator= (const ContainerMD& other)
{
  mFullPath = other.mFullPath;
}

//------------------------------------------------------------------------------
// Add container
//------------------------------------------------------------------------------
void addContainer(IContainerMD* container)
{
  std::string path = container->getName();

  // Make sure the paths overlap
  if (path.find(mFullPath) != 0)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mFullPath << " add subcontainer failed";
    throw e;
  }

  // Create the container on the filesystem
  mode_t mode = S_IRWXU | S_IRGRP | S_IROTH;
  int retc = mkdir(path.c_str(), mode);

  if (retc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << path << " mkdir failed";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Remove container
//------------------------------------------------------------------------------
void
FsContainerMD::removeContainer(const std::string& name)
{
  std::string full_path = mFullPath + name;
  int rc = rmdir(full_path.c_str());

  if (rc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << full_path << " rmdir failed";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Find sub container
//------------------------------------------------------------------------------
IContainerMD*
FsContainerMD::findContainer(const std::string& name)
{
  (void) getEntries();
  auto iter_cont = mSubContainer.find(name);

  if (it != mSubContainer.end())
    return *iter_cont;

  return (IContainerMD*)0;
}

//------------------------------------------------------------------------------
// Get number of containers
//------------------------------------------------------------------------------
size_t
FsContainerMD::getNumContainers() const
{
  (void) getEntries();
  return mSubContainers.size();
}

//------------------------------------------------------------------------------
// Add file
//------------------------------------------------------------------------------
void
FsContainerMD::addFile(IFileMD* file)
{
  std::string path = file->getName();

  // Make sure the paths overlap
  if (path.find(mFullPath) != 0)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mFullPath << " adding file: "
                   << path << " failed";
    throw e;
  }

  // All this is equivalent to touch
  mode_t open_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
  int fd = creat(path.c_str(), open_mode);

  if (fd == -1)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mFullPath << " failed creating file: "
                   << path;
    throw e;
  }

  int rc = close(fd);

  if (rc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mFullPath << " failed closing file: "
                   << path;
    throw e;
  }
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
void
FsContainerMD::removeFile(const std::string& name)
{
  std::string full_path = mFullPath + name;
  int rc = unlink(full_path.c_str());

  if (rc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mFullPath << " failed to remove file: "
                   << full_path;
    throw e;
  }
}

//------------------------------------------------------------------------------
// Find file
//------------------------------------------------------------------------------
IFileMD*
FsContainerMD::findFile(const std::string& name)
{
  (void) getEntries();
  std::string full_path = mFull_path + name;
  auth iter_file = mFiles.find(full_path);

  if (iter_file == mFiles.end())
    return *iter_file;

  return(IFileMD*)0;
}

//------------------------------------------------------------------------------
//! Get number of files
//------------------------------------------------------------------------------
size_t
FsContainerMD::getNumFiles() const
{
  (void) getEntries();
  return mFiles.size();
}

//------------------------------------------------------------------------------
// Get full path to container
//------------------------------------------------------------------------------
const std::string&
FsContainerMD::getName() const
{
  return mFullPath;
}

//------------------------------------------------------------------------------
// Set name
//------------------------------------------------------------------------------
void
FsContainerMD::setName(const std::string& full_path)
{
  mFullPath = full_path;
}

//------------------------------------------------------------------------------
// Get container id
//------------------------------------------------------------------------------
id_t
FsContainerMD::getId() const
{
  return (id_t)0;
}

//------------------------------------------------------------------------------
// Get parent id
//------------------------------------------------------------------------------
id_t
FsContainerMD::getParentId() const
{
  return (id_t)0;
}

//------------------------------------------------------------------------------
// Set parent id
//------------------------------------------------------------------------------
void
FsContainerMD::setParentId(id_t parentId)
{
  std::cerr << "Function " << __FUNCTION__ << " is not implemented" << std::endl;
  return;
}

//------------------------------------------------------------------------------
// Get the flags
//------------------------------------------------------------------------------
uint16_t&
FsContainerMD::getFlags()
{
  return pFlags;
}

//------------------------------------------------------------------------------
// Get the flags
//------------------------------------------------------------------------------
uint16_t
FsContainerMD::getFlags() const
{
  return pFlags;
}

//------------------------------------------------------------------------------
// Get creation time
//------------------------------------------------------------------------------
void getCTime(ctime_t& ctime) const;

//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void setCTime(ctime_t ctime);

//------------------------------------------------------------------------------
//! Set creation time to now
//------------------------------------------------------------------------------
void setCTimeNow();

//------------------------------------------------------------------------------
// Get uid
//------------------------------------------------------------------------------
uid_t getCUid() const;


//------------------------------------------------------------------------------
// Set creation time
//------------------------------------------------------------------------------
void
FsContainerMD::setCTime(ctime_t ctime)
{
  struct timeval times[2];
  times[0].tv_sec = times[1].tv_sec = ctime.tv_sec;
  times[0].tv_usec = times[2].tv_usec = ctime.tv_nsec / 1000;
  int retc = utimes(mFullPath.c_str(), times);

  if (retc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mId << " utime failed";
    throw e;
  }
}

//------------------------------------------------------------------------------
// Set creation time to now
//------------------------------------------------------------------------------
void
FsContainerMD::setCTimeNow()
{
  int retc = utimes(mFullPath.c_str(), static_cast<const struct timeval*>(0));

  if (retc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mId << " utime failed";
    throw e;
  }
}

//----------------------------------------------------------------------------
// Get all entries from current container
//----------------------------------------------------------------------------
void
FsContainerMD::getEntries()
{
  int rc = stat(mFullPath.c_str(), &mInfo);

  if (rc)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mId << " failed to stat";
    throw e;
  }

  if (mMtime)
  {
    // Check for any modifications
    if (std::difftime(mMtime, mInfo.st_mtime))
    {
      mMtime = mInfo.st_mtime;
      mCtime = mInfo.st_ctime;
    }
    else
    {
      // Nothing to do
      return;
    }
  }
  else
  {
    // Save initial mtime and ctime
    mMtime = mInfo.st_mtime;
    mCtime = mInfo.st_ctime;
  }

  // Read directory entries
  DIR* dir = opendir(mFullPath.c_str());

  if (!dir)
  {
    MDException e(errno);
    e.getMessage() << "Container #" << mId << " opendir failed";
    throw e;
  }

  std::string name;
  std::string full_path;
  struct dirent* entry;

  while ((entry = readdir(dir))
  {
    name = entry.d_name;
    full_path = mFullPath + name;

    if (entry.d_type & DT_DIR)
    {
      // Entry is a directory
      IContainerMD* cont = new FsContainerMD(full_path);

      if (mSubContainers.find(name) != mSubContainers.end())
      {
        std::cerr << "Container " << full_path << " already in map";
        continue;
      }

      mSubContainers[name] = cont;
    }
    else
    {
      // Entry is a file
      IFileMD* cont = new IFileMD(full_path);

      if (mFiles.find(name) != mFiles.end())
      {
        std::cerr << "File " << full_path << " already in map";
        continue;
      }

      mFiles[name] = cont;
    }
  }
}

EOSNSNAMESPACE_END
