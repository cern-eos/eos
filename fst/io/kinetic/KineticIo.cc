//------------------------------------------------------------------------------
// File: KineticIO.cc
// Author: Paul Hermann Lensing <paul.lensing@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "KineticIo.hh"
#include <system_error>
#include <functional>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Forward logging function to the Kinetic library
//------------------------------------------------------------------------------
static void
logmsg(const char* func, const char* file, int line, int priority,
       const char* msg)
{
  static eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.log(func, file, line, "LIBKINETICIO", eos::common::Logging::gZeroVid,
                "", priority, msg);
}

//------------------------------------------------------------------------------
//                         **** KineticLib ****
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
KineticLib::KineticLib():
  mFactory(NULL)
{
  std::string error;
  mLibrary.reset(eos::common::DynamicLibrary::Load("libkineticio.so", error));

  if (!mLibrary) {
    eos_static_notice("Failed loading libkineticio: %s", error.c_str());
    return;
  }

  void* symbol = mLibrary->GetSymbol("getKineticIoFactory");

  if (!symbol) {
    eos_static_notice("Failed loading getKineticIoFactory from libkineticio");
    return;
  }

  typedef kio::LoadableKineticIoFactoryInterface* (*function_t)();
  mFactory = reinterpret_cast<function_t>(symbol)();
  static eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  auto f = std::bind(&eos::common::Logging::shouldlog, &g_logging,
                     std::placeholders::_1, std::placeholders::_2);
  mFactory->registerLogFunction(logmsg, f);
}

//----------------------------------------------------------------------------
//! Destructor
//----------------------------------------------------------------------------
KineticLib::~KineticLib()
{
  delete mFactory;
}

//----------------------------------------------------------------------------
// Acess a factory object. Function throws if if kineticio library has not
// been loaded correctly.
//----------------------------------------------------------------------------
kio::LoadableKineticIoFactoryInterface* KineticLib::access()
{
  static KineticLib k;

  if (!k.mFactory) {
    throw std::runtime_error("Kineticio library cannot be accessed.");
  }

  return k.mFactory;
}

//----------------------------------------------------------------------------
//                        **** KineticIo ****
//----------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
KineticIo::KineticIo(std::string path) :
  FileIo(path, "kinetic")
{
  eos_debug("path: %s", mFilePath.c_str());
  kio = std::move(KineticLib::access()->makeFileIo(path));
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
KineticIo::~KineticIo()
{
  eos_debug("path: %s", mFilePath.c_str());
}

//----------------------------------------------------------------------------
// Open file
//----------------------------------------------------------------------------
int KineticIo::fileOpen(XrdSfsFileOpenMode flags, mode_t mode,
                        const std::string& opaque, uint16_t timeout)
{
  eos_debug("path: %s, flags: %d, mode: %d, opaque: %s, timeout: %d",
            mFilePath.c_str(), flags, mode, opaque.c_str(), timeout);

  try {
    kio->Open(flags, mode, opaque, timeout);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Read from file - sync
//----------------------------------------------------------------------------
int64_t
KineticIo::fileRead(XrdSfsFileOffset offset, char* buffer,
                    XrdSfsXferSize length,
                    uint16_t timeout)
{
  eos_debug("path: %s, offset: %lld, buffer: %p, length: %d, timeout: %d",
            mFilePath.c_str(), offset, buffer, length, timeout);

  try {
    auto rv = kio->Read(offset, buffer, length, timeout);
    eos_debug("path: %s, result: %lld", mFilePath.c_str(), rv);
    return rv;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Write to file - sync
//----------------------------------------------------------------------------
int64_t
KineticIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                     XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("path: %s, offset: %lld, buffer: %p, length: %d, timeout: %d",
            mFilePath.c_str(), offset, buffer, length, timeout);

  try {
    auto rv = kio->Write(offset, buffer, length, timeout);
    eos_debug("path: %s, result: %lld", mFilePath.c_str(), rv);
    return rv;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Read from file - async
//----------------------------------------------------------------------------
int64_t
KineticIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                         XrdSfsXferSize length, bool readahead, uint16_t timeout)
{
  eos_debug("forwarding to sync read");
  return fileRead(offset, buffer, length, timeout);
}

//----------------------------------------------------------------------------
// Write to file - async
//----------------------------------------------------------------------------
int64_t
KineticIo::fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                          XrdSfsXferSize length, uint16_t timeout)
{
  eos_debug("forwarding to sync write");
  return fileWrite(offset, buffer, length, timeout);
}

//----------------------------------------------------------------------------
// Truncate file
//----------------------------------------------------------------------------
int
KineticIo::fileTruncate(XrdSfsFileOffset offset, uint16_t timeout)
{
  eos_debug("path: %s, offset: %lld, timeout %d:", mFilePath.c_str(), offset,
            timeout);

  try {
    kio->Truncate(offset, timeout);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Allocate file space
//----------------------------------------------------------------------------
int
KineticIo::fileFallocate(XrdSfsFileOffset length)
{
  eos_debug("path: %s, length: %d",  mFilePath.c_str(), length);
  return 0;
}

//----------------------------------------------------------------------------
// Deallocate file space
//----------------------------------------------------------------------------
int
KineticIo::fileFdeallocate(XrdSfsFileOffset fromOffset,
                           XrdSfsFileOffset toOffset)
{
  eos_debug("path: %s, fromOffset: %lld, toOffset: %lld", mFilePath.c_str(),
            fromOffset, toOffset);
  return 0;
}

//----------------------------------------------------------------------------
// Remove file
//----------------------------------------------------------------------------
int
KineticIo::fileRemove(uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);

  try {
    kio->Remove(timeout);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Sync file
//----------------------------------------------------------------------------
int
KineticIo::fileSync(uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);

  try {
    kio->Sync(timeout);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Close file
//----------------------------------------------------------------------------
int
KineticIo::fileClose(uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);

  try {
    kio->Close(timeout);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Get stats about the file
//----------------------------------------------------------------------------
int
KineticIo::fileStat(struct stat* buf, uint16_t timeout)
{
  eos_debug("path: %s, timeout: %d", mFilePath.c_str(), timeout);

  try {
    kio->Stat(buf, timeout);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Get pointer to async meta handler object - not applicable in this case
//----------------------------------------------------------------------------
void*
KineticIo::fileGetAsyncHandler()
{
  eos_debug("path: %s", mFilePath.c_str());
  return NULL;
}

//----------------------------------------------------------------------------
// Plug-in function to fill a statfs structure about the storage filling
// state.
//----------------------------------------------------------------------------
int
KineticIo::Statfs(struct statfs* statFs)
{
  eos_debug("path: %s", mFilePath.c_str());

  try {
    kio->Statfs(statFs);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return errno;
}

//----------------------------------------------------------------------------
// Check for the existence of a file
//----------------------------------------------------------------------------
int
KineticIo::fileExists()
{
  eos_debug("path: %s", mFilePath.c_str());

  try {
    kio->Open(0);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Open a cursor to traverse a storage system
//----------------------------------------------------------------------------
FileIo::FtsHandle*
KineticIo::ftsOpen()
{
  eos_debug("");
  return new KineticIo::FtsHandle(mFilePath.c_str());
}

//----------------------------------------------------------------------------
// Return the next path related to a traversal cursor obtained with ftsOpen
//----------------------------------------------------------------------------
std::string
KineticIo::ftsRead(FileIo::FtsHandle* fts_handle)
{
  eos_debug("");

  if (!fts_handle) {
    eos_err("handle nullpointer supplied.");
    return "";
  }

  auto handle = dynamic_cast<KineticIo::FtsHandle*>(fts_handle);

  if (!handle) {
    eos_err("failed dynamic cast to KineticIO::FtsHandle");
    return "";
  }

  if (handle->cached.size() > handle->current_index) {
    return handle->cached.at(handle->current_index++);
  }

  if (handle->cached.empty() || (handle->cached.size() != 100 &&
                                 handle->cached.back() != mFilePath)) {
    return "";
  }

  try {
    handle->cached = kio->ListFiles(handle->cached.back() + " ", 100);
    handle->current_index = 0;
    return ftsRead(handle);
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return "";
}

//----------------------------------------------------------------------------
// Close a traversal cursor
//----------------------------------------------------------------------------
int
KineticIo::ftsClose(FileIo::FtsHandle* fts_handle)
{
  eos_debug("");
  delete fts_handle;
  return 0;
}

//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------
int
KineticIo::attrGet(const char* name, char* value, size_t& size)
{
  eos_debug("path: %s, name: %s", mFilePath.c_str(), name);

  try {
    auto val = kio->attrGet(name);
    eos_debug("path: %s, value: %s", mFilePath.c_str(), val.c_str());
    size = std::min(size, val.length());
    strncpy(value, val.c_str(), size);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Get a binary attribute by name
//----------------------------------------------------------------------------
int
KineticIo::attrGet(string name, std::string& value)
{
  eos_debug("path: %s, name: %s", mFilePath.c_str(), name.c_str());

  try {
    value = kio->attrGet(name);
    eos_debug("path: %s, value: %s", mFilePath.c_str(), value.c_str());
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Set a binary attribute (name has to start with 'user.' !!!)
//----------------------------------------------------------------------------
int
KineticIo::attrSet(const char* name, const char* value, size_t len)
{
  eos_debug("path: %s, name: %s, value: %s, len: %ld", mFilePath.c_str(), name,
            value, len);

  try {
    kio->attrSet(name, std::string(value, len));
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Set a binary attribute (name has to start with 'user.' !!!)
//----------------------------------------------------------------------------
int
KineticIo::attrSet(string name, std::string value)
{
  eos_debug("path: %s, name: %s, value: %s", mFilePath.c_str(), name.c_str(),
            value.c_str());

  try {
    kio->attrSet(name, value);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// Delete a binary attribute by name
//----------------------------------------------------------------------------
int
KineticIo::attrDelete(const char* name)
{
  eos_debug("path: %s, name: %s", mFilePath.c_str(), name);

  try {
    kio->attrDelete(name);
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

//----------------------------------------------------------------------------
// List all attributes for the associated path
//----------------------------------------------------------------------------
int
KineticIo::attrList(std::vector<std::string>& list)
{
  eos_debug("path: %s", mFilePath.c_str());

  try {
    list = kio->attrList();
    return SFS_OK;
  } catch (const std::system_error& e) {
    errno = e.code().value();
  }

  return SFS_ERROR;
}

EOSFSTNAMESPACE_END
