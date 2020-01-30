//------------------------------------------------------------------------------
// File: BaseViewLocator.hh
// Author: Georgios Bitzes - CERN
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

#ifndef EOS_COMMON_BASE_VIEW_LOCATOR_HH
#define EOS_COMMON_BASE_VIEW_LOCATOR_HH

#include "common/Namespace.hh"
#include <string>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Describes how to physically locate a filesystem:
//!   - Host + port of the corresponding FST
//!   - Storage path of the filesystem
//!   - Type of access (remote or local)
//!
//! Filesystem locators can also be constructed from a queuepath
//! which has the following form:
//! /eos/<host>:<port>/fst<storage_path>
//------------------------------------------------------------------------------
class FileSystemLocator
{
public:
  //! Storage type of the filesystem
  enum class StorageType {
    Local,
    Xrd,
    S3,
    WebDav,
    HTTP,
    HTTPS,
    Unknown
  };

  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  FileSystemLocator() {}

  //----------------------------------------------------------------------------
  //! Constructor, pass manually individual components
  //----------------------------------------------------------------------------
  FileSystemLocator(const std::string& host, int port,
                    const std::string& storagepath);

  //----------------------------------------------------------------------------
  //! Try to parse a "queuepath"
  //----------------------------------------------------------------------------
  static bool fromQueuePath(const std::string& queuepath, FileSystemLocator& out);

  //----------------------------------------------------------------------------
  //! Parse storage type from storage path string
  //----------------------------------------------------------------------------
  static StorageType parseStorageType(const std::string& storagepath);

  //----------------------------------------------------------------------------
  //! Get host
  //----------------------------------------------------------------------------
  std::string getHost() const;

  //----------------------------------------------------------------------------
  //! Get port
  //----------------------------------------------------------------------------
  int getPort() const;

  //----------------------------------------------------------------------------
  //! Get hostport, concatenated together as "host:port"
  //----------------------------------------------------------------------------
  std::string getHostPort() const;

  //----------------------------------------------------------------------------
  //! Get queuepath
  //----------------------------------------------------------------------------
  std::string getQueuePath() const;

  //----------------------------------------------------------------------------
  //! Get "FST queue", ie /eos/example.com:3002/fst
  //----------------------------------------------------------------------------
  std::string getFSTQueue() const;

  //----------------------------------------------------------------------------
  //! Get storage path
  //----------------------------------------------------------------------------
  std::string getStoragePath() const;

  //----------------------------------------------------------------------------
  //! Get storage type
  //----------------------------------------------------------------------------
  StorageType getStorageType() const;

  //----------------------------------------------------------------------------
  //! Check whether filesystem is local or remote
  //----------------------------------------------------------------------------
  bool isLocal() const;

  //----------------------------------------------------------------------------
  //! Get transient channel for this filesystem - that is, the channel through
  //! which all transient, non-important information will be transmitted.
  //----------------------------------------------------------------------------
  std::string getTransientChannel() const;

private:
  std::string host;
  int32_t port = 0;
  std::string storagepath;
  StorageType storageType;
};

//------------------------------------------------------------------------------
//! This type helps figure out how to locate the appropriate shared hash for
//! a given node / group / space.
//!
//! Abstracts away the "config queue" / "broadcast queue" madness.
//------------------------------------------------------------------------------
class SharedHashLocator {
public:
  enum class Type {
    kSpace,
    kGroup,
    kNode,
    kGlobalConfigHash,
    kFilesystem
  };

  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  SharedHashLocator();

  //----------------------------------------------------------------------------
  //! Constructor: Pass the EOS instance name, BaseView type, and name.
  //!
  //! Once we drop the MQ entirely, the instance name can be removed.
  //----------------------------------------------------------------------------
  SharedHashLocator(const std::string &instanceName, Type type,
    const std::string &name);

  //----------------------------------------------------------------------------
  //! Constructor: Same as above, but auto-discover instance name.
  //----------------------------------------------------------------------------
  SharedHashLocator(Type type, const std::string &name);

  //----------------------------------------------------------------------------
  //! Constructor: Special case for FileSystems, as they work a bit differently
  //! than the rest.
  //----------------------------------------------------------------------------
  SharedHashLocator(const FileSystemLocator &fsLocator, bool bc2mgm);

  //----------------------------------------------------------------------------
  //! Convenience "Constructors": Make locator for space, group, node
  //----------------------------------------------------------------------------
  static SharedHashLocator makeForSpace(const std::string &name);
  static SharedHashLocator makeForGroup(const std::string &name);
  static SharedHashLocator makeForNode(const std::string &name);
  static SharedHashLocator makeForGlobalHash();

  //----------------------------------------------------------------------------
  //! Get "config queue" for shared hash
  //----------------------------------------------------------------------------
  std::string getConfigQueue() const;

  //----------------------------------------------------------------------------
  //! Get "broadcast queue" for shared hash
  //----------------------------------------------------------------------------
  std::string getBroadcastQueue() const;

  //----------------------------------------------------------------------------
  //! Check if this object is actually pointing to something
  //----------------------------------------------------------------------------
  bool empty() const;

  //----------------------------------------------------------------------------
  //! Produce SharedHashLocator by parsing config queue
  //----------------------------------------------------------------------------
  static bool fromConfigQueue(const std::string &configQueue, SharedHashLocator &out);

private:
  bool mInitialized;

  std::string mInstanceName;
  Type mType;
  std::string mName;

  std::string mMqSharedHashPath;
  std::string mBroadcastQueue;

  std::string mChannel;
};

//------------------------------------------------------------------------------
//! Class to fully specify a TransferQueue
//------------------------------------------------------------------------------
class TransferQueueLocator {
public:
  //----------------------------------------------------------------------------
  //! Constructor: Queue tied to a FileSystem
  //----------------------------------------------------------------------------
  TransferQueueLocator(const FileSystemLocator &fsLocator, const std::string &tag);

  //----------------------------------------------------------------------------
  //! Constructor: Queue tied to an FST
  //----------------------------------------------------------------------------
  TransferQueueLocator(const std::string &fstQueue, const std::string &tag);

  //----------------------------------------------------------------------------
  //! Get "queue"
  //----------------------------------------------------------------------------
  std::string getQueue() const;

  //----------------------------------------------------------------------------
  //! Get "queuepath"
  //----------------------------------------------------------------------------
  std::string getQueuePath() const;

  //----------------------------------------------------------------------------
  //! Get QDB key for this queue
  //----------------------------------------------------------------------------
  std::string getQDBKey() const;

private:
  FileSystemLocator mLocator;
  std::string mFstQueue;
  std::string mTag;
};

EOSCOMMONNAMESPACE_END

#endif

