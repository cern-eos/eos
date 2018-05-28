/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Single class which builds redis requests towards the backend.
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/Identifiers.hh"
#include <vector>
#include <string>

EOSNSNAMESPACE_BEGIN

class IContainerMD;
class IFileMD;

using RedisRequest = std::vector<std::string>;

class RequestBuilder {
public:

  //----------------------------------------------------------------------------
  //! Write container protobuf metadata.
  //----------------------------------------------------------------------------
  static RedisRequest writeContainerProto(IContainerMD *obj);

  //----------------------------------------------------------------------------
  //! Write container protobuf metadata - low level API.
  //----------------------------------------------------------------------------
  static RedisRequest writeContainerProto(ContainerIdentifier id, const std::string &hint, const std::string &blob);

  //----------------------------------------------------------------------------
  //! Write file protobuf metadata.
  //----------------------------------------------------------------------------
  static RedisRequest writeFileProto(IFileMD *obj);

  //----------------------------------------------------------------------------
  //! Write file protobuf metadata - low level API.
  //----------------------------------------------------------------------------
  static RedisRequest writeFileProto(FileIdentifier id, const std::string &hint, const std::string &blob);

  //----------------------------------------------------------------------------
  //! Read container protobuf metadata.
  //----------------------------------------------------------------------------
  static RedisRequest readContainerProto(ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Read file protobuf metadata.
  //----------------------------------------------------------------------------
  static RedisRequest readFileProto(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Delete container protobuf metadata.
  //----------------------------------------------------------------------------
  static RedisRequest deleteContainerProto(ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Delete file protobuf metadata.
  //----------------------------------------------------------------------------
  static RedisRequest deleteFileProto(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Calculate number of containers.
  //----------------------------------------------------------------------------
  static RedisRequest getNumberOfContainers();

  //----------------------------------------------------------------------------
  //! Calculate number of files.
  //----------------------------------------------------------------------------
  static RedisRequest getNumberOfFiles();

  //----------------------------------------------------------------------------
  //! Get key for files contained within a filesystem.
  //----------------------------------------------------------------------------
  static std::string keyFilesystemFiles(IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Get key for unlinked files contained within a filesystem.
  //! (files pending deletion)
  //----------------------------------------------------------------------------
  static std::string keyFilesystemUnlinked(IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Get container bucket which is computed as the id of the container modulo
  //! the number of container buckets.
  //!
  //! @param id container id
  //!
  //! @return container bucket key
  //----------------------------------------------------------------------------
  static std::string getContainerBucketKey(ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Get file bucket which is computed as the id of the container modulo the
  //! number of file buckets (1M).
  //!
  //! @param id file id
  //!
  //! @return file bucket key
  //! Calculate number of files.
  //----------------------------------------------------------------------------
  static std::string getFileBucketKey(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Override number of container buckets
  //----------------------------------------------------------------------------
  static void OverrideNumberOfContainerBuckets(uint64_t buckets = 128 * 1024) {
    sNumContBuckets = buckets;
  }

  //----------------------------------------------------------------------------
  //! Override number of file buckets
  //----------------------------------------------------------------------------
  static void OverrideNumberOfFileBuckets(uint64_t buckets = 128 * 1024) {
    sNumFileBuckets = buckets;
  }

  static std::uint64_t sNumContBuckets; ///< Number of buckets power of 2
  static std::uint64_t sNumFileBuckets; ///< Number of buckets power of 2

};


EOSNSNAMESPACE_END
