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
//! @brief Class to retrieve metadata from the backend - no caching!
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/Identifiers.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"
#include "proto/ContainerMd.pb.h"
#include <future>
#include <folly/futures/Future.h>

//! Forward declaration
namespace qclient {
  class QClient;
}

namespace folly {
  class Executor;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class MetadataFetcher
//------------------------------------------------------------------------------
class MetadataFetcher
{
public:
  //----------------------------------------------------------------------------
  //! Fetch file metadata info for current id
  //!
  //! @param qcl qclient object
  //! @param id file id
  //!
  //! @return future holding the file metadata object
  //----------------------------------------------------------------------------
  static folly::Future<eos::ns::FileMdProto>
  getFileFromId(qclient::QClient& qcl, FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Fetch container metadata info for current id
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future holding the container metadata object
  //----------------------------------------------------------------------------
  static folly::Future<eos::ns::ContainerMdProto>
  getContainerFromId(qclient::QClient& qcl, ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Check if given file id exists on the namespace
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future holding whether given file exists
  //----------------------------------------------------------------------------
  static folly::Future<bool>
  doesFileMdExist(qclient::QClient& qcl, FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Fetch file map for a container id
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future the map of files
  //----------------------------------------------------------------------------
  static folly::Future<IContainerMD::FileMap>
  getFileMap(qclient::QClient& qcl, ContainerIdentifier container);

  //----------------------------------------------------------------------------
  //! Fetch all FileMDs contained within the given FileMap. Vector is sorted
  //! by filename.
  //----------------------------------------------------------------------------
  static std::vector<folly::Future<eos::ns::FileMdProto>>
  getFilesFromFilemap(qclient::QClient& qcl, IContainerMD::FileMap &fileMap);

  //----------------------------------------------------------------------------
  //! Fetch all file metadata within the given container.
  //----------------------------------------------------------------------------
  static folly::Future<std::vector<folly::Future<eos::ns::FileMdProto>>>
  getFileMDsInContainer(qclient::QClient& qcl, ContainerIdentifier container,
    folly::Executor *executor);

  //----------------------------------------------------------------------------
  //! Fetch subcontainers map for a container id
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future the map of subcontainers
  //----------------------------------------------------------------------------
  static folly::Future<IContainerMD::ContainerMap>
  getContainerMap(qclient::QClient& qcl, ContainerIdentifier container);

  //----------------------------------------------------------------------------
  //! Fetch all ContainerMDs contained within the given ContainerMap.
  //! Vector is sorted by filename.
  //----------------------------------------------------------------------------
  static std::vector<folly::Future<eos::ns::ContainerMdProto>>
  getContainersFromContainerMap(qclient::QClient& qcl,
    IContainerMD::ContainerMap &containerMap);

  //----------------------------------------------------------------------------
  //! Fetch a file id given its parent and its name
  //!
  //! @param qcl qclient object
  //! @param parent_id parent container id
  //! @param name file name
  //!
  //! @return future holding the id of the file
  //----------------------------------------------------------------------------
  static folly::Future<FileIdentifier>
  getFileIDFromName(qclient::QClient& qcl, ContainerIdentifier parent_id,
                    const std::string& name);

  //----------------------------------------------------------------------------
  //! Fetch a container id given its parent and its name
  //!
  //! @param qcl qclient object
  //! @param parent_id parent container id
  //! @param name subcontainer name
  //!
  //! @return future holding the id of the subcontainer
  //----------------------------------------------------------------------------
  static folly::Future<ContainerIdentifier>
  getContainerIDFromName(qclient::QClient& qcl, ContainerIdentifier parent_id,
                         const std::string& name);

private:
  //----------------------------------------------------------------------------
  //! Construct hmap key of subcontainers in container
  //!
  //! @param id container id
  //!
  //! @return string representing the key
  //----------------------------------------------------------------------------
  static std::string keySubContainers(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Construct hmap key of files in container
  //!
  //! @param id container id
  //!
  //! @return string representing the key
  //----------------------------------------------------------------------------
  static std::string keySubFiles(IContainerMD::id_t id);
};

EOSNSNAMESPACE_END
