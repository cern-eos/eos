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
//!        TODO: Make asynchronous, try building continuations out of std::future
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/IContainerMD.hh"
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"
#include "proto/ContainerMd.pb.h"
#include <future>

//! Forward declaration
namespace qclient
{
class QClient;
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
  static std::future<eos::ns::FileMdProto>
  getFileFromId(qclient::QClient& qcl, id_t id);

  //----------------------------------------------------------------------------
  //! Fetch container metadata info for current id
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future holding the container metadata object
  //----------------------------------------------------------------------------
  static std::future<eos::ns::ContainerMdProto>
  getContainerFromId(qclient::QClient& qcl, id_t id);

  //----------------------------------------------------------------------------
  //! Fetch file map for a container id
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future the map of files
  //----------------------------------------------------------------------------
  static std::future<IContainerMD::FileMap>
  getFilesInContainer(qclient::QClient& qcl, id_t container);

  //----------------------------------------------------------------------------
  //! Fetch subcontainers map for a container id
  //!
  //! @param qcl qclient object
  //! @param id container id
  //!
  //! @return future the map of subcontainers
  //----------------------------------------------------------------------------
  static std::future<IContainerMD::ContainerMap>
  getSubContainers(qclient::QClient& qcl, id_t container);

  //----------------------------------------------------------------------------
  //! Fetch a file id given its parent and its name
  //!
  //! @param qcl qclient object
  //! @param parent_id parent container id
  //! @param name file name
  //!
  //! @return future holding the id of the file
  //----------------------------------------------------------------------------
  static std::future<id_t>
  getFileIDFromName(qclient::QClient& qcl, id_t parent_id,
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
  static std::future<id_t>
  getContainerIDFromName(qclient::QClient& qcl, id_t parent_id,
                         const std::string& name);

private:
  //----------------------------------------------------------------------------
  //! Construct hmap key of subcontainers in container
  //!
  //! @param id container id
  //!
  //! @return string representing the key
  //----------------------------------------------------------------------------
  static std::string keySubContainers(id_t id);

  //----------------------------------------------------------------------------
  //! Construct hmap key of files in container
  //!
  //! @param id container id
  //!
  //! @return string representing the key
  //----------------------------------------------------------------------------
  static std::string keySubFiles(id_t id);
};

EOSNSNAMESPACE_END
