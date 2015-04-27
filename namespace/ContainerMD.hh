/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Class representing the container metadata
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_MD_HH
#define EOS_NS_CONTAINER_MD_HH

#include <stdint.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <google/sparse_hash_map>
#include <google/dense_hash_map>
#include <map>
#include <sys/time.h>

#include "namespace/IContainerMD.hh"
#include "namespace/persistency/Buffer.hh"

namespace eos
{
  class FileMD;

  //----------------------------------------------------------------------------
  //! Class holding the metadata information concerning a single container
  //----------------------------------------------------------------------------
  class ContainerMD: public IContainerMD
  {
    public:
      //------------------------------------------------------------------------
      // Type definitions
      //------------------------------------------------------------------------
      typedef google::dense_hash_map<std::string, IContainerMD*> ContainerMap;
      typedef google::dense_hash_map<std::string, FileMD*>       FileMap;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ContainerMD(id_t id);

      //------------------------------------------------------------------------
      //! Virtual copy constructor
      //------------------------------------------------------------------------
      virtual ContainerMD* clone() const;

      //------------------------------------------------------------------------
      //! Copy constructor
      //------------------------------------------------------------------------
      ContainerMD(const ContainerMD &other);

      //------------------------------------------------------------------------
      //! Assignment operator
      //------------------------------------------------------------------------
      ContainerMD& operator= (const ContainerMD &other);

      //------------------------------------------------------------------------
      //! Copy constructor from IContainerMD
      //------------------------------------------------------------------------
      ContainerMD(const IContainerMD &other);

      //------------------------------------------------------------------------
      //! Assignment operator from IContainer
      //------------------------------------------------------------------------
      ContainerMD& operator= (const IContainerMD &other);

      //------------------------------------------------------------------------
      //! Find sub container
      //------------------------------------------------------------------------
      virtual IContainerMD *findContainer(const std::string &name);

      //------------------------------------------------------------------------
      //! Remove container
      //------------------------------------------------------------------------
      virtual void removeContainer(const std::string &name);

      //------------------------------------------------------------------------
      //! Add container
      //------------------------------------------------------------------------
      virtual void addContainer(IContainerMD *container);

      //------------------------------------------------------------------------
      //! Get the start iterator to the container list
      //------------------------------------------------------------------------
      ContainerMap::iterator containersBegin()
      {
        return pSubContainers.begin();
      }

      //------------------------------------------------------------------------
      //! Get the end iterator of the contaienr list
      //------------------------------------------------------------------------
      ContainerMap::iterator containersEnd()
      {
        return pSubContainers.end();
      }

      //------------------------------------------------------------------------
      //! Get number of containers
      //------------------------------------------------------------------------
      size_t getNumContainers() const
      {
        return pSubContainers.size();
      }

      //------------------------------------------------------------------------
      //! Find file
      //------------------------------------------------------------------------
      virtual FileMD *findFile( const std::string &name );

      //------------------------------------------------------------------------
      //! Add file
      //------------------------------------------------------------------------
      virtual void addFile( FileMD *file );

      //------------------------------------------------------------------------
      //! Remove file
      //------------------------------------------------------------------------
      virtual void removeFile( const std::string &name );

      //------------------------------------------------------------------------
      //! Clean up the entire contents for the container. Delete files and
      //! containers recurssively
      //!
      //! @param cont_svc container metadata service
      //! @param file_svc file metadata service
      //!
      //------------------------------------------------------------------------
      virtual void cleanUp(IContainerMDSvc* cont_svc, IFileMDSvc* file_svc);

      //------------------------------------------------------------------------
      //! Get the start iterator to the file list
      //------------------------------------------------------------------------
      FileMap::iterator filesBegin()
      {
        return pFiles.begin();
      }

      //------------------------------------------------------------------------
      //! Get the end iterator of the contaienr list
      //------------------------------------------------------------------------
      FileMap::iterator filesEnd()
      {
        return pFiles.end();
      }

      //------------------------------------------------------------------------
      //! Get number of files
      //------------------------------------------------------------------------
      virtual size_t getNumFiles() const
      {
        return pFiles.size();
      }

    protected:

      ContainerMap pSubContainers;
      FileMap      pFiles;
  };
}

#endif // EOS_NS_CONTAINER_MD_HH
