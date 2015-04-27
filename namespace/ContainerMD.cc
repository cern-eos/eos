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

#include "namespace/ContainerMD.hh"
#include "namespace/FileMD.hh"
#include "namespace/IContainerMDSvc.hh"
#include "namespace/IFileMDSvc.hh"

namespace eos
{

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ContainerMD::ContainerMD(id_t id):
      IContainerMD(id)
  {
    pSubContainers.set_deleted_key( "" );
    pFiles.set_deleted_key( "" );
    pSubContainers.set_empty_key( "##_EMPTY_##" );
    pFiles.set_empty_key( "##_EMPTY_##" );
  }

  //----------------------------------------------------------------------------
  // Virtual copy constructor
  //----------------------------------------------------------------------------
  ContainerMD*
  ContainerMD::clone() const
  {
    return new ContainerMD(*this);
  }

  //----------------------------------------------------------------------------
  // Copy constructor
  //----------------------------------------------------------------------------
  ContainerMD::ContainerMD(const ContainerMD &other):
      IContainerMD(other.getId())
  {
    *this = other;
  }

  //----------------------------------------------------------------------------
  // Asignment operator
  //----------------------------------------------------------------------------
  ContainerMD& ContainerMD::operator= (const ContainerMD &other)
  {
    *this = dynamic_cast<const IContainerMD&>(other);
    return *this;
  }

  //----------------------------------------------------------------------------
  // Copy constructor from IContainer
  //----------------------------------------------------------------------------
  ContainerMD::ContainerMD(const IContainerMD &other):
      IContainerMD(other.getId())
  {
    *this = other;
   }

  //----------------------------------------------------------------------------
  // Asignment operator from IContainer
  //----------------------------------------------------------------------------
  ContainerMD &ContainerMD::operator= (const IContainerMD &other)
  {
    *this = other;
    return *this;
  }

  //----------------------------------------------------------------------------
  // Find sub container
  //----------------------------------------------------------------------------
  IContainerMD*
  ContainerMD::findContainer( const std::string &name )
  {
    ContainerMap::iterator it = pSubContainers.find( name );
    if( it == pSubContainers.end() )
      return 0;
    return it->second;
  }

  //----------------------------------------------------------------------------
  // Remove container
  //----------------------------------------------------------------------------
  void
  ContainerMD::removeContainer( const std::string &name )
  {
    pSubContainers.erase( name );
  }

  //----------------------------------------------------------------------------
  // Add container
  //----------------------------------------------------------------------------
  void
  ContainerMD::addContainer( IContainerMD *container )
  {
    container->setParentId( pId );
    pSubContainers[container->getName()] = container;
  }

  //----------------------------------------------------------------------------
  // Find file
  //----------------------------------------------------------------------------
  FileMD*
  ContainerMD::findFile( const std::string &name )
  {
    FileMap::iterator it = pFiles.find( name );
    if( it == pFiles.end() )
      return 0;
    return it->second;
  }

  //----------------------------------------------------------------------------
  // Add file
  //----------------------------------------------------------------------------
  void
  ContainerMD::addFile( FileMD *file )
  {
    file->setContainerId( pId );
    pFiles[file->getName()] = file;
  }

  //----------------------------------------------------------------------------
  // Remove file
  //----------------------------------------------------------------------------
  void
  ContainerMD::removeFile( const std::string &name )
  {
    pFiles.erase( name );
  }

  //------------------------------------------------------------------------
  // Clean up the entire contents for the container. Delete files and
  // containers recurssively
  //------------------------------------------------------------------------
  void
  ContainerMD::cleanUp(IContainerMDSvc* cont_svc, IFileMDSvc* file_svc)
  {
    for (auto itf = pFiles.begin(); itf != pFiles.end(); ++itf)
      file_svc->removeFile(itf->second);

    for (auto itc = pSubContainers.begin(); itc != pSubContainers.end(); ++itc)
    {
      (void) itc->second->cleanUp(cont_svc, file_svc);
      cont_svc->removeContainer(itc->second);
    }
  }
}
