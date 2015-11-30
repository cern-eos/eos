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
//! @author Andreas-Joachim Peters <apeters@cern.ch>
//! @brief Container subtree accounting
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_ACCOUNTING_HH
#define EOS_NS_CONTAINER_ACCOUNTING_HH

#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_in_memory/ContainerMD.hh"
#include "namespace/ns_in_memory/FileMD.hh"
#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include <utility>
#include <list>
#include <deque>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Container subtree accounting listener
//------------------------------------------------------------------------------
class ContainerAccounting : public IFileMDChangeListener
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ContainerAccounting(IContainerMDSvc* svc);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ContainerAccounting() {}

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e);

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  virtual void fileMDRead(IFileMD* obj) {}

 private:

  IContainerMDSvc* pContainerMDSvc; ///< container MD service

  //----------------------------------------------------------------------------
  //! Account a file in the respective container
  //!
  //! @param obj file meta-data object
  //! @param dsize size change
  //----------------------------------------------------------------------------
  void Account(IFileMD* obj , int64_t dsize);

};

EOSNSNAMESPACE_END

#endif
