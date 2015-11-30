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
//! @brief Synchronous mtime propagation listener
//------------------------------------------------------------------------------

#ifndef EOS_NS_SYNCTIME_ACCOUNTING_HH
#define EOS_NS_SYNCTIME_ACCOUNTING_HH
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Synchronous mtime propagation listener
//------------------------------------------------------------------------------
class SyncTimeAccounting : public IContainerMDChangeListener
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SyncTimeAccounting(IContainerMDSvc* svc);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~SyncTimeAccounting() { }

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  void containerMDChanged(IContainerMD* obj, Action type);

 private:
  IContainerMDSvc* pContainerMDSvc;

  //----------------------------------------------------------------------------
  //! Propagate a container change
  //!
  //! @param id container id
  //----------------------------------------------------------------------------
  void Propagate(IContainerMD::id_t id);
};

EOSNSNAMESPACE_END

#endif
