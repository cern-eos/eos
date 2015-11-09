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
// author: Andreas-Joachim Peters <apeters@cern.ch>
// desc:   The container subtree accounting
//------------------------------------------------------------------------------

#ifndef EOS_NS_SYNCTIME_ACCOUNTING_HH
#define EOS_NS_SYNCTIME_ACCOUNTING_HH

#include "namespace/IContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/ContainerMD.hh"
#include "namespace/MDException.hh"
#include <utility>
#include <list>
#include <deque>

namespace eos
{
  class SyncTimeAccounting : public IContainerMDChangeListener
  {
    public:
    
    //------------------------------------------------------------------------
    //! Constructor
    //------------------------------------------------------------------------
    SyncTimeAccounting(eos::ChangeLogContainerMDSvc* svc);
    
    //------------------------------------------------------------------------
    //! Destructor
    //------------------------------------------------------------------------
    virtual ~SyncTimeAccounting(){}

    //------------------------------------------------------------------------
    //! Propagate a container change
    //------------------------------------------------------------------------
    void Propagate( ContainerMD::id_t id );

    //------------------------------------------------------------------------
    //! Notify me about the changes in the main view
    //------------------------------------------------------------------------
    void containerMDChanged( ContainerMD *obj, Action type  );

    //------------------------------------------------------------------------
    //! Initizalie
    //------------------------------------------------------------------------
    void initialize();
    
    //------------------------------------------------------------------------
    //! Finalize
    //------------------------------------------------------------------------
    void finalize();
    
  private:
    eos::ChangeLogContainerMDSvc* pContainerMDSvc;

  };
}

#endif // EOS_NS_FILESYSTEM_VIEW_HH
