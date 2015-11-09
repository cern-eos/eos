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
// desc:   Sync Time Accounting
//------------------------------------------------------------------------------

#include "namespace/accounting/SyncTimeAccounting.hh"
#include <iostream>

namespace eos
{
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  SyncTimeAccounting::SyncTimeAccounting( eos::ChangeLogContainerMDSvc* svc) : pContainerMDSvc(svc)
  {
  }

  //----------------------------------------------------------------------------
  // Notify the me about the changes in the main view
  //----------------------------------------------------------------------------
  void SyncTimeAccounting::containerMDChanged( ContainerMD *obj, Action type  )
  {
    switch( type )
    {
      //------------------------------------------------------------------------
      // MTime change
      //------------------------------------------------------------------------
      case IContainerMDChangeListener::MTimeChange:
	Propagate(obj->getId());
        break;
	
      default:
        break;
    }
  }

  //------------------------------------------------------------------------                                                                                              
  //! Propagate the sync time
  //------------------------------------------------------------------------                                                                                              
  void SyncTimeAccounting::Propagate( ContainerMD::id_t id )
  {
    size_t deepness = 0;
    if (!id)
      return;

    ContainerMD::ctime_t mTime;
    mTime.tv_sec = mTime.tv_nsec = 0 ;

    ContainerMD::id_t iId = id;
    
    while ( (iId > 1 ) && (deepness < 255) ) 
    {

      ContainerMD* iCont = 0;
      try 
      {
	iCont = pContainerMDSvc->getContainerMD(iId);
	// only traverse if there is an attribute saying so 
	if (!iCont->hasAttribute("sys.mtime.propagation"))
	  return;

	if (!deepness)
	  iCont->getMTime(mTime);

	if (!iCont->setTMTime(mTime) && deepness)
	  return;
      }
      catch( MDException &e )
      {

      }

      if (!iCont)
	return;
      
      iId = iCont->getParentId();
      deepness++;
    } 
  }

  //----------------------------------------------------------------------------
  // Initialize
  //----------------------------------------------------------------------------
  void SyncTimeAccounting::initialize()
  {
  }

  //----------------------------------------------------------------------------
  // Finalize
  //----------------------------------------------------------------------------
  void SyncTimeAccounting::finalize()
  {
  }
}
