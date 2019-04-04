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
//! @brief Helper function to check if it's safe to rename a directory into
//!        another
//------------------------------------------------------------------------------

#ifndef EOS_NS_RENAME_SAFETY_CHECK_HH
#define EOS_NS_RENAME_SAFETY_CHECK_HH

#include <iostream>
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Namespace.hh"
#include "common/Logging.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Is it safe to make "source" directory a subdirectory of "target"?
// Assumes eosViewRWMutex is at-least read-locked when calling this function.
//------------------------------------------------------------------------------
bool isSafeToRename(IView *view, IContainerMD *source, IContainerMD *target) {
  if(source == target) return false;

  IContainerMDSvc *svc = view->getContainerMDSvc();
  IContainerMDPtr current = svc->getContainerMD(target->getParentId());

  size_t iterations = 0;
  while(true) {
    iterations++;

    if(iterations > 1024) {
      std::string msg = SSTR("potential loop when scanning parents of container "
      << target->getId() << " - serious namespace corruption");

      eos_static_crit("%s", msg.c_str());
      throw_mdexception(EFAULT, msg);
    }

    if(current.get() == source) {
      return false; // Nope, sound alarm, this rename is not safe
    }

    if(current->getId() == source->getId()) {
      // Should not happen.
      eos_static_crit("%s", SSTR("Two containers with the same ID ended up with different objects in memory - " <<
      current->getId() << " == " << source->getId() << " - " << current << " vs " << source).c_str());
      return false;
    }

    if(current->getId() == 1) {
      // We've reached root, this rename looks safe.
      return true;
    }

    // Move up one step.
    current = svc->getContainerMD(current->getParentId());
  }
}

EOSNSNAMESPACE_END

#endif