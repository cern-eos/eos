//------------------------------------------------------------------------------
// File: test-utils.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#include "test-utils.hh"

//------------------------------------------------------------------------------
// Lazy-initialize AuthenticationGroup.
//------------------------------------------------------------------------------
AuthenticationGroup* AuthenticationFixture::group() {
  if(!groupPtr) {
    groupPtr.reset(new AuthenticationGroup(config));
  }

  return groupPtr.get();
}

//------------------------------------------------------------------------------
// Lazy-initialize ProcessCache.
//------------------------------------------------------------------------------
ProcessCache* AuthenticationFixture::processCache() {
  return group()->processCache();
}

//------------------------------------------------------------------------------
// Lazy-initialize BoundIdentityProvider.
//------------------------------------------------------------------------------
BoundIdentityProvider* AuthenticationFixture::boundIdentityProvider() {
  return group()->boundIdentityProvider();
}

