// ----------------------------------------------------------------------
// File: ShouldRoute.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::ShouldRoute (const char* function,
			int __AccessMode__,
			eos::common::Mapping::VirtualIdentity &vid,
			const char* path, 
			const char* info,
			XrdOucString &host,
			int &port)
/*----------------------------------------------------------------------------*/
/*
 * @brief Function to test if a client based on the called function and his identity should be re-routed
 *
 * @param function name of the function to check
 * @param __AccessMode__ macro generated parameter defining if this is a reading or writing (namespace modifying) function
 * @param host returns the target host of a redirection
 * @param port returns the target port of a redirection
 * @return true if client should get a redirected otherwise false
 *
 * The routing table is defined using the 'route' CLI
 */
/*----------------------------------------------------------------------------*/
{
  if ((vid.host == "localhost") || (vid.host == "localhost.localdomain") || (vid.uid == 0))
  {
    return false;
  }
  
  return gOFS->PathReroute(path, info, vid, host, port);
}
