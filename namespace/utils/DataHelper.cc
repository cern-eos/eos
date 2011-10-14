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
// desc:   Checksumming, data conversion and other stuff
//------------------------------------------------------------------------------

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cerrno>
#include "namespace/utils/DataHelper.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  // Copy file ownership information
  //----------------------------------------------------------------------------
  void DataHelper::copyOwnership( const std::string &target,
                                  const std::string &source,
                                  bool ignoreNoPerm )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Check the root-ness
    //--------------------------------------------------------------------------
    uid_t uid = getuid();
    if( uid != 0 && ignoreNoPerm )
      return;

    if( uid != 0 )
      {
        MDException e( EFAULT );
        e.getMessage() << "Only root can change ownership";
        throw e;
      }

    //--------------------------------------------------------------------------
    // Get the thing done
    //--------------------------------------------------------------------------
    struct stat st;
    if( stat( source.c_str(), &st ) != 0 )
      {
        MDException e( errno );
        e.getMessage() << "Unable to stat source: " << source;
        throw e;
      }

    if( chown( target.c_str(), st.st_uid, st.st_gid ) != 0 )
      {
        MDException e( errno );
        e.getMessage() << "Unable to change the ownership of the target: ";
        e.getMessage() << target;
        throw e;
      }
  }
}

