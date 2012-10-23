//------------------------------------------------------------------------------
// File: LayoutPlugin.cc
// Author: Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
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

/*----------------------------------------------------------------------------*/
#include "fst/layout/LayoutPlugin.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/PlainLayout.hh"
#include "fst/layout/ReplicaParLayout.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------
LayoutPlugin::LayoutPlugin()
{
  // empty
};


//--------------------------------------------------------------------------
// Destructor
//--------------------------------------------------------------------------
LayoutPlugin::~LayoutPlugin()
{
  //empty
};


//--------------------------------------------------------------------------
// Get layout object
//--------------------------------------------------------------------------
Layout*
LayoutPlugin::GetLayoutObject( XrdFstOfsFile*      file,
                               unsigned int        layoutId,
                               const XrdSecEntity* client,
                               XrdOucErrInfo*      error )
{
  if ( LayoutId::GetLayoutType( layoutId ) == LayoutId::kPlain ) {
    return dynamic_cast<Layout*>( new PlainLayout( file, layoutId, client, error ) );
  }

  if ( LayoutId::GetLayoutType( layoutId ) == LayoutId::kReplica ) {
    return dynamic_cast<Layout*>( new ReplicaParLayout( file, layoutId, client, error ) );
  }

  if ( LayoutId::GetLayoutType( layoutId ) == LayoutId::kRaidDP ) {
    return static_cast<Layout*>( new RaidDpLayout( file, layoutId, client, error ) );
  }

  if ( LayoutId::GetLayoutType( layoutId ) == LayoutId::kReedS ) {
    return static_cast<Layout*>( new ReedSLayout( file, layoutId, client, error ) );
  }
  
  return 0;
}

EOSFSTNAMESPACE_END


