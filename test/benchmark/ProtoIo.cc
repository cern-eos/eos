//------------------------------------------------------------------------------
// File: ProtoIo.cc
// Author: Elvin-Alin Sindrilaru - CERN
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
#include "ProtoIo.hh"
/*----------------------------------------------------------------------------*/

EOSBMKNAMESPACE_BEGIN

/******************************************************************************/
/*                            P r o t o W r i t e r                           */
/******************************************************************************/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ProtoWriter::ProtoWriter(const std::string& file):
  mFs(file, std::ios::out | std::ios::binary | std::ios::app)
{
  assert(mFs.good());
  _OstreamOutputStream = new OstreamOutputStream(&mFs);
  _CodedOutputStream = new CodedOutputStream(_OstreamOutputStream);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ProtoWriter:: ~ProtoWriter()
{
  delete _CodedOutputStream;
  delete _OstreamOutputStream;
  mFs.close();
}


/******************************************************************************/
/*                            P r o t o R e a d e r                           */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ProtoReader::ProtoReader(const std::string& file):
  mFs(file, std::ios::in | std::ios::binary)
{
  assert(mFs.good());
  _IstreamInputStream = new IstreamInputStream(&mFs);
  _CodedInputStream = new CodedInputStream(_IstreamInputStream);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ProtoReader::~ProtoReader()
{
  delete _CodedInputStream;
  delete _IstreamInputStream;
  mFs.close();
}

EOSBMKNAMESPACE_END
