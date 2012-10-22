//------------------------------------------------------------------------------
// File: HeaderCRC.cc
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
#include <stdint.h>
/*----------------------------------------------------------------------------*/
#include "fst/io/HeaderCRC.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

int HeaderCRC::sizeHeader = 4096;             // 4kb
char HeaderCRC::tagName[] = "_HEADER_RAIDIO_";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
HeaderCRC::HeaderCRC():
  valid( true ),
  noBlocks( -1 ),
  idStripe( -1 ),
  sizeLastBlock( -1 )
{
  //empty
}


//------------------------------------------------------------------------------
// Constructor with parameter
//------------------------------------------------------------------------------
HeaderCRC::HeaderCRC( long int noblocks ):
  valid( true ),
  noBlocks( noblocks ),
  idStripe( -1 ),
  sizeLastBlock( -1 )
{
  strncpy( tag, tagName, strlen( tagName ) );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
HeaderCRC::~HeaderCRC()
{
  //empty
}


//------------------------------------------------------------------------------
// Read header from file
//------------------------------------------------------------------------------
bool
HeaderCRC::readFromFile( XrdCl::File* f )
{
  uint32_t ret;
  long int offset = 0;
  char* buff = static_cast< char* >( calloc( sizeHeader, sizeof( char ) ) );

  eos_debug( "offset: %li, sizeHeader: %i \n", offset, sizeHeader );

  if ( !( f->Read( offset, sizeHeader, buff, ret ).IsOK() )
       || ( ret != static_cast< uint32_t >( sizeHeader ) ) ) {
    free( buff );
    valid = false;
    return valid;
  }

  memcpy( tag, buff, sizeof tag );

  if ( strncmp( tag, tagName, strlen( tagName ) ) ) {
    free( buff );
    valid = false;
    return valid;
  }

  offset += sizeof tag;
  memcpy( &idStripe, buff + offset, sizeof idStripe );
  offset += sizeof idStripe;
  memcpy( &noBlocks, buff + offset, sizeof noBlocks );
  offset += sizeof noBlocks;
  memcpy( &sizeLastBlock, buff + offset, sizeof sizeLastBlock );

  free( buff );
  valid = true;
  return valid;
}


//------------------------------------------------------------------------------
// Write header to file
//------------------------------------------------------------------------------
bool
HeaderCRC::writeToFile( XrdCl::File* f )
{
  int offset = 0;
  char* buff = static_cast< char* >( calloc( sizeHeader, sizeof( char ) ) );

  memcpy( buff + offset, tagName, sizeof tagName );
  offset += sizeof tag;
  memcpy( buff + offset, &idStripe, sizeof idStripe );
  offset += sizeof idStripe;
  memcpy( buff + offset, &noBlocks, sizeof noBlocks );
  offset += sizeof noBlocks;
  memcpy( buff + offset, &sizeLastBlock, sizeof sizeLastBlock );
  offset += sizeof sizeLastBlock;
  memset( buff + offset, 0, sizeHeader - offset );

  if ( !( f->Write( 0, sizeHeader, buff ).IsOK() ) ) {
    valid = false;
  } else {
    valid = true;
  }

  free( buff );
  return valid;
}

EOSFSTNAMESPACE_END
