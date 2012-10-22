// ----------------------------------------------------------------------
// File: HeaderCRC.cc
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

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
#include "fst/io/HeaderCRC.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//default constructor
HeaderCRC::HeaderCRC()
{
  idStripe = -1;
  noBlocks = -1;
  sizeLastBlock = -1;
  valid = true;
}


/*----------------------------------------------------------------------------*/
//constructor
HeaderCRC::HeaderCRC( long int noblocks )
{
  strcpy( tag, HEADER );
  noBlocks = noblocks;
  sizeLastBlock = -1;
  idStripe = -1;
  valid = true;
}


/*----------------------------------------------------------------------------*/
//destructor
HeaderCRC::~HeaderCRC()
{
  //empty
}


/*----------------------------------------------------------------------------*/
//read the header from the file via XrdPosixXrootd
bool HeaderCRC::readFromFile( XrdCl::File* f )
{
  unsigned int ret;
  long int offset = 0;
  char* buff = ( char* ) calloc( sizeHeader, sizeof( char ) );

  eos_debug( "HeaderCRC::ReadFromFile: offset: %li, sizeHeader: %i \n", offset, sizeHeader );

  if ( !( f->Read( offset, sizeHeader, buff, ret ).IsOK() ) || ( ret != sizeHeader ) ) {
    free( buff );
    valid = false;
    return valid;
  }

  memcpy( tag, buff, sizeof tag );

  if ( strncmp( tag, HEADER, strlen( HEADER ) ) ) {
    free( buff );
    valid = false;
    return valid;
  }

  offset += sizeof tag;
  memcpy( &idStripe, buff + offset, sizeof idStripe );
  offset += sizeof idStripe;
  memcpy( &noBlocks, buff + offset, sizeof noBlocks );
  offset += sizeof noBlocks;
  memcpy ( &sizeLastBlock, buff + offset, sizeof sizeLastBlock );

  free( buff );
  valid = true;
  return valid;
}


/*----------------------------------------------------------------------------*/
//write the header to the file via XrdPosixXrootd
bool HeaderCRC::writeToFile( XrdCl::File* f )
{
  int offset = 0;
  char* buff = ( char* ) calloc( sizeHeader, sizeof( char ) );

  memcpy( buff + offset, HEADER, sizeof tag );
  offset += sizeof tag;
  memcpy( buff + offset, &idStripe, sizeof idStripe );
  offset += sizeof idStripe;
  memcpy( buff + offset, &noBlocks, sizeof noBlocks );
  offset += sizeof noBlocks;
  memcpy( buff + offset, &sizeLastBlock, sizeof sizeLastBlock );
  offset += sizeof sizeLastBlock;
  memset( buff + offset, 0, sizeHeader - offset );

  if ( !( f->Write( 0, sizeHeader, buff ).IsOK() ) ) {
    free( buff );
    valid = false;
    return valid;
  }

  free( buff );
  valid = true;
  return valid;
}


/*----------------------------------------------------------------------------*/
char*
HeaderCRC::getTag()
{
  return tag;
}

/*----------------------------------------------------------------------------*/
int
HeaderCRC::getSize() const
{
  return sizeHeader;
}

/*----------------------------------------------------------------------------*/
long int
HeaderCRC::getNoBlocks() const
{
  return noBlocks;
}

/*----------------------------------------------------------------------------*/
size_t
HeaderCRC::getSizeLastBlock() const
{
  return sizeLastBlock;
}

/*----------------------------------------------------------------------------*/
unsigned int
HeaderCRC::getIdStripe() const
{
  return idStripe;
}

/*----------------------------------------------------------------------------*/
void
HeaderCRC::setNoBlocks( long int nblocks )
{
  noBlocks = nblocks;
}

/*----------------------------------------------------------------------------*/
void
HeaderCRC::setSizeLastBlock( size_t sizelastblock )
{
  sizeLastBlock = sizelastblock;
}

/*----------------------------------------------------------------------------*/
void
HeaderCRC::setIdStripe( unsigned int idstripe )
{
  idStripe = idstripe;
}

/*----------------------------------------------------------------------------*/
bool
HeaderCRC::isValid() const
{
  return valid;
}

/*----------------------------------------------------------------------------*/
void
HeaderCRC::setState( bool state )
{
  valid = state;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END
