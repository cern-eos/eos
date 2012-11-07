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

char HeaderCRC::msTagName[] = "_HEADER__RAIDIO_";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
HeaderCRC::HeaderCRC( int sizeHeader, int sizeBlock ):
  mValid( false ),
  mNumBlocks( -1 ),
  mIdStripe( -1 ),
  mSizeLastBlock( -1 ),
  mSizeBlock( sizeBlock ),
  mSizeHeader( sizeHeader )
  
{
  //empty
}


//------------------------------------------------------------------------------
// Constructor with parameter
//------------------------------------------------------------------------------
HeaderCRC::HeaderCRC( int sizeHeader, long long numBlocks, int sizeBlock ):
  mValid( false ),
  mNumBlocks( numBlocks ),
  mIdStripe( -1 ),
  mSizeLastBlock( -1 ),
  mSizeBlock( sizeBlock ),
  mSizeHeader( sizeHeader )
{
  strncpy( mTag, msTagName, strlen( msTagName ) );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
HeaderCRC::~HeaderCRC()
{
  //empty
}


//------------------------------------------------------------------------------
// Read header from XrdCl file
//------------------------------------------------------------------------------
bool
HeaderCRC::ReadFromFile( XrdCl::File*& pFile )
{
  uint32_t ret;
  long int offset = 0;
  size_t read_sizeblock = 0;
  
  char* buff = static_cast< char* >( calloc( mSizeHeader, sizeof( char ) ) );

  if ( !( pFile->Read( offset, mSizeHeader, buff, ret ).IsOK() )
       || ( ret != static_cast< uint32_t >( mSizeHeader ) ) ) {
    free( buff );
    mValid = false;
    return mValid;
  }

  memcpy( mTag, buff, sizeof mTag );

  if ( strncmp( mTag, msTagName, strlen( msTagName ) ) ) {
    free( buff );
    mValid = false;
    return mValid;
  }

  offset += sizeof mTag;
  memcpy( &mIdStripe, buff + offset, sizeof mIdStripe );
  offset += sizeof mIdStripe;
  memcpy( &mNumBlocks, buff + offset, sizeof mNumBlocks );
  offset += sizeof mNumBlocks;
  memcpy( &mSizeLastBlock, buff + offset, sizeof mSizeLastBlock );
  offset += sizeof mSizeLastBlock;
  memcpy( &read_sizeblock, buff + offset, sizeof read_sizeblock );

  if ( mSizeBlock != read_sizeblock ) {
    eos_err( "error=block size read from file does not match block size expected" );
  }
  
  free( buff );
  mValid = true;
  return mValid;
}


//------------------------------------------------------------------------------
// Read header from generic file
//------------------------------------------------------------------------------
bool
HeaderCRC::ReadFromFile( FileIo*& pFile )
{
  long int offset = 0;
  size_t read_sizeblock = 0;
    
  char* buff = static_cast< char* >( calloc( mSizeHeader, sizeof( char ) ) );

  if ( pFile->Read( offset, buff, mSizeHeader ) !=
       static_cast<uint32_t>( mSizeHeader ) )
  {
    free( buff );
    mValid = false;
    return mValid;
  }

  memcpy( mTag, buff, sizeof mTag );
  std::string tag = mTag;

  if ( strncmp( mTag, msTagName, strlen( msTagName ) ) ) {
    free( buff );
    mValid = false;
    return mValid;
  }

  offset += sizeof mTag;
  memcpy( &mIdStripe, buff + offset, sizeof mIdStripe );
  offset += sizeof mIdStripe;
  memcpy( &mNumBlocks, buff + offset, sizeof mNumBlocks );
  offset += sizeof mNumBlocks;
  memcpy( &mSizeLastBlock, buff + offset, sizeof mSizeLastBlock );
  offset += sizeof mSizeLastBlock;
  memcpy( &read_sizeblock, buff + offset, sizeof read_sizeblock );

  if ( mSizeBlock != read_sizeblock ) {
    eos_err( "error=block size read from file does not match block size expected" );
  }

  free( buff );
  mValid = true;
  return mValid;
}



//------------------------------------------------------------------------------
// Write header to XrdCl file
//------------------------------------------------------------------------------
bool
HeaderCRC::WriteToFile( XrdCl::File*& pFile )
{
  int offset = 0;
  char* buff = static_cast< char* >( calloc( mSizeHeader, sizeof( char ) ) );

  memcpy( buff + offset, msTagName, sizeof msTagName );
  offset += sizeof mTag;
  memcpy( buff + offset, &mIdStripe, sizeof mIdStripe );
  offset += sizeof mIdStripe;
  memcpy( buff + offset, &mNumBlocks, sizeof mNumBlocks );
  offset += sizeof mNumBlocks;
  memcpy( buff + offset, &mSizeLastBlock, sizeof mSizeLastBlock );
  offset += sizeof mSizeLastBlock;
  memcpy( buff + offset, &mSizeBlock, sizeof mSizeBlock );
  offset += sizeof mSizeBlock;
  memset( buff + offset, 0, mSizeHeader - offset );

  if ( !( pFile->Write( 0, mSizeHeader, buff ).IsOK() ) ) {
    mValid = false;
  } else {
    mValid = true;
  }

  free( buff );
  return mValid;
}


//------------------------------------------------------------------------------
// Write header to generic file
//------------------------------------------------------------------------------
bool
HeaderCRC::WriteToFile( FileIo*& pFile )
{
  int offset = 0;
  char* buff = static_cast< char* >( calloc( mSizeHeader, sizeof( char ) ) );
  
  memcpy( buff + offset, msTagName, sizeof msTagName );
  offset += sizeof mTag;
  memcpy( buff + offset, &mIdStripe, sizeof mIdStripe );
  offset += sizeof mIdStripe;
  memcpy( buff + offset, &mNumBlocks, sizeof mNumBlocks );
  offset += sizeof mNumBlocks;
  memcpy( buff + offset, &mSizeLastBlock, sizeof mSizeLastBlock );
  offset += sizeof mSizeLastBlock;
  memcpy( buff + offset, &mSizeBlock, sizeof mSizeBlock );
  offset += sizeof mSizeBlock;
  memset( buff + offset, 0, mSizeHeader - offset );

  if ( pFile->Write( 0, buff, mSizeHeader ) < 0 ) {
    mValid = false;
  } else {
    mValid = true;
  }

  free( buff );
  return mValid;
}


EOSFSTNAMESPACE_END
