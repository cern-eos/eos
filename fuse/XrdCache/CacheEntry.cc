//------------------------------------------------------------------------------
// File: CachEntry.cc
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

//------------------------------------------------------------------------------
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

size_t CacheEntry::msMaxSize = 4 * 1048576;          //1MB=1048576 512KB=524288


//------------------------------------------------------------------------------
// Construct a block object which is to be saved in cache
//------------------------------------------------------------------------------
CacheEntry::CacheEntry( XrdCl::File*&    rpFile,
                        char*            buf,
                        off_t            off,
                        size_t           len,
                        FileAbstraction& rFileAbst,
                        bool             isWr ):
  mpFile( rpFile ),
  mIsWrType( isWr ),
  mSizeData( len ),
  pParentFile( &rFileAbst )
{
  char* pBuffer;
  off_t off_relative;

  if ( len > GetMaxSize() ) {
    fprintf( stderr, "error=len should be smaller than GetMaxSize()\n" );
    exit( -1 );
  }

  mCapacity = GetMaxSize();
  mOffsetStart = ( off / GetMaxSize() ) * GetMaxSize();
  off_relative = off % GetMaxSize();
  mpBuffer = static_cast<char*>( calloc( mCapacity, sizeof( char ) ) );
  pBuffer = mpBuffer + off_relative;
  pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
  mMapPieces.insert( std::make_pair( off, len ) );
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CacheEntry::~CacheEntry()
{
  mpFile = NULL;
  free( mpBuffer );
}


//------------------------------------------------------------------------------
// Reinitialise the block attributes for the recycling process
//------------------------------------------------------------------------------
void
CacheEntry::DoRecycle( XrdCl::File*&    rpFile,
                       char*            buf,
                       off_t            off,
                       size_t           len,
                       FileAbstraction& rFileAbst,
                       bool             isWr )
{
  char* pBuffer;
  off_t off_relative;

  mpFile = rpFile;
  mIsWrType = isWr;
  mOffsetStart = ( off / GetMaxSize() ) * GetMaxSize();
  pParentFile = &rFileAbst;

  if ( len > mCapacity ) {
    fprintf( stderr, "error=len should never be bigger than capacity.\n" );
    exit( -1 );
  }

  mMapPieces.clear();
  mSizeData = len;
  off_relative = off % GetMaxSize();
  pBuffer = mpBuffer + off_relative;
  pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
  mMapPieces.insert( std::make_pair( off, len ) );
}


//------------------------------------------------------------------------------
// Add a new pice of data to the block. The new piece can overlap with previous
// pieces existing the the block. In that case, the overlapping parts are
// overwritten. The map containg the piece is also updated by doing any necessary
// merging.
//------------------------------------------------------------------------------
size_t
CacheEntry::AddPiece( const char* buf, off_t off, size_t len )
{
  off_t off_new;
  size_t size_added;
  size_t size_new;
  size_t size_erased = 0;
  off_t off_old_end;
  off_t off_relative = off % GetMaxSize();
  off_t off_piece_end = off + len;
  char* pBuffer = mpBuffer + off_relative;
  bool add_new_piece = false;

  std::map<off_t, size_t>::iterator iBefore;
  std::map<off_t, size_t>::reverse_iterator iReverse;
  std::map<off_t, size_t>::iterator iAfter = mMapPieces.lower_bound( off );

  if ( iAfter->first == off ) {
    size_added = ( iAfter->second >= len ) ? 0 : ( len - iAfter->second );
    std::map<off_t, size_t>::iterator iTmp;
    iTmp = iAfter;
    iTmp++;

    while ( ( iTmp != mMapPieces.end() ) && ( off_piece_end >= iTmp->first ) ) {
      off_old_end = iTmp->first + iTmp->second;

      if ( off_piece_end > off_old_end ) {
        size_added -= iTmp->second;
        size_erased += iTmp->second;
        mMapPieces.erase( iTmp++ );
      } else {
        size_added -= ( off_piece_end - iTmp->first );
        size_erased += iTmp->second;
        mMapPieces.erase( iTmp++ );
        break;
      }
    }

    pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
    iAfter->second += ( size_added + size_erased );
    mSizeData += size_added;
  } else {
    if ( iAfter == mMapPieces.begin() ) {
      // We only have pieces with bigger offset
      if ( off_piece_end >= iAfter->first ) {
        // Merge with next block
        off_old_end = iAfter->first + iAfter->second;

        if ( off_piece_end > off_old_end ) {
          // New block also longer then old block
          size_added = ( iAfter->first - off ) + ( off_piece_end - off_old_end ) ;
          size_erased += iAfter->second;
          mMapPieces.erase( iAfter++ );

          while ( ( iAfter != mMapPieces.end() ) &&
                  ( off_piece_end >= iAfter->first ) ) {
            off_old_end = iAfter->first + iAfter->second;

            if ( off_piece_end > off_old_end ) {
              size_added -= iAfter->second;
              size_erased += iAfter->second;
              mMapPieces.erase( iAfter++ );
            } else {
              size_added -= ( off_piece_end - iAfter->first );
              size_erased += iAfter->second;
              mMapPieces.erase( iAfter++ );
              break;
            }
          }

          off_new = off;
          size_new = size_added + size_erased;
        } else {
          // New block shorter than old block
          size_added = iAfter->first - off;
          off_new = off;
          size_new = iAfter->second + size_added;
          mMapPieces.erase( iAfter );
        }

        pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
        mMapPieces.insert( std::make_pair( off_new, size_new ) );
        mSizeData += size_added;
      } else {
        add_new_piece = true;
      }
    } else if ( iAfter == mMapPieces.end() ) {
      // We only have pieces with smaller offset
      iReverse = mMapPieces.rbegin();
      off_old_end = iReverse->first + iReverse->second;

      if ( off_old_end >= off ) {
        // Merge with previous block
        if ( off_old_end >= off_piece_end ) {
          // Just update the data, no off or len modification
          size_added = 0;
        } else {
          // Extend the current block at the end
          size_added = off_piece_end - off_old_end;
          iReverse->second += size_added;
        }

        pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
        mSizeData += size_added;
      } else {
        add_new_piece = true;
      }
    } else {
      // Not first, not last, and bigger than new block offset
      iBefore = iAfter;
      iBefore--;
      off_old_end = iBefore->first + iBefore->second;

      if ( off_old_end >= off ) {
        // Merge with previous block
        if ( off_old_end >= off_piece_end ) {
          // Just update the data, no off or len modification
          pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
          size_added = 0;
        } else {
          size_added = off_piece_end - off_old_end;

          if ( off_piece_end >= iAfter->first ) {
            // New block overlaps with iAfter block
            if ( off_piece_end > ( off_t )( iAfter->first + iAfter->second ) ) {
              // New block spanns both old blocks and more
              size_added -= iAfter->second;
              size_erased = iAfter->second;
              mMapPieces.erase( iAfter++ );

              while ( ( iAfter != mMapPieces.end() ) &&
                      ( off_piece_end >= iAfter->first ) ) {
                off_old_end = iAfter->first + iAfter->second;

                if ( off_piece_end > off_old_end ) {
                  size_added -= iAfter->second;
                  size_erased += iAfter->second;
                  mMapPieces.erase( iAfter++ );
                } else {
                  size_added -= ( off_piece_end - iAfter->first );
                  size_erased += iAfter->second;
                  mMapPieces.erase( iAfter++ );
                  break;
                }
              }

              iBefore->second += ( size_added + size_erased );
            } else {
              //  New block spanns both old blocks but not more
              size_added -= ( off_piece_end - iAfter->first );
              iBefore->second += ( size_added + iAfter->second );
              mMapPieces.erase( iAfter );
            }
          } else {
            // New block does no overlap with iAfter block
            iBefore->second += size_added;
          }

          pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
          mSizeData += size_added;
        }
      } else if ( off_piece_end >= iAfter->first ) {
        // Merge with next block
        off_old_end = iAfter->first + iAfter->second;

        if ( off_piece_end > off_old_end ) {
          // New block bigger than iAfter block
          size_added = len - iAfter->second;
          size_erased = iAfter->second;
          mMapPieces.erase( iAfter++ );

          while ( ( iAfter != mMapPieces.end() ) &&
                  ( off_piece_end >= iAfter->first ) ) {
            off_old_end = iAfter->first + iAfter->second;

            if ( off_piece_end > off_old_end ) {
              size_added -= iAfter->second;
              size_erased += iAfter->second;
              mMapPieces.erase( iAfter++ );
            } else {
              size_added -= ( off_piece_end - iAfter->first );
              size_erased += iAfter->second;
              mMapPieces.erase( iAfter++ );
              break;
            }
          }

          off_new = off;
          size_new = size_erased + size_added;
        } else {
          // New block shorter than iAfter block
          size_added = len - ( off_piece_end - iAfter->first );
          off_new = off;
          size_new = iAfter->second + size_added;
          mMapPieces.erase( iAfter );
        }

        pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
        mMapPieces.insert( std::make_pair( off_new, size_new ) );
        mSizeData += size_added;
      } else {
        add_new_piece = true;
      }
    }
  }

  if ( add_new_piece ) {
    pBuffer = static_cast<char*>( memcpy( pBuffer, buf, len ) );
    mMapPieces.insert( std::make_pair( off, len ) );
    size_added = len;
    mSizeData += size_added;
  }

  return size_added;
}



//------------------------------------------------------------------------------
// Try to get a piece from the current block
//------------------------------------------------------------------------------
bool
CacheEntry::GetPiece( char* buf, off_t off, size_t len )
{
  bool found = false;
  off_t off_relative = off % GetMaxSize();
  std::map<off_t, size_t>::reverse_iterator iReverse;
  std::map<off_t, size_t>::iterator i = mMapPieces.lower_bound( off );

  if ( i->first == off ) {
    // Exact match
    if ( i->second >= len ) {
      buf = static_cast<char*>( memcpy( buf, mpBuffer + off_relative, len ) );
      found = true;
    } else {
      found = false;
    }
  } else {
    if ( i == mMapPieces.begin() ) {
      found = false;
    } else if ( i == mMapPieces.end() ) {
      iReverse = mMapPieces.rbegin();

      if ( ( iReverse->first <= off ) &&
           ( static_cast<off_t>( ( iReverse->first + iReverse->second ) ) > off ) &&
           ( iReverse->first + iReverse->second >= off + len ) ) {
        found = true;
        buf = static_cast<char*>( memcpy( buf, mpBuffer + off_relative, len ) );
      } else {
        found = false;
      }
    } else {
      i--;

      if ( ( i->first <= off ) &&
           ( static_cast<off_t>( ( i->first + i->second ) ) > off ) &&
           ( i->first + i->second >= off + len ) ) {
        found = true;
        buf = static_cast<char*>( memcpy( buf, mpBuffer + off_relative, len ) );
      } else {
        found = false;
      }
    }
  }

  return found;
}


//------------------------------------------------------------------------------
// Write the whole part of the meaningful data to the corresponding file
//------------------------------------------------------------------------------
int
CacheEntry::DoWrite()
{
  int retc = 0;
  off_t off_relative;
  std::map<off_t, size_t>::iterator iCurrent = mMapPieces.begin();
  const std::map<off_t, size_t>::iterator iEnd = mMapPieces.end();

  for ( ; iCurrent != iEnd; iCurrent++ ) {
    off_relative = iCurrent->first % GetMaxSize();
    XrdCl::XRootDStatus status =
      mpFile->Write( iCurrent->first, iCurrent->second, mpBuffer + off_relative );

    if ( status.IsOK() ) {
      retc = iCurrent->second;
    } else {
      fprintf( stderr, "\n[%s] error=error while writing using XrdCl::File\n\n",
               __FUNCTION__ );
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Test if block is for writing
//------------------------------------------------------------------------------
bool
CacheEntry::IsWr()
{
  return mIsWrType;
}


//------------------------------------------------------------------------------
// Test if block is full
//------------------------------------------------------------------------------
bool
CacheEntry::IsFull()
{
  return ( mCapacity == mSizeData );
}


//------------------------------------------------------------------------------
// Get handler to data buffer
//------------------------------------------------------------------------------
char*
CacheEntry::GetDataBuffer()
{
  return mpBuffer;
}


//------------------------------------------------------------------------------
// Get capacity of the block
//------------------------------------------------------------------------------
size_t
CacheEntry::GetCapacity() const
{
  return mCapacity;
}


//------------------------------------------------------------------------------
// Get size of the meaningful data currently in the block
//------------------------------------------------------------------------------
size_t
CacheEntry::GetSizeData() const
{
  return mSizeData;
}


//------------------------------------------------------------------------------
// Get absolute value of block start offset in the file
//------------------------------------------------------------------------------
off_t
CacheEntry::GetOffsetStart() const
{
  return mOffsetStart;
}


//------------------------------------------------------------------------------
// Get absolute value of the block end offset in the file
//------------------------------------------------------------------------------
off_t
CacheEntry::GetOffsetEnd() const
{
  return ( mOffsetStart + mCapacity );
}


//------------------------------------------------------------------------------
// Get parent file handler
//------------------------------------------------------------------------------
FileAbstraction*
CacheEntry::GetParentFile() const
{
  return pParentFile;
}


