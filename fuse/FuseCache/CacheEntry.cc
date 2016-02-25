//------------------------------------------------------------------------------
// File: CachEntry.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
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
#include "LayoutWrapper.hh"
//------------------------------------------------------------------------------

size_t CacheEntry::msMaxSize = 4 * 1048576; //1MB=1048576 512KB=524288


//------------------------------------------------------------------------------
// Construct a block object which is to be saved in cache
//------------------------------------------------------------------------------
CacheEntry::CacheEntry (FileAbstraction*& fabst,
                        char* buf,
                        off_t off,
                        size_t len) :
eos::common::LogId(),
mParentFile (fabst),
mCapacity(msMaxSize),
mSizeData (len)
{
  char* ptr_buf = 0;
  off_t off_relative;

  if (len > msMaxSize)
  {
    eos_err("len=%ji should be smaller than msMaxSize=%ji", len, msMaxSize );
    exit(-1);
  }

  mOffsetStart = (off / msMaxSize) * msMaxSize;
  off_relative = off % msMaxSize;
  mBuffer = new char[mCapacity];
  ptr_buf = mBuffer + off_relative;
  ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
  mMapPieces.insert(std::make_pair(off, len));
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CacheEntry::~CacheEntry ()
{
  mParentFile = NULL;
  delete[] mBuffer;
}


//------------------------------------------------------------------------------
// Reinitialise the block attributes for the recycling process
//------------------------------------------------------------------------------
void
CacheEntry::DoRecycle (FileAbstraction*& file,
                       char* buf,
                       off_t off,
                       size_t len)
{
  char* ptr_buf;
  off_t off_relative;

  mParentFile = file;
  mOffsetStart = (off / msMaxSize) * msMaxSize;

  if (len > mCapacity)
  {
    eos_err("len=%ji should never be bigger than capacity=%ji", len, mCapacity);
    exit(-1);
  }

  mMapPieces.clear();
  mSizeData = len;
  off_relative = off % msMaxSize;
  ptr_buf = mBuffer + off_relative;
  ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
  mMapPieces.insert(std::make_pair(off, len));
}


//------------------------------------------------------------------------------
// Add a new pice of data to the block. The new piece can overlap with previous
// pieces existing the the block. In that case, the overlapping parts are
// overwritten. The map containg the piece is also updated by doing any necessary
// merging.
//------------------------------------------------------------------------------
size_t
CacheEntry::AddPiece (const char* buf, off_t off, size_t len)
{
  eos_debug("off=%ji, len=%ji", off, len);
  size_t size_added;
  size_t size_new;
  size_t size_erased = 0;
  off_t off_new;
  off_t off_old_end;
  off_t off_relative = off % msMaxSize;
  off_t off_piece_end = off + len;
  char* ptr_buf = mBuffer + off_relative;
  bool add_new_piece = false;

  std::map<off_t, size_t>::iterator iBefore;
  std::map<off_t, size_t>::reverse_iterator iReverse;
  std::map<off_t, size_t>::iterator iAfter = mMapPieces.lower_bound(off);

  if (iAfter != mMapPieces.end() && iAfter->first == off)
  {
    size_added = (iAfter->second >= len) ? 0 : (len - iAfter->second);
    std::map<off_t, size_t>::iterator iTmp;
    iTmp = iAfter;
    iTmp++;

    while ((iTmp != mMapPieces.end()) && (off_piece_end >= iTmp->first))
    {
      off_old_end = iTmp->first + iTmp->second;

      if (off_piece_end > off_old_end)
      {
        size_added -= iTmp->second;
        size_erased += iTmp->second;
        mMapPieces.erase(iTmp++);
      }
      else
      {
        size_added -= (off_piece_end - iTmp->first);
        size_erased += iTmp->second;
        mMapPieces.erase(iTmp++);
        break;
      }
    }

    ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
    iAfter->second += (size_added + size_erased);
    mSizeData += size_added;
  }
  else
  {
    if (iAfter == mMapPieces.begin())
    {
      // We only have pieces with bigger offset
      if (off_piece_end >= iAfter->first)
      {
        // Merge with next block
        off_old_end = iAfter->first + iAfter->second;

        if (off_piece_end > off_old_end)
        {
          // New block also longer then old block
          size_added = (iAfter->first - off) + (off_piece_end - off_old_end);
          size_erased += iAfter->second;
          mMapPieces.erase(iAfter++);

          while ((iAfter != mMapPieces.end()) &&
                 (off_piece_end >= iAfter->first))
          {
            off_old_end = iAfter->first + iAfter->second;

            if (off_piece_end > off_old_end)
            {
              size_added -= iAfter->second;
              size_erased += iAfter->second;
              mMapPieces.erase(iAfter++);
            }
            else
            {
              size_added -= (off_piece_end - iAfter->first);
              size_erased += iAfter->second;
              mMapPieces.erase(iAfter++);
              break;
            }
          }

          off_new = off;
          size_new = size_added + size_erased;
        }
        else
        {
          // New block shorter than old block
          size_added = iAfter->first - off;
          off_new = off;
          size_new = iAfter->second + size_added;
          mMapPieces.erase(iAfter);
        }

        ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
        mMapPieces.insert(std::make_pair(off_new, size_new));
        mSizeData += size_added;
      }
      else
        add_new_piece = true;
    }
    else if (iAfter == mMapPieces.end())
    {
      // We only have pieces with smaller offset
      iReverse = mMapPieces.rbegin();
      off_old_end = iReverse->first + iReverse->second;

      if (off_old_end >= off)
      {
        // Merge with previous block
        if (off_old_end >= off_piece_end)
        {
          // Just update the data, no off or len modification
          size_added = 0;
        }
        else
        {
          // Extend the current block at the end
          size_added = off_piece_end - off_old_end;
          iReverse->second += size_added;
        }

        ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
        mSizeData += size_added;
      }
      else
        add_new_piece = true;
    }
    else
    {
      // Not first, not last, and bigger than new block offset
      iBefore = iAfter;
      iBefore--;
      off_old_end = iBefore->first + iBefore->second;

      if (off_old_end >= off)
      {
        // Merge with previous block
        if (off_old_end >= off_piece_end)
        {
          // Just update the data, no off or len modification
          ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
          size_added = 0;
        }
        else
        {
          size_added = off_piece_end - off_old_end;

          if (off_piece_end >= iAfter->first)
          {
            // New block overlaps with iAfter block
            if (off_piece_end > (off_t) (iAfter->first + iAfter->second))
            {
              // New block spanns both old blocks and more
              size_added -= iAfter->second;
              size_erased = iAfter->second;
              mMapPieces.erase(iAfter++);

              while ((iAfter != mMapPieces.end()) &&
                     (off_piece_end >= iAfter->first))
              {
                off_old_end = iAfter->first + iAfter->second;

                if (off_piece_end > off_old_end)
                {
                  size_added -= iAfter->second;
                  size_erased += iAfter->second;
                  mMapPieces.erase(iAfter++);
                }
                else
                {
                  size_added -= (off_piece_end - iAfter->first);
                  size_erased += iAfter->second;
                  mMapPieces.erase(iAfter++);
                  break;
                }
              }

              iBefore->second += (size_added + size_erased);
            }
            else
            {
              //  New block spanns both old blocks but not more
              size_added -= (off_piece_end - iAfter->first);
              iBefore->second += (size_added + iAfter->second);
              mMapPieces.erase(iAfter);
            }
          }
          else
          {
            // New block does no overlap with iAfter block
            iBefore->second += size_added;
          }

          ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
          mSizeData += size_added;
        }
      }
      else if (off_piece_end >= iAfter->first)
      {
        // Merge with next block
        off_old_end = iAfter->first + iAfter->second;

        if (off_piece_end > off_old_end)
        {
          // New block bigger than iAfter block
          size_added = len - iAfter->second;
          size_erased = iAfter->second;
          mMapPieces.erase(iAfter++);

          while ((iAfter != mMapPieces.end()) &&
                 (off_piece_end >= iAfter->first))
          {
            off_old_end = iAfter->first + iAfter->second;

            if (off_piece_end > off_old_end)
            {
              size_added -= iAfter->second;
              size_erased += iAfter->second;
              mMapPieces.erase(iAfter++);
            }
            else
            {
              size_added -= (off_piece_end - iAfter->first);
              size_erased += iAfter->second;
              mMapPieces.erase(iAfter++);
              break;
            }
          }

          off_new = off;
          size_new = size_erased + size_added;
        }
        else
        {
          // New block shorter than iAfter block
          size_added = len - (off_piece_end - iAfter->first);
          off_new = off;
          size_new = iAfter->second + size_added;
          mMapPieces.erase(iAfter);
        }

        ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
        mMapPieces.insert(std::make_pair(off_new, size_new));
        mSizeData += size_added;
      }
      else
      {
        add_new_piece = true;
      }
    }
  }

  if (add_new_piece)
  {
    ptr_buf = static_cast<char*> (memcpy(ptr_buf, buf, len));
    mMapPieces.insert(std::make_pair(off, len));
    size_added = len;
    mSizeData += size_added;
  }

  mParentFile->IncrementWrites(size_added); 
  return size_added;
}


//------------------------------------------------------------------------------
// Write the whole part of the meaningful data to the corresponding file
//------------------------------------------------------------------------------
int64_t
CacheEntry::DoWrite ()
{
  int64_t ret = 0;
  off_t off_relative;
  auto iCurrent = mMapPieces.begin();
  const auto iEnd = mMapPieces.end();

  for (/*empty*/; iCurrent != iEnd; iCurrent++)
  {
    eos_debug("write cache piece off=%ji len=%ji, raw_file=%p",
             iCurrent->first, iCurrent->second, mParentFile->GetRawFile());
    off_relative = iCurrent->first % msMaxSize;
    // TODO: investigate using WriteAsync
    ret = mParentFile->GetRawFile()->Write(iCurrent->first,
                                           mBuffer + off_relative,
                                           iCurrent->second);

    if (ret == -1)
      eos_err("error while writing piece off=%ji, len=%ji",
              iCurrent->first, iCurrent->second);
  }

  return ret;
}


//------------------------------------------------------------------------------
// Get handler to data buffer
//------------------------------------------------------------------------------
char*
CacheEntry::GetDataBuffer ()
{
  return mBuffer;
}


//------------------------------------------------------------------------------
// Get parent file handler
//------------------------------------------------------------------------------

FileAbstraction*
CacheEntry::GetParentFile () const
{
  return mParentFile;
}


