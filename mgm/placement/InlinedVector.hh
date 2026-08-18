//------------------------------------------------------------------------------
//! @file InlinedVector.hh
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Class InlinedVector - a tiny growable array with a fixed inline capacity: the
//! first N elements live in the object itself (on the stack for a local), and it
//! spills to a heap buffer only if it grows past N. It exists to keep the
//! per-bucket scratch of the hot placement paths off the allocator - a bucket
//! rarely holds more than a handful of items, so the common case never touches
//! the heap while a large bucket still works, just with one allocation.
//!
//! Storage is contiguous and begin()/end() hand out plain pointers, so it drops
//! into std::partial_sort, std::upper_bound and friends. It is deliberately
//! minimal: restricted to trivially copyable elements (so growth is a memcpy and
//! there is no per-element construction or destruction to track) and neither
//! copyable nor movable, since every use is a short-lived local.
//------------------------------------------------------------------------------
template <typename T, std::size_t N>
class InlinedVector {
  static_assert(std::is_trivially_copyable_v<T>,
                "InlinedVector only supports trivially copyable elements");

public:
  //----------------------------------------------------------------------------
  //! Constructor, starts empty backed by the inline storage
  //----------------------------------------------------------------------------
  InlinedVector()
      : mData(reinterpret_cast<T*>(mInline))
      , mSize(0)
      , mCap(N)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor, frees the spill buffer if one was taken
  //----------------------------------------------------------------------------
  ~InlinedVector()
  {
    if (OnHeap()) {
      ::operator delete[](mData);
    }
  }

  InlinedVector(const InlinedVector&) = delete;
  InlinedVector& operator=(const InlinedVector&) = delete;
  InlinedVector(InlinedVector&&) = delete;
  InlinedVector& operator=(InlinedVector&&) = delete;

  //----------------------------------------------------------------------------
  //! Reserve capacity for at least n elements, spilling to the heap if n > N
  //!
  //! @param n number of elements to make room for
  //----------------------------------------------------------------------------
  void
  reserve(std::size_t n)
  {
    if (n > mCap) {
      Grow(n);
    }
  }

  //----------------------------------------------------------------------------
  //! Append an element, growing the storage if the capacity is exhausted
  //!
  //! @param value element to append
  //----------------------------------------------------------------------------
  void
  push_back(const T& value)
  {
    if (mSize == mCap) {
      Grow(mSize + 1);
    }

    mData[mSize++] = value;
  }

  //----------------------------------------------------------------------------
  //! Drop the last element. Nothing to destroy, the elements are trivially
  //! copyable, so this only walks the size back.
  //----------------------------------------------------------------------------
  void
  pop_back()
  {
    if (mSize > 0) {
      --mSize;
    }
  }

  std::size_t
  size() const
  {
    return mSize;
  }
  bool
  empty() const
  {
    return mSize == 0;
  }
  T&
  operator[](std::size_t i)
  {
    return mData[i];
  }
  const T&
  operator[](std::size_t i) const
  {
    return mData[i];
  }
  T*
  data()
  {
    return mData;
  }
  const T*
  data() const
  {
    return mData;
  }
  T*
  begin()
  {
    return mData;
  }
  T*
  end()
  {
    return mData + mSize;
  }
  const T*
  begin() const
  {
    return mData;
  }
  const T*
  end() const
  {
    return mData + mSize;
  }

private:
  //----------------------------------------------------------------------------
  //! Check whether the storage currently lives on the heap
  //!
  //! @return true if spilled to the heap, otherwise false
  //----------------------------------------------------------------------------
  bool
  OnHeap() const
  {
    return mData != reinterpret_cast<const T*>(mInline);
  }

  //----------------------------------------------------------------------------
  //! Grow the storage to hold at least min_cap elements, doubling to keep
  //! push_back amortized O(1), and copy the existing elements over
  //!
  //! @param min_cap smallest capacity the caller needs
  //----------------------------------------------------------------------------
  void
  Grow(std::size_t min_cap)
  {
    std::size_t new_cap = mCap * 2;

    if (new_cap < min_cap) {
      new_cap = min_cap;
    }

    T* fresh = static_cast<T*>(::operator new[](new_cap * sizeof(T)));
    std::copy(mData, mData + mSize, fresh);

    if (OnHeap()) {
      ::operator delete[](mData);
    }

    mData = fresh;
    mCap = new_cap;
  }

  alignas(T) unsigned char mInline[N * sizeof(T)]; ///< Inline storage for N items
  T* mData;                                        ///< Active storage, inline or heap
  std::size_t mSize;                               ///< Number of elements held
  std::size_t mCap;                                ///< Current capacity
};

} // namespace eos::mgm::placement
