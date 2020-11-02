//------------------------------------------------------------------------------
//! @file BufferManager.hh
//! @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include <memory>
#include <mutex>
#include <vector>
#include <list>
#include <atomic>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Buffer
//------------------------------------------------------------------------------
class Buffer
{
  friend class BufferManager;
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Buffer(uint64_t size):
    mCapacity(size),
    mData(size, '\0')
  {}

  //----------------------------------------------------------------------------
  //! Get pointer to underlying data
  //----------------------------------------------------------------------------
  inline char* GetDataPtr()
  {
    return &mData.front();
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Buffer() = default;

  uint64_t mCapacity; ///< Available size of the buffer
  uint64_t mLength; ///< Length of the useful data
  std::vector<char> mData; ///< Buffer holding the data
};


//------------------------------------------------------------------------------
//! Class BufferSlot
//------------------------------------------------------------------------------
class BufferSlot
{
  friend class BufferManager;
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param size size of buffers allocated by the current slot
  //----------------------------------------------------------------------------
  BufferSlot(uint64_t size):
    mBuffSize(size), mNumBuffers(0)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~BufferSlot()
  {
    std::unique_lock<std::mutex> lock(mSlotMutex);
    mAvailableBuffers.clear();
  }

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  BufferSlot& operator =(BufferSlot&& other) noexcept
  {
    if (this != &other) {
      mBuffSize = other.mBuffSize;
      mNumBuffers.store(other.mNumBuffers);
      mAvailableBuffers = other.mAvailableBuffers;
      other.mAvailableBuffers.clear();
    }

    return *this;
  }

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  BufferSlot(BufferSlot&& other) noexcept
  {
    *this = std::move(other);
  }

  //----------------------------------------------------------------------------
  //! Get buffer
  //----------------------------------------------------------------------------
  std::shared_ptr<Buffer> GetBuffer()
  {
    std::unique_lock<std::mutex> lock(mSlotMutex);

    if (!mAvailableBuffers.empty()) {
      auto buff =  mAvailableBuffers.front();
      mAvailableBuffers.pop_front();
      return buff;
    }

    ++mNumBuffers;
    return std::make_shared<Buffer>(mBuffSize);
  }

  //----------------------------------------------------------------------------
  //! Recycle buffer object
  //!
  //! @param buffer buffer object to be recycled
  //! @param keep true if buffer is to be saved otherwise false
  //----------------------------------------------------------------------------
  void Recycle(std::shared_ptr<Buffer> buffer, bool keep)
  {
    if (keep) {
      std::unique_lock<std::mutex> lock(mSlotMutex);
      mAvailableBuffers.push_back(buffer);
    } else {
      --mNumBuffers;
    }
  }

  //----------------------------------------------------------------------------
  //! Try to pop a buffer from the list of available ones if possible
  //----------------------------------------------------------------------------
  void Pop()
  {
    std::unique_lock<std::mutex> lock(mSlotMutex);

    if (!mAvailableBuffers.empty()) {
      mAvailableBuffers.pop_front();
      --mNumBuffers;
    }
  }

private:
  std::mutex mSlotMutex;
  std::list<std::shared_ptr<Buffer>> mAvailableBuffers;
  std::atomic<uint64_t> mNumBuffers;
  uint64_t mBuffSize;
};


//------------------------------------------------------------------------------
//! Class BufferManager
//------------------------------------------------------------------------------
class BufferManager: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_size maximum total size of allocated buffers
  //! @param slots number of slots for different buffer sizes which are power
  //!        of 2 and multiple of slot_base_size e.g. 1MB
  //!        slot 0 -> 1MB
  //!        slot 1 -> 2MB
  //!        slot 2 -> 4MB
  //! @param slot_base_sz size of the blocks in the first slot
  //----------------------------------------------------------------------------
  BufferManager(uint64_t max_size = 256 * 1024 * 1024 , uint32_t slots = 2,
                uint64_t slot_base_sz = 1024 * 1024):
    mMaxSize(max_size), mNumSlots(slots)
  {
    for (uint32_t i = 0u; i <= mNumSlots; ++i) {
      mSlots.emplace_back((1 << i) * slot_base_sz);
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~BufferManager() = default;

  //----------------------------------------------------------------------------
  //! Get buffer for the given length
  //!
  //! @param size minimum size for requested buffer
  //!
  //! @return buffer object
  //----------------------------------------------------------------------------
  std::shared_ptr<Buffer> GetBuffer(uint64_t size)
  {
    uint32_t slot {UINT32_MAX};

    // Find appropriate slot for the given size
    for (uint32_t i = 0; i <= mNumSlots; ++i) {
      if (size <= (1 << (i + 20))) {
        slot = i;
        break;
      }
    }

    // Can not provide a buffer big enough for the required size
    if (slot == UINT32_MAX) {
      return nullptr;
    }

    return mSlots[slot].GetBuffer();
  }

  //----------------------------------------------------------------------------
  //! Recycle buffer object
  //!
  //! @param buffer objec to be recycled
  //----------------------------------------------------------------------------
  void Recycle(std::shared_ptr<Buffer> buffer)
  {
    if (buffer == nullptr) {
      return;
    }

    uint32_t slot {UINT32_MAX};

    // Find appropriate slot for given buffer
    for (uint32_t i = 0; i <= mNumSlots; ++i) {
      if (buffer->mCapacity == (1 << (i + 20))) {
        slot = i;
        break;
      }
    }

    // Not a buffer allocated by us
    if (slot == UINT32_MAX) {
      return;
    }

    uint64_t total_size {0ull};
    auto sorted_slots = GetSortedSlotSizes(total_size);
    bool keep = (total_size <= mMaxSize);

    if (!keep) {
      eos_debug("msg=\"buffer pool is full\" max_size=%s",
                eos::common::StringConversion::GetPrettySize(mMaxSize).c_str());

      // Perform clean up for rest of slots depending on their size
      for (auto it = sorted_slots.rbegin(); it != sorted_slots.rend(); ++it) {
        if (it->first > slot) {
          mSlots[it->first].Pop();
          break;
        }

        if (it->first < slot) {
          // Free the equivalent of a block from the current slot
          int free_blocks = 1 << (slot - it->first);

          while (free_blocks) {
            mSlots[it->first].Pop();
            --free_blocks;
          }

          break;
        }
      }
    }

    mSlots[slot].Recycle(buffer, keep);
  }

  //----------------------------------------------------------------------------
  //! Get sorted distribution of slot sizes from smallest to biggest
  //!
  //! @param total_size compute the total size allocated so far
  //!
  //! @return sorted vector of pairs of slot ids and size of allocated buffers
  //!         for that corresponding slot
  //----------------------------------------------------------------------------
  std::vector< std::pair<uint32_t, uint64_t> >
  GetSortedSlotSizes(uint64_t& total_size) const
  {
    std::vector< std::pair<uint32_t, uint64_t> > elem;
    total_size = 0ull;

    for (uint32_t i = 0; i <= mNumSlots; ++i) {
      elem.push_back(std::make_pair(i,
                                    (mSlots[i].mNumBuffers * (1 << i) * 1024 * 1024)));
      total_size += elem.rbegin()->second;
    }

    auto comparator = [](std::pair<uint32_t, uint64_t> a,
    std::pair<uint32_t, uint64_t> b) {
      return (a.second < b.second);
    };
    std::sort(elem.begin(), elem.end(), comparator);
    return elem;
  }

  //----------------------------------------------------------------------------
  //! Get number of slots handled by the current buffer manager
  //----------------------------------------------------------------------------
  uint32_t GetNumSlots() const
  {
    return mNumSlots.load();
  }

  //----------------------------------------------------------------------------
  //! Get max size of buffers stored by buffer manager
  //----------------------------------------------------------------------------
  uint64_t GetMaxSize() const
  {
    return mMaxSize.load();
  }

private:
  std::atomic<uint64_t> mMaxSize;
  std::atomic<uint32_t> mNumSlots;
  std::vector<BufferSlot> mSlots;
};

EOSCOMMONNAMESPACE_END
