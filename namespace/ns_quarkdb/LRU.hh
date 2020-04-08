/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief LRU cache for namespace objects making sure we never evict an entry
//!        which is still referenced in other parts of the program.
//------------------------------------------------------------------------------

#ifndef __EOS_NS_LRU_HH__
#define __EOS_NS_LRU_HH__

#include "common/AssistedThread.hh"
#include "common/ConcurrentQueue.hh"
#include "common/Murmur3.hh"
#include "namespace/Namespace.hh"
#include <google/dense_hash_map>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Helper struct to test if EntryT implements the getId method. If EntryT
//! implements getId method then hasGetId::value will be true, otherwise false.
//! This struct is to be used in other template definitions.
//------------------------------------------------------------------------------
template <class EntryT>
struct hasGetId {
  template <typename C>
  static constexpr decltype(std::declval<C>().getId(), bool())
  test(int)
  {
    return true;
  }

  template <typename C>
  static constexpr bool
  test(...)
  {
    return false;
  }

  // int is used to give precedence!
  static constexpr bool value = test<EntryT>(int());
};

//------------------------------------------------------------------------------
//! LRU cache for namespace entries
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
class LRU
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param maxSize maximum number of entries in the cache
  //----------------------------------------------------------------------------
  LRU(std::uint64_t maxSize);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~LRU();

  //----------------------------------------------------------------------------
  //! Get entry
  //!
  //! @param id entry id
  //!
  //! @return shared ptr to requested object or nullptr if not found
  //----------------------------------------------------------------------------
  std::shared_ptr<EntryT> get(IdT id);

  //----------------------------------------------------------------------------
  //! Put entry
  //!
  //! @param id entry id
  //! @param entry entry object
  //!
  //! @return true if successfully added to the cache, false otherwise. If
  //!         cache is full then the least recently used entry is evicted
  //!         provided that it's not referenced anywhere else in the program.
  //----------------------------------------------------------------------------
  typename
  std::enable_if<hasGetId<EntryT>::value, std::shared_ptr<EntryT>>::type
      put(IdT id, std::shared_ptr<EntryT> obj);

  //----------------------------------------------------------------------------
  //! Remove entry from cache
  //!
  //! @param id entry id
  //!
  //! @return true if successfully removed from the cache, false otherwise
  //----------------------------------------------------------------------------
  bool remove(IdT id);

  //----------------------------------------------------------------------------
  //! Get cache size
  //!
  //! @return cache size
  //----------------------------------------------------------------------------
  inline std::uint64_t
  size() const
  {
    std::unique_lock<std::mutex> lock(mMutex);
    return mMap.size();
  }

  //----------------------------------------------------------------------------
  //! Get maximim number of entries in the cache
  //!
  //! @return maximum cache num entries
  //----------------------------------------------------------------------------
  inline std::uint64_t
  get_max_num() const
  {
    std::unique_lock<std::mutex> lock(mMutex);
    return mMaxNum;
  }

  //----------------------------------------------------------------------------
  //! Set max num entries
  //!
  //! @param max_num new maximum number of entries, if 0 then just drop the
  //!                the current cache
  //----------------------------------------------------------------------------
  inline void
  set_max_num(const std::uint64_t max_num)
  {
    std::unique_lock<std::mutex> lock(mMutex);

    if (max_num == 0ull) {
      // Flush and disable cache
      Purge(0.0);
      mMaxNum = 0ull;
    } else if (max_num == UINT64_MAX) {
      Purge(0.0); // Flush cache
    } else {
      mMaxNum = max_num;
    }
  }

  //----------------------------------------------------------------------------
  //! Forbid copying or moving LRU objects
  //----------------------------------------------------------------------------
  LRU(const LRU& other) = delete;
  LRU& operator=(const LRU& other) = delete;
  LRU(LRU&& other) = delete;
  LRU& operator=(LRU&& other) = delete;

private:

  //----------------------------------------------------------------------------
  //! Cleaner job taking care of deallocating entries that are passed through
  //! the queue to delete
  //----------------------------------------------------------------------------
  void CleanerJob(ThreadAssistant& assistant);

  //----------------------------------------------------------------------------
  //! Purge entries until stop ratio is achieved
  //!
  //! @param stop_ratio stop purge ratio
  //! @note This method must be called with the mutex protecting the map and
  //! the list locked.
  //----------------------------------------------------------------------------
  void Purge(double stop_ratio);

  //! Percentage at which the cache purging stops
  static constexpr double sPurgeStopRatio = 0.9;
  using ListT = std::list<std::shared_ptr<EntryT>>;
  typename std::list<std::shared_ptr<EntryT>>::iterator ListIterT;
  //using MapT = std::map<IdT, decltype(ListIterT)>;
  using MapT = google::dense_hash_map<IdT, decltype(ListIterT),
        Murmur3::MurmurHasher<IdT>>;
  MapT mMap;   ///< Internal map pointing to obj in list
  ListT mList; ///< Internal list of objects where new/used objects are at the
  ///< end of the list
  //! Mutext to protect access to the map and list
  mutable std::mutex mMutex;
  std::uint64_t mMaxNum; ///< Maximum number of entries
  eos::common::ConcurrentQueue< std::shared_ptr<EntryT> > mToDelete;
  AssistedThread mCleanerThread; ///< Thread doing the deallocations
};

// Definition of class static member
template <typename IdT, typename EntryT>
constexpr double LRU<IdT, EntryT>::sPurgeStopRatio;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
LRU<IdT, EntryT>::LRU(std::uint64_t max_num) :
  mMap(), mList(), mMutex(), mMaxNum(max_num), mToDelete()
{
  mMap.set_empty_key(IdT(UINT64_MAX - 1));
  mMap.set_deleted_key(IdT(UINT64_MAX));
  mCleanerThread.reset(&LRU::CleanerJob, this);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
LRU<IdT, EntryT>::~LRU()
{
  std::shared_ptr<EntryT> sentinel(nullptr);
  mCleanerThread.stop();
  mToDelete.push(sentinel);
  mCleanerThread.join();
  std::unique_lock<std::mutex> lock(mMutex);
  mMap.clear();
  mList.clear();
}

//------------------------------------------------------------------------------
// Get object
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
std::shared_ptr<EntryT>
LRU<IdT, EntryT>::get(IdT id)
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto iter_map = mMap.find(id);

  if (iter_map == mMap.end()) {
    return nullptr;
  }

  // Move object to the end of the list i.e. recently accessed
  auto iter_new = mList.insert(mList.end(), *iter_map->second);
  mList.erase(iter_map->second);
  iter_map->second = iter_new;
  return *iter_new;
}

//------------------------------------------------------------------------------
// Put object
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
typename std::enable_if<hasGetId<EntryT>::value, std::shared_ptr<EntryT>>::type
    LRU<IdT, EntryT>::put(IdT id, std::shared_ptr<EntryT> obj)
{
  std::unique_lock<std::mutex> lock(mMutex);

  if (mMaxNum == 0ull) {
    return obj;
  }

  auto iter_map = mMap.find(id);

  if (iter_map != mMap.end()) {
    return *(iter_map->second);
  }

  // Check if map full and purge some entries if necessary 10% of max size
  if (mMap.size() >= mMaxNum) {
    Purge(sPurgeStopRatio);
  }

  // @todo (esindril): add time based and for a fixed number of entries purging
  auto iter = mList.insert(mList.end(), obj);
  mMap[id] = iter;
  return *iter;
}

//------------------------------------------------------------------------------
// Remove object
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
bool
LRU<IdT, EntryT>::remove(IdT id)
{
  std::unique_lock<std::mutex> lock(mMutex);
  auto iter_map = mMap.find(id);

  if (iter_map == mMap.end()) {
    return false;
  }

  (void)mList.erase(iter_map->second);
  mMap.erase(iter_map);
  return true;
}

//----------------------------------------------------------------------------
// Cleaner job taking care of deallocating entries that are passed through
// the queue to delete
//----------------------------------------------------------------------------
template <typename IdT, typename EntryT>
void
LRU<IdT, EntryT>::CleanerJob(ThreadAssistant& assistant)
{
  std::shared_ptr<EntryT> tmp;

  while (!assistant.terminationRequested()) {
    while (true) {
      mToDelete.wait_pop(tmp);

      if (tmp == nullptr) {
        break;
      } else {
        tmp.reset();
      }
    }
  }
}

//------------------------------------------------------------------------------
// Purge entries until stop ratio is achieved
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
void
LRU<IdT, EntryT>::Purge(double stop_ratio)
{
  auto iter = mList.begin();

  while ((iter != mList.end()) &&
         (mMap.size() > stop_ratio * mMaxNum)) {
    // If object is referenced also by someone else then skip it
    if (iter->use_count() > 1) {
      ++iter;
      continue;
    }

    mMap.erase(IdT((*iter)->getId()));
    mToDelete.push(*iter);
    iter = mList.erase(iter);
  }

  mMap.resize(0); // compact after deletion
}

EOSNSNAMESPACE_END

#endif // __EOS_NS_LRU_HH__
