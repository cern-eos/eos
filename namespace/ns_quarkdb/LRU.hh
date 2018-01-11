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

#ifndef __EOS_NS_REDIS_LRU_HH__
#define __EOS_NS_REDIS_LRU_HH__

#include "common/RWMutex.hh"
#include "namespace/Namespace.hh"
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
    eos::common::RWMutexWriteLock lock_w(mMutex);
    return mMap.size();
  }

  //----------------------------------------------------------------------------
  //! Set max size
  //!
  //! @param max_size new maximum number of entries
  //----------------------------------------------------------------------------
  inline void
  set_max_size(const std::uint64_t max_size)
  {
    eos::common::RWMutexWriteLock lock_w(mMutex);
    mMaxSize = max_size;
  }

private:
  //! Percentage at which the cache purging stops
  static constexpr double sPurgeStopRatio = 0.9;

  //! Forbid copying or moving LRU objects
  LRU(const LRU& other) = delete;
  LRU& operator=(const LRU& other) = delete;
  LRU(LRU&& other) = delete;
  LRU& operator=(LRU&& other) = delete;

  using ListT = std::list<std::shared_ptr<EntryT>>;
  typename std::list<std::shared_ptr<EntryT>>::iterator ListIterT;
  using MapT = std::map<IdT, decltype(ListIterT)>;
  MapT mMap;   ///< Internal map pointing to obj in list
  ListT mList; ///< Internal list of objects where new/used objects are at the
  ///< end of the list
  // TODO: in C++17 use std::shared_mutex
  //! Mutext to protect access to the map and list which is set to blocking
  // mutable eos::common::RWMutex mMutex;
  mutable eos::common::RWMutex mMutex;
  std::uint64_t mMaxSize; ///< Maximum number of entries
};

// Definition of class static member
template <typename IdT, typename EntryT>
constexpr double LRU<IdT, EntryT>::sPurgeStopRatio;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
LRU<IdT, EntryT>::LRU(std::uint64_t max_size) : mMutex(), mMaxSize(max_size)
{
  mMutex.SetBlocking(true);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
LRU<IdT, EntryT>::~LRU()
{
  eos::common::RWMutexWriteLock lock_w(mMutex);
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
  eos::common::RWMutexWriteLock lock_w(mMutex);
  auto iter_map = mMap.find(id);

  if (iter_map == mMap.end()) {
    return nullptr;
  }

  // Move object to the end of the list i.e. recently accessed
  auto iter_new = mList.insert(mList.end(), *iter_map->second);
  mList.erase(iter_map->second);
  mMap[id] = iter_new;
  return *iter_new;
}

//------------------------------------------------------------------------------
// Put object
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
typename std::enable_if<hasGetId<EntryT>::value, std::shared_ptr<EntryT>>::type
    LRU<IdT, EntryT>::put(IdT id, std::shared_ptr<EntryT> obj)
{
  eos::common::RWMutexWriteLock lock_w(mMutex);
  auto iter_map = mMap.find(id);

  if (iter_map != mMap.end()) {
    return *(iter_map->second);
  }

  // Check if map full and purge some entries if necessary 10% of max size
  if (mMap.size() >= mMaxSize) {
    auto iter = mList.begin();

    while ((iter != mList.end()) &&
           (mMap.size() > sPurgeStopRatio * mMaxSize)) {
      // If object is referenced also by someone else then skip it
      if (iter->use_count() > 1) {
        ++iter;
        continue;
      }

      mMap.erase((*iter)->getId());
      iter = mList.erase(iter);
    }
  }

  auto iter = mList.insert(mList.end(), obj);
  mMap.emplace(id, iter);
  return *iter;
}

//------------------------------------------------------------------------------
// Remove object
//------------------------------------------------------------------------------
template <typename IdT, typename EntryT>
bool
LRU<IdT, EntryT>::remove(IdT id)
{
  eos::common::RWMutexWriteLock lock_w(mMutex);
  auto iter_map = mMap.find(id);

  if (iter_map == mMap.end()) {
    return false;
  }

  (void)mList.erase(iter_map->second);
  mMap.erase(iter_map);
  return true;
}

EOSNSNAMESPACE_END

#endif // __EOS_NS_REDIS_LRU_HH__
