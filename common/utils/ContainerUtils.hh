//------------------------------------------------------------------------------
// File: ContainerUtils.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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
#include "common/utils/TypeTraits.hh"
#include <cstdint>
#include <iterator>

namespace eos::common
{


//----------------------------------------------------------------------------
//! erase_if that erases elements in place for elements matching a predicate
//! This is useful in erase remove idiom useful for assoc. containers where
//! std::remove_if will not compile, almost borrowed from erase_if C++ ref page
//!
//! @param C the associative container, elements will be removed in place
//! @param pred the predicate to evaluate, please note the container
//!             value_type aka the pair of values will be the input for the
//!             predicate
//! @return the no of elements removed
//! Usage eg:
//!   eos::common::erase_if(m, [](const auto& p){ return p.first % 2 == 0;})
//----------------------------------------------------------------------------
template <typename C, typename Pred>
typename C::size_type
erase_if(C& c, Pred pred)
{
  // TODO: (abhi) sfinae for normal overloads where you can just erase
  //  (remove_if) for container types like vector/lists
  static_assert(detail::is_assoc_container_v<C>,
                "This method is only implemented for assoc. containers just "
                "use std::erase(std::remove_if(C,pred)) instead");
  auto init_sz = c.size();

  for (auto it = c.begin(), last = c.end(); it != last;) {
    if (pred(*it)) {
      it = c.erase(it);
    } else {
      ++it;
    }
  }

  return init_sz - c.size();
}

inline constexpr uint64_t clamp_index(uint64_t index, uint64_t size)
{
  return index >= size ? (index % size) : index;
}

//----------------------------------------------------------------------------
//! A simple Round Robin like picker for a container, for indices past the size
//! we simply wrap around giving a feel of circular iterator
//! NOTE: In case of an empty container this function raises out_of_range
//! exception, so please ensure container is not empty!
//!
//! @param C Container to pick from
//! @param index
//! @return item at index
//! Usage eg:
//!   vector<int> v {1,2,3};
//!   pickIndexRR(v,3) -> 1 (v,4)->2 (v,5) -> 3  ...
//----------------------------------------------------------------------------
template <typename C>
typename C::value_type
pickIndexRR(const C& c, uint64_t index)
{
  if (!c.size()) {
    throw std::out_of_range("Empty Container!");
  }

  auto iter = c.begin();
  std::advance(iter, clamp_index(index, c.size()));
  return *iter;
}

//----------------------------------------------------------------------------
//! Transfer a container onto another, this variants destructively move values
//! from other container onto source container at a given pos.
//! \tparam C container type -  will be inferred
//! \param c container where other container will be spliced onto
//! \param other container whose elements will be consumed
//! \param pos position where we need to splice
//----------------------------------------------------------------------------
template <typename C>
void
splice(C& c, C&& other,
       typename C::const_iterator pos)
{
  c.insert(pos,
           std::make_move_iterator(other.begin()),
           std::make_move_iterator(other.end()));
}

//----------------------------------------------------------------------------
//! Transfer a container onto another at the end, this variants destructively move values
//! from other container onto source container at a given pos.
//! \tparam C container type -  will be inferred
//! \param c container where other container will be spliced onto
//! \param other container whose elements will be consumed
//----------------------------------------------------------------------------
template <typename C>
void splice(C& c, C&& other)
{
  splice(c, std::move(other), c.end());
}

} // eos::common

