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
#include <type_traits>

namespace eos::common
{

namespace detail {

// A simple test for containers that expose a C::key_type
// Ideally std::remove_if requires a dereferenced iterator to be MoveAssignable
// ie *it = std::move(value) to be legal
// However for all assoc. containers this would be illegal as keys have
// pointer stability, so this will fail (with long compiler messages)
// A poor man's choice of tests for detecting associative containers is just
// testing for key_type which is kind of ok for std:: containers
template <typename, typename = void>
struct is_assoc_container_t : std::false_type {};

template <typename T>
struct is_assoc_container_t<T, std::void_t<typename T::key_type>>:
    std::true_type {};

template <typename T>
inline constexpr bool is_assoc_container_v = is_assoc_container_t<T>::value;

template <typename T>
inline constexpr bool can_remove_if =
    std::is_move_assignable<typename T::value_type>::value;
}

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

} // eos::common

