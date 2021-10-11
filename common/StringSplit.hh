// ----------------------------------------------------------------------
// File: StringSplit.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include <string_view>
#include <type_traits>
#include <algorithm>
#include <vector>

namespace eos::common {
namespace detail {
// A simple type checker to decide between types having a (const) iterator
template <typename T, typename = void>
struct has_const_iter : std::false_type {};

template <typename T>
struct has_const_iter<T, std::void_t<decltype(std::declval<T>().cbegin(),
                                              std::declval<T>().cend())>>
  : std::true_type {};

template <typename T>
bool constexpr has_const_iter_v = has_const_iter<T>::value;

// A helper function to find the next position of a delimiter in a sequence
// given a starting position
// While tested for strings, this should work for any
// iterator sequence which requires a subset match of a delimiter pattern.
// Technically these can take univ. references if you decay the template
// arguments and do a enable_if_t on the delim_t, given we don't have a lot of
// variants the less generic version is a bit more readable
template <typename str_t, typename delim_t>
auto get_delim_p(const str_t& str, const delim_t& delim,
                 typename str_t::size_type start_pos) -> typename str_t::size_type
{
  static_assert(has_const_iter_v<delim_t>, "delimiter must implement a const iterator!");
  auto p = std::find_first_of(str.cbegin()+start_pos, str.cend(),
                              delim.cbegin(), delim.cend());
  return std::distance(str.cbegin(), p);
}

template <typename str_t>
auto get_delim_p(const str_t& str, char delim,
                 typename str_t::size_type start_pos) -> typename str_t::size_type
{
  return str.find(delim, start_pos);
}

} // detail


// A non owning iterator for splitting a string with delimiters As far as the
// pointed string is valid, this is a really fast way to iterate over split parts
//
//   for (std::string_view part : StringSplit(input,"/")) {
//
//  ...}
//
// The current implementation is similar to boost::split and the like in the sense of
// Given a string of delimiters, presence of any of them will trigger a match. For eg.
//
// StringSplit("ab,cd\nde,gh", ",\n") -> ["ab","cd","de", "gh"]
//
// Though it is easy enough to modify this to use find instead of find_first_of
// for a pattern by using a different detail::get_delim_p function (This can be
// an additional template param/tag which is currently not implemented though
// fairly easy to do)
//
// For copying onto a container the same code above can be used for eg.
// std::vector<std::string> v
// for (std::string_view part : StringSplit(input,delim)) {
//      v.emplace_back(part) }
//
// Though if it is sure that the parent string is in scope, using either the iterator
// directly or using a vector<std::string_view> would yield the most performant results
// A helper is provided which will move these to a desired container Use the
// StringSplit or CharSplit aliases for most cases, we do need an explicit
// std::string_view template specification in other cases as by default the
// compiler will try to default to const char* which is not desirable
template <typename str_type = std::string_view,
          typename delim_type = std::string_view>
class LazySplit {
public:
    LazySplit(str_type s, delim_type d) : str(s), delim(d) {}

  class iterator {

  public:
    // A base declaration of the underlying string type so that we don't have to
    // decay every time, this is to ensure that we correctly have a reference type
    // when we hold a const std::string&,
    using base_string_type = typename std::decay<str_type>::type;

    // Basic iterator definition member types
    using iterator_category = std::forward_iterator_tag;
    using value_type = str_type;
    using difference_type = std::string_view::difference_type; // basically std::ptrdiff_t
    using pointer = std::add_pointer_t<base_string_type>;
    using const_pointer = std::add_const_t<pointer>;
    using reference = std::add_lvalue_reference_t<base_string_type>;
    using const_reference = std::add_const_t<reference>;
    using size_type = std::string_view::size_type;

    iterator(str_type s, delim_type d): str(s), delim(d), segment(next(0)) {}
    iterator(size_type sz) : pos(sz) {}

    iterator& operator++() {
      segment = next(pos);
      return *this;
    }

    iterator operator++(int) {
      iterator curr = *this;
      segment = next(pos);
      return curr;
    }

    reference operator*() { return segment; }
    pointer operator->()  { return &segment; }

    friend bool operator==(const iterator& a, const iterator& b) {
      return a.segment == b.segment;
    }

    friend bool operator!=(const iterator& a, const iterator& b) {
      return !(a==b);
    }

  private:
    // we need to collapse the reference here, hence we have to return by value We
    // have a special variant accepting char delimiters, this is useful for
    // functions which split on nullbyte etc. For allmost everything else the
    // other string_view splitter is more preferred as it allows for
    // multicharacter splits while still maintaining speed. The member function*
    // find_first_of is slightly slower than the std::find_first_of with iterators
    base_string_type next(size_type start_pos)
    {
      // this loop is needed to advance past empty delims
      while (start_pos < str.size()) {
        pos = detail::get_delim_p(str, delim, start_pos);
        // check if we are at the end or at a delim
        if (pos != start_pos) {
          return str.substr(start_pos, pos - start_pos);
        }
        start_pos = pos + 1;
      }
      return {};
    }

    size_type pos {0};
    str_type str;
    delim_type delim;
    str_type segment;

  };

  using const_iterator = iterator;
  iterator begin() const { return {str, delim}; }
  const_iterator cbegin() const { return {str, delim}; }

  iterator end() const { return { std::string::npos }; }
  const_iterator cend() const { return { std::string::npos }; }
private:
  str_type str;
  delim_type delim;
};

template <typename C, typename K, typename V>
bool operator==(const LazySplit<K,V>& split, const C& cont)
{
  return std::equal(split.begin(),split.end(),
                    cont.begin(),cont.end());
}
template <typename C, typename K, typename V>
bool operator==(const C& cont, const LazySplit<K,V>& split)
{
    return std::equal(split.begin(),split.end(),
                      cont.begin(),cont.end());
}

using StringSplitIt = LazySplit<std::string_view,std::string_view>;
using CharSplitIt = LazySplit<std::string_view,char>;

template <typename C=std::vector<std::string_view>>
C StringSplit(std::string_view input, std::string_view delim)
{
  C c;
  auto split_iter = StringSplitIt(input, delim);
  for(std::string_view part: split_iter) {
    c.emplace_back(part);
  }
  return c;
}

template <typename C=std::vector<std::string>>
C SplitPath(std::string_view input)
{
  return StringSplit<C>(input, "/");
}

} // namespace eos::common
