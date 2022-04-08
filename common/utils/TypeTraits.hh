//------------------------------------------------------------------------------
// File: TypeTraits.hh
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

/**
 * @file   TypeTraits.hh
 *
 * @brief  Some useful compile time type traits utilities
 *
 *  A common place for some simple template type inspection utilities, so that
 *  classes/functions defining them needn't define them at site, everything is
 *  defined in a detail namespace as this functionality isn't meant to be exposed
 *  to outside world but for internal static_asserts/SFINAE use cases
 */

#include <type_traits>
#include <utility>  // declval, and charconv

// version can be used for feature testing macros, while we're not C++20 yet
// helpful in case of identifying compiler support for partially supported
// C++17 features, for eg. charconv's float conversions which are only
// implemented by newer GCC/VS compilers
#if __has_include(<version>)
#include <version>
#endif

namespace eos::common::detail
{

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


// A simple test for containers that expose a data() type which is usually defined
// for contiguous sequences like string/_view/arrays/vectors
template <typename, typename = void>
struct has_data_t : std::false_type {};

template <typename T>
struct has_data_t<T, std::void_t<decltype(std::declval<T>().data())>> :
    std::true_type {};

// While technically this will be only defined by compilers implementing, given
// that preprocessors will replace unknowns with 0s, we'll have valid arithmetic
// expressions to write forward compatible code
#if __cpp_lib_to_chars >= 201611
// We are dealing with a compiler fully implementing charconv including floats
template <typename T>
inline constexpr bool is_charconv_numeric_v = std::is_arithmetic<T>::value;
#else
// Partial implementation only for integral types for eg. clang/gcc < 11
template <typename T>
inline constexpr bool is_charconv_numeric_v = std::is_integral<T>::value;
#endif

} // eos::common::detail
