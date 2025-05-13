/************************************************************************
* EOS - the CERN Disk Storage System                                   *
* Copyright (C) 2025 CERN/Switzerland                                  *
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

#ifndef EOS_RAWPTR_HH
#define EOS_RAWPTR_HH

#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN
// a no-op “deleter” so you don't accidentally free anything
struct no_delete {
  void operator()(void*) noexcept {}
};

template<typename T>
struct raw_ptr {
  using element_type = T;
  using pointer = T*;

  constexpr raw_ptr(pointer ptr = nullptr) noexcept : m_ptr(ptr) {}

  constexpr pointer get() const noexcept       { return m_ptr; }
  constexpr T&      operator*() const          { return *m_ptr; }
  constexpr pointer operator->() const noexcept{ return m_ptr; }
  constexpr operator bool() const noexcept                { return m_ptr != nullptr; }
  constexpr bool operator !=(const raw_ptr &other) { return m_ptr != other.m_ptr; }

private:
  pointer m_ptr;
};

EOSNSNAMESPACE_END

#endif // EOS_RAWPTR_HH
