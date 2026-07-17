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
 ***********************************************************************/

#ifndef EOS_NSSLOOKUP_HH
#define EOS_NSSLOOKUP_HH

#include "common/Namespace.hh"
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

constexpr size_t kNssInitialBufferSize = 16 * 1024;
constexpr size_t kNssMaxBufferSize = 16 * 1024 * 1024;

//------------------------------------------------------------------------------
//! Run a reentrant NSS lookup (getpwnam_r, getpwuid_r, getgrnam_r, getgrgid_r)
//! with a heap-allocated scratch buffer, growing it on ERANGE up to
//! kNssMaxBufferSize. The record must stay on the caller's stack since NSS
//! fills it with pointers into the scratch buffer.
//!
//! @param buffer scratch buffer, resized as needed; must outlive any use of
//!        the looked-up record
//! @param record storage for the looked-up entry
//! @param result set to &record on success, nullptr if no entry was found
//! @param fn the *_r lookup function, called as
//!        fn(keys..., &record, buffer, buflen, &result)
//! @param keys lookup key(s) forwarded as the leading argument(s) of fn
//!
//! @return 0 on success or the error code returned by the *_r function
//------------------------------------------------------------------------------
template <typename Record, typename Fn, typename... Keys>
int
LookupNssRecord(std::vector<char>& buffer, Record& record, Record*& result, Fn fn,
                Keys... keys)
{
  buffer.resize(kNssInitialBufferSize);

  while (true) {
    result = nullptr;
    const int errc = fn(keys..., &record, buffer.data(), buffer.size(), &result);

    if (errc != ERANGE || buffer.size() == kNssMaxBufferSize) {
      return errc;
    }

    buffer.resize(std::min(2 * buffer.size(), kNssMaxBufferSize));
  }
}

EOSCOMMONNAMESPACE_END

#endif // EOS_NSSLOOKUP_HH
