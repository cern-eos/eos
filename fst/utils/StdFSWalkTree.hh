// ----------------------------------------------------------------------
// File: StdFSWalkDirTree
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

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

#include <string_view>

#if defined(__clang__) && __clang_major__ < 6
#include <experimental/filesystem>
#else
#include <filesystem>
#endif



// A std::filesystem version of Filesystem like functions for eos
// Clang support is in experimental namespace until clang 6
namespace eos::fst::stdfs {

#if defined(__clang__) && __clang_major__ < 6
namespace fs = std::experimental::filesystem::v1;
#else
namespace fs = std::filesystem;
#endif


// A walk directory tree function using the recursive directory iterator.
// Hidden files or directories are not visited, and symlinks not followed
// This version throws exceptions
template <typename FilterFn, typename PathOp>
uint64_t WalkFSTree(std::string_view path, FilterFn&& filter, PathOp&& path_op)
{
  uint64_t count {0};
  for (auto p = fs::recursive_directory_iterator(path,
                                                 fs::directory_options::skip_permission_denied);
       p != fs::recursive_directory_iterator();
       ++p) {
    if (p->path().filename().c_str()[0] == '.') {
      p.disable_recursion_pending();
      continue;
    }
    if (filter(p)) {
      path_op(p->path(), ++count);
    }
  }

  return count;
}

// A non throwing version of WalkFSTree; if allocator throws this is of course raised,
// but then that would anyway warrant a critical failure
template <typename FilterFn, typename PathOp>
uint64_t WalkFSTree(std::string_view path, FilterFn&& filter, PathOp&& path_op,
                    std::error_code& ec) noexcept
{
  uint64_t count {0};
  for (auto p = fs::recursive_directory_iterator(path,
                                                 fs::directory_options::skip_permission_denied,
                                                 ec);
       p != fs::recursive_directory_iterator();
       ++p) {
    if (ec) {
      return count;
    }

    if (p->path().filename().c_str()[0] == '.') {
      p.disable_recursion_pending();
      continue;
    }
    if (filter(p)) {
      path_op(p->path(), ++count);
    }
  }

  return count;
}

// This works with multiple directory iterator types hence the template
template <typename It>
bool IsRegularFile(It it)
{
#if defined(__clang__) && __clang_major__ < 6
  return fs::is_regular_file(it->path());
#else
  return it->is_regular_file();
#endif
}

} // namespace eos::fst::fsutils

