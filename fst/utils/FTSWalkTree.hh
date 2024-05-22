// ----------------------------------------------------------------------
// File: WalkDirTree
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

#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include <fts.h>
#include <utility>
#include <type_traits>
#include <system_error>
#include <vector>

EOSFSTNAMESPACE_BEGIN

static constexpr std::string_view XSMAP_SUFFIX = ".xsmap";
static constexpr std::string_view SCRUB_PREFIX = "/scrub.";

inline bool exclude_xs_and_scrub(std::string_view filename)
{
  return (common::endsWith(filename, XSMAP_SUFFIX) ||
          (filename.find(SCRUB_PREFIX) != std::string::npos));
}

// A function to walk the dir tree and apply a function with arguments
// It is necessary that the function's first argument is a const char* path
// This function uses FTS to walk through directory entries and
// doesn't follow symlinks and only operates on regular files atm
template <typename ExcludeFn, typename PathOp>
uint64_t
WalkDirTree(std::vector<char*>&& paths, ExcludeFn exclude_fn, PathOp path_op,
            std::error_code& ec)
{
  FTS* tree = fts_open(paths.data(), FTS_NOCHDIR, 0);

  if (!tree) {
    eos_static_err("msg=\"fts_open failed\" errno=%d", errno);
    ec = std::make_error_code(static_cast<std::errc>(errno));
    return 0;
  }

  uint64_t cnt {0};
  FTSENT* node;

  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
    } else {
      if (node->fts_info == FTS_F) {
        if (!exclude_fn(node->fts_accpath)) {
          path_op(node->fts_path);
          ++cnt;
        }
      }
    }
  }

  if (fts_close(tree)) {
    eos_static_err("msg=\"fts_close failed\" errno=%d", errno);
    ec = std::make_error_code(static_cast<std::errc>(errno));
  }

  return cnt;
}

//------------------------------------------------------------------------------
//! A function useful for walking FST trees, where xsmap and scrub files are
//! usually excluded. This variant expects a member function to be applied
//! across the tree.
//------------------------------------------------------------------------------
template <typename UnaryOp>
uint64_t
WalkFSTree(std::string path, UnaryOp&& op, std::error_code& ec)
{
  return WalkDirTree({path.data(), nullptr},
                     exclude_xs_and_scrub,
                     std::forward<UnaryOp>(op),
                     ec);
}

//------------------------------------------------------------------------------
//! Method to travers the subtree and check the file if they satisfy a certain
//! condition. The files are counted and only the ones with the index matching
//! the given ones are checked.
//!
//! @param path path of the sub-tree to check
//! @param check_fn operation to be applied to individual files
//! @param exclude_fn operator that should skip check the file if it returns
//!                   true
//! @param match_indexes set of indexes to check inside the subtree
//------------------------------------------------------------------------------
template <typename CheckFn, typename ExcludeFn>
bool
WalkFsTreeCheckCond(std::vector<char*>&& paths,
                    CheckFn check_fn,
                    ExcludeFn exclude_fn,
                    const std::set<uint64_t>& match_indexes)
{
  FTS* tree = fts_open(paths.data(), FTS_NOCHDIR, 0);

  if (!tree) {
    eos_static_err("msg=\"fts_open failed\" path=\"%s\" errno=%d",
                   paths.data(), errno);
    return false;
  }

  std::set<uint64_t> checked_indexes;
  uint64_t cnt {0};
  FTSENT* node {nullptr};

  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
    } else {
      if (node->fts_info == FTS_F) {
        if (!exclude_fn(node->fts_name)) {
          ++cnt;

          if (match_indexes.find(cnt) != match_indexes.end()) {
            if (!check_fn(node->fts_path)) {
              eos_static_crit("msg=\"file not matching condition\" fn=\"%s\" "
                              "index=%llu", node->fts_path, cnt);
              (void) fts_close(tree);
              return false;
            } else {
              checked_indexes.insert(cnt);

              if (checked_indexes.size() == match_indexes.size()) {
                break;
              }
            }
          }
        }
      }
    }
  }

  if (fts_close(tree)) {
    eos_static_err("msg=\"fts_close failed\" errno=%d", errno);
    return false;
  }

  return true;
}

EOSFSTNAMESPACE_END
