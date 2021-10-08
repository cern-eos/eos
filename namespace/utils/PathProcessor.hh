/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#ifndef EOS_NS_PATH_PROCESSOR_HH
#define EOS_NS_PATH_PROCESSOR_HH

#include <cstring>
#include <vector>
#include <string>
#include <list>
#include <sstream>
#include "common/StringSplit.hh"
namespace eos
{
//----------------------------------------------------------------------------
//! Helper class responsible for spliting the path
//----------------------------------------------------------------------------
class PathProcessor
{
public:

  //------------------------------------------------------------------------
  //! Split the path and prepend its elements into a deque.
  //------------------------------------------------------------------------
  static std::deque<std::string> insertChunksIntoDeque(std::string_view path)
  {
    return eos::common::SplitPath<std::deque<std::string>>(path);
  }

  static void insertChunksIntoDeque(std::deque<std::string>& elements,
                                    std::string_view path)
  {
    if (elements.empty()) {
      elements = insertChunksIntoDeque(path);
      return;
    }

    auto vpath = eos::common::SplitPath(path);
    // This is to ensure old_path = [a,b] new_path = [c,d,e] -> [c,d,e,a,b]
    std::move(vpath.rbegin(), vpath.rend(), std::front_inserter(elements));
  }

  // The following function is only used in ns_in_memory and not preferred for
  // newer apis
  //------------------------------------------------------------------------
  //!Split the path and put its element in a vector, the split is done !
  //in-place and the buffer is overwritten
  //------------------------------------------------------------------------
  static void splitPath(std::vector<char*>& elements, char* buffer)
  {
    elements.clear();
    elements.reserve(10);
    char* cursor = buffer;
    char* beg    = buffer;

    //----------------------------------------------------------------------
    // Go by the characters one by one
    //----------------------------------------------------------------------
    while (*cursor) {
      if (*cursor == '/') {
        *cursor = 0;

        if (beg != cursor) {
          elements.push_back(beg);
        }

        beg = cursor + 1;
      }

      ++cursor;
    }

    if (beg != cursor) {
      elements.push_back(beg);
    }
  }

  //------------------------------------------------------------------------
  //! Absolute Path sanitizing all '/../' and '/./' entries
  //------------------------------------------------------------------------
  static void absPath(std::string& mypath)
  {
    std::vector<std::string> abs_path;
    std::vector<std::string> elements = eos::common::SplitPath(mypath);
    std::ostringstream oss;
    int skip = 0;

    for (auto it = elements.rbegin(); it != elements.rend(); ++it) {
      if ((*it == ".") || it->empty()) {
        continue;
      }

      if (*it == "..") {
        ++skip;
        continue;
      }

      if (skip) {
        --skip;
        continue;
      }

      abs_path.push_back(*it);
    }

    for (auto it = abs_path.rbegin(); it != abs_path.rend(); ++it) {
      oss << "/" << *it;
    }

    mypath = oss.str();

    if (mypath.empty()) {
      mypath = "/";
    }
  }
};
}

#endif // EOS_NS_PATH_PROCESSOR_HH
