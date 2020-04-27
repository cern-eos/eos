//------------------------------------------------------------------------------
//! @file filename.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class implementing some convenience functions for filenames
//------------------------------------------------------------------------------

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

#ifndef FUSE_FILENAME_HH_
#define FUSE_FILENAME_HH_

class filename
{
public:

  static bool endswith_case_sensitive(std::string mainStr, std::string toMatch)
  {
    auto it = toMatch.begin();
    return mainStr.size() >= toMatch.size() &&
           std::all_of(std::next(mainStr.begin(), mainStr.size() - toMatch.size()),
    mainStr.end(), [&it](const char& c) {
      return ::tolower(c) == ::tolower(*(it++));
    });
  }

  static bool matches_suffix(const std::string& name,
                             const std::vector<std::string>& suffixes)
  {
    for (auto it = suffixes.begin(); it != suffixes.end(); ++it) {
      if (endswith_case_sensitive(name, *it)) {
        return true;
      }
    }

    return false;
  }

  static bool matches(const std::string& name,
		      const std::vector<std::string>& namelist)
  {
    for (auto it = namelist.begin(); it != namelist.end(); ++it) {
      if (name == *it) {
	return true;
      }
    }

    return false;
  }
};

#endif
