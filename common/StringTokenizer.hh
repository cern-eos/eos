// ----------------------------------------------------------------------
// File: StringTokenizer.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#pragma once
#include "common/Namespace.hh"
#include "XrdOuc/XrdOucString.hh"
#include <string>
#include <vector>
#include <sstream>
#include <stdio.h>
#include <errno.h>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Utility class with convenience functions for string command line parsing
//! Works like XrdOucTokenizer but wants each argument in " ".
//! Escaped quotes are ignored and & is replaced with #AND# !!!
//------------------------------------------------------------------------------
class StringTokenizer
{
  char* fBuffer;
  int fCurrentLine;
  int fCurrentArg;
  std::vector<size_t> fLineStart;
  std::vector<std::string> fLineArgs;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StringTokenizer(const char* s);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StringTokenizer(XrdOucString s)
  {
    StringTokenizer(s.c_str());
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StringTokenizer(std::string s)
  {
    StringTokenizer(s.c_str());
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~StringTokenizer();

  //----------------------------------------------------------------------------
  //! Get next parsed line seperated by \n
  //!
  //! @return next line
  //----------------------------------------------------------------------------
  const char* GetLine();

  //----------------------------------------------------------------------------
  //! Return next parsed space seperated token taking into account escaped
  //! blanks and quoted strings
  //!
  //! @param escapeand if true escape & with #AND# !! UGLY!!
  //!
  //! @return next token or null if no token found
  //----------------------------------------------------------------------------
  const char* GetToken(bool escapeand = true);

  //----------------------------------------------------------------------------
  //! Split given string based on the delimiter
  //!
  //! @param str given string
  //! @param delimiter delimiter
  //!
  //! @return vector of tokens
  //----------------------------------------------------------------------------
  template<typename C>
  static C split(const std::string& str, const char delimiter);

  //----------------------------------------------------------------------------
  //! Merge vector's contents using the provided delimter
  //!
  //! @param container container of tokens
  //! @param delimiter delimiter
  //!
  //! @return string obtained from concatenating the tokens using the delimiter
  //----------------------------------------------------------------------------
  template<typename C>
  static std::string merge(const C& container, const char delimiter);

  //----------------------------------------------------------------------------
  //! Check if string represents unsigned number - could be dropped ?!
  //----------------------------------------------------------------------------
  static bool IsUnsignedNumber(const std::string& str);
};


//------------------------------------------------------------------------------
// Split given string based on the delimiter
//------------------------------------------------------------------------------
template<typename C>
C StringTokenizer::split(const std::string& str, char delimiter)
{
  istringstream iss(str);
  C container;
  std::string part;

  while (std::getline(iss, part, delimiter)) {
    if (!part.empty()) {
      container.emplace_back(part);
    }
  }

  return container;
}

//------------------------------------------------------------------------------
// Merge container's contents using the provided delimter
//------------------------------------------------------------------------------
template<typename C>
std::string
StringTokenizer::merge(const C& container, char delimiter)
{
  std::ostringstream oss;

  for (const auto& elem : container) {
    oss << elem << delimiter;
  }

  std::string output = oss.str();

  if (output.length()) {
    output.resize(output.length() - 1);
  }

  return output;
}


EOSCOMMONNAMESPACE_END
