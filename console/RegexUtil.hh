//------------------------------------------------------------------------------
//! @file RegexUtil.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
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

#ifndef __REGEXUTIL__HH__
#define __REGEXUTIL__HH__

#include <iostream>
#include<string>
#include <regex.h>

//------------------------------------------------------------------------------
//! Class RegexUtil
//!
//! @description Simple wrapper aroud posix regex implementation.
//!     TODO: This will be obsolete when min supported gcc is 4.9 and std::regex
//!           become available.
//------------------------------------------------------------------------------
class RegexUtil
{
  static const unsigned max_num_of_matches = 128; //< Length of matched regex arr
  //< Enum containing signal values if smth gone wrong
  enum RegexErr {NOTOKENMODEON = -1, NOMOREMATCHES = -2};

  regex_t m_regex; //< Posix regex object
  regmatch_t m_matches[max_num_of_matches]; //< Matches from regex_t
  bool m_tokenize; //< Tokenizer mode indicator

  int m_regex_flags; //< Flages for posix regex_t
  std::string m_origin; //< pointer to source string
  std::string m_regex_string; //< Regex string

public:
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  RegexUtil();

  //----------------------------------------------------------------------------
  //! Setting regex string and flags
  //!
  //! @param in regex string
  //! @param flags int
  //----------------------------------------------------------------------------
  void SetRegex(std::string regex, int flags = 0); /*throw (std::string)*/

  //----------------------------------------------------------------------------
  //! Setting origin pointer to string
  //!
  //! @param in pointer to origin string
  //----------------------------------------------------------------------------
  inline void SetOrigin(const std::string& origin)
  {
    m_origin = origin;
  }

  //----------------------------------------------------------------------------
  //! Applying regex actually on string and storing matches
  //----------------------------------------------------------------------------
  void initTokenizerMode(); /*throw (std::string)*/

  //----------------------------------------------------------------------------
  //! Getting match (if there is any)
  //----------------------------------------------------------------------------
  std::string Match(); /*throw (std::string)*/
};

#endif //__REGEXUTIL__HH__
