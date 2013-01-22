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

/**
 * @file   StringTokenizer.hh
 * 
 * @brief  Convenience class to deal with command line strings
 * 
 * 
 */

#ifndef __EOSCOMMON_STRINGTOKENIZER__
#define __EOSCOMMON_STRINGTOKENIZER__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <vector>
#include <set>
#include <stdio.h>
#include <errno.h>
#include <string.h>

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Utility class with convenience functions for string command line parsing 
//! Works like XrdOucTokenizer but carse about escaped blanks and double quotes
/*----------------------------------------------------------------------------*/
class StringTokenizer
{
 char* fBuffer;
 int fCurrentLine;
 int fCurrentArg;
 std::vector<size_t> fLineStart;
 std::vector<std::string> fLineArgs;

public:
 StringTokenizer (const char* s);

 StringTokenizer (XrdOucString s)
 {
  StringTokenizer (s.c_str ());
 }

 StringTokenizer (std::string s)
 {
  StringTokenizer (s.c_str ());
 }
 ~StringTokenizer ();

 const char* GetLine (); // return the next parsed line seperated by \n
 const char* GetToken (); // return the next token 
};

EOSCOMMONNAMESPACE_END
#endif
