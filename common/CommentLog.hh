// ----------------------------------------------------------------------
// File: CommentLog.hh
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
 * @file   CommentLog.hh
 * 
 * @brief  Class to log all commands which include a comment specified on the EOS shell
 * 
 * 
 */

#ifndef __EOSCOMMON_COMMENTLOG__HH__
#define __EOSCOMMON_COMMENTLOG__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class to log all commands which include a comment specified on the EOS shell
/*----------------------------------------------------------------------------*/

class CommentLog {
private:
  std::string fName; //< File Name storing the comments
  int fFd;           //< File Descriptor to comment log file
public:
  // ------------------------------------------------------------------------
  //! Add a comment with 'exectime','cmd','subcmd','args','comment','stdErr','retc'
  // ------------------------------------------------------------------------
  bool Add(time_t, const char*, const char*, const char*, const char*, const char*, int);

  // ------------------------------------------------------------------------
  //! Check if the comment log file has been created/opened
  bool IsValid();

  // ------------------------------------------------------------------------
  //! Constructor
  // ------------------------------------------------------------------------
  CommentLog(const char* file);
  
  // ------------------------------------------------------------------------
  //! Destructor
  // ------------------------------------------------------------------------
  ~CommentLog();
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
#endif

