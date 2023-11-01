//------------------------------------------------------------------------------
// File: XrdCopy.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "XrdCl/XrdClURL.hh"
#include <atomic>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class XrdCopy abstracting parallel file copy
//------------------------------------------------------------------------------
class XrdCopy
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //----------------------------------------------------------------------------
  XrdCopy();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdCopy() = default;

  static std::atomic<size_t> s_bp;
  static std::atomic<size_t> s_sp;
  static std::atomic<size_t> s_bt;
  static std::atomic<size_t> s_n;
  static std::atomic<size_t> s_tot;
  static std::atomic<bool> s_verbose;
  static std::atomic<bool> s_silent;

  // job description: name => pair(src,target)
  typedef std::map<std::string, std::pair<std::string,std::string>> job_t;
  // job result: name => pair(errc, errmsg)
  typedef std::map<std::string, std::pair<int, std::string>> result_t;

  result_t run(const job_t& job, const std::string& filter, size_t parallel);

private:
};

EOSCOMMONNAMESPACE_END
