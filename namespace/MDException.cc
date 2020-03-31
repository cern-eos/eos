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
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   Metadata exception
//------------------------------------------------------------------------------

#include "MDException.hh"
#include "common/Logging.hh"
#include <folly/ExceptionWrapper.h>

namespace eos
{

MDStatus::MDStatus(int localerrn, const std::string& error)
  : localerrno(localerrn), err(error)
{
  if (localerrno != ENOENT) {
    eos_static_crit("MDStatus (%d): %s", localerrn, error.c_str());
  } else {
    eos_static_debug("MDStatus (%d): %s", localerrn, error.c_str());
  }
}
}
