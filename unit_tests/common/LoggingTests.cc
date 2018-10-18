//------------------------------------------------------------------------------
// File: LoggingTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "Namespace.hh"
#include "gtest/gtest.h"

//------------------------------------------------------------------------------
// Test the proper static allocation and destruction of the global logging
// object in Logging.hh
//------------------------------------------------------------------------------

extern void function_using_logging();

EOSCOMMONTESTING_BEGIN

TEST(Logging, Log)
{
  using namespace eos::common;
  gLogging.SetLogPriority(LOG_INFO);
  eos_static_info("%s %s", __FUNCTION__, "print a test log line");
  function_using_logging();
}

EOSCOMMONTESTING_END
