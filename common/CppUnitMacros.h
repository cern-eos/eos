//------------------------------------------------------------------------------
// File: CppUnitMacros.h
// Author: Elvin Sindrilaru <esindril@cern.ch>
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

#ifndef __EOSCOMMON_CPPUNITMACROS_HH__
#define __EOSCOMMON_CPPUNITMACROS_HH__

#include <cppunit/extensions/HelperMacros.h>
#include <sstream>

//------------------------------------------------------------------------------
//! Helper macro to print message from a stream
//------------------------------------------------------------------------------
#define CPPUNIT_ASSERT_STREAM(MSG, CONDITION)          \
  do {                                                 \
  std::ostringstream oss;                              \
  CPPUNIT_ASSERT_MESSAGE(                              \
  static_cast<std::ostringstream &>(oss << MSG).str(), \
  CONDITION);                                          \
  } while (0)

#endif // __EOSCOMMON_CPPUNITMACROS_HH__
