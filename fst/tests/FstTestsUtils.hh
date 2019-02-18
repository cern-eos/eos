//------------------------------------------------------------------------------
// File: FstTestsUtils.hh
// Author: Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef __EOSFST_TESTS_FSTTESTSUTILS_HH__
#define __EOSFST_TESTS_FSTTESTSUTILS_HH__

#include <string>

#include "gtest/gtest.h"
#include "Namespace.hh"
#include "TestEnv.hh"

EOSFSTTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Global Environment class
//------------------------------------------------------------------------------
class FstTestsEnv : public ::testing::Environment
{
public:
  static std::string instanceName;
  static bool verbose;
};

//------------------------------------------------------------------------------
//! Logging class
//------------------------------------------------------------------------------
class GTest_Logger
{
public:
  GTest_Logger(bool enabled) : enabled(enabled) {}
  bool isEnabled() { return this->enabled; }

  template<typename T> GTest_Logger& operator<<(T const& t)
  {
    if (enabled) { std::cout << t; }
    return *this;
  }

  GTest_Logger& operator<<(std::ostream& (*manipulator)(std::ostream&))
  {
    if (enabled) { std::cout << manipulator; }
    return *this;
  }

private:
  bool enabled;
};

// Macro to print GTest similar output
// Uses the GTest_Logger mLogger variable available in the FstFileTest fixtures
#define GLOG if (mLogger.isEnabled()) { std::cout << "[ INFO     ] "; } mLogger

EOSFSTTEST_NAMESPACE_END

#endif //__EOSFST_TESTS_FSTTESTSUTILS_HH__
