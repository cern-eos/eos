//------------------------------------------------------------------------------
//! @file TestEnv.hh
//! @author Elvin Sindrilaru <esindril@cern.ch>
//! @bried Class containing all the variables need for the test done on the FST
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#include "Namespace.hh"
#include <map>
#include <string>
#include <iostream>
#include <memory>

EOSFSTTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! TestEnv class - not thread safe
//------------------------------------------------------------------------------
class TestEnv
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param EOS endpoint
  //----------------------------------------------------------------------------
  TestEnv(const std::string& endpoint);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~TestEnv();

  //----------------------------------------------------------------------------
  //! Add new entry to the map of parameters
  //!
  //! @param key key to be inserted
  //! @param value value to the inserted
  //----------------------------------------------------------------------------
  void SetMapping(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Get value corresponding to the key from the map
  //!
  //! @param key key to be searched in the map
  //!
  //! @return value stored in the map
  //----------------------------------------------------------------------------
  std::string GetMapping(const std::string& key) const;

private:
  std::map<std::string, std::string> mMapParam; ///< map testing parameters
  std::string mPathPrefix; ///< Path prefix inside the instate for tests
  std::string mHostName; ///< EOS instance hostname
};

//------------------------------------------------------------------------------
//! Logging class
//------------------------------------------------------------------------------
class GTest_Logger
{
public:
  GTest_Logger(bool enabled) : mEnabled(enabled) {}
  bool isEnabled()
  {
    return mEnabled;
  }

  template<typename T> GTest_Logger& operator<<(T const& t)
  {
    if (mEnabled) {
      std::cout << t;
    }

    return *this;
  }

  GTest_Logger& operator<<(std::ostream & (*manipulator)(std::ostream&))
  {
    if (mEnabled) {
      std::cout << manipulator;
    }

    return *this;
  }

  void SetEnabled(bool enable)
  {
    mEnabled = enable;
  }

private:
  bool mEnabled;
};

EOSFSTTEST_NAMESPACE_END

extern std::unique_ptr<eos::fst::test::TestEnv> gEnv;
extern eos::fst::test::GTest_Logger gLogger;

// Macro to print GTest similar output
// Uses the GTest_Logger gLogger variable available in the FstFileTest fixtures
#define GLOG if (gLogger.isEnabled()) { std::cout << "[ INFO     ] "; } gLogger
