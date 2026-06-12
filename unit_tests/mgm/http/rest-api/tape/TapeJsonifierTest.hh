//------------------------------------------------------------------------------
//! @file TapeJsonifierTest.hh
//! @author Cedric Caffy - CERN
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

#ifndef EOS_TAPEJSONIFIERTEST_HH
#define EOS_TAPEJSONIFIERTEST_HH

#include "common/json/Jsonifiable.hh"
#include <gtest/gtest.h>
#include <json/json.h>
#include <sstream>
#include <string>

class TapeJsonifierTest : public ::testing::Test
{
protected:
  template<typename Model>
  static std::string toJson(const Model& model)
  {
    std::stringstream ss;
    model.jsonify(ss);
    return ss.str();
  }

  static Json::Value parseJson(const std::string& json)
  {
    Json::Value root;
    Json::Reader reader;
    EXPECT_TRUE(reader.parse(json, root)) << reader.getFormattedErrorMessages();
    return root;
  }
};

#endif // EOS_TAPEJSONIFIERTEST_HH
