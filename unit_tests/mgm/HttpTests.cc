//------------------------------------------------------------------------------
// File: HttpTests.cc
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

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/http/HttpServer.hh"
#undef IN_TEST_HARNESS

//------------------------------------------------------------------------------
// Test clientDN formatting according to different standards
//------------------------------------------------------------------------------
TEST(Http, FormatClientDN)
{
  eos::mgm::HttpServer http;
  std::string ref =
    "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=hroussea/CN=660542/CN=Herve Rousseau";
  std::string old_cnd =
    "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=hroussea/CN=660542/CN=Herve Rousseau";
  ASSERT_TRUE(ref == http.ProcessClientDN(old_cnd));
  std::string new_cnd =
    "CN=Herve Rousseau,CN=660542,CN=hroussea,OU=Users,OU=Organic Units,DC=cern,DC=ch";
  ASSERT_TRUE(ref == http.ProcessClientDN(new_cnd));
}
