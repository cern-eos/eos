//------------------------------------------------------------------------------
//! @file JwkGeneratorTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "console/commands/helpers/jwk_generator/jwk_generator.hpp"
#include "gtest/gtest.h"
#include <openssl/obj_mac.h>

//------------------------------------------------------------------------------
// About 1 in 128 P-256 keys has a coordinate with a leading zero byte, which
// must be kept in the fixed-width JWK encoding (EOS-6646)
//------------------------------------------------------------------------------
TEST(JwkGenerator, ES256PointIsOnCurve)
{
  std::unique_ptr<EC_GROUP, decltype(&EC_GROUP_free)> group(
      EC_GROUP_new_by_curve_name(NID_X9_62_prime256v1), EC_GROUP_free);
  std::unique_ptr<EC_POINT, decltype(&EC_POINT_free)> point(EC_POINT_new(group.get()),
                                                            EC_POINT_free);
  ASSERT_TRUE(point);

  for (int i = 0; i < 2000; ++i) {
    jwk_generator::JwkGenerator<jwk_generator::ES256> jwk;
    const auto json = jwk.to_json();
    const auto x = jwk_generator::detail::base64_url_decode(json["x"].get<std::string>());
    const auto y = jwk_generator::detail::base64_url_decode(json["y"].get<std::string>());
    ASSERT_EQ(32u, x.size());
    ASSERT_EQ(32u, y.size());
    std::vector<uint8_t> oct{0x04};
    oct.insert(oct.end(), x.begin(), x.end());
    oct.insert(oct.end(), y.begin(), y.end());
    ASSERT_EQ(
        1, EC_POINT_oct2point(group.get(), point.get(), oct.data(), oct.size(), nullptr))
        << "x=" << json["x"] << " y=" << json["y"];
  }
}
