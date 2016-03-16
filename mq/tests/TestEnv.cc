//------------------------------------------------------------------------------
// File: TestEnv.hh
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

#include "TestEnv.hh"
#include <iostream>

EOSMQTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TestEnv::TestEnv()
{
  // RSA private and public keys used for testing
  std::string rsa_private_key = "-----BEGIN PRIVATE KEY-----\n"
"MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC06+jNd6ESn5YY\n"
"eElhek8zOrBqGU30KtPI7FI/4aK+6Zql7KZmvBTUn2z9ci0LXjPd1j0Byw67fgj9\n"
"OyXU2Y9nuTHcuj4hHd4puaAnJeBWZvXIEU44Mui9l7HuLW97baodmfUYMPiQAiYm\n"
"FAlKD0RtDb/YSQjRHe+EFIEl9hZKk3bLb6imUR7hLNGqAQiPH/Tm7OpO4NUEp9C1\n"
"FzGXITTeCLQLr9KlSRyrrdOBup602+1k4Nu0/5CnhhN4TQ4KvMlpiy2brXiBFkgw\n"
"X4hm3+FcvEwa/p4k7oPTSfHFXBblTsbMpES/zdCeAgSWLZzxdFdRhawhKW1bDnRX\n"
"n1PYGRDbAgMBAAECggEBAK4k7T7oyWfNqIIBNlDXk+hxs2FM3hYKKzSZFEpc+3Pc\n"
"E3lmonz8yOgoVJZYEjeBA1eiYbKaK6IZHLny9uU8TKbAQdh+hFMIFtH+1MMZ2CgS\n"
"jr12ut2pUxE7NI9XuJkL49T+XkZczMSA7Qt1+cMJkwmNVH3xPsValTODMRTDHI3G\n"
"aRxknuaVy177T1qf2QIAeOAGYA9kZg3bguUg/uiCj5mBEVmwlOXH9g9+d2jMFycq\n"
"5Cyqev6RJeukttccGFkCptFQWWeYdPfdTTepQ+1TtIsXTL324oZeDPcczwFnaBZs\n"
"TO8ubQOhRV+NTPp4BlnA5lVlgw69eJM1cCPA3O+bj/ECgYEA2XONDClmHUw4oh7p\n"
"FQ1hG11vc0lF6rpuXrJPTlYqs10LZLQukJAW88Zo9m3FWKYWL/iiqdl3sVmsu21y\n"
"eH7CbsNqWf8s5HS5ye39ccX4r0L0JLbqQ9GbP41Io8cq3Xxv8pLTt8R9T16OoJV0\n"
"u0jcz2KPenwVOLPlAeEePj0P1+8CgYEA1P6Mgc3s1zYDaSdpMrOuCX6267RZjDxf\n"
"FhTp0G8M01l+Blb0CcUEuYQjPeCbh9YQFLVm1cXDDiQH10avLgRyJzULp/QN/Qhq\n"
"Zn2TAfpbDM1ykzROygCZshFShF9BP4WiKU/3iovqWwxz2eL6xGp/CQgyPtdTVoUh\n"
"NBdspw7dCdUCgYEA1I+Gng/N2O/MIHX0w/Z7KSPRsTE8HjK1du34ZgwG26QlYgBb\n"
"0EZ5mTwnGFS/Z2ObJrN2Vm/U99E/70sSbcUDTQDK8kRlXsDXaBOy/sdVzAS34TfI\n"
"khjQGHSEQEyNk0pzp/xs5yM0lyRIaaMPI5AbAMJInKO0nuQDBS5IwPAxj5MCgYEA\n"
"zMCbH9lu2YASDU8WoOf74SLQA4xPHTGYEuktz/JBORdpv/xtAstD/HcbTcuSmCVf\n"
"Nhkgb1Z6aSiX5QoNM0aQ2kHzH5TMsbcaWcZTwO4EAy2o+/un1iZ3madvVNMhLUhw\n"
"mBhIlgZk1vwEjqvVd2YNEwivDJwAgEbgoWv3Ri1SrbUCgYBIWoUfAtaT+Pp4g1kr\n"
"S2xq2Cng6nky5dhtNRO+hA+N9PX3mlkmlBWd8ogpN4nL/9Nvltg6cnjNPd1UzuIG\n"
"M2R6GCTMpG1PhwRmzphD83j5bkYtbatF9+QSsXMLtuuL0Y1AkY3oXqqRwtuyXXzc\n"
"HbMJh/xijuTrqdinM+2u5My8mA==\n"
"-----END PRIVATE KEY-----\n";

  std::string rsa_public_key = "-----BEGIN PUBLIC KEY-----\n"
"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtOvozXehEp+WGHhJYXpP\n"
"MzqwahlN9CrTyOxSP+GivumapeymZrwU1J9s/XItC14z3dY9AcsOu34I/Tsl1NmP\n"
"Z7kx3Lo+IR3eKbmgJyXgVmb1yBFOODLovZex7i1ve22qHZn1GDD4kAImJhQJSg9E\n"
"bQ2/2EkI0R3vhBSBJfYWSpN2y2+oplEe4SzRqgEIjx/05uzqTuDVBKfQtRcxlyE0\n"
"3gi0C6/SpUkcq63TgbqetNvtZODbtP+Qp4YTeE0OCrzJaYstm614gRZIMF+IZt/h\n"
"XLxMGv6eJO6D00nxxVwW5U7GzKREv83QngIEli2c8XRXUYWsISltWw50V59T2BkQ\n"
"2wIDAQAB\n"
"-----END PUBLIC KEY-----\n";

  mMapParam.insert(std::make_pair("rsa_public_key", rsa_public_key));
  mMapParam.insert(std::make_pair("rsa_private_key", rsa_private_key));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TestEnv::~TestEnv()
{
  // empty
}

//------------------------------------------------------------------------------
// Set key value mapping
//------------------------------------------------------------------------------
void
TestEnv::SetMapping(const std::string& key, const std::string& value)
{
  auto pair_res = mMapParam.insert(std::make_pair(key, value));

  if (!pair_res.second)
  {
    std::cerr << "Mapping already exists, key=" << key
              << " value=" << value << std::endl;
  }
}

//------------------------------------------------------------------------------
// Get key value mapping
//------------------------------------------------------------------------------
std::string
TestEnv::GetMapping(const std::string& key) const
{
  auto iter = mMapParam.find(key);

  if (iter != mMapParam.end())
    return iter->second;
  else
    return std::string("");
}

EOSMQTEST_NAMESPACE_END
