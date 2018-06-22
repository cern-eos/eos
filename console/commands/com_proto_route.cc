//------------------------------------------------------------------------------
//! @file com_proto_route.cc
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

#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

constexpr static int sDefaultXrdPort = 1094;
constexpr static int sDefaultHttpPort = 8000;
void com_route_help();

//------------------------------------------------------------------------------
//! Class RouteHelper
//------------------------------------------------------------------------------
class RouteHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RouteHelper()
  {
    mIsAdmin = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RouteHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
RouteHelper::ParseCommand(const char* arg)
{
  const char* option;
  std::string soption;
  eos::console::RouteProto* route = mReq.mutable_route();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  option = tokenizer.GetToken();
  std::string cmd = (option ? option : "");

  if (cmd == "ls") {
    route->set_list(true);
  } else if (cmd == "unlink") {
    using eos::console::RouteProto_UnlinkProto;
    RouteProto_UnlinkProto* unlink = route->mutable_unlink();

    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    // Do basic checks that this is a path
    soption = option;

    if (soption.empty() || soption[0] != '/') {
      return false;
    }

    unlink->set_path(soption);
  } else if (cmd == "link") {
    eos::console::RouteProto_LinkProto* link = route->mutable_link();

    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    // Do basic checks that this is a path
    soption = option;

    if (soption.empty() || soption[0] != '/') {
      return false;
    }

    link->set_path(soption);

    // Parse redirection locations which are "," separated
    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    soption = option;
    std::vector<std::string> endpoints;
    eos::common::StringConversion::Tokenize(soption, endpoints, ",");

    if (endpoints.empty()) {
      return false;
    }

    for (const auto& endpoint : endpoints) {
      eos::console::RouteProto_LinkProto_Endpoint* ep = link->add_endpoints();
      std::vector<std::string> elems;
      eos::common::StringConversion::Tokenize(endpoint, elems, ":");
      ep->set_fqdn(elems[0]);
      int xrd_port, http_port;

      if (elems.size() == 3) {
        try {
          xrd_port = std::stoi(elems[1]);
          http_port = std::stoi(elems[2]);
        } catch (const std::exception& e) {
          return false;
        }

        ep->set_xrd_port(xrd_port);
        ep->set_http_port(http_port);
      } else if (elems.size() == 2) {
        try {
          xrd_port = std::stoi(elems[1]);
        } catch (const std::exception& e) {
          return false;
        }

        ep->set_xrd_port(xrd_port);
        ep->set_http_port(sDefaultHttpPort);
      } else if (elems.size() == 1) {
        ep->set_xrd_port(sDefaultXrdPort);
        ep->set_http_port(sDefaultXrdPort);
      } else {
        return false;
      }
    }
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_route_help()
{
  std::ostringstream oss;
  oss << "Usage: route [ls|link|unlink]" << std::endl
      << "    namespace routing to redirect clients to external instances"
      << std::endl
      << "  route ls " << std::endl
      << "    list all defined routings" << std::endl
      << std::endl
      << "  route link <src_path> <dst_host>[:<xrd_port>[:<http_port>]],..."
      << std::endl
      << "    create routing from src_path to destination host. If the xrd_port"
      << std::endl
      << "    is ommited the default 1094 is used, if the http_port is ommited"
      << std::endl
      << "    the default 8000 is used. Several dst_hosts can be specified by"
      << std::endl
      << "    separating them with \",\". The redirection will go to the MGM"
      << std::endl
      << "    from the specified list"
      << std::endl
      << "    e.g route /eos/dummy/ foo.bar:1094:8000" << std::endl
      << std::endl
      << "  route unlink <src_path" << std::endl
      << "    remove routing from source path" << std::endl;
  std::cerr << oss.str() << std::endl;
}
