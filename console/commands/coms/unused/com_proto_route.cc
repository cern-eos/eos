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
#include "common/ParseUtils.hh"
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
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  RouteHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

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

private:
  //----------------------------------------------------------------------------
  //! Check path validity - it shouldn't contain spaces, '/./' or '/../' or
  //! backslash characters. Append if necessary and end '/'.
  //!
  //! @param path given path
  //!
  //! @return true if valid, otherwise false
  //----------------------------------------------------------------------------
  bool ValidatePath(std::string& path) const;
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
    using eos::console::RouteProto_ListProto;
    RouteProto_ListProto* list = route->mutable_list();

    if (!(option = tokenizer.GetToken())) {
      list->set_path("");
    } else {
      soption = option;

      if (!ValidatePath(soption)) {
        return false;
      }

      list->set_path(soption);
    }
  } else if (cmd == "unlink") {
    using eos::console::RouteProto_UnlinkProto;
    RouteProto_UnlinkProto* unlink = route->mutable_unlink();

    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    // Do basic checks that this is a path
    soption = option;

    if (!ValidatePath(soption)) {
      return false;
    }

    unlink->set_path(soption);
  } else if (cmd == "link") {
    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    // Do basic checks that this is a path
    soption = option;

    if (!ValidatePath(soption)) {
      return false;
    }

    eos::console::RouteProto_LinkProto* link = route->mutable_link();
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
      const std::string fqdn = elems[0];

      if (!eos::common::ValidHostnameOrIP(fqdn)) {
        std::cerr << "error: invalid hostname specified" << std::endl;
        return false;
      }

      uint32_t xrd_port = sDefaultXrdPort;
      uint32_t http_port = sDefaultHttpPort;

      if (elems.size() == 3) {
        try {
          xrd_port = std::stoul(elems[1]);
          http_port = std::stoul(elems[2]);
        } catch (const std::exception& e) {
          std::cerr << "error: failed to parse ports for route" << std::endl;
          return false;
        }
      } else if (elems.size() == 2) {
        try {
          xrd_port = std::stoi(elems[1]);
        } catch (const std::exception& e) {
          std::cerr << "error: failed to parse xrd port for route" << std::endl;
          return false;
        }
      }

      ep->set_fqdn(fqdn);
      ep->set_xrd_port(xrd_port);
      ep->set_http_port(http_port);
    }
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if path is valid
//------------------------------------------------------------------------------
bool
RouteHelper::ValidatePath(std::string& path) const
{
  if (path.empty() || path[0] != '/') {
    std::cerr << "error: path should be non-empty and start with '/'"
              << std::endl;
    return false;
  }

  if (path.back() != '/') {
    path += '/';
  }

  std::set<std::string> forbidden {" ", "/../", "/./", "\\"};

  for (const auto& needle : forbidden) {
    if (path.find(needle) != std::string::npos) {
      std::cerr << "error: path should no contain any of the following "
                << "sequences of characters: \" \", \"/../\", \"/./\" or "
                << "\"\\\"" << std::endl;
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Route command entrypoint
//------------------------------------------------------------------------------
int com_route(char* arg)
{
  if (wants_help(arg)) {
    com_route_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  RouteHelper route(gGlobalOpts);

  if (!route.ParseCommand(arg)) {
    com_route_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = route.Execute();

  return global_retc;
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
      << std::endl
      << "  route ls [<path>]" << std::endl
      << "    list all routes or the one matching for the given path"
      << std::endl
      << "      * as the first character means the node is a master"
      << std::endl
      << "      _ as the first character means the node is offline"
      << std::endl
      << std::endl
      << "  route link <path> <dst_host>[:<xrd_port>[:<http_port>]],..."
      << std::endl
      << "    create routing from <path> to destination host. If the xrd_port"
      << std::endl
      << "    is omitted the default 1094 is used, if the http_port is omitted"
      << std::endl
      << "    the default 8000 is used. Several dst_hosts can be specified by"
      << std::endl
      << "    separating them with \",\". The redirection will go to the MGM"
      << std::endl
      << "    from the specified list"
      << std::endl
      << "    e.g route /eos/dummy/ foo.bar:1094:8000" << std::endl
      << std::endl
      << "  route unlink <path>" << std::endl
      << "    remove routing matching path" << std::endl;
  std::cerr << oss.str() << std::endl;
}
