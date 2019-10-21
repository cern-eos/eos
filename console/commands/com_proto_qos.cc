//------------------------------------------------------------------------------
// File com_proto_qos.cc
// Author: Mihai Patrascoiu - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_qos_help();

//------------------------------------------------------------------------------
//! Class QoSHelper
//------------------------------------------------------------------------------
class QoSHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  QoSHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QoSHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
QoSHelper::ParseCommand(const char* arg)
{
  eos::console::QoSProto* qos = mReq.mutable_qos();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  // Lambda function to parse identifier
  auto parseIdentifier = [](XrdOucString path) {
    auto identifier = new eos::console::QoSProto_IdentifierProto {};
    auto id = 0ull;

    if (Path2FileDenominator(path, id)) {
      identifier->set_fileid(id);
    } else {
      path = abspath(path.c_str());
      identifier->set_path(path.c_str());
    }

    return identifier;
  };

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "list") {
    eos::console::QoSProto_ListProto* list = qos->mutable_list();

    if (tokenizer.NextToken(token)) {
      list->set_classname(token);
    }
  } else if (token == "get") {
    eos::console::QoSProto_GetProto* get = qos->mutable_get();
    XrdOucString path;

    if (!tokenizer.NextToken(path)) {
      return false;
    }

    get->set_allocated_identifier(parseIdentifier(path));

    while (tokenizer.NextToken(token)) {
      get->add_key(token);
    }
  } else if (token == "set") {
    eos::console::QoSProto_SetProto* set = qos->mutable_set();
    XrdOucString path;

    if (!tokenizer.NextToken(path)) {
      return false;
    }

    set->set_allocated_identifier(parseIdentifier(path));

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    set->set_classname(token);
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// QoS command entry point
//------------------------------------------------------------------------------
int com_qos(char* arg)
{
  if (wants_help(arg)) {
    com_qos_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  QoSHelper qos(gGlobalOpts);

  if (!qos.ParseCommand(arg)) {
    com_qos_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = qos.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_qos_help()
{
  std::ostringstream oss;
  oss << "Usage: qos list [<name>]               : list available QoS classes" << std::endl
      << "                                         If <name> is provided, list the properties of the given class" << std::endl
      << "       qos get <identifier> [<key>]    : get QoS property of item" << std::endl
      << "                                         If no <key> is provided, defaults to 'all'" << std::endl
      << "       qos set <identifier> <class>    : set QoS class of item" << std::endl
      << std::endl
      << "Note: <identifier> = fid|fxid|cid|cxid|path" << std::endl
      << "      Recognized `qos get` keys: all | cdmi | checksum | class | disksize |" << std::endl
      << "                                 layout | id | path | placement | replica | size" << std::endl;
  std::cerr << oss.str() << std::endl;
}
