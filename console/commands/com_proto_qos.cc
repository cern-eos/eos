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
  //----------------------------------------------------------------------------
  QoSHelper() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QoSHelper() = default;

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

  // Lambda function to parse and set QoS <key>=<value> pair
  auto parseKVPair = [](std::string token) {
    eos::console::QoSProto_KVPairProto* pair = NULL;
    size_t pos = token.find('=');

    if ((pos != std::string::npos) &&
        (pos > 0) && (pos < token.length() - 1)) {
      pair = new eos::console::QoSProto_KVPairProto {};
      pair->set_key(token.substr(0, pos));
      pair->set_value(token.substr(pos + 1));
    }

    return pair;
  };

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "get") {
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

    while (tokenizer.NextToken(token)) {
      auto kvPair = parseKVPair(token);

      if (kvPair == NULL) {
        std::cerr << "error: invalid pair '" << token.c_str()
                  << "'" << std::endl;
        return false;
      }

      set->mutable_pair()->AddAllocated(kvPair);
    }

    if (set->pair_size() == 0) {
      return false;
    }
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

  QoSHelper qos;

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
  oss << "Usage: qos get <identifier> [<key>]        : get QoS property of item" << std::endl
      << "       qos set <identifier> <key>=<value>  : set QoS property of item" << std::endl
      << std::endl
      << "Note: <identifier> = fid|fxid|path" << std::endl
      << "      Recognized `qos get` keys: all | cdmi | checksum | disksize | layout |" << std::endl
      << "                                 id | path | placement | redundancy | size" << std::endl
      << std::endl
      << "      Allowed `qos set` properties:" << std::endl
      << "          checksum  = none | adler32 | crc32 | crc32c | md5 | sha1" << std::endl
      << "          layout    = plain | replica | raiddp | raid5 | raid6 | qrain | archive" << std::endl
      << "          placement = gathered | hybrid | scattered" << std::endl
      << "          replica   = integer between 1 - 16"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
