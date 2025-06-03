//------------------------------------------------------------------------------
// File com_proto_convert.cc
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

#include "common/LayoutId.hh"
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_convert_help();

//------------------------------------------------------------------------------
//! Class ConvertHelper
//------------------------------------------------------------------------------
class ConvertHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  ConvertHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConvertHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;

private:
  //----------------------------------------------------------------------------
  //! Parse identifier string and construct identifier proto object.
  //----------------------------------------------------------------------------
  eos::console::ConvertProto_IdentifierProto*
  ParseIdentifier(std::string path);

  //----------------------------------------------------------------------------
  //! Parse conversion string and construct conversion proto object.
  //! Returns null if conversion string is invalid.
  //----------------------------------------------------------------------------
  eos::console::ConvertProto_ConversionProto*
  ParseConversion(eos::common::StringTokenizer& tokenizer);
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
ConvertHelper::ParseCommand(const char* arg)
{
  using eos::console::ConvertProto_ConfigProto;
  eos::console::ConvertProto* convert = mReq.mutable_convert();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "config") {
    ConvertProto_ConfigProto* config = convert->mutable_config();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "list") {
      config->set_op(ConvertProto_ConfigProto::LIST);
    } else if (token == "set") {
      if (!tokenizer.NextToken(token)) {
        std::cerr << "error: config set takes a parameter" << std::endl;
        return false;
      }

      size_t pos = token.find("=");

      if ((pos == std::string::npos) ||
          (pos == token.length() - 1)) {
        std::cerr << "error: config set takes a key=value parameter"
                  << std::endl;
        return false;
      }

      config->set_op(ConvertProto_ConfigProto::SET);
      config->set_key(token.substr(0, pos));
      config->set_value(token.substr(pos + 1));
    } else {
      std::cerr << "error: unknown config option '"
                << token << "'" << std::endl;
      return false;
    }
  } else if (token == "file") {
    eos::console::ConvertProto_FileProto* file = convert->mutable_file();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    file->set_allocated_identifier(ParseIdentifier(token));
    eos::console::ConvertProto_ConversionProto*
    conversion = ParseConversion(tokenizer);

    if (conversion == nullptr) {
      return false;
    }

    file ->set_allocated_conversion(conversion);
    // Placeholder for options
  } else if (token == "rule") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::ConvertProto_RuleProto* rule = convert->mutable_rule();
    rule->set_allocated_identifier(ParseIdentifier(token));
    eos::console::ConvertProto_ConversionProto*
    conversion = ParseConversion(tokenizer);

    if (conversion == nullptr) {
      return false;
    }

    rule->set_allocated_conversion(conversion);
  } else if (token == "list") {
    convert->mutable_list();
  } else if (token == "clear") {
    convert->mutable_clear();
  } else {
    return false;
  }

  return true;
}

//----------------------------------------------------------------------------
// Parse string identifier and construct identifier proto object
//----------------------------------------------------------------------------
eos::console::ConvertProto_IdentifierProto*
ConvertHelper::ParseIdentifier(std::string spath)
{
  XrdOucString path = spath.c_str();
  auto identifier = new eos::console::ConvertProto_IdentifierProto{};
  auto id = 0ull;

  if (Path2FileDenominator(path, id)) {
    identifier->set_fileid(id);
  } else if (Path2ContainerDenominator(path, id)) {
    identifier->set_containerid(id);
  } else {
    identifier->set_path(abspath(path.c_str()));
  }

  return identifier;
}

//----------------------------------------------------------------------------
// Parse conversion string and construct conversion proto object.
// Returns null if conversion string is invalid
//----------------------------------------------------------------------------
eos::console::ConvertProto_ConversionProto*
ConvertHelper::ParseConversion(eos::common::StringTokenizer& tokenizer)
{
  std::string token;
  std::string layout;
  std::string space;
  std::string placement;
  std::string checksum;
  int replica = 0;
  size_t pos;
  bool ok;
  // Lambda function to validate layout string
  auto validLayout = [](const std::string & layout) {
    return eos::common::LayoutId::GetLayoutFromString(layout) != -1;
  };
  // Lambda function to validate placement policy string
  auto validPlacement = [](const std::string & placement) {
    if (placement == "scattered" || placement == "hybrid" ||
        placement == "gathered") {
      return true;
    }

    return false;
  };
  // Lambda function to validate checksum string
  auto validChecksum = [](const std::string & checksum) {
    using eos::common::LayoutId;
    auto xs_id = LayoutId::GetChecksumFromString(checksum);
    return ((xs_id > -1) && (xs_id != LayoutId::eChecksum::kNone));
  };

  if (!tokenizer.NextToken(token)) {
    std::cerr << "error: missing <layout:replica> argument" << std::endl;
    return nullptr;
  }

  if ((pos = token.find(":")) == std::string::npos) {
    std::cerr << "error: invalid <layout:replica> format" << std::endl;
    return nullptr;
  }

  layout = token.substr(0, pos);

  try {
    replica = std::stol(token.substr(pos + 1));
  } catch (...) {
    std::cerr << "error: failed to interpret replica number '"
              << token.substr(pos + 1) << "'" << std::endl;
    return nullptr;
  }

  if (!validLayout(layout)) {
    std::cerr << "error: invalid layout '" << layout << "'" << std::endl;
    return nullptr;
  }

  if (replica < 1 || replica > 32) {
    std::cerr << "error: invalid replica number=" << replica
              << " (must be between 1 and 32)" << std::endl;
    return nullptr;
  }

  while (tokenizer.NextToken(token)) {
    if ((ok = validChecksum(token))) {
      checksum = std::move(token);
    } else if ((ok = validPlacement(token))) {
      placement = std::move(token);
    } else if ((ok = space.empty())) {
      space = std::move(token);
    }

    if (!ok) {
      std::cerr << "error: could not interpret '" << token << "' argument"
                << std::endl;
      return nullptr;
    }
  }

  auto conversion = new eos::console::ConvertProto_ConversionProto{};
  conversion->set_layout(layout);
  conversion->set_replica(replica);
  conversion->set_space(space);
  conversion->set_placement(placement);
  conversion->set_checksum(checksum);
  return conversion;
}

//------------------------------------------------------------------------------
// Convert command entry point
//------------------------------------------------------------------------------
int com_convert(char* arg)
{
  if (wants_help(arg)) {
    com_convert_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  ConvertHelper convert(gGlobalOpts);

  if (!convert.ParseCommand(arg)) {
    com_convert_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = convert.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_convert_help()
{
  std::ostringstream oss;
  oss << "Usage: convert <subcomand>                         \n"
      << "  convert config list|set [<key>=<value>]          \n"
      << "    list: list converter configuration parameters and status\n"
      << "    set : set converter configuration parameters. Options:\n"
      << "      max-thread-pool-size: max number of threads in converter pool [default 100]\n"
      << "      max-queue-size      : max number of queued conversion jobs [default 1000]\n"
      << std::endl
      << "  convert list                                     \n"
      << "    list conversion jobs                           \n"
      << std::endl
      << "  convert clear                                    \n"
      << "    clear list of jobs stored in the backend       \n"
      << std::endl
      << "  convert file <identifier> <conversion>           \n"
      << "    schedule a file conversion                     \n"
      << "    <identifier> = fid|fxid|path                   \n"
      << "    <conversion> = <layout:replica> [space] [placement] [checksum]\n"
      << std::endl
      << "  convert rule <identifier> <conversion>           \n"
      << "    apply a conversion rule on the given directory \n"
      << "    <identifier> = cid|cxid|path                   \n"
      << "    <conversion> = <layout:replica> [space] [placement] [checksum]\n";
  std::cerr << oss.str() << std::endl;
}
