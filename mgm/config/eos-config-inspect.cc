// ----------------------------------------------------------------------
// File: eos-config-export.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "mgm/config/ConfigParsing.hh"
#include "common/CLI11.hpp"
#include "common/PasswordHandler.hh"
#include <qclient/QClient.hh>
#include <qclient/ResponseParsing.hh>
#include <qclient/MultiBuilder.hh>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

struct MemberValidator : public CLI::Validator {
  MemberValidator() : Validator("MEMBER")
  {
    func_ = [](const std::string & str) {
      qclient::Members members;

      if (!members.parse(str)) {
        return SSTR("Could not parse members: '" << str <<
                    "'. Expected format is a comma-separated list of servers: example1:1111,example2:2222");
      }

      return std::string();
    };
  }
};

//------------------------------------------------------------------------------
// Given a subcommand, add common-to-all options such as --members
// and --password
//----------------------------------------------------------------------------
void addClusterOptions(CLI::App* subcmd, std::string& membersStr,
                       MemberValidator& memberValidator, std::string& password,
                       std::string& passwordFile)
{
  subcmd->add_option("--members", membersStr,
                     "One or more members of the QDB cluster")
  ->required()
  ->check(memberValidator);
  auto passwordGroup = subcmd->add_option_group("Authentication",
                       "Specify QDB authentication options");
  passwordGroup->add_option("--password", password,
                            "The password for connecting to the QDB cluster - can be empty");
  passwordGroup->add_option("--password-file", passwordFile,
                            "The passwordfile for connecting to the QDB cluster - can be empty");
  passwordGroup->require_option(0, 1);
}

//------------------------------------------------------------------------------
// Check connection to QDB cluster
//----------------------------------------------------------------------------
bool checkConnection(qclient::QClient& qcl)
{
  qclient::redisReplyPtr reply = qcl.exec("PING").get();

  if (!reply) {
    std::cerr << "Could not connect to the given QDB cluster" << std::endl;
    return false;
  }

  if (reply->type != REDIS_REPLY_STATUS ||
      std::string(reply->str, reply->len) != "PONG") {
    std::cerr << "Received unexpected response in checkConnection: " <<
              qclient::describeRedisReply(reply) << std::endl;
    return false;
  }

  return true;
}

int main(int argc, char* argv[])
{
  CLI::App app("Tool to inspect contents of the QuarkDB-based EOS configuration.");
  app.require_subcommand();
  //----------------------------------------------------------------------------
  // Set-up export subcommand..
  //----------------------------------------------------------------------------
  auto exportSubcommand = app.add_subcommand("export",
                          "Read a legacy file-based configuration file, and export to QDB. Ensure the MGM is not running while you run this command!");
  //----------------------------------------------------------------------------
  // Set-up source
  //----------------------------------------------------------------------------
  std::string sourceFile;
  exportSubcommand->add_option("--source", sourceFile,
                               "Path to the source configuration file to export")
  ->required();
  bool overwrite = false;
  exportSubcommand->add_flag("--overwrite", overwrite,
                             "Overwrite already-existing configuration in QDB.");
  //----------------------------------------------------------------------------
  // Set-up target
  //----------------------------------------------------------------------------
  std::string membersStr;
  MemberValidator memberValidator;
  std::string password;
  std::string passwordFile;
  addClusterOptions(exportSubcommand, membersStr, memberValidator, password,
                    passwordFile);

  //----------------------------------------------------------------------------
  // Parse
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // Read source configuration file
  //----------------------------------------------------------------------------
  std::ifstream infile(sourceFile.c_str());

  if (!infile.is_open()) {
    std::cerr << "Unable to open source file" << std::endl;
    return 1;
  }

  std::ostringstream ss;

  while (!infile.eof()) {
    std::string s;
    std::getline(infile, s);

    if (!s.empty()) {
      ss << s << "\n";
    }
  }

  infile.close();
  std::string fullContents = ss.str();
  std::cerr << "--- Successfully read configuration file, size in bytes: " <<
            fullContents.size() << std::endl;
  //----------------------------------------------------------------------------
  // Parse configuration file
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> configuration;
  std::string err;

  if (!eos::mgm::ConfigParsing::parseConfigurationFile(fullContents,
      configuration, err)) {
    std::cerr << "Could not parse configuration file: " << err << std::endl;
    return 1;
  }

  std::cerr << "--- Successfully parsed configuration file" << std::endl;

  //----------------------------------------------------------------------------
  // Validate --password and --password-file options..
  //----------------------------------------------------------------------------
  if (!passwordFile.empty()) {
    if (!eos::common::PasswordHandler::readPasswordFile(passwordFile, password)) {
      std::cerr << "Could not read passwordfile: '" << passwordFile <<
                "'. Ensure the file exists, and its permissions are 400." << std::endl;
      return 1;
    }
  }

  //----------------------------------------------------------------------------
  // Set-up QClient object towards QDB, ensure sanity
  //----------------------------------------------------------------------------
  qclient::Members members = qclient::Members::fromString(membersStr);
  eos::QdbContactDetails contactDetails(members, password);
  qclient::QClient qcl(contactDetails.members, contactDetails.constructOptions());

  //----------------------------------------------------------------------------
  // Ensure connection is sane
  //----------------------------------------------------------------------------
  if (!checkConnection(qcl)) {
    std::cerr << "Could not connect to QDB backend" << std::endl;
    return 1;
  }

  std::cerr << "--- Successfully connected to QDB backend" << std::endl;
  //----------------------------------------------------------------------------
  // Is there any configuration stored in QDB already?
  //----------------------------------------------------------------------------
  qclient::IntegerParser existsResp(qcl.exec("EXISTS",
                                    "eos-config:default").get());

  if (!existsResp.ok()) {
    std::cerr << "Received unexpected response in EXISTS check: " <<
              existsResp.err() << std::endl;
    return 1;
  }

  if (!overwrite && existsResp.value() != 0) {
    std::cerr <<
              "ERROR: There's MGM configuration stored in QDB already -- will not delete." <<
              std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Prepare write batch
  //----------------------------------------------------------------------------
  qclient::MultiBuilder multiBuilder;
  multiBuilder.emplace_back("DEL", "eos-config:default");

  for (auto it = configuration.begin(); it != configuration.end(); it++) {
    multiBuilder.emplace_back("HSET", "eos-config:default", it->first, it->second);
  }

  std::cerr << "--- Prepared write batch towards QDB, " << multiBuilder.size() <<
            " commands" << std::endl;
  std::cerr << "--- Executing write batch: " <<  qclient::describeRedisReply(
              qcl.execute(multiBuilder.getDeque()).get()) << std::endl;
  return 0;
}
