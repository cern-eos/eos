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

#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "common/PasswordHandler.hh"
#include "CLI11.hpp"
#include <qclient/QClient.hh>
#include "qclient/structures/QLocalityHash.hh"
#include <folly/executors/IOThreadPoolExecutor.h>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

struct MemberValidator : public CLI::Validator {
  MemberValidator() {
    tname = "MEMBER";
    func = [](const std::string &str) {
      qclient::Members members;
      if(!members.parse(str)) {
        return SSTR("Could not parse members: '" << str << "'. Expected format is a comma-separated list of servers: example1:1111,example2:2222");
      }

      return std::string();
    };
  }
};

using namespace eos;

int runDump(qclient::QClient &qcl, const std::string &dumpPath) {
  ExplorationOptions explorerOpts;
  std::unique_ptr<folly::Executor> executor(new folly::IOThreadPoolExecutor(4));

  NamespaceExplorer explorer(dumpPath, explorerOpts, qcl, executor.get());
  NamespaceItem item;

  while(explorer.fetch(item)) {
    std::cout << "path=" << item.fullPath << std::endl;
  }

  return 0;
}

int runConsistencyCheck(qclient::QClient &qcl) {
  qclient::QLocalityHash::Iterator iter(&qcl, "eos-container-md");

  std::map<std::string, uint64_t> containerContents;
  uint64_t currentContainer = -1;
  int64_t processed = 0;

  for(; iter.valid(); iter.next()) {
    processed++;
    std::string item = iter.getValue();

    eos::ns::ContainerMdProto proto;
    eos::MDStatus status = Serialization::deserialize(item.c_str(), item.size(), proto);

    if(!status.ok()) {
      std::cerr << "Error while deserializing: " << item << std::endl;
      continue;
    }

    if(processed % 100000 == 0) {
      std::cerr << "Processed so far: " << processed << std::endl;
    }

    if(currentContainer == proto.parent_id()) {
      auto conflict = containerContents.find(proto.name());
      if(conflict != containerContents.end()) {
        std::cout << "Detected conflict for '" << proto.name() << "' in container " << currentContainer << ", between containers " << conflict->second << " and " << proto.id() << std::endl;
      }
    }
    else {
      currentContainer = proto.parent_id();
      containerContents.clear();
    }

    containerContents[proto.name()] = proto.id();
  }

  return 0;
}

int main(int argc, char* argv[]) {
  CLI::App app("Tool to inspect contents of the QuarkDB-based EOS namespace.");
  app.require_subcommand();

  //----------------------------------------------------------------------------
  // Basic parameters, common to all subcommands
  //----------------------------------------------------------------------------
  std::string membersStr;
  MemberValidator memberValidator;

  std::string password;
  std::string passwordFile;

  //----------------------------------------------------------------------------
  // Set-up dump subcommand..
  //----------------------------------------------------------------------------
  auto dumpSubcommand = app.add_subcommand("dump", "Dump entire namespace contents under a specific path");

  std::string dumpPath;
  dumpSubcommand->add_option("--path", dumpPath, "The target path to dump")
    ->required();

  dumpSubcommand->add_option("--members", membersStr, "One or more members of the QDB cluster")
    ->required()
    ->check(memberValidator);

  dumpSubcommand->add_option("--password", password, "The password for connecting to the QDB cluster - can be empty");
  dumpSubcommand->add_option("--password-file", passwordFile, "The passwordfile for connecting to the QDB cluster - can be empty");

  //----------------------------------------------------------------------------
  // Set-up consistency-check subcommand..
  //----------------------------------------------------------------------------
  auto consistencyCheckSubcommand  = app.add_subcommand("consistency-check", "Scan through the entire namespace for inconsistencies");

  consistencyCheckSubcommand->add_option("--members", membersStr, "One or more members of the QDB cluster")
    ->required()
    ->check(memberValidator);

  consistencyCheckSubcommand->add_option("--password", password, "The password for connecting to the QDB cluster - can be empty");
  consistencyCheckSubcommand->add_option("--password-file", passwordFile, "The passwordfile for connecting to the QDB cluster - can be empty");

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // Validate --password and --password-file options..
  //----------------------------------------------------------------------------
  if(!password.empty() && !passwordFile.empty()) {
    std::cerr << "Only one of --password / --password-file is allowed." << std::endl;
    return 1;
  }

  if(!passwordFile.empty()) {
    if(!common::PasswordHandler::readPasswordFile(passwordFile, password)) {
      std::cerr << "Could not read passwordfile: '" << passwordFile << "'. Ensure the file exists, and its permissions are 400." << std::endl;
      return 1;
    }
  }

  //----------------------------------------------------------------------------
  // Set-up QClient object towards QDB, ensure sanity
  //----------------------------------------------------------------------------
  qclient::Members members = qclient::Members::fromString(membersStr);
  QdbContactDetails contactDetails(members, password);

  qclient::QClient qcl(contactDetails.members, contactDetails.constructOptions());

  qclient::redisReplyPtr reply = qcl.exec("PING").get();
  if(!reply) {
    std::cerr << "Could not connect to the given QDB cluster!" << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Dispatch subcommand
  //----------------------------------------------------------------------------
  if(dumpSubcommand->parsed()) {
    return runDump(qcl, dumpPath);
  }

  if(consistencyCheckSubcommand->parsed()) {
    return runConsistencyCheck(qcl);
  }

  std::cerr << "No subcommand was supplied - should never happen" << std::endl;
  return 1;
}
