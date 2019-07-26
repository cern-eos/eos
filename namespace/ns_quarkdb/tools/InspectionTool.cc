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

#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/inspector/Inspector.hh"
#include "common/PasswordHandler.hh"
#include "CLI11.hpp"
#include <qclient/QClient.hh>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

struct MemberValidator : public CLI::Validator {
  MemberValidator() : Validator("MEMBER") {
    func_ = [](const std::string &str) {
      qclient::Members members;
      if(!members.parse(str)) {
        return SSTR("Could not parse members: '" << str << "'. Expected format is a comma-separated list of servers: example1:1111,example2:2222");
      }

      return std::string();
    };
  }
};

using namespace eos;

//------------------------------------------------------------------------------
// Given a subcommand, add common-to-all options such as --members
// and --password
//----------------------------------------------------------------------------
void addClusterOptions(CLI::App *subcmd, std::string &membersStr, MemberValidator &memberValidator, std::string &password, std::string &passwordFile) {
  subcmd->add_option("--members", membersStr, "One or more members of the QDB cluster")
    ->required()
    ->check(memberValidator);

  auto passwordGroup = subcmd->add_option_group("Authentication", "Specify QDB authentication options");
  passwordGroup->add_option("--password", password, "The password for connecting to the QDB cluster - can be empty");
  passwordGroup->add_option("--password-file", passwordFile, "The passwordfile for connecting to the QDB cluster - can be empty");
  passwordGroup->require_option(0, 1);
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
  auto dumpSubcommand = app.add_subcommand("dump", "Recursively dump entire namespace contents under a specific path");
  addClusterOptions(dumpSubcommand, membersStr, memberValidator, password, passwordFile);

  std::string dumpPath;
  dumpSubcommand->add_option("--path", dumpPath, "The target path to dump")
    ->required();

  //----------------------------------------------------------------------------
  // Set-up scan-directories subcommand..
  //----------------------------------------------------------------------------
  auto scanDirsSubcommand = app.add_subcommand("scan-dirs", "Dump the full list of container metadata across the entire namespace");
  addClusterOptions(scanDirsSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up scan-files subcommand..
  //----------------------------------------------------------------------------
  auto scanFilesSubcommand = app.add_subcommand("scan-files", "Dump the full list of file metadata across the entire namespace");
  addClusterOptions(scanFilesSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up check-naming-conflicts subcommand..
  //----------------------------------------------------------------------------
  auto namingConflictsSubcommand = app.add_subcommand("check-naming-conflicts", "Scan through the entire namespace looking for naming conflicts");
  addClusterOptions(namingConflictsSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up stripediff subcommand..
  //----------------------------------------------------------------------------
  auto stripediffSubcommand = app.add_subcommand("stripediff", "Find files which have non-nominal number of stripes (replicas)");
  addClusterOptions(stripediffSubcommand, membersStr, memberValidator, password, passwordFile);

  bool printTime = false;
  stripediffSubcommand->add_flag("--time", printTime, "Print mtime and ctime of found files");

  //----------------------------------------------------------------------------
  // Set-up one-replica-layout subcommand..
  //----------------------------------------------------------------------------
  auto oneReplicaLayoutSubcommand = app.add_subcommand("one-replica-layout", "Find all files whose layout asks for a single replica");
  addClusterOptions(oneReplicaLayoutSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up check-orphans subcommand..
  //----------------------------------------------------------------------------
  auto checkOrphansSubcommand = app.add_subcommand("check-orphans", "Find files and directories with invalid parents");
  addClusterOptions(checkOrphansSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up check-fsview-missing subcommand..
  //----------------------------------------------------------------------------
  auto checkFsViewMissingSubcommand = app.add_subcommand("check-fsview-missing", "Check which FileMDs have locations / unlinked locations not present in the filesystem view");
  addClusterOptions(checkFsViewMissingSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up check-fsview-extra subcommand..
  //----------------------------------------------------------------------------
  auto checkFsViewExtraSubcommand = app.add_subcommand("check-fsview-extra", "Check whether there exist FsView entries without a corresponding FMD location");
  addClusterOptions(checkFsViewExtraSubcommand, membersStr, memberValidator, password, passwordFile);

  //----------------------------------------------------------------------------
  // Set-up print subcommand..
  //----------------------------------------------------------------------------
  auto printSubcommand = app.add_subcommand("print", "Print everything known about a given file, or container");
  addClusterOptions(printSubcommand, membersStr, memberValidator, password, passwordFile);

  uint64_t fid;
  printSubcommand->add_option("--fid", fid, "Specify the FileMD to print, through its ID (decimal form)");

  //----------------------------------------------------------------------------
  // Change fid protobuf properties
  //----------------------------------------------------------------------------
  auto changeFidSubcommand = app.add_subcommand("change-fid", "Change specified properties of a single fid. Better know what you're doing before using this!");
  addClusterOptions(changeFidSubcommand, membersStr, memberValidator, password, passwordFile);

  uint64_t newParent = 0;
  changeFidSubcommand->add_option("--fid", fid, "Specify the FileMD to print, through its ID (decimal form)");
  changeFidSubcommand->add_option("--new-parent", newParent, "Change the parent container of the specified fid. This _DOES NOT_ modify the respective container maps, only the protobuf FMD!");

  //----------------------------------------------------------------------------
  // Rename a fid from its current location
  //----------------------------------------------------------------------------
  std::string newName;

  auto renameFidSubcommand = app.add_subcommand("rename-fid", "Rename a file onto the specified container ID - the respective container maps are modified as well.");
  addClusterOptions(renameFidSubcommand, membersStr, memberValidator, password, passwordFile);

  renameFidSubcommand->add_option("--fid", fid, "Specify the FileMD to rename")
    ->required();
  renameFidSubcommand->add_option("--destination-cid", newParent, "The destination container ID in which to put the FileMD")
    ->required();
  renameFidSubcommand->add_option("--new-pathname", newName, "The new name of the specified fid - must only contain alphanumeric characters, and can be left empty");

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

  //----------------------------------------------------------------------------
  // Set-up Inspector object, ensure sanity
  //----------------------------------------------------------------------------
  Inspector inspector(qcl);
  std::string connectionErr;

  if(!inspector.checkConnection(connectionErr)) {
    std::cerr << connectionErr << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Dispatch subcommand
  //----------------------------------------------------------------------------
  if(dumpSubcommand->parsed()) {
    return inspector.dump(dumpPath, std::cout);
  }

  if(namingConflictsSubcommand->parsed()) {
    return inspector.checkNamingConflicts(std::cout, std::cerr);
  }

  if(printSubcommand->parsed()) {
    return inspector.printFileMD(fid, std::cout, std::cerr);
  }

  if(scanDirsSubcommand->parsed()) {
    return inspector.scanDirs(std::cout, std::cerr);
  }

  if(stripediffSubcommand->parsed()) {
    return inspector.stripediff(printTime, std::cout, std::cerr);
  }

  if(oneReplicaLayoutSubcommand->parsed()) {
    return inspector.oneReplicaLayout(std::cout, std::cerr);
  }

  if(scanFilesSubcommand->parsed()) {
    return inspector.scanFileMetadata(std::cout, std::cerr);
  }

  if(checkOrphansSubcommand->parsed()) {
    return inspector.checkOrphans(std::cout, std::cerr);
  }

  if(checkFsViewMissingSubcommand->parsed()) {
    return inspector.checkFsViewMissing(std::cout, std::cerr);
  }

  if(checkFsViewExtraSubcommand->parsed()) {
    return inspector.checkFsViewExtra(std::cout, std::cerr);
  }

  if(changeFidSubcommand->parsed()) {
    return inspector.changeFid(fid, newParent, std::cout, std::cerr);
  }

  if(renameFidSubcommand->parsed()) {
    return inspector.renameFid(fid, newParent, newName, std::cout, std::cerr);
  }

  std::cerr << "No subcommand was supplied - should never reach here" << std::endl;
  return 1;
}
