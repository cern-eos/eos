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
#include "namespace/ns_quarkdb/inspector/OutputSink.hh"
#include "common/PasswordHandler.hh"
#include "common/ParseUtils.hh"
#include "common/CLI11.hpp"
#include <qclient/QClient.hh>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

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

struct IdValidator : public CLI::Validator {
  IdValidator() : Validator("ID")
  {
    func_ = [](const std::string & str) {
      uint64_t parsed;

      if (!eos::common::ParseUInt64(str, parsed)) {
        return SSTR("Could not parse id, was expecting uint64_t: '" << str << "'");
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
// Control dry-run, common to all dangerous commands
//----------------------------------------------------------------------------
void addDryRun(CLI::App* subcmd, bool& noDryRun)
{
  subcmd->add_flag("--no-dry-run", noDryRun,
                   "Execute changes for real.\nIf not supplied, planned changes are only shown and not applied.");
}

int main(int argc, char* argv[])
{
  CLI::App app("Tool to inspect contents of the QuarkDB-based EOS namespace.");
  app.require_subcommand();
  //----------------------------------------------------------------------------
  // Basic parameters, common to all subcommands
  //----------------------------------------------------------------------------
  std::string membersStr;
  MemberValidator memberValidator;
  std::string password;
  std::string passwordFile;
  bool noDryRun = false;
  //----------------------------------------------------------------------------
  // Set-up dump subcommand..
  //----------------------------------------------------------------------------
  auto dumpSubcommand = app.add_subcommand("dump",
                        "[DEPRECATED] Recursively dump entire namespace contents under a specific path");
  addClusterOptions(dumpSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  std::string dumpPath;
  std::string attrQuery;
  bool relativePaths = false;
  bool rawPaths = false;
  bool noDirs = false;
  bool noFiles = false;
  bool showSize = false;
  bool showMtime = false;
  bool withParents = false;
  bool json = false;
  dumpSubcommand->add_option("--path", dumpPath, "The target path to dump")
  ->required();
  dumpSubcommand->add_option("--attr-query", attrQuery,
                             "Print the specified extended attribute");
  dumpSubcommand->add_flag("--relative-paths", relativePaths,
                           "Print paths relative to --path");
  dumpSubcommand->add_flag("--raw-paths", rawPaths,
                           "Print the raw paths without path= in front, and nothing else");
  dumpSubcommand->add_flag("--no-dirs", noDirs,
                           "Don't print directories, only files");
  dumpSubcommand->add_flag("--no-files", noFiles,
                           "Don't print files, only directories");
  dumpSubcommand->add_flag("--show-size", showSize, "Show file size");
  dumpSubcommand->add_flag("--show-mtime", showMtime,
                           "Show file modification time");
  //----------------------------------------------------------------------------
  // Set-up scan subcommand..
  //----------------------------------------------------------------------------
  auto scanSubcommand = app.add_subcommand("scan",
                        "Recursively scan and print entire namespace contents under a specific path");
  addClusterOptions(scanSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  scanSubcommand->add_option("--path", dumpPath, "The target path to scan")
  ->required();
  scanSubcommand->add_flag("--relative-paths", relativePaths,
                           "Print paths relative to --path");
  scanSubcommand->add_flag("--raw-paths", rawPaths,
                           "Print the raw paths without path= in front, and nothing else");
  scanSubcommand->add_flag("--no-dirs", noDirs,
                           "Don't print directories, only files");
  scanSubcommand->add_flag("--no-files", noFiles,
                           "Don't print files, only directories");
  scanSubcommand->add_flag("--json", json, "Use json output");
  //----------------------------------------------------------------------------
  // Set-up print subcommand..
  //----------------------------------------------------------------------------
  auto printSubcommand = app.add_subcommand("print",
                         "Print everything known about a given file, or container");
  addClusterOptions(printSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  printSubcommand->add_flag("--with-parents", withParents,
                            "Show detailed information for each parent container as well");
  uint64_t fid = 0;
  uint64_t cid = 0;
  auto idGroup = printSubcommand->add_option_group("ID", "Specify what to print");
  idGroup->add_option("--fid", fid,
                      "Specify the FileMD to print, through its ID (decimal form)");
  idGroup->add_option("--cid", cid,
                      "Specify the ContainerMD to print, through its ID (decimal form)");
  idGroup->require_option(1, 1);
  //----------------------------------------------------------------------------
  // Set-up stripediff subcommand..
  //----------------------------------------------------------------------------
  auto stripediffSubcommand = app.add_subcommand("stripediff",
                              "Find files which have non-nominal number of stripes (replicas)");
  addClusterOptions(stripediffSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  bool printTime = false;
  stripediffSubcommand->add_flag("--time", printTime,
                                 "Print mtime and ctime of found files");
  //----------------------------------------------------------------------------
  // Set-up one-replica-layout subcommand..
  //----------------------------------------------------------------------------
  auto oneReplicaLayoutSubcommand = app.add_subcommand("one-replica-layout",
                                    "Find all files whose layout asks for a single replica");
  addClusterOptions(oneReplicaLayoutSubcommand, membersStr, memberValidator,
                    password, passwordFile);
  bool showName = false;
  bool fullPaths = false;
  bool filterInternal = false;
  oneReplicaLayoutSubcommand->add_flag("--show-name", showName, "Show filenames");
  oneReplicaLayoutSubcommand->add_flag("--full-paths", fullPaths,
                                       "Show full paths, if possible");
  oneReplicaLayoutSubcommand->add_flag("--filter-internal", filterInternal,
                                       "Filter internal entries, such as versioning, aborted atomic uploads, etc");
  //----------------------------------------------------------------------------
  // Set-up scan-dirs subcommand..
  //----------------------------------------------------------------------------
  auto scanDirsSubcommand = app.add_subcommand("scan-dirs",
                            "Dump the full list of container metadata across the entire namespace");
  addClusterOptions(scanDirsSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  bool onlyNoAttrs = false;
  bool countContents = false;
  size_t countThreshold = 0;
  scanDirsSubcommand->add_flag("--only-no-attrs", onlyNoAttrs,
                               "Only show directories which have no extended attributes whatsoever");
  scanDirsSubcommand->add_flag("--full-paths", fullPaths,
                               "Show full container paths, if possible");
  scanDirsSubcommand->add_flag("--count-contents", countContents,
                               "Count how many files and containers are in each directory (non-recursive)");
  scanDirsSubcommand->add_option("--count-threshold", countThreshold,
                                 "Only print containers which contain more than the specified number of items. Useful for detecting huge containers on which 'ls' might hang");
  scanDirsSubcommand->add_flag("--json", json, "Use json output");
  //----------------------------------------------------------------------------
  // Set-up scan-files subcommand..
  //----------------------------------------------------------------------------
  auto scanFilesSubcommand = app.add_subcommand("scan-files",
                             "Dump the full list of file metadata across the entire namespace");
  addClusterOptions(scanFilesSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  bool onlySizes = false;
  bool findUnknownFsids = false;

  scanFilesSubcommand->add_flag("--only-sizes", onlySizes,
                                "Only print file sizes, one per line.");
  scanFilesSubcommand->add_flag("--full-paths", fullPaths,
                                "Show full file paths, if possible");
  scanFilesSubcommand->add_flag("--find-unknown-fsids", findUnknownFsids,
                                "Only print files for which there is one or more unrecognized fsids in location vector.");
  scanFilesSubcommand->add_flag("--json", json, "Use json output");
  //----------------------------------------------------------------------------
  // Set-up scan-deathrow subcommand..
  //----------------------------------------------------------------------------
  auto scanDeathrowSubcommand = app.add_subcommand("scan-deathrow",
                                "Show all files currently scheduled to be deleted");
  addClusterOptions(scanDeathrowSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up check-naming-conflicts subcommand..
  //----------------------------------------------------------------------------
  auto namingConflictsSubcommand = app.add_subcommand("check-naming-conflicts",
                                   "Scan through the entire namespace looking for naming conflicts");
  addClusterOptions(namingConflictsSubcommand, membersStr, memberValidator,
                    password, passwordFile);
  bool onePerLine = false;
  namingConflictsSubcommand->add_flag("--one-per-line", onePerLine,
                                      "Don't group results in a single line - useful to count how many conflicts there are in total");
  //----------------------------------------------------------------------------
  // Set-up check-cursed-names subcommand..
  //----------------------------------------------------------------------------
  auto cursedNamesSubcommand = app.add_subcommand("check-cursed-names",
                               "Scan through the namespace to find files / containers with invalid names");
  addClusterOptions(cursedNamesSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up check-orphans subcommand..
  //----------------------------------------------------------------------------
  auto checkOrphansSubcommand = app.add_subcommand("check-orphans",
                                "Find files and directories with invalid parents");
  addClusterOptions(checkOrphansSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up check-fsview-missing subcommand..
  //----------------------------------------------------------------------------
  auto checkFsViewMissingSubcommand = app.add_subcommand("check-fsview-missing",
                                      "Check which FileMDs have locations / unlinked locations not present in the filesystem view");
  addClusterOptions(checkFsViewMissingSubcommand, membersStr, memberValidator,
                    password, passwordFile);
  //----------------------------------------------------------------------------
  // Set-up check-fsview-extra subcommand..
  //----------------------------------------------------------------------------
  auto checkFsViewExtraSubcommand = app.add_subcommand("check-fsview-extra",
                                    "Check whether there exist FsView entries without a corresponding FMD location");
  addClusterOptions(checkFsViewExtraSubcommand, membersStr, memberValidator,
                    password, passwordFile);
  //----------------------------------------------------------------------------
  // Set-up check-shadow-directories subcommand..
  //----------------------------------------------------------------------------
  auto checkShadowDirectories = app.add_subcommand("check-shadow-directories",
                                "Check for naming conflicts between directories inside the same subdirectory");
  addClusterOptions(checkShadowDirectories, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up check-simulated-hardlinks subcommand..
  //----------------------------------------------------------------------------
  auto checkSimulatedHardlinks = app.add_subcommand("check-simulated-hardlinks",
                                 "Check for corruption in simulated hardlinks");
  addClusterOptions(checkSimulatedHardlinks, membersStr, memberValidator,
                    password, passwordFile);
  //----------------------------------------------------------------------------
  // Set-up fix-detached-parent subcommand..
  //----------------------------------------------------------------------------
  auto fixDetachedParent = app.add_subcommand("fix-detached-parent",
                           "[CAUTION] Attempt to fix a detached parent of the given fid / cid,\nby re-creating said parent in a given destination");
  addClusterOptions(fixDetachedParent, membersStr, memberValidator, password,
                    passwordFile);
  addDryRun(fixDetachedParent, noDryRun);
  std::string destinationPath;
  fixDetachedParent->add_option("--destination-path", destinationPath,
                                "Path in which the detached file / container will be stored.")
  ->required();
  auto idGroup2 = fixDetachedParent->add_option_group("ID",
                  "Specify what to fix");
  // idGroup2->add_option("--fid", fid, "Fix the parents of the given file ID (decimal form)");
  idGroup2->add_option("--cid", cid,
                       "Fix the parents of the given container ID (decimal form)");
  idGroup2->add_option("--fid", fid,
                       "Fix the parents of the given file ID (decimal form)");
  idGroup2->require_option(1, 1);
  //----------------------------------------------------------------------------
  // Set-up fix-shadow-file subcommand..
  //----------------------------------------------------------------------------
  auto fixShadowFileSubcommand = app.add_subcommand("fix-shadow-file",
                                 "[CAUTION] Attempt to fix a shadowed file.\nIf the given fid is indeed shadowed by a different fid / cid, it's moved to the given destination.");
  addClusterOptions(fixShadowFileSubcommand, membersStr, memberValidator,
                    password, passwordFile);
  addDryRun(fixShadowFileSubcommand, noDryRun);
  fixShadowFileSubcommand->add_option("--destination-path", destinationPath,
                                      "Path in which the conflicting file will be stored.")
  ->required();
  fixShadowFileSubcommand->add_option("--fid", fid,
                                      "Specify the suspected shadowed file")
  ->required();
  //----------------------------------------------------------------------------
  // Set-up drop-from-deathrow subcommand..
  //----------------------------------------------------------------------------
  auto dropFromDeathrow = app.add_subcommand("drop-from-deathrow",
                          "[CAUTION] Delete a FileMD which is currently on deathrow.\nAny pending replicas on the FSTs will not be touched, potentially resulting in dark data!");
  addClusterOptions(dropFromDeathrow, membersStr, memberValidator, password,
                    passwordFile);
  addDryRun(dropFromDeathrow, noDryRun);
  dropFromDeathrow->add_option("--fid", fid,
                               "Specify which file to drop - it should currently be stuck on deathrow")
  ->required();
  //----------------------------------------------------------------------------
  // Change fid protobuf properties
  //----------------------------------------------------------------------------
  auto changeFidSubcommand = app.add_subcommand("change-fid",
                             "[DANGEROUS] Change specified properties of a single fid. Better know what you're doing before using this!");
  addClusterOptions(changeFidSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  addDryRun(changeFidSubcommand, noDryRun);
  uint64_t newParent = 0;
  std::string newChecksum;
  int64_t newSize = -1;
  changeFidSubcommand->add_option("--fid", fid,
                                  "Specify the FileMD to print, through its ID (decimal form)")
  ->required();
  changeFidSubcommand->add_option("--new-parent", newParent,
                                  "Change the parent container of the specified fid. This _DOES NOT_ modify the respective container maps, only the protobuf FMD!");
  changeFidSubcommand->add_option("--new-checksum", newChecksum,
                                  "Change the checksum of the specified fid.");
  changeFidSubcommand->add_option("--new-size", newSize,
                                  "Change the size of the specified fid.");
  //----------------------------------------------------------------------------
  // Rename a fid from its current location
  //----------------------------------------------------------------------------
  std::string newName;
  auto renameFidSubcommand = app.add_subcommand("rename-fid",
                             "[DANGEROUS] Rename a file onto the specified container ID - the respective container maps are modified.");
  addClusterOptions(renameFidSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  addDryRun(renameFidSubcommand, noDryRun);
  renameFidSubcommand->add_option("--fid", fid, "Specify the FileMD to rename")
  ->required();
  renameFidSubcommand->add_option("--destination-cid", newParent,
                                  "The destination container ID in which to put the FileMD")
  ->required();
  renameFidSubcommand->add_option("--new-name", newName,
                                  "The new name of the specified fid - must only contain alphanumeric characters, and can be left empty to preserve old name");
  //----------------------------------------------------------------------------
  // Rename a cid from its current location
  //----------------------------------------------------------------------------
  auto renameCidSubcommand = app.add_subcommand("rename-cid",
                             "[DANGEROUS] Rename a container onto the specified container ID - the respective container maps are modified.");
  addClusterOptions(renameCidSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  addDryRun(renameCidSubcommand, noDryRun);
  renameCidSubcommand->add_option("--cid", cid, "Specify the FileMD to rename")
  ->required();
  renameCidSubcommand->add_option("--destination-cid", newParent,
                                  "The destination container ID in which to put the FileMD")
  ->required();
  renameCidSubcommand->add_option("--new-name", newName,
                                  "The new name of the specified cid - must only contain alphanumeric characters, and can be left empty to preserve old name");
  //----------------------------------------------------------------------------
  // Set-up overwrite-container subcommand..
  //----------------------------------------------------------------------------
  auto overwriteContainerSubcommand = app.add_subcommand("overwrite-container",
                                      "[DANGEROUS] Overwrite the given ContainerMD - USE WITH CAUTION");
  addClusterOptions(overwriteContainerSubcommand, membersStr, memberValidator,
                    password, passwordFile);
  addDryRun(overwriteContainerSubcommand, noDryRun);
  uint64_t parent;
  std::string containerName;
  overwriteContainerSubcommand->add_option("--cid", cid,
      "Specify which container ID to overwrite")
  ->required();
  overwriteContainerSubcommand->add_option("--parent-id", parent,
      "Specify which ID to set as parent")
  ->required();
  overwriteContainerSubcommand->add_option("--name", containerName,
      "Specify the container's name")
  ->required();

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  bool dryRun = !noDryRun;

  //----------------------------------------------------------------------------
  // Validate --password and --password-file options..
  //----------------------------------------------------------------------------
  if (!passwordFile.empty()) {
    if (!common::PasswordHandler::readPasswordFile(passwordFile, password)) {
      std::cerr << "Could not read passwordfile: '" << passwordFile <<
                "'. Ensure the file exists, and its permissions are 400." << std::endl;
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
  std::unique_ptr<OutputSink> outputSink;

  if (json) {
    outputSink.reset(new JsonStreamSink(std::cout, std::cerr));
  } else {
    outputSink.reset(new StreamSink(std::cout, std::cerr));
  }

  Inspector inspector(qcl, *outputSink);
  std::string connectionErr;

  if (!inspector.checkConnection(connectionErr)) {
    std::cerr << connectionErr << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Dispatch subcommand
  //----------------------------------------------------------------------------
  if (dumpSubcommand->parsed()) {
    return inspector.dump(dumpPath, relativePaths, rawPaths, noDirs, noFiles,
                          showSize, showMtime, attrQuery, std::cout);
  }

  if (scanSubcommand->parsed()) {
    return inspector.scan(dumpPath, relativePaths, rawPaths, noDirs, noFiles);
  }

  if (namingConflictsSubcommand->parsed()) {
    return inspector.checkNamingConflicts(onePerLine, std::cout, std::cerr);
  }

  if (cursedNamesSubcommand->parsed()) {
    return inspector.checkCursedNames(std::cout, std::cerr);
  }

  if (printSubcommand->parsed()) {
    if (fid > 0) {
      return inspector.printFileMD(fid, withParents, std::cout, std::cerr);
    }

    return inspector.printContainerMD(cid, withParents, std::cout, std::cerr);
  }

  if (scanDirsSubcommand->parsed()) {
    return inspector.scanDirs(onlyNoAttrs, fullPaths, countContents,
                              countThreshold);
  }

  if (stripediffSubcommand->parsed()) {
    return inspector.stripediff(printTime, std::cout, std::cerr);
  }

  if (oneReplicaLayoutSubcommand->parsed()) {
    return inspector.oneReplicaLayout(showName, fullPaths, filterInternal,
                                      std::cout, std::cerr);
  }

  if (scanFilesSubcommand->parsed()) {
    return inspector.scanFileMetadata(onlySizes, fullPaths, findUnknownFsids);
  }

  if (scanDeathrowSubcommand->parsed()) {
    return inspector.scanDeathrow(std::cout, std::cerr);
  }

  if (checkOrphansSubcommand->parsed()) {
    return inspector.checkOrphans(std::cout, std::cerr);
  }

  if (checkFsViewMissingSubcommand->parsed()) {
    return inspector.checkFsViewMissing(std::cout, std::cerr);
  }

  if (checkFsViewExtraSubcommand->parsed()) {
    return inspector.checkFsViewExtra(std::cout, std::cerr);
  }

  if (checkShadowDirectories->parsed()) {
    return inspector.checkShadowDirectories(std::cout, std::cerr);
  }

  if (checkSimulatedHardlinks->parsed()) {
    return inspector.checkSimulatedHardlinks(std::cout, std::cerr);
  }

  if (fixDetachedParent->parsed()) {
    if (cid > 0) {
      return inspector.fixDetachedParentContainer(dryRun, cid, destinationPath,
             std::cout, std::cerr);
    } else {
      return inspector.fixDetachedParentFile(dryRun, fid, destinationPath, std::cout,
                                             std::cerr);
    }
  }

  if (fixShadowFileSubcommand->parsed()) {
    return inspector.fixShadowFile(dryRun, fid, destinationPath, std::cout,
                                   std::cerr);
  }

  if (dropFromDeathrow->parsed()) {
    return inspector.dropFromDeathrow(dryRun, fid, std::cout, std::cerr);
  }

  if (changeFidSubcommand->parsed()) {
    return inspector.changeFid(dryRun, fid, newParent, newChecksum, newSize,
                               std::cout, std::cerr);
  }

  if (renameFidSubcommand->parsed()) {
    return inspector.renameFid(dryRun, fid, newParent, newName, std::cout,
                               std::cerr);
  }

  if (renameCidSubcommand->parsed()) {
    return inspector.renameCid(dryRun, cid, newParent, newName, std::cout,
                               std::cerr);
  }

  if (overwriteContainerSubcommand->parsed()) {
    return inspector.overwriteContainerMD(dryRun, cid, parent, containerName,
                                          std::cout, std::cerr);
  }

  std::cerr << "No subcommand was supplied - should never reach here" <<
            std::endl;
  return 1;
}
