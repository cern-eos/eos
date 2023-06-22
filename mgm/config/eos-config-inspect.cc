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

#include "common/config/ConfigParsing.hh"
#include "common/CLI11.hpp"
#include "common/PasswordHandler.hh"
#include "common/StringUtils.hh"

#include "mgm/config/QuarkConfigHandler.hh"

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
// Read source configuration file
//------------------------------------------------------------------------------
bool readConfigurationFile(const std::string& sourceFile,
                           std::string& fullContents)
{
  std::ifstream infile(sourceFile.c_str());

  if (!infile.is_open()) {
    return false;
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
  fullContents = ss.str();
  return true;
}

//------------------------------------------------------------------------------
// Read and parse configuration file
//------------------------------------------------------------------------------
bool readAndParseConfiguration(const std::string& path,
                               std::map<std::string, std::string>& configuration)
{
  //----------------------------------------------------------------------------
  // Read source configuration file
  //----------------------------------------------------------------------------
  std::string fullContents;

  if (!readConfigurationFile(path, fullContents) || fullContents.empty()) {
    std::cerr << "could not read configuration file: " << path << std::endl;
    return false;
  }

  //----------------------------------------------------------------------------
  // Parse
  //----------------------------------------------------------------------------
  std::string err;

  if (!eos::common::ConfigParsing::parseConfigurationFile(fullContents,
      configuration, err)) {
    std::cerr << "Could not parse configuration file: " << err << std::endl;
    return false;
  }

  std::cerr << "--- Successfully parsed configuration file" << std::endl;
  return true;
}

int runDumpSubcommand(const std::string& configEntry,
                      eos::mgm::QuarkConfigHandler& configHandler)
{
  std::map<std::string, std::string> configuration;
  eos::common::Status status = configHandler.fetchConfiguration(configEntry,
                               configuration);

  if (!status) {
    std::cerr << "error while fetching configuration '" << configEntry << "' : " <<
              status.toString() << std::endl;
    return 1;
  }

  for (auto it = configuration.begin(); it != configuration.end(); it++) {
    std::cout << it->first << " => " << it->second << std::endl;
  }

  return 0;
}

int runExportSubcommand(const std::string& sourceFile,
                        eos::mgm::QuarkConfigHandler& configHandler,
                        bool overwrite)
{
  std::map<std::string, std::string> configuration;

  if (!readAndParseConfiguration(sourceFile, configuration)) {
    return 1;
  }

  eos::common::Status st = configHandler.writeConfiguration("default",
                           configuration, overwrite).get();

  if (!st) {
    std::cerr << "ERROR: " << st.toString() << std::endl;
    return 1;
  }

  std::cerr << "--- Operation successful - wrote configuration 'default' with " <<
            configuration.size() << " entries" << std::endl;
  return 0;
}

int runListSubcommand(eos::mgm::QuarkConfigHandler& configHandler)
{
  std::vector<std::string> configs, backups;
  eos::common::Status st = configHandler.listConfigurations(configs, backups);

  if (!st) {
    std::cerr << "ERROR: " << st.toString() << std::endl;
    return 1;
  }

  std::cout << "Stored configurations:" << std::endl;

  for (auto it = configs.begin(); it != configs.end(); it++) {
    std::cout << "    " << *it << std::endl;
  }

  std::cout << std::endl;
  std::cout << "Stored backups:" << std::endl;

  for (auto it = backups.begin(); it != backups.end(); it++) {
    std::cout << "    " << *it << std::endl;
  }

  return 0;
}

int runTailSubcommand(size_t nlines,
                      eos::mgm::QuarkConfigHandler& configHandler)
{
  std::vector<std::string> changelog;
  eos::common::Status st = configHandler.tailChangelog(nlines, changelog);

  if (!st) {
    std::cerr << st.toString() << std::endl;
    return 1;
  }

  for (auto it = changelog.begin(); it != changelog.end(); it++) {
    std::cout << *it << std::endl;
  }

  return 0;
}

int runRelocateFilesystemSubcommand(eos::mgm::QuarkConfigHandler& configHandler,
                                    uint32_t fsid, const std::string& newhost, int newport)
{
  std::map<std::string, std::string> configMap;
  eos::common::Status st = configHandler.fetchConfiguration("default", configMap);

  if (!st) {
    std::cerr << "could not fetch configuration: " << st.toString() << std::endl;
    return 1;
  }

  for (auto it = configMap.begin(); it != configMap.end(); it++) {
    if (eos::common::startsWith(it->first, "fs:")) {
      std::map<std::string, std::string> configEntry;

      if (!eos::common::ConfigParsing::parseFilesystemConfig(it->second,
          configEntry)) {
        std::cerr << "could not parse fs entry: " << it->first << it->second <<
                  std::endl;
        return 1;
      }

      if (configEntry["id"] == SSTR(fsid)) {
        // We have a match
        std::cout << "Found filesystem with fsid=" << fsid << ": " <<
                  configEntry["queue"] << std::endl;
        std::cout << it->first << " " << it->second << std::endl;
        st = eos::common::ConfigParsing::relocateFilesystem(newhost, newport,
             configEntry);

        if (!st) {
          std::cerr << "filesystem relocation failed: " << st.toString() << std::endl;
          return 1;
        }

        std::string configKey = SSTR("fs:" << configEntry["queuepath"]);
        std::string newConfig = eos::common::joinMap(configEntry, " ");
        std::cout << "After relocation: " << configKey << " " << newConfig << std::endl;
        configMap.erase(it);
        configMap[configKey] = newConfig;
        char buff[128];
        time_t timestamp = time(NULL);
        strftime(buff, 127, "%Y%m%d%H%M%S", localtime(&timestamp));
        std::string backupName = SSTR("default-" << buff << "-relocation");
        st = configHandler.writeConfiguration("default", configMap, true,
                                              backupName).get();

        if (!st) {
          std::cerr << "writing configuration failed: " << st.toString() << std::endl;
          return 1;
        }

        std::cout << "Successfully wrote configuration, backup key: " << backupName <<
                  std::endl;
        return 0;
      }
    }
  }

  std::cerr << "no filesystem found with fsid=" << fsid << std::endl;
  return 1;
}

//------------------------------------------------------------------------------
// Trim backups subcommand
//------------------------------------------------------------------------------
int runTrimBackupsSubcommand(size_t limit,
                             eos::mgm::QuarkConfigHandler& configHandler)
{
  size_t deleted = 0;
  eos::common::Status st = configHandler.trimBackups("default", limit, deleted);

  if (!st) {
    std::cerr << st.toString() << std::endl;
    return st.getErrc();
  }

  std::cout << "deleted " << deleted << " config backups" << std::endl;
  return 0;
}

int main(int argc, char* argv[])
{
  CLI::App app("Tool to inspect contents of the QuarkDB-based EOS configuration.");
  app.require_subcommand();
  //----------------------------------------------------------------------------
  // Parameters common to all subcommands
  //----------------------------------------------------------------------------
  std::string membersStr;
  MemberValidator memberValidator;
  std::string password;
  std::string passwordFile;
  //----------------------------------------------------------------------------
  // Set-up export subcommand..
  //----------------------------------------------------------------------------
  auto exportSubcommand = app.add_subcommand("export",
                          "[DANGEROUS] Read a legacy file-based configuration file, and export to QDB. Ensure the MGM is not running while you run this command!");
  std::string sourceFile;
  exportSubcommand->add_option("--source", sourceFile,
                               "Path to the source configuration file to export")
  ->required();
  bool overwrite = false;
  exportSubcommand->add_flag("--overwrite", overwrite,
                             "Overwrite already-existing configuration in QDB.");
  addClusterOptions(exportSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up relocate-filesystem subcommand..
  //----------------------------------------------------------------------------
  auto relocateFilesystemSubcommand = app.add_subcommand("relocate-filesystem",
                                      "[DANGEROUS] Change the FST to which a filesystem belongs to");
  uint32_t fsid;
  relocateFilesystemSubcommand->add_option("--fsid", fsid,
      "The ID of the filesystem to change")
  ->required();
  std::string newFstHost;
  relocateFilesystemSubcommand->add_option("--new-fst-host", newFstHost,
      "The new FST host")
  ->required();
  int newFstPort;
  relocateFilesystemSubcommand->add_option("--new-fst-port", newFstPort,
      "The new FST port")
  ->required();
  addClusterOptions(relocateFilesystemSubcommand, membersStr, memberValidator,
                    password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up dump subcommand..
  //----------------------------------------------------------------------------
  auto dumpSubcommand = app.add_subcommand("dump",
                        "Dump the contents of a given configuration stored in QDB");
  std::string configEntry = "default";
  dumpSubcommand->add_option("--config", configEntry,
                             "Configuration to dump (from 'list'), default is actual");
  addClusterOptions(dumpSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up list subcommand..
  //----------------------------------------------------------------------------
  auto listSubcommand = app.add_subcommand("list",
                        "List all stored configurations, including backups");
  addClusterOptions(listSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up tail-changelog subcommand..
  //----------------------------------------------------------------------------
  auto tailSubcommand = app.add_subcommand("tail-changelog",
                        "Tail configuration changelog");
  size_t nlines = 1000;
  tailSubcommand->add_option("--nlines", nlines,
                             "The maximum number of changelog entries to print");
  addClusterOptions(tailSubcommand, membersStr, memberValidator, password,
                    passwordFile);
  //----------------------------------------------------------------------------
  // Set-up trim-backups subcommand..
  //----------------------------------------------------------------------------
  auto trimBackupsSubcommand = app.add_subcommand("trim-backups",
                               "Trim number of configuration backups");
  size_t nbackups = 1000;
  trimBackupsSubcommand->add_option("--limit", nbackups,
                                    "The maximum number of backups to keep");
  addClusterOptions(trimBackupsSubcommand, membersStr, memberValidator, password,
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
  eos::mgm::QuarkConfigHandler configHandler(contactDetails);
  //----------------------------------------------------------------------------
  // Ensure connection is sane
  //----------------------------------------------------------------------------
  eos::common::Status status = configHandler.checkConnection(std::chrono::seconds(
                                 3));

  if (!status) {
    std::cerr << "could not connect to QDB backend: " << status.toString() <<
              std::endl;
    return 1;
  }

  if (exportSubcommand->parsed()) {
    return runExportSubcommand(sourceFile, configHandler, overwrite);
  } else if (relocateFilesystemSubcommand->parsed()) {
    return runRelocateFilesystemSubcommand(configHandler, fsid, newFstHost,
                                           newFstPort);
  } else if (dumpSubcommand->parsed()) {
    return runDumpSubcommand(configEntry, configHandler);
  } else if (listSubcommand->parsed()) {
    return runListSubcommand(configHandler);
  } else if (tailSubcommand->parsed()) {
    return runTailSubcommand(nlines, configHandler);
  } else if (trimBackupsSubcommand->parsed()) {
    return runTrimBackupsSubcommand(nbackups, configHandler);
  }

  std::cerr << "No subcommand was supplied - should never reach here" <<
            std::endl;
  return 1;
}
