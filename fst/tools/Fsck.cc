// ----------------------------------------------------------------------
// File: Fsck.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "fst/Fsck.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/Config.hh"
#include "common/LayoutId.hh"
#include "common/StringConversion.hh"
#include "common/PasswordHandler.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

eos::fst::FmdDbMapHandler gFmdDbMapHandler; // needed for compilation

/*----------------------------------------------------------------------------*/
void usage()
{
  fprintf(stderr,
          "usage: eos-fsck-fs [--silent|-s] [--rate rate] [--nomgm ] <directory>\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "       error output format:\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ DETACHE ] fsid:1 cxid:???????? fxid:0013a549 ... file exists on disk , but is not registered in MGM\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ CKS     ] fsid:1 cxid:???????? fxid:0013a549 ... file checksum differs from MGM checksum\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ CKSFLAG ] fsid:1 cxid:???????? fxid:0013a549 ... file is flagged with a checksum error on disk\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ BXSFLAG ] fsid:1 cxid:???????? fxid:0013a549 ... file is flagged with a blockchecksum error on disk\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ MISSING ] fsid:1 cxid:00000006 fxid:0013da3f ... file was suppossed to be here, but is missing on disk\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ ZEROMIS ] fsid:1 cxid:00000006 fxid:0013da3f ... an empty file was suppossed to be here, but is missing on disk\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ SIZE    ] fsid:1 cxid:00000007 fxid:0013da3a ... file size differes from MGM size\n");
  fprintf(stderr,
          "                      [Fsck] [ERROR] [ REPLICA ] fsid:1 cxid:00000007 fxid:0013da3a ... file replica count is inconsistent for the given layout\n");
  exit(-1);
}


std::string
ParseMgmConfig()
{
  std::ifstream inFile("/etc/sysconfig/eos_env");
  std::string dumpentry;

  while (std::getline(inFile, dumpentry)) {
    if (dumpentry.substr(0, std::string("EOS_MGM_ALIAS=").length()) ==
        "EOS_MGM_ALIAS=") {
      return dumpentry.substr(std::string("EOS_MGM_ALIAS=").length());
    }
  }

  return "";
}


bool
ParseQdbConfig(eos::QdbContactDetails& qdb)
{
  std::ifstream inFile("/etc/xrd.cf.fst");
  std::string dumpentry;
  std::string qdbcluster;
  std::string qdbpassword_file;

  while (std::getline(inFile, dumpentry)) {
    // fstofs.qdbcluster  eosalice-qdb.cern.ch:7777
    // fstofs.qdbpassword_file  /etc/eos.keytab
    if (dumpentry.substr(0, std::string("fstofs.qdbcluster").length()) ==
        "fstofs.qdbcluster") {
      qdbcluster = dumpentry.substr(std::string("fstofs.qdbcluster").length());
    }

    if (dumpentry.substr(0, std::string("fstofs.qdbpassword_file").length()) ==
        "fstofs.qdbpassword_file") {
      qdbpassword_file = dumpentry.substr(
                           std::string("fstofs.qdbpassword_file").length());
    }
  }

  if (qdbcluster.length()) {
    qdbcluster.erase(0, qdbcluster.find_first_not_of(" \t\n\r\f\v"));
    qdbcluster.erase(qdbcluster.find_last_not_of(" \t\n\r\f\v") + 1);
  }

  if (qdbpassword_file.length()) {
    qdbpassword_file.erase(0, qdbpassword_file.find_first_not_of(" \t\n\r\f\v"));
    qdbpassword_file.erase(qdbpassword_file.find_last_not_of(" \t\n\r\f\v") + 1);
  }

  return (qdb.members.parse(qdbcluster) &&
          eos::common::PasswordHandler::readPasswordFile(qdbpassword_file, qdb.password));
}

int
main(int argc, char* argv[])
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.SetLogPriority(LOG_INFO);
  g_logging.SetUnit("Fsck");
  long int rate = 1000;
  std::string manager = "";
  XrdOucString dirName;
  bool silent = false;

  if ((argc < 2) || (argc > 7)) {
    usage();
  }

  eos::QdbContactDetails myQDB;
  bool hasQDB = true;

  if (!ParseQdbConfig(myQDB)) {
    manager = ParseMgmConfig();
    hasQDB = false;
  }

  for (int i = 1; i < argc; ++i) {
    std::string thisarg = argv[i];

    if (thisarg == "--rate") {
      if (argc > (i + 1)) {
        std::string srate = argv[++i];
        rate = strtol(srate.c_str(), 0, 10);
        continue;
      } else {
        usage();
      }
    }

    if (thisarg == "--nomgm") {
      manager = "";
      continue;
    }

    if ((thisarg == "-s") ||
        (thisarg == "--silent")) {
      silent = true;
      continue;
    }

    if (thisarg[0] == '-') {
      usage();
    }

    dirName = argv[i];
  }

  srand((unsigned int) time(NULL));
  eos::fst::Load fstLoad(1);
  fstLoad.Monitor();
  usleep(100000);
  std::string sfsid;
  std::string fsidpath = dirName.c_str();
  fsidpath += "/.eosfsid";
  eos::common::StringConversion::LoadFileIntoString(fsidpath.c_str(), sfsid);
  eos::fst::Fsck* sd = new eos::fst::Fsck(dirName.c_str(), strtol(sfsid.c_str(),
                                          0, 10), &fstLoad,
                                          10, rate, manager, silent);

  if (hasQDB) {
    fprintf(stdout, "# connecting to QDB \n");
    sd->SetQdbContactDetails(myQDB);
  } else {
    if (manager.length()) {
      fprintf(stdout, "# connecting to MGM <%s>\n", manager.c_str());
    } else {
      fprintf(stdout, "# disabled MGM connections\n");
    }
  }

  if (sd) {
    eos::fst::Fsck::StaticThreadProc((void*) sd);
    delete sd;
  }
}
