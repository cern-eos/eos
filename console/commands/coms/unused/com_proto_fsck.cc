//------------------------------------------------------------------------------
//! @file com_fsck.cc
//! @autor Elvin Sindrilaru - CERN
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
#include "console/commands/helpers/FsckHelper.hh"

void com_fsck_help();

//------------------------------------------------------------------------------
// Fsck command entry point
//------------------------------------------------------------------------------
int com_proto_fsck(char* arg)
{
  if (wants_help(arg)) {
    com_fsck_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  FsckHelper fsck(gGlobalOpts);

  if (!fsck.ParseCommand(arg)) {
    com_fsck_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  return fsck.Execute();
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_fsck_help()
{
  std::ostringstream oss;
  oss << "Usage: fsck [stat|config|report|repair]\n"
      << "    control and display file system check information\n"
      << std::endl
      << "  fsck stat [-m]\n"
      << "    print summary of consistency checks\n"
      << "    -m         : print in monitoring format\n"
      << std::endl
      << "  fsck config <key> <value>\n"
      << "    configure the fsck with the following possible options:\n"
      << "    collect-interval-min : collection interval in minutes [default 30]\n"
      << "    collect              : control error collection thread - on/off\n"
      << "    repair               : control error repair thread - on/off\n"
      << "    best-effort          : control best-effort repair mode - on/off\n"
      << "    repair-category      : specify error types that the repair thread will handle\n"
      << "                           e.g all, m_cx_diff, m_mem_sz_diff, d_cx_diff, d_mem_sz_diff,\n"
      << "                           unreg_n, rep_diff_n, rep_missing_n, blockxs_err, stripe_err\n"
      << "    max-queued-jobs      : maximum number of queued jobs\n"
      << "    max-thread-pool-size : maximum number of threads in the fsck pool\n"
      << "    show-dark-files      : on/off [default off] - might affect instance performance\n"
      << "    show-offline         : on/off [default off] - might affect instance performance\n"
      << "    show-no-replica      : on/off [default off] - might affect instance performance\n"
      << std::endl
      << "  fsck report [-a] [-h] [-i] [-l] [-j|--json] [--error <tag1> <tag2> ...]\n"
      << "    report consistency check results, with the following options\n"
      << "    -a         : break down statistics per file system" << std::endl
      << "    -i         : display file identifiers" << std::endl
      << "    -l         : display logical file name" << std::endl
      << "    -j|--json  : display in JSON output format" << std::endl
      << "    --error    : display information about the following error tags\n"
      << std::endl
      << "  fsck repair --fxid <val> [--fsid <val>] [--error <err_type>] [--async]\n"
      << "    repair the given file if there are any errors\n"
      << "    --fxid  : hexadecimal file identifier\n"
      << "    --fsid  : file system id used for collecting info\n"
      << "    --error : error type for given file system id e.g. m_cx_diff unreg_n etc\n"
      << "    --async : job queued and ran by the repair thread if enabled\n"
      << std::endl
      << "  fsck clean_orphans [--fsid <val>] [--force-qdb-cleanup]\n"
      << "     clean orphans by removing the entries from disk and local\n "
      << "     database for all file systems or only for the given fsid.\n"
      << "     This operation is synchronous but the fsck output will be\n"
      << "     updated once the inconsistencies are refreshed.\n"
      << "     --force-qdb-cleanup : force remove orphan entries from qdb\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
