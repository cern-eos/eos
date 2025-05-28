// ----------------------------------------------------------------------
// File: com_rclone.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright(C) 2023 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 *(at your option) any later version.                                  *
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
#include "console/commands/classes/RClone.hh"
#include "common/StringTokenizer.hh"
#include "common/Path.hh"

/**
 * @brief Implements the rclone command for synchronizing directories between EOS and local filesystems
 *
 * The rclone command provides functionality to copy or synchronize directories between:
 * - EOS to EOS
 * - EOS to local filesystem
 * - Local filesystem to EOS
 * - Local filesystem to local filesystem
 *
 * It supports two main operations:
 * - copy: One-way synchronization from source to destination
 * - sync: Two-way synchronization between directories based on modification times
 *
 * Command syntax:
 * @code
 * rclone copy src-dir dst-dir [options]  : Copy from source to destination
 * rclone sync dir1 dir2 [options]        : Bi-directional sync based on mtimes
 * @endcode
 *
 * Available options:
 * @param --delete         Delete files in destination that don't exist in source (by default this is disabled)
 * @param --noreplace     Never update existing files, only create new ones
 * @param --dryrun        Simulate the operation without making any changes
 * @param --atomic        Include EOS atomic files in sync operation
 * @param --versions      Include EOS version files in sync operation
 * @param --hidden        Include hidden files and directories in sync operation
 * @param --sparse <size> Create sparse files above specified size
 * @param --sparse-dump <file> Write list of sparse files to specified file
 * @param --debug         Enable debug output
 * @param --lowres        Use low resolution timestamp comparison (seconds only)
 * @param -p, --parallel <n> Set number of parallel copy streams (default: 16)
 * @param -v, --verbose   Display all actions, not just summary
 * @param -s, --silent    Only show errors
 *
 * Features:
 * - Preserves modification times
 * - Handles files, directories, and symbolic links
 * - Supports sparse files
 * - Provides detailed progress information
 * - Can filter atomic files, version files, and hidden files
 * - Configurable parallel copy streams for performance
 * - Supports both high and low resolution timestamp comparison
 *
 * Example:
 * @code
 * eos -b rclone copy /eos/user/foo /tmp/foo
 * eos -b rclone sync /eos/user/foo /tmp/foo
 * @endcode
 *
 * @note The command must be run in batch mode using 'eos -b rclone ...'
 */
 


void rclone_usage() {
  fprintf(stdout,
          "Usage: rclone <cmd> <src> <dst> [options]\n"
          "\n"
          "Commands:\n"
          "  copy                   : copy files from src to dst\n"
          "  sync                   : sync files between src and dst\n"
          "\n"
          "Options:\n"
          "  --delete              : delete files in dst not present in src\n"
          "  --noreplace           : don't replace existing files\n"
          "  --dryrun              : show what would be done\n"
          "  --atomic              : don't filter atomic files\n"
          "  --versions            : don't filter version files\n"
          "  --hidden              : don't filter hidden files\n"
          "  -v|--verbose          : verbose output\n"
          "  -s|--silent           : silent operation\n"
          "  --sparse <size>       : create sparse files above size\n"
          "  --sparse-dump <file>  : dump sparse file list to file\n"
          "  --debug               : enable debug output\n"
          "  --lowres              : use low resolution timestamp comparison (seconds only)\n"
          "  -p|--parallel <n>     : set number of parallel copy streams (default: 16)\n"
          "\n"
          "Example:\n"
          "  rclone copy /eos/user/foo /tmp/foo\n"
          "  rclone sync /eos/user/foo /tmp/foo\n");
  global_retc = EINVAL;
}

int com_rclone(char* arg1) {
  
  if (interactive) {
    fprintf(stderr,
            "error: don't call <rclone> from an interactive shell - run 'eos -b rclone ...'!\n");
    global_retc = -1;
    return 0;
  }

  // split subcommands
  XrdOucString mountpoint = "";
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString src = subtokenizer.GetToken();
  XrdOucString dst = subtokenizer.GetToken();
  eos::common::Path srcPath(src.c_str());
  eos::common::Path dstPath(dst.c_str());
  src = srcPath.GetFullPath();
  dst = dstPath.GetFullPath();

  if (!src.length() || !dst.length()) {
    rclone_usage();
  }

  eos::console::RClone rclone;
  XrdOucString option;

  do {
    option = subtokenizer.GetToken();

    if (!option.length()) {
      break;
    }

    if (option == "--delete") {
      rclone.setNoDelete(false);
    } else if (option == "--noreplace") {
      rclone.setNoReplace(true);
    } else if (option == "--dryrun") {
      rclone.setDryRun(true);
    } else if (option == "--atomic") {
      rclone.setFilterAtomic(false);
    } else if (option == "--versions") {
      rclone.setFilterVersions(false);
    } else if (option == "--hidden") {
      rclone.setFilterHidden(false);
    } else if (option == "-v" || option == "--verbose") {
      rclone.setVerbose(true);
    } else if (option == "-s" || option == "--silent") {
      rclone.setSilent(true);
    } else if (option == "--sparse") {
      option = subtokenizer.GetToken();
      if (!option.length()) {
        rclone_usage();
      } else {
        rclone.setMakeSparse(std::strtoul(option.c_str(), 0, 10));
      }
    } else if (option == "--sparse-dump") {
      option = subtokenizer.GetToken();
      if (!option.length()) {
        rclone_usage();
      } else {
        rclone.setSparseFilesDump(option.c_str());
      }
    } else if (option == "--debug") {
      rclone.setDebug(true);
    } else if (option == "--lowres") {
      rclone.setLowRes(true);
    } else if (option == "-p" || option == "--parallel") {
      option = subtokenizer.GetToken();
      if (!option.length()) {
        rclone_usage();
      } else {
        rclone.setCopyParallelism(std::strtoul(option.c_str(), 0, 10));
      }
    } else {
      rclone_usage();
    }
  } while (1);

  if (cmd == "copy") {
    global_retc = rclone.copy(src.c_str(), dst.c_str());
  } else if (cmd == "sync") {
    global_retc = rclone.sync(src.c_str(), dst.c_str());
  } else {
    rclone_usage();
  }

  return 0;
}
