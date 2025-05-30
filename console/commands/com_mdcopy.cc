// ----------------------------------------------------------------------
// File: com_mdcopy.cc
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

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/NewfindHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
#include "console/commands/classes/Backup.hh"
/*----------------------------------------------------------------------------*/

/**
 * @brief The mdcopy command provides efficient metadata-aware directory copying
 * 
 * This command implements a sophisticated directory copying mechanism that:
 * - Preserves all file metadata (permissions, ownership, timestamps)
 * - Supports sparse file handling for efficient storage
 * - Can create squashfs archives of the copied data
 * - Provides filtering options for version files, atomic files, and hidden files
 * - Supports dry-run mode for testing
 * - Can generate lists of sparse files for later processing
 * 
 * The command is particularly useful for:
 * - Creating efficient backups of directory structures
 * - Migrating data between storage systems
 * - Creating compressed archives of directory trees
 * - Testing copy operations before execution
 * 
 * Example usage:
 * @code
 * eos mdcopy /source/path /dest/path --verbose
 * eos mdcopy /data /backup --minsparse 1G --mksquash /backup.squashfs
 * eos mdcopy /src /dst --dryrun --verbose
 * @endcode
 */

/**
 * @brief Display usage information for the mdcopy command
 * 
 * Prints the command syntax and available options to stderr and exits
 * with an error code.
 */
void mdcopy_usage() {
    fprintf(stderr,"usage: mdcopy <local-src> <local-dst> [--delete] [--dryrun] [--noreplace] [--noatomic] [--noversions] [--nohidden][-v | --verbose] [-s | --silent] [--debug] [--minsparse <bytes>] [--mksquash <path>] [--sparsefilelist <path>]\n");
    exit(-1);
}

/**
 * @brief Main implementation of the mdcopy command
 * 
 * @param arg1 Command line arguments as a space-separated string
 * @return int 0 on success, -1 on error
 * 
 * Processes command line arguments and configures the backup operation with the following options:
 * @param --delete      Enable deletion of files in destination not present in source
 * @param --dryrun      Show what would be done without actually copying
 * @param --noreplace   Don't replace existing files
 * @param --noatomic    Skip atomic files during copy
 * @param --noversion   Skip versioned files during copy
 * @param --nohidden    Skip hidden files during copy
 * @param --verbose|-v  Show detailed progress information
 * @param --silent|-s   Suppress all output
 * @param --minsparse   Minimum size in bytes for treating files as sparse
 * @param --mksquash    Create a squashfs archive at the specified path
 * @param --sparsefilelist Write list of sparse files to specified path
 */
int com_mdcopy(char* arg1) {
    eos::console::BackupConfig config;

    eos::common::StringTokenizer subtokenizer(arg1);
    subtokenizer.GetLine();

    XrdOucString ssrc = subtokenizer.GetToken();
    XrdOucString sdst = subtokenizer.GetToken();

    if (!ssrc.length() || !sdst.length()) {
        mdcopy_usage();
        return -1;
    }

    while (true) {
        XrdOucString option = subtokenizer.GetToken();
        if (!option.length()) break;

        if (option == "--delete") config.nodelete = false;
        else if (option == "--noreplace") config.noreplace = true;
        else if (option == "--dryrun") config.dryrun = true;
        else if (option == "--noatomic") config.filter_atomic = true;
        else if (option == "--noversion") config.filter_versions = true;
        else if (option == "--nohidden") config.filter_hidden = true;
        else if (option == "-v" || option == "--verbose") config.verbose = true;
        else if (option == "-s" || option == "--silent") config.is_silent = true;
        else if (option == "--debug") config.debug = true;
        else if (option == "--minsparse") {
            config.min_sparse_size = strtoul(subtokenizer.GetToken(), nullptr, 10);
        } else if (option == "--mksquash") {
            config.mksquash = subtokenizer.GetToken();
        } else if (option == "--sparsefilelist") {
            config.sparsefilelist = subtokenizer.GetToken();
        } else {
            fprintf(stderr, "unknown option: %s\n", option.c_str());
            mdcopy_usage();
            return -1;
        }
    }

    eos::common::Path srcPath(ssrc.c_str()), dstPath(sdst.c_str());
    std::string src = srcPath.GetFullPath().c_str();
    std::string dst = dstPath.GetFullPath().c_str();

    eos::console::Backup backup(src, dst, config);
    backup.run();

    return 0;
}
