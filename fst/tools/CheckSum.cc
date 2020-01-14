// ----------------------------------------------------------------------
// File: CRC32C.cc
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

#include <stdio.h>
#include <stdlib.h>
#include "fst/checksum/ChecksumPlugins.hh"
#include "common/LayoutId.hh"


void usage() 
{
  fprintf(stderr,"usage: eos-checksum adler|crc32|crc32c|crc64|md5|sha|sha256|xxhash64 <path>|/dev/stdin\n");
}

int
main(int argc, char* argv[])
{
  if (argc != 3) {
    usage();
    exit(-1);
  }

  std::string requested_checksum = argv[1];
  int checksum_type=0;

  if ( (checksum_type = eos::common::LayoutId::GetChecksumFromString(requested_checksum) == eos::common::LayoutId::kNone) ) {
    fprintf(stderr,"error: checksum <%s> is not supported\n", argv[1]);
    exit(-EINVAL);
  }

  std::unique_ptr<eos::fst::CheckSum> normalXS =
    std::unique_ptr<eos::fst::CheckSum> (eos::fst::ChecksumPlugins::GetXsObj(eos::common::LayoutId::GetChecksumFromString(requested_checksum)));

  if (normalXS) {
    XrdOucString path = (argv[2]) ? argv[2] : "";
    unsigned long long scansize;
    float scantime;

    if (!normalXS->ScanFile(path.c_str(), scansize, scantime)) {
      fprintf(stderr, "error: unable to scan file path=%s\n", argv[2]);
      exit(-1);
    } else {
      fprintf(stdout, "path=%s size=%llu time=%.02f %s=%s\n", argv[2], scansize,
              scantime, argv[1], normalXS->GetHexChecksum());
      exit(0);
    }
  }

  fprintf(stderr, "error: failed to get checksum object\n");
  exit(-1);
}
