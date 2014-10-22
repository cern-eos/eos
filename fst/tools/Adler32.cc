// ----------------------------------------------------------------------
// File: Adler32.cc
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

int
main (int argc, char* argv[])
{
  if (argc != 2)
  {
    fprintf(stderr, "error: you have to provide a path name\n");
    exit(-1);
  }

  eos::fst::CheckSum *normalXS;
  normalXS = eos::fst::ChecksumPlugins::GetChecksumObject(eos::common::LayoutId::kAdler);
  if (normalXS)
  {
    XrdOucString path = (argv[1]) ? argv[1] : "";
    unsigned long long scansize;
    float scantime;
    if (!normalXS->ScanFile(path.c_str(), scansize, scantime))
    {
      fprintf(stderr,"error: unable to scan file path=%s\n", argv[1]);
      exit(-1);
    } 
    else 
    {
      fprintf(stdout, "path=%s size=%llu time=%.02f adler32=%s\n", argv[1], scansize, scantime, normalXS->GetHexChecksum());
      exit(0);
    }
  }
  fprintf(stderr, "error: failed to get checksum object\n");
  exit(-1);
}
