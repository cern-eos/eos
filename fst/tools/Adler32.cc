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
#include "common/CLI11.hpp"

int
main(int argc, char* argv[])
{
  size_t offset{0};
  std::string path{""};
  CLI::App app("Compute adler32 checksum on a file");
  app.add_option("--offset", offset, "Offset");
  app.add_option("path", path, "Path")->required();

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  std::unique_ptr<eos::fst::CheckSum> normalXS =
    eos::fst::ChecksumPlugins::GetChecksumObject(eos::common::LayoutId::kAdler);

  if (normalXS) {
    unsigned long long scansize;
    float scantime;

    if (!normalXS->ScanFile(path.c_str(), scansize, scantime, 0, offset)) {
      fprintf(stderr, "error: unable to scan file path=%s\n", path.c_str());
      exit(-1);
    } else {
      fprintf(stdout, "path=%s size=%llu time=%.02f adler32=%s\n", path.c_str(),
              scansize, scantime, normalXS->GetHexChecksum());
      exit(0);
    }
  }

  fprintf(stderr, "error: failed to get checksum object\n");
  exit(-1);
}
