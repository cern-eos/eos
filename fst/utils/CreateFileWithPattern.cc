/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "common/CLI11.hpp"
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>


std::string GetFilePath(const std::string& fname)
{
  std::string fpath;

  // Absolute path with filename specified
  if (!fname.empty() && (*fname.begin() == '/') && (*fname.rbegin() != '/')) {
    return fname;
  }

  // Absolute path without filename
  if (!fname.empty() && (*fname.begin() == '/') && (*fname.rbegin() == '/')) {
    fpath = fname;
    char tmpfile[1024];
    snprintf(tmpfile, sizeof(tmpfile), "%seosfp.XXXXXX", fpath.c_str());
    int tmp_fd = mkstemp(tmpfile);

    if (tmp_fd == -1) {
      std::cerr << "error: failed to create file" << std::endl;
      std::exit(EIO);
    }

    (void) close(tmp_fd);
    fpath = tmpfile;
    return fpath;
  }

  // Just a filename specified, we put it in /tmp/
  if (!fname.empty() && (*fname.begin() != '/')) {
    fpath = "/tmp/";
    fpath += fname;
    return fpath;
  }

  // If nothing specified create a path in /tmp/
  char tmpfile[] = "/tmp/eosfp.XXXXXX";
  int tmp_fd = mkstemp(tmpfile);

  if (tmp_fd == -1) {
    std::cerr << "error: failed to create file" << std::endl;
    std::exit(EIO);
  }

  (void) close(tmp_fd);
  fpath = tmpfile;
  return fpath;
}

int CreateFileWithPattern(const std::string& fpath, const std::string& pattern,
                          uint64_t size)
{
  std::cout << "info: writing to file " << fpath << std::endl;
  uint64_t sz_pattern = pattern.length();
  uint64_t sz_file = 0ull;
  std::ofstream file(fpath);

  while (sz_file < size) {
    file << pattern;
    sz_file += sz_pattern;

    if ((size > sz_file) && (size - sz_file < sz_pattern)) {
      file.write(pattern.c_str(), size - sz_file);
      break;
    }
  }

  return 0;
}


int main(int argc, char* argv[])
{
  CLI::App app("Tool to create a file with a certain pattern");
  std::string fname;
  std::string pattern;
  uint64_t size = 0ull;
  app.add_option("-s,--size", size, "File size")->required();
  app.add_option("-p,--pattern", pattern, "Data pattern")->required();
  app.add_option("-f,--filename", fname, "File pathname");

  // Parse the inputs
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  fname = GetFilePath(fname);
  return CreateFileWithPattern(fname, pattern, size);
}
