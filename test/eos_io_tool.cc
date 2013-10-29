//------------------------------------------------------------------------------
// File: eos-io-tool.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

/*!
@details This program is used to test out various IO operations done on EOS files.
  One can write, read files in sequential mode or using a certain pattern defined
  in a separate file. The file outside EOS is read according to the pattern and
  then written in EOS using the same sequence of blocks. The same is valid for
  reading.
*/


//------------------------------------------------------------------------------
#include <map>
#include <cstdlib>
#include <cstdio>
#include <string>
#include <fstream>
#include <iostream>
#include <memory>
#include <sys/time.h>
#include <getopt.h>
//------------------------------------------------------------------------------
#include "XrdCl/XrdClFile.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "fst/io/FileIoPlugin.hh"
#include "fst/io/FileIo.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
//------------------------------------------------------------------------------

//! Type of operations supported
enum OperationType
{
  RD_SEQU,
  RD_PATT,
  WR_SEQU,
  WR_PATT,
  OP_NONE
};

uint64_t block_size = 1048576;  ///< block size of rd/wr operations default 1MB
uint32_t prefetch_size = block_size;   ///< prefetch size reading, default 1MB
int timeout = 60;         ///< asynchronous timeout value, default 60 seconda
bool do_async = false;    ///< by default do sync operations
bool do_update = false;   ///< write a file trying to update it
std::string pattern_file = ""; ///< file which contains the rd/wr pattern
int debug = 0; ///< flag to enable debug

//------------------------------------------------------------------------------
// Read the whole file sequentially in sync/async mode
//------------------------------------------------------------------------------
bool
ReadSequentially(XrdCl::URL& url, std::string& ext_file)
{
  int status;
  bool ret = true;
  char* buffer = new char[block_size];
  XrdSfsFileOpenMode flags_sfs =  SFS_O_RDONLY;
  eos::fst::AsyncMetaHandler* ptr_handler = NULL;
  eos::fst::FileIo* eosf = eos::fst::FileIoPlugin::GetIoObject(
                             eos::common::LayoutId::kXrdCl);
  XrdOucString open_opaque = "";

  if (do_async)
  {
    // Enable readahead
    std::ostringstream osstr;
    osstr << "fst.readahead=true&fst.blocksize="
          << static_cast<int>(prefetch_size);
    open_opaque = osstr.str().c_str();
  }

  // Open the file for reading from EOS
  status = eosf->Open(url.GetURL(), flags_sfs, 0, open_opaque.c_str());

  if (status == SFS_ERROR)
  {
    eos_static_err("Failed to open EOS file in rd mode:%s", url.GetURL().c_str());
    delete eosf;
    delete[] buffer;
    return false;;
  }

  // Do stat to find out the file size
  struct stat buf;
  status = eosf->Stat(&buf);

  if (status)
  {
    eos_static_err("Error doing stat on the EOS file");
    delete eosf;
    delete[] buffer;
    return false;
  }

  eos_static_debug("EOS file size: %zu", buf.st_size);
  // Open file outside EOS, where the data is written
  FILE* extf = fopen(ext_file.c_str(), "w+");

  if (!extf)
  {
    eos_static_err("Failed to open ext file:%s in rd mode", ext_file.c_str());
    fclose(extf);
    delete eosf;
    delete[] buffer;
    return false;
  }

  uint64_t eos_fsize = buf.st_size;
  uint64_t offset = 0;
  int64_t nread = 0;
  int64_t nwrite = 0;
  int32_t length;

  if (do_async)
  {
    ptr_handler = static_cast<eos::fst::AsyncMetaHandler*>(eosf->GetAsyncHandler());
  }

  // Read the whole file sequentially
  while (eos_fsize > 0)
  {
    eos_static_debug("Current file size:%llu", eos_fsize);
    length = ((eos_fsize < block_size) ? eos_fsize : block_size);

    // Read from the EOS file
    if (do_async)
    {
      nread = eosf->ReadAsync(offset, buffer, length, true, timeout);
    }
    else
    {
      nread = eosf->Read(offset, buffer, length);
    }

    if (nread == SFS_ERROR)
    {
      eos_static_err("Error while reading at offset:%llu", offset);
      ret = false;
      break;
    }

    offset += nread;
    eos_fsize -= nread;

    if (do_async)
    {
      // Wait async request to be satisfied
      uint16_t error_type = ptr_handler->WaitOK();

      if (error_type != XrdCl::errNone)
      {
        eos_static_err("Error while doing an async write operation");
        ret = false;
        break;
      }
    }

    // Write data to the file outside EOS
    nwrite = fwrite(static_cast<void*>(buffer), sizeof(char), nread, extf);

    if (nwrite != nread)
    {
      eos_static_err("Error while writing to file outside EOS");
      ret = false;
      break;
    }
  }

  // Close files
  eosf->Close(timeout);
  fclose(extf);
  // Free memory
  delete eosf;
  delete[] buffer;
  return ret;
}


//------------------------------------------------------------------------------
// Read the pattern map from the provided pattern_file
//------------------------------------------------------------------------------
void
LoadPattern(std::string& pattern_file,
            std::map<uint64_t, uint32_t>& map_pattern)
{
  // Open file containing the pattern to read
  uint64_t off_start;
  uint32_t off_end;
  std::string line;
  std::ifstream ifpattern(pattern_file.c_str());

  // Read pattern from file
  if (ifpattern.is_open())
  {
    while (std::getline(ifpattern, line))
    {
      eos_static_debug("Line:%s", line.c_str());
      std::istringstream iss(line);

      // Ignore comment lines
      if (!(line.find("#") == 0))
      {
        if (!(iss >> off_start >> off_end))
        {
          eos_static_err("Error while parsing the pattern file");
          return;
        }

        map_pattern.insert(std::make_pair(off_start, (off_end - off_start)));
      }
    }

    // Close pattern file
    ifpattern.close();
  }
  else
  {
    eos_static_err("Error while opening the pattern file");
    return;
  }

  // Print the pattern map
  eos_static_debug("The pattern map is:");

  for (auto iter = map_pattern.begin(); iter != map_pattern.end(); ++iter)
  {
    eos_static_debug("off:%ju len:%ju", iter->first, (uint64_t)iter->second);
  }
}


//------------------------------------------------------------------------------
// Read the file in sync/async mode using a certain patter specified in the
// patter file - list of offset <-> length pieces to be read from the EOS file
// and written to the external file
//------------------------------------------------------------------------------
bool
ReadPattern(XrdCl::URL& url,
            std::string& ext_file,
            std::string& pattern_file)
{
  int status;
  bool ret = true;
  char* buffer = new char[block_size];
  XrdSfsFileOpenMode flags_sfs = SFS_O_RDONLY;
  eos::fst::AsyncMetaHandler* ptr_handler = NULL;
  eos::fst::FileIo* eosf = eos::fst::FileIoPlugin::GetIoObject(
                             eos::common::LayoutId::kXrdCl);
  std::map<uint64_t, uint32_t> map_pattern;
  XrdOucString open_opaque = "";

  // Enable the readahead in async mode
  if (do_async)
  {
    std::ostringstream osstr;
    osstr << "fst.readahead=true&fst.blocksize="
          << static_cast<int>(prefetch_size);
    open_opaque = osstr.str().c_str();
  }

  // Open the file for reading from EOS
  status = eosf->Open(url.GetURL(), flags_sfs, 0, open_opaque.c_str(), timeout);

  if (status == SFS_ERROR)
  {
    eos_static_err("Failed to open EOS file:%s in rd mode", url.GetURL().c_str());
    delete eosf;
    delete[] buffer;
    return false;;
  }

  // Do stat to find out the file size
  struct stat buf;
  status = eosf->Stat(&buf);

  if (status)
  {
    eos_static_err("Error while doing the stat on the EOS file.");
    delete eosf;
    delete[] buffer;
    return false;
  }

  eos_static_debug("EOS file size:%zu", buf.st_size);
  // Load the pattern used for reading
  LoadPattern(pattern_file, map_pattern);

  if (map_pattern.empty())
  {
    eos_static_err("Error the pattern map is empty");
    delete eosf;
    delete[] buffer;
    return false;
  }

  // Open file outside EOS, where the data is written
  int ext_fd = open(ext_file.c_str(), O_CREAT | O_WRONLY | O_LARGEFILE, S_IRWXU);

  if (ext_fd == -1)
  {
    eos_static_err("Failed to open ext file:%s in wr mode: ", ext_file.c_str());
    close(ext_fd);
    delete eosf;
    delete[] buffer;
    return false;
  }

  uint64_t piece_off;
  uint32_t piece_len;
  int64_t nread = 0;
  int64_t nwrite = 0;
  int32_t length;

  if (do_async)
  {
    ptr_handler = static_cast<eos::fst::AsyncMetaHandler*>(eosf->GetAsyncHandler());
  }

  // Read each of the pieces from the pattern
  for (auto iter = map_pattern.begin(); iter != map_pattern.end(); ++iter)
  {
    piece_off = iter->first;
    piece_len = iter->second;
    eos_static_debug("Piece off:%ju len:%jd ", piece_off, piece_len);

    // Read a piece which can be bigger than the block size
    while (piece_len > 0)
    {
      length = (int32_t)((piece_len < block_size) ? piece_len : block_size);
      eos_static_debug("Reading at off:%ju, length:%jd", piece_off, (int64_t)length);

      // Read from the EOS file
      if (do_async)
      {
        nread = eosf->ReadAsync(piece_off, buffer, length, true, timeout);
      }
      else
      {
        nread = eosf->Read(piece_off, buffer, length);
      }

      if (nread == SFS_ERROR)
      {
        eos_static_err("Error while reading at offset:%zu", piece_off);
        ret = false;
        break;
      }

      if (do_async)
      {
        // Wait async request to be satisfied
        uint16_t error_type = ptr_handler->WaitOK();

        if (error_type != XrdCl::errNone)
        {
          eos_static_err("Error while doing async write operation");
          ret = false;
          break;
        }
      }

      // Write data to the file outside EOS at the same offset
      nwrite = pwrite(ext_fd, static_cast<const void*>(buffer), nread, piece_off);

      if (nwrite != nread)
      {
        eos_static_err("Error while writing to file outside EOS");
        ret = false;
        break;
      }

      piece_off += nread;
      piece_len -= nread;
    }
  }

  // Close files
  eosf->Close(timeout);
  close(ext_fd);
  // Free memory
  delete eosf;
  delete[] buffer;
  return ret;
}


//------------------------------------------------------------------------------
// Write file sequentially to EOS in sync/async mode
//------------------------------------------------------------------------------
bool
WriteSequentially(XrdCl::URL& url,
                  std::string& ext_file)
{
  int status;
  bool ret = true;
  char* buffer = new char[block_size];
  eos::fst::AsyncMetaHandler* ptr_handler = NULL;
  eos::fst::FileIo* eosf = eos::fst::FileIoPlugin::GetIoObject(
                             eos::common::LayoutId::kXrdCl);
  XrdOucString open_opaque = "";
  XrdSfsFileOpenMode flags_sfs;
  mode_t mode_sfs = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH  ;

  // Open the file for update or truncate it
  if (do_update)
  {
    eos_static_debug("EOS file opened for update");
    flags_sfs = SFS_O_RDWR;
  }
  else
  {
    eos_static_debug("EOS file opend for creation");
    flags_sfs = SFS_O_CREAT | SFS_O_RDWR;
  }

  // Open the file for writing/update from EOS
  status = eosf->Open(url.GetURL(), flags_sfs, mode_sfs, open_opaque.c_str());

  if (status == SFS_ERROR)
  {
    eos_static_err("Failed to open EOS file:%s in wr/upd mode",
                   url.GetURL().c_str());
    delete eosf;
    delete[] buffer;
    return false;;
  }

  // Open file outside EOS, from where the data is read
  int ext_fd = open(ext_file.c_str(), O_RDONLY | O_LARGEFILE);

  if (ext_fd == -1)
  {
    eos_static_err("Failed to open ext file:%s in rd mode", ext_file.c_str());
    close(ext_fd);
    delete eosf;
    delete[] buffer;
    return false;
  }

  // Do stat to find out the size of the file to be written
  struct stat buf;

  if (fstat(ext_fd, &buf))
  {
    eos_static_err("Error while trying to stat external file");
    close(ext_fd);
    delete eosf;
    delete[] buffer;
    return false;
  }

  eos_static_debug("External file size:%zu", buf.st_size);
  uint64_t ext_fsize = buf.st_size;
  uint64_t offset = 0;
  int64_t nread = 0;
  int64_t nwrite = 0;
  int32_t length;

  if (do_async)
  {
    ptr_handler = static_cast<eos::fst::AsyncMetaHandler*>(eosf->GetAsyncHandler());
  }

  // Read the whole file sequentially
  while (ext_fsize > 0)
  {
    length = ((ext_fsize < block_size) ? ext_fsize : block_size);
    eos_static_debug("Current file size:%llu", ext_fsize);
    // Read from the external file
    nread = pread(ext_fd, buffer, length, offset);

    if (nread != length)
    {
      eos_static_err("Error while reading at offset: %llu", offset);
      ret = false;
      break;
    }

    // Write data to the EOS file
    if (do_async)
    {
      nwrite = eosf->WriteAsync(offset, buffer, nread, timeout);
    }
    else
    {
      nwrite = eosf->Write(offset, buffer, nread);
    }

    if (nwrite != nread)
    {
      eos_static_err("Error while writing to EOS file");
      ret = false;
      break;
    }

    if (do_async)
    {
      // Wait async request to be satisfied
      uint16_t error_type = ptr_handler->WaitOK();

      if (error_type != XrdCl::errNone)
      {
        eos_static_err("Error while doing an async write operation");
        ret = false;
        break;
      }
    }

    offset += nread;
    ext_fsize -= nread;
  }

  // Close files
  eosf->Close(timeout);
  close(ext_fd);
  // Free memory
  delete eosf;
  delete[] buffer;
  return ret;
}


//------------------------------------------------------------------------------
// Write the file in sync/async mode using a certain patter specified in the
// patter file - list of offset <-> length pieces to be read fromt the external
// file and written to the EOS file
//------------------------------------------------------------------------------
bool
WritePattern(XrdCl::URL& url,
             std::string& ext_file,
             std::string& pattern_file)
{
  int status;
  bool ret = true;
  char* buffer = new char[block_size];
  eos::fst::AsyncMetaHandler* ptr_handler = NULL;
  eos::fst::FileIo* eosf = eos::fst::FileIoPlugin::GetIoObject(
                             eos::common::LayoutId::kXrdCl);
  XrdOucString open_opaque = "";
  XrdSfsFileOpenMode flags_sfs;
  XrdSfsMode mode_sfs = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH  ;
  std::map<uint64_t, uint32_t> map_pattern;

  // Open the file for update or truncate it
  if (do_update)
  {
    eos_static_debug("EOS file opened for update");
    flags_sfs = SFS_O_RDWR;
  }
  else
  {
    eos_static_debug("EOS file opend for creation");
    flags_sfs = SFS_O_CREAT | SFS_O_RDWR;
  }

  // Open the file for writing/update from EOS
  status = eosf->Open(url.GetURL(), flags_sfs, mode_sfs, open_opaque.c_str());

  if (status == SFS_ERROR)
  {
    eos_static_err("Failed to open EOS file:%s in wr/upd mode",
                   url.GetURL().c_str());
    delete eosf;
    delete[] buffer;
    return false;;
  }

  // Open file outside EOS, from where the data is read
  int ext_fd = open(ext_file.c_str(), O_RDONLY | O_LARGEFILE);

  if (ext_fd == -1)
  {
    eos_static_err("Failed to open ext file:%s in rd mode: ", ext_file.c_str());
    close(ext_fd);
    delete eosf;
    delete[] buffer;
    return false;
  }

  // Load the pattern used for reading
  LoadPattern(pattern_file, map_pattern);

  if (map_pattern.empty())
  {
    eos_static_err("Error the pattern map is empty");
    delete eosf;
    delete[] buffer;
    return false;
  }

  uint64_t piece_off;
  uint32_t piece_len;
  int64_t nread = 0;
  int64_t nwrite = 0;
  int32_t length;

  if (do_async)
  {
    ptr_handler = static_cast<eos::fst::AsyncMetaHandler*>(eosf->GetAsyncHandler());
  }

  // Read the pieces specified in the pattern map
  for (auto iter = map_pattern.begin(); iter != map_pattern.end(); ++iter)
  {
    piece_off = iter->first;
    piece_len = iter->second;
    eos_static_debug("Piece off:%llu len:%lu", piece_off, piece_len);

    while (piece_len > 0)
    {
      length = ((piece_len < block_size) ? piece_len : block_size);
      // Read from the external file
      nread = pread(ext_fd, buffer, length, piece_off);

      if (nread != length)
      {
        eos_static_err("Error while reading at offset:%llu", piece_off);
        ret = false;
        break;
      }

      // Write data to the EOS file
      if (do_async)
      {
        nwrite = eosf->WriteAsync(piece_off, buffer, nread, timeout);
      }
      else
      {
        eos_static_debug("wrpatt piece_off=%llu, piece_len=%llu", piece_off, piece_len);
        nwrite = eosf->Write(piece_off, buffer, nread);
      }

      if (nwrite != nread)
      {
        eos_static_err("Error while writing to EOS file");
        ret = false;
        break;
      }

      if (do_async)
      {
        // Wait async request to be satisfied
        uint16_t error_type = ptr_handler->WaitOK();

        if (error_type != XrdCl::errNone)
        {
          eos_static_err("Error while doing an async write operation");
          ret = false;
          break;
        }
      }

      piece_off += nread;
      piece_len -= nread;
    }
  }

  // Close files
  eosf->Close(timeout);
  close(ext_fd);
  // Free memory
  delete eosf;
  delete[] buffer;
  return ret;
}



//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  // Set the TimeoutResolution to 1 for XrdCl
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt("TimeoutResolution", 1);
  // Build the usage string
  std::ostringstream usage_sstr;
  usage_sstr << "Usage: " << std::endl
             << "eos-io-tool --operation <rdsequ/rdpatt/wrsequ/wrpatt> "
             << std::endl
             << "            --eosfile <eos_file> " << std::endl
             << "            --extfile <ext_file> " << std::endl
             << "            [--patternfile <pf>]" << std::endl
             << "            [--blocksize <bs>] " << std::endl
             << "            [--filesize <fs>] " << std::endl
             << "            [--timeout <val>] " << std::endl
             << "            [--prefetchsize <bytes>]" << std::endl
             << "            [--logfile <logfile>] " << std::endl
             << "            [--async] [--update] [--help]" << std::endl;

  // Initialise the logging
  eos::common::LogId logId;
  eos::common::Logging::Init();
  eos::common::Logging::SetLogPriority(LOG_INFO);
  eos::common::Logging::SetUnit("eosio@local");
  
  // Log only mesages from functions in this file
  eos::common::Logging::SetFilter("PASS:ReadSequentially,WriteSequentially,"
                                  "LoadPattern,ReadPattern,WritePattern,main");

  if (argc < 2)
  {
    std::cout << usage_sstr.str() << std::endl;
    return 1;
  }

  XrdCl::URL url_file;
  std::string ext_file;
  OperationType op_type = OP_NONE;
  static int async_op = 0;
  static int update_op = 0;

  // Parse the argument options
  while (1)
  {
    static struct option long_options[] =
    {
      {"operation",    required_argument, 0, 'a'},
      {"eosfile",      required_argument, 0, 'b'},
      {"extfile",      required_argument, 0, 'c'},
      {"blocksize",    required_argument, 0, 'd'},
      {"timeout",      required_argument, 0, 'f'},
      {"logfile",      required_argument, 0, 'e'},
      {"prefetchsize", required_argument, 0, 'g'},
      {"patternfile",  required_argument, 0, 'i'},
      {"async",        no_argument, &async_op,  1},
      {"update",       no_argument, &update_op, 1},
      {"debug",        no_argument, &debug,     1},
      {"help",         no_argument,       0, 'h'},
      {0, 0, 0, 0}
    };
    // getopt_long stores the option index here
    int option_index = 0;
    std::string val;
    int c = getopt_long(argc, argv, "a:b:c:d:e:f:g:h",
                        long_options, &option_index);

    // Detect the end of the options
    if (c == -1)
      break;

    switch (c)
    {
    case 0:
    {
      // If this option set a flag, do nothing now
      if (long_options[option_index].flag != 0)
      {
        break;
      }
    }

    case 'a':
    {
      val = optarg;

      if (val == "rdsequ") op_type = RD_SEQU;
      else if (val == "rdpatt") op_type = RD_PATT;
      else if (val == "wrsequ") op_type = WR_SEQU;
      else if (val == "wrpatt") op_type = WR_PATT;

      if (op_type == OP_NONE)
      {
        std::cerr << "No such operation type" << std::endl;
        exit(1);
      }

      break;
    }

    case 'b':
    {
      val = optarg;
      url_file.FromString(val);

      if (!url_file.IsValid())
      {
        std::cerr << "EOS file URL is no valid" << std::endl;
        return 1;
      }

      break;
    }

    case 'c':
    {
      ext_file = optarg;
      break;
    }

    case 'd':
    {
      block_size = atoi(optarg);
      break;
    }

    case 'f':
    {
      timeout = atoi(optarg);
      break;
    }

    case 'e':
    {
      val = optarg; // log file name/path
      FILE* fp = fopen(val.c_str(), "a+");

      if (fp)
      {
        // Redirect stdout and stderr to the log file
        dup2(fileno(fp), fileno(stdout));
        dup2(fileno(fp), fileno(stderr));
      }
      else
      {
        std::cerr << "Failed to open logging file" << std::endl;
      }

      break;
    }

    case 'g':
    {
      prefetch_size = atoi(optarg);
      break;
    }

    case 'i':
    {
      pattern_file = optarg;
      break;
    }

    case 'h':
    {
      std::cout << usage_sstr.str() << std::endl;
      exit(1);
    }

    default:
    {
      std::cerr << "No such option" << std::endl;
      break;
    }
    }
  }

  // Decide if the opertations are to be async or not
  if (async_op == 1) do_async = true;

  if (update_op == 1) do_update = true;

  if (debug == 1) eos::common::Logging::SetLogPriority(LOG_DEBUG);

  // Print the running configuration
  if (debug)
  {
    std::cout << "-----------------------------------------------------------"
              << std::endl
              << "Default block size: " << block_size << std::endl
              << "Default timeout: " << timeout << std::endl
              << "Default prefetch size: " << prefetch_size << std::endl
              << "Default async: " << do_async << std::endl
              << "Default debug: " << debug << std::endl
              << "-----------------------------------------------------------"
              << std::endl;
  }

  // Execute the required operation
  if (op_type == RD_SEQU)
  {
    if (!url_file.IsValid() || ext_file.empty())
    {
      eos_static_err("Set EOS file and output file name");
      return 1; // error
    }

    if (ReadSequentially(url_file, ext_file))
    {
      eos_static_info("Operation successful");
      return 0;
    }
    else
    {
      eos_static_info("Operation failed");
      return 1; // error
    }
  }
  else if (op_type == RD_PATT)
  {
    if (!url_file.IsValid() || ext_file.empty() || pattern_file.empty())
    {
      eos_static_err("Set EOS file, pattern file and output file name");
      return 1; // error
    }

    if (ReadPattern(url_file, ext_file, pattern_file))
    {
      eos_static_info("Operation successful");
      return 0;
    }
    else
    {
      eos_static_info("Operation failed");
      return 1; // error
    }
  }
  else if (op_type == WR_SEQU)
  {
    if (!url_file.IsValid() || ext_file.empty())
    {
      eos_static_err("Set EOS file and external file name");
      return 1; // error
    }

    if (WriteSequentially(url_file, ext_file))
    {
      eos_static_info("Operation successful");
      return 0;
    }
    else
    {
      eos_static_info("Operation failed");
      return 1; // error
    }
  }
  else if (op_type == WR_PATT)
  {
    if (!url_file.IsValid() || ext_file.empty() || pattern_file.empty())
    {
      eos_static_err("Set EOS file, pattern file and output file name");
      return 1; // error
    }

    if (WritePattern(url_file, ext_file, pattern_file))
    {
      eos_static_info("Operation successful");
      return 0;
    }
    else
    {
      eos_static_info("Operation failed");
      return 1; // error
    }
  }
}

