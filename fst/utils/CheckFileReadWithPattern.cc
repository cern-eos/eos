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
#include <random>
#include <map>
#include <chrono>
#include <thread>
#include <fstream>
#include <sstream>
#include "XrdCl/XrdClFile.hh"

//------------------------------------------------------------------------------
//! Generate a map of offset and length that represent individual read requests
//!
//! @param max_size file maximum size
//! @param block_sz size of a read block - corresponds to the RAIN stripe size
//! @param num_chunks number of entries to generate < max_size / 4
//!
//! @return map of offset and length of the individual read requests
//------------------------------------------------------------------------------
std::map<uint64_t, uint32_t>
GenerateReadRequests(uint64_t max_size, uint32_t block_sz, uint32_t num_chunks)
{
  if (num_chunks >= max_size / 4) {
    std::cout << "error: number of chunks to be genrated needs to be "
              << "smaller than file size / 4";
    std::exit(EINVAL);
  }

  uint64_t offset, next_offset;
  uint32_t length;
  std::map<uint64_t, uint32_t> chunks;
  float mean = 1.0 * block_sz;
  float stddev = 0.5 * block_sz;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> off_dist(0, max_size);
  std::normal_distribution<> len_dist(mean, stddev);
  bool found = false;

  for (uint32_t i = 0; i < num_chunks; ++i) {
    do {
      found = false;
      offset = off_dist(gen);
      auto it = chunks.lower_bound(offset);

      if (it != chunks.end()) {
        next_offset = it->first;

        if (it->first == offset) {
          found = true;
        } else {
          if (it != chunks.begin()) {
            --it;

            if ((it->first <= offset) && (it->first + it->second >= offset)) {
              found = true;
            }
          }
        }
      }
    } while (found);

    do {
      length = std::round(len_dist(gen));
      auto it = chunks.lower_bound(offset);

      if (it != chunks.end()) {
        next_offset = it->first;
        // Make sure we don't overlap with the next chunk
        found = (offset + length >= next_offset);
      } else {
        found = false;
      }

      // Make sure we don't go beyond the end of file
      if (!found) {
        found = (offset + length >= max_size);
      }
    } while (found);

    chunks[offset] = length;
  }

  return chunks;
}


//------------------------------------------------------------------------------
//! Write the read requests in "offset length" format to the given file
//!
//! @param fpattern pattern file where to write the map contents
//------------------------------------------------------------------------------
void
WritePatternToFile(const std::string& fpattern,
                   const std::map<uint64_t, uint32_t> chunks)
{
  std::ofstream file(fpattern);

  for (const auto& chunk : chunks) {
    file << chunk.first << " " << chunk.second << std::endl;
  }
}


//------------------------------------------------------------------------------
//! Read the individual chunk requests from the given file. Data is organized
//! in two colums, the first one represents the offset and the second the length
//! of the individual read requests.
//!
//! @param fpattern pattern file path
//!
//! @return map of offset and length of the individual read requests
//------------------------------------------------------------------------------
std::map<uint64_t, uint32_t>
GetReadRequestsFromFile(const std::string fpattern)
{
  std::string line;
  std::map<uint64_t, uint32_t> chunks;
  std::ifstream file(fpattern);
  std::string soff, slen;
  uint64_t offset;
  uint32_t length;

  if (file.is_open()) {
    while (std::getline(file, line)) {
      std::istringstream iss(line);
      iss >> soff;
      iss >> slen;

      try {
        offset = std::stoull(soff);
        length = std::stoul(slen);
      } catch (...) {
        std::cerr << "error: failed to parse offset/length from -" << line
                  << std::endl;
        std::exit(EINVAL);
      }

      chunks[offset] = length;
    }
  }

  return chunks;
}

//------------------------------------------------------------------------------
//! Check match between the reference file and the checked file
//!
//! @param fref path to the reference file
//! @param fcehck path to the file to be checked
//! @param chunks map of (offset, length) pairs that will make the readv req.
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool
CheckMatch(const std::string& fref, const std::string fcheck,
           const std::map<uint64_t, uint32_t> chunks, uint32_t block_sz)
{
  size_t readv_sz = 5;
  std::string ref_buff;
  std::string check_buff;
  ref_buff.reserve(2 * block_sz * readv_sz);
  check_buff.reserve(2 * block_sz * readv_sz);
  XrdCl::URL ref_url(fref);
  XrdCl::URL check_url(fcheck);

  if (!ref_url.IsValid() || !check_url.IsValid()) {
    std::cerr << "error: invalid reference or check URL - "
              << fref << " / " << fcheck << std::endl;
    return false;
  }

  XrdCl::File ref_file, check_file;
  XrdCl::XRootDStatus st = ref_file.Open(fref, XrdCl::OpenFlags::Read);

  if (!st.IsOK()) {
    std::cerr << "error: failed to open reference file - " << fref << std::endl;
    return false;
  }

  st = check_file.Open(fcheck, XrdCl::OpenFlags::Read);

  if (!st.IsOK()) {
    std::cerr << "error: failed to open check file - " << fcheck << std::endl;
    return false;
  }

  std::map<uint64_t, uint32_t> readv_req;

  for (const auto& chunk : chunks) {
    if (readv_req.size() < readv_sz) {
      readv_req.insert(chunk);
    } else {
      uint64_t total_sz = 0ull;
      XrdCl::ChunkList xrd_chunks;

      for (const auto& elem : readv_req) {
        //std::cout << "off: " << elem.first << " len: " << elem.second << std::endl;
        xrd_chunks.emplace_back(elem.first, elem.second);
        total_sz += elem.second;
      }

      //std::cout << "--------------------------" << std::endl;
      readv_req.clear();
      XrdCl::VectorReadInfo* vinfo_raw = new XrdCl::VectorReadInfo();
      std::unique_ptr<XrdCl::VectorReadInfo> vinfo;
      vinfo.reset(vinfo_raw);
      st = ref_file.VectorRead(xrd_chunks, ref_buff.data(), vinfo_raw);

      if (!st.IsOK()) {
        std::cerr << "error: failed readv from reference file" << std::endl
                  << "err_msg: " << st.ToStr() << std::endl;
        return false;
      }

      st = check_file.VectorRead(xrd_chunks, check_buff.data(), vinfo_raw);

      if (!st.IsOK()) {
        std::cerr << "error: failed readv from checked file" << std::endl;
        return false;
      }

      if (strncmp(ref_buff.data(), check_buff.data(), total_sz) != 0) {
        std::cerr << "error: mismatch in reference vs. checked buffer " << std::endl;
        return false;
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//! Get XRootD file size
//!
//! @param fref path to XRootD file
//!
//! @return file size or 0 if failure
//------------------------------------------------------------------------------
uint64_t
GetXrdFileSize(const std::string& fref)
{
  uint64_t size = 0ull;
  XrdCl::URL url(fref);

  if (!url.IsValid()) {
    std::cerr << "error: invalid XRootD URL - " << fref << std::endl;
    return size;
  }

  XrdCl::File file;
  auto st = file.Open(fref, XrdCl::OpenFlags::Read);

  if (!st.IsOK()) {
    std::cerr << "error: failed to open file - " << fref << std::endl;
    return size;
  }

  XrdCl::StatInfo* info_raw = new XrdCl::StatInfo();
  std::unique_ptr<XrdCl::StatInfo> info;
  info.reset(info_raw);
  st = file.Stat(true, info_raw);

  if (!st.IsOK()) {
    std::cerr << "error: failed to stat file - " << fref << std::endl;
    return size;
  }

  size = info_raw->GetSize();
  return size;
}


//------------------------------------------------------------------------------
//! Main function
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  CLI::App app("Tool to do vector reads and check ");
  uint32_t num_chunks = 100;
  uint32_t sz_chunk = 128 * 1024;
  std::string fref, fcheck, fpattern;
  bool output_pattern = false;
  app.add_option("-r,--reference_file", fref,
                 "File path used as reference")->required();
  app.add_option("-c,--check_file", fcheck,
                 "File path used for testing")->required();
  app.add_option("-n,--num_chunks", num_chunks,
                 "Number of generated chunks [default 100]");
  app.add_option("-s,--size_chunk", sz_chunk, "Average size of the chunks");
  app.add_option("-p,--pattern_file", fpattern,
                 "File holding the read pattern (offset -> length)");
  app.add_flag("-o,--output_pattern", output_pattern,
               "Write generated pattern to file");

  // Parse the inputs
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  uint64_t file_size = GetXrdFileSize(fref);

  if (file_size == 0) {
    std::cerr << "error: failed to stat reference file - " << fref << std::endl;
    std::exit(EIO);
  }

  std::map<uint64_t, uint32_t> chunks;

  if (output_pattern) {
    if (fpattern.empty()) {
      std::cerr << "error: no output pattern file specified" << std::endl;
      std::exit(EINVAL);
    }

    chunks = GenerateReadRequests(file_size, sz_chunk, num_chunks);
    std::cout << "Write pattern to file: " << fpattern << std::endl;
    WritePatternToFile(fpattern, chunks);
    return 0;
  }

  if (fpattern.empty()) {
    chunks = GenerateReadRequests(file_size, sz_chunk, num_chunks);
  } else {
    chunks = GetReadRequestsFromFile(fpattern);
  }

  if (CheckMatch(fref, fcheck, chunks, sz_chunk)) {
    std::cout << "info: readv requests matched!" << std::endl;
  }

  return 0;
}
