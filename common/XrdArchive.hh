//------------------------------------------------------------------------------
// File: XrdArchive.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#pragma once
#include "common/Namespace.hh"
#include "XrdCl/XrdClZipArchive.hh"
#include "XrdOuc/XrdOucString.hh"
#include <atomic>
#include <mutex>
#include <zlib.h>
#include <json/json.h>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class XrdCopy abstracting parallel file copy
//------------------------------------------------------------------------------
class XrdArchive
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //----------------------------------------------------------------------------
  XrdArchive(const std::string& archiveurl, const std::string target="");
  int Open(bool showbytes, bool json, bool silent); // Lists existing archives
  int Download(size_t nparallel, bool json, bool silent); // use Open, then download and specify a target in the constructor
  int Upload(const std::vector<std::string>& files, size_t nparallel, bool json, bool silent, size_t splitsize, const std::string& stagefile); // use Create, then Upload
  int Create();
  int Close();
  
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdArchive() = default;

  static int uploadToArchive(const std::string& membername, const std::string& stagefile, const std::string& archiveurl);
  static int getAndCompressAPI(std::string fname, std::string stagefile, std::string archive);
  static int getAndUncompressAPI(std::string source, std::string target);
  static std::string getArchiveUrl(std::string url, size_t idx);
  static std::vector<std::string> loadFileList(std::string path);

  // job description: name => pair(src,target)
  typedef std::map<std::string, std::pair<std::string,std::string>> job_t;
  
  static std::atomic<bool> s_silent;
  static std::atomic<bool> s_verbose;

  job_t downloadJobs;
  
  // CRC32 inlined to avoid linker problems with FST libraries
  
  class CRC32 
  {
  private:
    off_t crc32offset;
    unsigned int crcsum;
    XrdOucString Checksum;
  public:
    CRC32 () { Reset(); }
    
    bool Add (const char* buffer, size_t length, off_t offset) { if (offset != crc32offset) { return false; } crcsum = crc32(crcsum, (const Bytef*) buffer, length); crc32offset += length; return true; }
    const char* GetHexChecksum () {char scrc32[1024]; sprintf(scrc32, "%08x", crcsum); Checksum = scrc32; return Checksum.c_str(); }
    const char* GetBinChecksum (int &len) { len = sizeof (unsigned int); return (char*) &crcsum; }
    void Reset () { crc32offset = 0; crcsum = crc32(0L, Z_NULL, 0); }
    virtual ~CRC32 () { };
  };
  
  static std::atomic<size_t> napi;
  static std::atomic<bool> ziperror;
  static std::atomic<bool> zipdebug;
  static std::atomic<size_t> bytesarchived;
  static std::atomic<size_t> bytestoupload;
  static std::atomic<size_t> bytesread;
  static std::atomic<bool> zstdcompression;
  static std::atomic<size_t> splitsize;
  static std::atomic<size_t> archiveindex;
  static std::atomic<size_t> archiveindexbytes;
  static std::string archiveurl;
  static std::string targeturl;
  static XrdCl::ZipArchive archive;
  static std::mutex archiveMutex;

private:
  Json::Value gjson;
};

EOSCOMMONNAMESPACE_END
