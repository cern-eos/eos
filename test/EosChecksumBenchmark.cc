// ----------------------------------------------------------------------
// File: EosChecksumBenchmark.cc
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

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/wait.h>
/*-----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/StringConversion.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*-----------------------------------------------------------------------------*/
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdOuc/XrdOucString.hh>
/*-----------------------------------------------------------------------------*/

XrdPosixXrootd posixXrootd;

// 1GB mem buffer
#define MEMORYBUFFERSIZE 256ll*1024ll*1024ll 

int main (int argc, char* argv[]) {
  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("eoschecksumbenchmark@localhost");
  eos::common::Logging::gShortFormat=true;
  eos::common::Logging::SetLogPriority(LOG_DEBUG);

  std::vector<std::string> checksumnames;
  std::vector<unsigned long long> checksumids;

  checksumnames.push_back("adler32");
  checksumnames.push_back("crc32");
  checksumnames.push_back("md5");
  checksumnames.push_back("crc32c");
  checksumnames.push_back("sha1");

  checksumids.push_back(eos::common::LayoutId::kAdler);
  checksumids.push_back(eos::common::LayoutId::kCRC32);
  checksumids.push_back(eos::common::LayoutId::kMD5);
  checksumids.push_back(eos::common::LayoutId::kCRC32C);
  checksumids.push_back(eos::common::LayoutId::kSHA1);
  
  size_t nforks = 1;

  if (argc == 2 ) {

    nforks = atoi(argv[1]);
  }


  for (size_t foker = 0; foker < nforks; foker ++) {
    if (!fork()) {
      srandom(foker);
      XrdOucString size;
      eos::common::StringConversion::GetReadableSizeString(size, MEMORYBUFFERSIZE,"B");

      // allocate a block
      eos_static_info("allocating %s", size.c_str());
      char* buffer   = (char*) malloc( MEMORYBUFFERSIZE);
      char* xsbuffer = (char*) malloc( MEMORYBUFFERSIZE/100);
      
      if ( (!buffer) || (!xsbuffer) ) {
	fprintf(stderr,"error: failed to allocate reference buffers!\n");
	exit(-1);
      }
      
      eos_static_info("write randomized contents into %s", size.c_str());
      for (off_t i = 0; i< MEMORYBUFFERSIZE; i++) {
	buffer[i]= (rand())%256;
      }
      eos_static_info("write zeros into xs buffers");
      for (off_t i =0; i< MEMORYBUFFERSIZE/100; i++) {
	xsbuffer[i] = 0;
      }
      eos_static_info("allocated %s", size.c_str());
      
      std::vector<unsigned long long> blocksize;
      blocksize.push_back(4096);
      blocksize.push_back(128*1024);
      blocksize.push_back(1024*1024);
      blocksize.push_back(4*1024*1024);
      blocksize.push_back(128*1024*1024);
      
      for (size_t bs = 0; bs < blocksize.size(); bs++) {
	for (size_t i = 0; i< checksumnames.size(); i++) {
	  eos_static_info("benchmarking checksum algorithm %s", checksumnames[i].c_str());
	  eos::fst::CheckSum* checksum = eos::fst::ChecksumPlugins::GetChecksumObject(checksumids[i]);
	  if (!checksum) {
	    eos_static_err("failed to get checksum algorithm %s", checksumnames[i].c_str());
	  } else {
	    eos::common::Timing tm("Checksumming");
	    COMMONTIMING("START",&tm);
	    char*  ptr = buffer;
	    off_t offset = 0;
	    for (size_t j = 0; j< MEMORYBUFFERSIZE/blocksize[bs]; j++) {
	      checksum->Add(ptr,blocksize[bs], offset);
	      offset += blocksize[bs];
	      ptr += blocksize[bs];
	    }
	    checksum->Finalize();
	    COMMONTIMING("STOP",&tm);
	    XrdOucString sizestring;
	    eos::common::StringConversion::GetReadableSizeString(sizestring,blocksize[bs], "B");
	    eos_static_info("checksum( %-10s ) = %s realtime=%.02f [ms] blocksize=%s rate=%.02f", checksumnames[i].c_str(), checksum->GetHexChecksum(), tm.RealTime(), sizestring.c_str(), MEMORYBUFFERSIZE/tm.RealTime()/1000.0);
	    delete checksum;
	  }
	}
      }
      exit(0);
    }
  }
  for (size_t foker = 0; foker < nforks; foker ++) {
    wait(0);
  }
}
