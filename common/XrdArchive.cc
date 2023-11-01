//------------------------------------------------------------------------------
// File: XrdArchive.cc
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

#include "common/XrdArchive.hh"
#include "common/Timing.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/BufferManager.hh"
#include "common/XrdCopy.hh"

#include <XrdCl/XrdClURL.hh>
#include "XrdOuc/XrdOucString.hh"
#include "XrdCl/XrdClFile.hh"
#include "private/XrdCl/XrdClZipArchive.hh"
#include "private/XrdCl/XrdClOperations.hh"
#include "private/XrdCl/XrdClZipOperations.hh"

#include <mutex>
#include <fcntl.h> 
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <thread>
#include <future>
#include <cmath>
#include <vector>
#include <chrono>
#include <sys/stat.h>
#include <zstd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <json/json.h>
#include <unistd.h>

EOSCOMMONNAMESPACE_BEGIN

std::atomic<bool> XrdArchive::s_verbose=false;
std::atomic<bool> XrdArchive::s_silent=false;

std::atomic<size_t> XrdArchive::napi;
std::atomic<bool> XrdArchive::ziperror;
std::atomic<bool> XrdArchive::zipdebug;
std::atomic<size_t> XrdArchive::bytesarchived;
std::atomic<size_t> XrdArchive::bytestoupload;
std::atomic<size_t> XrdArchive::bytesread;
std::atomic<bool> XrdArchive::zstdcompression;
std::atomic<size_t> XrdArchive::splitsize;
std::atomic<size_t> XrdArchive::archiveindex;
std::atomic<size_t> XrdArchive::archiveindexbytes;
std::string XrdArchive::archiveurl;
std::string XrdArchive::targeturl;
XrdCl::ZipArchive XrdArchive::archive;
std::mutex XrdArchive::archiveMutex;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdArchive::XrdArchive(const std::string& url, std::string target)
{
  napi=0;
  ziperror=false;
  zipdebug=false;
  bytesarchived=0;
  bytestoupload=0;
  bytesread=0;
  zstdcompression=false;
  splitsize=32*1000ll*1000ll*1000ll;
  archiveindex=0;
  archiveindexbytes=0;
  archiveurl = url;
  targeturl = target;
}

int
XrdArchive::Open(bool showbytes, bool json, bool silent)
{
  std::string archivefile;
  size_t index=0;
  do {
    std::string sindex = std::to_string((unsigned long)index);
    archivefile = getArchiveUrl(archiveurl, index);
    
    // listarchive
    auto st = XrdCl::WaitFor( XrdCl::OpenArchive ( archive, archivefile.c_str(), XrdCl::OpenFlags::Read) );
    if (!st.IsOK()) {
      if ( (st.errNo == kXR_NotFound) && index) {
	// there are no more index archives
	break;
      }
      fprintf(stderr,"error: unable to open '%s' [%s]\n", archivefile.c_str(), st.GetErrorMessage().c_str());
      return -1;
    }
    gjson["archive"][sindex]["url"] = archiveurl;
    
    if (!json && !silent) {
      fprintf(stderr,"# Archive %s\n", archivefile.c_str());
    }
    XrdCl::DirectoryList *list = nullptr;
    st = archive.List( list );
    if (!st.IsOK()) {
      fprintf(stderr,"error: unable to list '%s' [%s]\n", archivefile.c_str(), st.GetErrorMessage().c_str());
      return -1;
    }
    
    st = XrdCl::WaitFor ( XrdCl::CloseArchive (archive) );
    if (!st.IsOK()) {
      fprintf(stderr, "error: failed to close archive '%s'\n", archiveurl.c_str());
      return -1;
    }
    
    for (auto it = list->Begin(); it != list->End(); it++) {
      std::string ssize;
      if (showbytes) {
	ssize = std::to_string((*it)->GetStatInfo()->GetSize());
      } else {
	eos::common::StringConversion::GetReadableSizeString(ssize, (*it)->GetStatInfo()->GetSize(), "B");
      }
      if (!json) {
	if (!silent) {
	  fprintf(stdout,"%-32s %s\n", ssize.c_str(), (*it)->GetName().c_str());
	}
      } else {
	Json::Value jentry;
	jentry["path"] = (*it)->GetName();
	jentry["size"] = (*it)->GetStatInfo()->GetSize();
	jentry["url"]  = archivefile + std::string("?xrdcl.unzip=/") + (*it)->GetName();
	gjson["archive"]["files"].append(jentry);
      }
      std::string cname = (*it)->GetName();
      // remove .zst
      std::string uname = cname.substr(0, (cname.length()>4)?cname.length()-4:cname.length());
      
      downloadJobs[cname].first = archivefile + std::string("?xrdcl.unzip=/") + cname;
      downloadJobs[cname].second = targeturl + std::string("/") + uname;
    }
    delete list;
    st =archive.CloseArchive(0,0);
    if (!st.IsOK()) {
      fprintf(stderr, "error: failed to close archive '%s'\n", archiveurl.c_str());
      exit(-1);
    }
    index++;
  } while (1);
  
  
  if (json) {
    if (!silent) {
      std::cout << SSTR(gjson) << std::endl;
    }
  }
  
  return 0;
}

int
XrdArchive::Download(size_t pjobs, bool json, bool silent)
{
  fprintf(stderr,"# Downloading %lu files\n", downloadJobs.size());
   
  std::vector<std::unique_ptr<std::future<int>>> futures;
  size_t cnt=0;
  for (auto it:downloadJobs) {
    napi++;
    if (!json) {
      std::cerr << "\33[2K" << "[ progress  ]: " << std::setw(6) << cnt << "/" << downloadJobs.size() << " [ " << right << std::setw(40) << it.first.c_str() << "]" << " bytes: "<< bytestoupload << "/" << bytesarchived << "                   " << "\r" << std::flush;
    }
    futures.emplace_back(std::make_unique<std::future<int>>(std::async(std::launch::async, getAndUncompressAPI, it.second.first, it.second.second)));
    if (zipdebug) {
      fprintf(stderr,"#File '%s'\n", it.first.c_str());
    }
    cnt++;
    if (ziperror) {
      fprintf(stderr,"error: aborting due to previous error!\n");
      exit(-1);
    }
    while (napi>=pjobs) {
      for ( size_t i=0; i<futures.size(); ++i) {
	if (futures[i] && futures[i]->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
	  futures[i]->wait();
	  futures[i] = nullptr;
	}
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (ziperror) {
	break;
      }
    }
  }
  if (!json) {
    if (!silent) {
      std::cerr << "\33[2K" << "[ progress  ]: " << std::setw(6) << cnt << "/" << downloadJobs.size() << " [ done ]";
      std::cerr << std::endl << std::flush;
    }
  }
  for ( size_t i=0; i<futures.size(); ++i) {
    std::future_status status;
    if (!futures[i]) {
      continue;
    }
    while ( (status=futures[i]->wait_for(std::chrono::milliseconds(10))) != std::future_status::ready) {
	if (!json) {
	  if (!silent) {
	    std::cerr << "\33[2K" << "[ finishing ]: " << std::setw(6) << (futures.size()-napi) << "/" << futures.size() << " bytes: " << bytestoupload << "/" << bytesarchived << "\n" << std::flush;
	  }
	}
    }
    futures[i]->wait();
    futures[i]=nullptr;
  }
  if (!json) {
    if (!silent) {
      std::cerr << "\33[2K" << "[ finishing ]: " << std::setw(6) << napi << "/" << futures.size() << " bytes: " << bytestoupload << "/" << bytesarchived << "\n" << std::flush;      
      std::cerr << std::endl << std::flush;
    }
  }
  if (archive.IsOpen()) {
    auto st = XrdCl::WaitFor ( XrdCl::CloseArchive (archive) );
    if (!st.IsOK()) {
      fprintf(stderr, "error: failed to close archive\n");
      return -1;
    }
  }
  
  XrdOucString inb;
  eos::common::StringConversion::GetReadableSizeString(inb, bytesread, "B");
  XrdOucString oub;
  eos::common::StringConversion::GetReadableSizeString(oub, bytestoupload, "B");
  if (!json) {
    if (!silent) {
      fprintf(stderr,"# tape archiving %lu files ucompressing %s to %s (%.02f%%)\n", downloadJobs.size(), inb.c_str(), oub.c_str(), bytesread?(100.0*bytestoupload/bytesread):0);
    }
  } else {
    Json::Value gjson;
    gjson["archive"]["url"] = archiveurl;
    gjson["archive"]["bytes::in"] = bytesread.load();
    gjson["archive"]["bytes::out"] = bytestoupload.load();
    gjson["archive"]["compression::ratio"] = bytesread?(1.00*bytestoupload/bytesread):0;
    gjson["archive"]["files::n"] = downloadJobs.size();
    gjson["archive"]["compression"] = zstdcompression?"zstd":"none";
    gjson["archive"]["splitsize"] = splitsize.load();
    if (!silent) {
      std::cout << SSTR(gjson) << std::endl;
    }
  }
  
  return 0;
}

int
XrdArchive::Close()
{
  auto st =archive.CloseArchive(0,0);
  
  if (!st.IsOK()) {
    errno = st.errNo;
    return -1;
  }
  return 0;
}

int
XrdArchive::Create()
{
  std::lock_guard g(archiveMutex);
  std::string archiveurlindex=archiveurl+".zip";
  auto st = XrdCl::WaitFor( XrdCl::OpenArchive ( archive, archiveurlindex.c_str(), XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write ) );
  if (!st.IsOK() ) {
    fprintf(stderr,"error: failed to open archive %s\n", archiveurl.c_str());
    ziperror = true;
    return -1;
  }
  return 0;
}
  
int
XrdArchive::uploadToArchive(const std::string& membername, const std::string& stagefile, const std::string& archiveurl)
{
  struct stat stbuf;
  const size_t blocksize=8*1024*1024ll;
  std::unique_ptr<eos::common::Buffer> buf = std::make_unique<eos::common::Buffer>(blocksize);
  CRC32 crc32;
  
  // compute crc32 for the stagefile
  int fd = ::open(stagefile.c_str(), 0);
  if (::fstat(fd, &stbuf)) {
    fprintf(stderr,"error: failed to stat stagefile %s\n", stagefile.c_str());
    close(fd);
    ziperror = true;
    return -1;
  }
  size_t n=0;
  off_t offset=0;
  do {
    n = ::read(fd, buf->GetDataPtr(), blocksize);
    if (zipdebug) {
      fprintf(stderr,"debug: read %ld bytes\n", n);
    }
    if (n>0) {
      crc32.Add(buf->GetDataPtr(), n, offset);
    }
    offset += n;
  } while( n>0) ;
  ::close(fd);

  // run only one upload at a time
  std::lock_guard g(archiveMutex);
  std::string archiveurlindex=archiveurl;
  
  if (!archive.IsOpen()) {
    archiveurlindex = getArchiveUrl(archiveurl, archiveindex);
    auto st = XrdCl::WaitFor( XrdCl::OpenArchive ( archive, archiveurlindex.c_str(), XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write ) );
    if (!st.IsOK() ) {
      fprintf(stderr,"error: failed to open archive %s\n", archiveurlindex.c_str());
      ziperror = true;
      return -1;
    }
  }

  fd = ::open(stagefile.c_str(), 0);
  
  if (::fstat(fd, &stbuf)) {
    fprintf(stderr,"error: failed to stat stagefile %s\n", stagefile.c_str());
    close(fd);
    ziperror = true;
    return -1;
  }
  int len=0;
  auto st = archive.OpenFile(membername, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Write | XrdCl::OpenFlags::Update, stbuf.st_size, *((uint32_t*)crc32.GetBinChecksum(len)));
  if (!st.IsOK() ) {
    fprintf(stderr,"error: failed to open new archivefile\n");
    close(fd);
    ziperror = true;
    return -1;
  }
  
  n=0;
  do {
    n = ::read(fd, buf->GetDataPtr(), blocksize);
    if (zipdebug) {
      fprintf(stderr,"debug: read %ld bytes\n", n);
    }
    if (n>0) {
      st = XrdCl::WaitFor ( Write(archive, n, buf->GetDataPtr()) );
      if (!st.IsOK() ) {
	fprintf(stderr,"error: write failed to archive '%s'\n", archiveurl.c_str());
	::close(fd);
	ziperror = true;
	return -1;
      }
    }
    bytesarchived += n; // total bytes
    archiveindexbytes += n; // bytes in current index archive file
  } while( n>0) ;

  st = XrdCl::WaitFor ( XrdCl::CloseFile (archive) ) ;
  if (!st.IsOK()) {
    fprintf(stderr,"error: closing failed of archive '%s'\n", archiveurl.c_str());
    ziperror = true;
    ::close(fd);
    return -1;
  }
  ::close(fd);
  
  // check if we need a new archive file
  if (archiveindexbytes >= splitsize) {
    // yes, file is already bigger
    st = XrdCl::WaitFor ( XrdCl::CloseArchive (archive) );
    if (!st.IsOK()) {
      fprintf(stderr, "error: failed to close archive '%s'\n", archiveurlindex.c_str());
      return -1;
    }
    // move to next index
    archiveindex++;
    archiveindexbytes=0;
  }
  
  return 0;
}


int
XrdArchive::getAndCompressAPI(std::string fname, std::string stagefile, std::string archive)
{
  // remove quotes
  fname.pop_back();
  fname.erase(0,1);

  XrdOucString contractedpath = fname.c_str();
  while (contractedpath.replace("/","::")) {}
  stagefile += contractedpath.c_str();

    
  FILE* const fout = ::fopen(stagefile.c_str(), "wb");
  if (!fout) {
    fprintf(stderr,"error: unable to open stage file '%s'\n", stagefile.c_str());
    napi--;
    ziperror = true;
    return -1;
  }
  XrdCl::File xfile;
  auto st=xfile.Open(fname, XrdCl::OpenFlags::Read);
  if (!st.IsOK()) {
    fprintf(stderr,"error: failed to open '%s'\n", fname.c_str());
    napi--;
    fclose(fout);
    ziperror = true;
    return -1;
  }

  const size_t blocksize=8*1024*1024ll;
  std::unique_ptr<eos::common::Buffer> buf = std::make_unique<eos::common::Buffer>(blocksize);
  std::unique_ptr<eos::common::Buffer> compbuf = std::make_unique<eos::common::Buffer>(2*blocksize);

  uint64_t offset = 0;
  uint32_t bytesRead=0;
  uint32_t bytesComp=2*blocksize;
  
  ZSTD_CCtx* const cctx = ZSTD_createCCtx();
  int cLevel = 10;
  ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, cLevel);
  ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
  
  if (!cctx) {
    fprintf(stderr,"error: failed to create compression context\n");
    fclose(fout);
    ziperror = true;
    return -1;
  }
  do {
    st = xfile.Read(offset, blocksize, buf->GetDataPtr(), bytesRead,0);
    ZSTD_EndDirective const mode = (bytesRead!=blocksize) ? ZSTD_e_end : ZSTD_e_continue;
    if (bytesRead>0) {
      if (zstdcompression) {
	// compressed output
	ZSTD_inBuffer input = { buf->GetDataPtr(), bytesRead, 0 };
	ZSTD_outBuffer output = { compbuf->GetDataPtr(), bytesComp, 0 };
	size_t const remaining = ZSTD_compressStream2(cctx, &output , &input, mode);
	if (remaining) {
	  napi--;
	  ZSTD_freeCCtx(cctx);
	  fclose(fout);
	  ziperror = true;
	  return -1;
	}
	size_t nwrite = fwrite(compbuf->GetDataPtr(), output.pos, 1,  fout);
	if (nwrite != 1) {
	  fprintf(stderr,"error: write error writing '%s'\n", stagefile.c_str());
	  napi--;
	  ZSTD_freeCCtx(cctx);
	  fclose(fout);
	  ziperror = true;
	  return -1;
	}
	offset += bytesRead;
	bytestoupload += output.pos;
	bytesread += bytesRead;
      } else {
	// uncompressed output
	size_t nwrite = fwrite(buf->GetDataPtr(), bytesRead, 1,  fout);
	if (nwrite != 1) {
	  fprintf(stderr,"error: write error writing '%s'\n", stagefile.c_str());
	  napi--;
	  ZSTD_freeCCtx(cctx);
	  fclose(fout);
	  ziperror = true;
	  return -1;
	}
	offset += bytesRead;
	bytestoupload += bytesRead;
	bytesread += bytesRead;
      }
    }
    if (zipdebug) {
      fprintf(stderr,"debug: read=%u %s\n", bytesRead, fname.c_str());
    }
  } while (bytesRead == blocksize);
  
  st=xfile.Close();
  napi--;
  ZSTD_freeCCtx(cctx);
  
  XrdCl::URL url(fname.c_str());
  fname = url.GetPath();
  if (zstdcompression) {
    fname += ".zst";
  }
  int rc = uploadToArchive(fname, stagefile, archive);
  ::unlink(stagefile.c_str());

  if (rc) {
    ziperror = true;
  }
  return rc;
}


int
XrdArchive::getAndUncompressAPI(std::string source, std::string target)
{

  XrdCl::File xfile;
  XrdCl::File yfile;
  XrdCl::ZipArchive archive;

  auto st = XrdCl::WaitFor( XrdCl::OpenArchive ( archive, source.c_str(), XrdCl::OpenFlags::Read) );
  if (!st.IsOK()) {
    fprintf(stderr,"error: unable to open '%s' [%s]\n", source.c_str(), st.GetErrorMessage().c_str());
    ziperror = true;
    return -1;
  }

  XrdCl::URL url(source);
  auto params = url.GetParams();
  
  st = archive.OpenFile(params["xrdcl.unzip"], XrdCl::OpenFlags::Read);
  if (!st.IsOK() ) {
    fprintf(stderr,"error: failed to open new archivefile\n");
    ziperror = true;
    return -1;
  }
  
  XrdCl::StatInfo *info = 0;
  uint64_t rSize=0;
  st = archive.Stat( info );
  if(! st.IsOK() ) {
    ziperror = true;
    return -1;
  } else {
    rSize = info->GetSize();
    delete info;
  }

  st=yfile.Open(target, XrdCl::OpenFlags::New | XrdCl::OpenFlags::Update | XrdCl::OpenFlags::MakePath);
  if (!st.IsOK()) {
    fprintf(stderr,"error: failed to open target '%s'\n", target.c_str());
    napi--;
    ziperror = true;
    return -1;
  }
  
  const size_t blocksize=ZSTD_DStreamInSize();
  const size_t blockoutsize =ZSTD_DStreamOutSize();
  
  std::unique_ptr<eos::common::Buffer> buf = std::make_unique<eos::common::Buffer>(blocksize);
  std::unique_ptr<eos::common::Buffer> compbuf = std::make_unique<eos::common::Buffer>(blockoutsize);

  uint64_t offset = 0;
  uint32_t bytesRead=0;
  uint32_t bytesComp=blockoutsize;
  
  ZSTD_DCtx* const dctx = ZSTD_createDCtx();
  
  if (!dctx) {
    fprintf(stderr,"error: failed to create decompression context\n");
    ziperror = true;
    return -1;
  }
  do {
    bytesRead=blocksize;
    auto bytesleft = rSize - offset;
    if (bytesleft < blocksize) {
      bytesRead = bytesleft;
    }
    st = archive.Read(offset, blocksize, buf->GetDataPtr(),0,0);
    if (st.IsOK()) {
      if (zstdcompression) {
	// compressed output
	ZSTD_inBuffer input = { buf->GetDataPtr(), bytesRead, 0 };
	size_t bytesout=0;
	while (input.pos < input.size) {
	  ZSTD_outBuffer output = { compbuf->GetDataPtr(), bytesComp, 0 };
	  size_t const remaining = ZSTD_decompressStream(dctx, &output , &input);
	  if (remaining) {
	    napi--;
	    ZSTD_freeDCtx(dctx);
	    ziperror = true;
	    return -1;
	  }
	  bytesout+=output.pos;
	  fprintf(stderr,"decompressed %lu to %lu\n", bytesRead, bytesout);
	  st = yfile.Write( offset, output.pos, compbuf->GetDataPtr(), (uint16_t)0);
	  if (!st.IsOK()) {
	    fprintf(stderr,"error: write error writing '%s'\n", target.c_str());
	    napi--;
	    ZSTD_freeDCtx(dctx);
	    ziperror = true;
	    return -1;
	  }
	}
	offset += bytesRead;
	bytestoupload += bytesout;
	bytesread += bytesRead;
      } else {
	fprintf(stderr,"copying %lu of %lu \n", bytesRead, blocksize);
	// uncompressed output
	st = yfile.Write( offset, bytesRead, buf->GetDataPtr(),(uint16_t)0);
	if (!st.IsOK()) {
	  fprintf(stderr,"error: write error writing '%s'\n", target.c_str());
	  napi--;
	  ZSTD_freeDCtx(dctx);
	  ziperror = true;
	  return -1;
	}
	offset += bytesRead;
	bytestoupload += bytesRead;
	bytesread += bytesRead;
      }
    } else {
      fprintf(stderr,"error: error reading '%s'\n", source.c_str());
      ZSTD_freeDCtx(dctx);
      ziperror = true;
      return -1;
    }
    if (zipdebug) {
      fprintf(stderr,"debug: read=%u %s\n", bytesRead, source.c_str());
    }
  } while (bytesRead == blocksize);
  
  st=xfile.Close();
  if (!st.IsOK()) {
    fprintf(stderr,"error: failed to close source file '%s'\n", source.c_str());
    ziperror = true;
    return -1;
  }
 
  ZSTD_freeDCtx(dctx);

  st=yfile.Close();

  if (!st.IsOK()) {
    napi--;
    fprintf(stderr,"error: failed to close target file '%s'\n", target.c_str());
    ziperror = true;
    return -1;
  }
  st = XrdCl::WaitFor( XrdCl::CloseArchive ( archive) );
  napi--;
  return 0;
}

std::string
XrdArchive::getArchiveUrl(std::string url, size_t idx) {
  if (idx == 0) {
    url+= ".zip";
  } else {
    char index[128];
    snprintf(index,sizeof(index), "%02lu", idx);
    url+= ".z";
    url += index;
  }
  return url;
}

std::vector<std::string>
XrdArchive::loadFileList(std::string path) 
{
  std::vector<std::string> files;
  std::string inputfiles;
  eos::common::StringConversion::LoadFileIntoString(path.c_str(), inputfiles);
  if (!inputfiles.length()) {
    fprintf(stderr,"error: input file list cannot be read or is empty\n");
  }
  if (inputfiles.back() != '\n') {
    inputfiles += "\n";
  }
  eos::common::StringConversion::Tokenize(inputfiles, files, "\n");
  return files;
}


int
XrdArchive::Upload(const std::vector<std::string>& files, size_t pjobs, bool json, bool silent, size_t spsize, const std::string& stagefile) {
  if (spsize) {
    splitsize = spsize;
  }
  
  std::vector<std::unique_ptr<std::future<int>>> futures;
  size_t cnt=0;
  for (auto it:files) {
    napi++;
    if (!json) {
      std::cerr << "\33[2K" << "[ progress  ]: " << std::setw(6) << cnt << "/" << files.size() << " [ " << right << std::setw(40) << it.c_str() << "]" << " bytes: "<< bytestoupload << "/" << bytesarchived << "                   " << "\r" << std::flush;
    }
    futures.emplace_back(std::make_unique<std::future<int>>(std::async(std::launch::async, getAndCompressAPI, it, stagefile, archiveurl)));
    if (zipdebug) {
      fprintf(stderr,"#File '%s'\n", it.c_str());
    }
    cnt++;
    if (ziperror) {
      fprintf(stderr,"error: aborting due to previous error!\n");
      exit(-1);
    }
    while (napi>=pjobs) {
      for ( size_t i=0; i<futures.size(); ++i) {
	if (futures[i] && futures[i]->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
	  futures[i]->wait();
	  futures[i] = nullptr;
	}
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (ziperror) {
	break;
      }
    }
  }
  if (!json) {
    if (!silent) {
      std::cerr << "\33[2K" << "[ progress  ]: " << std::setw(6) << cnt << "/" << files.size() << " [ done ]";
      std::cerr << std::endl << std::flush;
    }
  }
  for ( size_t i=0; i<futures.size(); ++i) {
    std::future_status status;
    if (!futures[i]) {
      continue;
    }
    while ( (status=futures[i]->wait_for(std::chrono::milliseconds(10))) != std::future_status::ready) {
	if (!json) {
	  if (!silent) {
	    std::cerr << "\33[2K" << "[ finishing ]: " << std::setw(6) << (futures.size()-napi) << "/" << futures.size() << " bytes: " << bytestoupload << "/" << bytesarchived << "\r" << std::flush;
	  }
	}
    }
    futures[i]->wait();
    futures[i]=nullptr;
  }
  if (!json) {
    if (!silent) {
      std::cerr << "\33[2K" << "[ finishing ]: " << std::setw(6) << napi << "/" << futures.size() << " bytes: " << bytestoupload << "/" << bytesarchived << "\r" << std::flush;      
      std::cerr << std::endl << std::flush;
    }
  }
  if (archive.IsOpen()) {
    auto st = XrdCl::WaitFor ( XrdCl::CloseArchive (archive) );
    if (!st.IsOK()) {
      fprintf(stderr, "error: failed to close archive\n");
      return -1;
    }
  }
  
  XrdOucString inb;
  eos::common::StringConversion::GetReadableSizeString(inb, bytesread, "B");
  XrdOucString oub;
  eos::common::StringConversion::GetReadableSizeString(oub, bytestoupload, "B");
  if (!json) {
    if (!silent) {
      fprintf(stderr,"# tape archiving %lu files compressing %s to %s (%.02f%%)\n", files.size(), inb.c_str(), oub.c_str(), bytesread?(100.0*bytestoupload/bytesread):0);
    }
  } else {
    Json::Value gjson;
    gjson["archive"]["url"] = archiveurl;
    gjson["archive"]["bytes::in"] = bytesread.load();
    gjson["archive"]["bytes::out"] = bytestoupload.load();
    gjson["archive"]["compression::ratio"] = bytesread?(1.00*bytestoupload/bytesread):0;
    gjson["archive"]["files::n"] = files.size();
    gjson["archive"]["compression"] = zstdcompression?"zstd":"none";
    gjson["archive"]["splitsize"] = splitsize.load();
    if (!silent) {
      std::cout << SSTR(gjson) << std::endl;
    }
  }
  return 0;
}



EOSCOMMONNAMESPACE_END
