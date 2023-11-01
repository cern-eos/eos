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

#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/BufferManager.hh"
#include "common/XrdCopy.hh"
#include "common/XrdArchive.hh"

#include "fst/checksum/CRC32.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdCl/XrdClFile.hh"
#include "private/XrdCl/XrdClZipArchive.hh"
#include "private/XrdCl/XrdClOperations.hh"
#include "private/XrdCl/XrdClZipOperations.hh"
#include <unistd.h>
void com_zip_usage() {
  fprintf(stderr,"usage: zip create <sourcelistfile> <targeturl> zstd|none --split <bytes> - create a new archive with files from filelist\n");
  fprintf(stderr,"                  <sourcelistfile> : a linewise file with urls to the files to archive\n");
  fprintf(stderr,"                       <targeturl> : a URL pointing to a named archive - don't append '.zip' to the named archive\n");
  fprintf(stderr,"       zip create ... split <bytes : when targeturl has reached <bytes> [default 32GB], it automatically creates a new archive file appending an index when the archive reached <bytes> in size. The size has to be atleast 1MB!\n");
  fprintf(stderr,"                        example:   archive.zip, archive.z01, archive.z02 archive.z03 ... \n");
  fprintf(stderr,"\n");
  fprintf(stderr,"       zip ls <archiveurl> [-b]\n - list archive");
  fprintf(stderr,"                      <archiveurl> : archive url without .zip suffix!\n");
  fprintf(stderr,"                     -b : show file size in bytes\n");
  fprintf(stderr,"       eos --json zip ..: print output in JSON format\n");
  fprintf(stderr,"\n");
  fprintf(stderr,"       zip get <archiveurl> <targetdir> [<filter>] - download archive\n");
  fprintf(stderr,"                      <archiveurl> : archive url without .zip suffix!\n");
  fprintf(stderr,"                      <targeturl>  : url where to unpack zip file!\n");
  fprintf(stderr,"                      <filter>     : regex to match files from the archive!\n");
  exit(-1);
}

extern bool json;

int com_zip(char* arg1) {
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  eos::common::XrdArchive::zipdebug = false;
  eos::common::XrdArchive::ziperror = false;
  XrdOucString cmd = subtokenizer.GetToken();

  if ( (cmd != "create") &&
       (cmd != "ls") &&
       (cmd != "get") ) {
    com_zip_usage();
  }
  XrdOucString src = subtokenizer.GetToken();
  XrdOucString dst = subtokenizer.GetToken();
  XrdOucString compressor = subtokenizer.GetToken();

  if (cmd == "ls") {
    if (!src.length()) {
      com_zip_usage();
    }
  } else if (cmd == "get") {
    if (!src.length() || !dst.length()) {   
      com_zip_usage();
    }
  } else if (cmd == "create") {
    if (!src.length() || !dst.length()) {
      com_zip_usage();
    }
    if (compressor == "zstd") {
      eos::common::XrdArchive::zstdcompression = true;
    } else if (compressor == "none") {
      eos::common::XrdArchive::zstdcompression = false;
    } else {
      com_zip_usage();
    }
  } else {
    com_zip_usage();
  }

  if (cmd == "ls") {
    bool showbytes=false;
    std::string option = (dst.length())?dst.c_str():"";
    if (option == "-b") {
      showbytes = true;
    }

    eos::common::XrdArchive archive(src.c_str());
    if (!archive.Open(showbytes,json, false)) {
      exit(0);
    } else {
      exit(-1);
    }
  }

  if (cmd == "get") {
    std::string filter = (compressor.length())?compressor.c_str():"";

    eos::common::XrdArchive archive(src.c_str(), dst.c_str());
    if (!archive.Open(false, false, true)) {
      archive.Download(1, json, false);
      exit(0);
    } else {
      exit(-1);
    }
  }
  
  std::string stagearea = "/var/tmp/";
  size_t splitsize=0;
  if (cmd == "create") {
    // create new archive    
    do {
      XrdOucString option = subtokenizer.GetToken();

      if (option == "--split") {
        option = subtokenizer.GetToken();
        if (!option.length()) {
          com_zip_usage();
        } else {
          size_t n = strtoull(option.c_str(), 0, 10);
          if (!n || n < 1*1000*1000) {
            com_zip_usage();
          }
          splitsize = n;
          fprintf(stderr,"info: setting splitsize to %lu\n", splitsize);
        }
      }
      if (!option.length()) {
        break;
      }
    } while (1);

    std::vector<std::string>files;
    files = eos::common::XrdArchive::loadFileList(src.c_str());

    size_t pjobs=32;
    
    std::string stagefile;
    stagefile += stagearea;
    stagefile += "/";
    stagefile += "zip.";

    eos::common::XrdArchive archive(dst.c_str());
    if (archive.Upload(files, pjobs, json, false, splitsize, stagefile)) {
      exit(-1);
    }
   
    return 0;
  }
  return 0;
}
