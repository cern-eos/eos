// ----------------------------------------------------------------------
// File: ComputeBlockXS.cc
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

#include "common/Attr.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "common/LayoutId.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int
main (int argc, const char* argv[])
{
  if (argc != 2)
  {
    fprintf(stderr, "usage: eos-check-blockxs <path> \n");
    exit(-1);
  }

  XrdOucString path = argv[1];
  XrdOucString pathXS = path;
  pathXS += ".xsmap";

  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
  {
    fprintf(stderr, "error: cannot open path %s\n", path.c_str());
    exit(-1);
  }

  int fdxs = open(pathXS.c_str(), O_RDONLY);
  if (fdxs < 0)
  {
    fprintf(stderr, "error: cannot open block checksum file for path %s\n", pathXS.c_str());
    exit(-1);
  }

  int ngood = 0;
  int nerr = 0;

  eos::common::Attr *attr = eos::common::Attr::OpenAttr(pathXS.c_str());
  if (attr)
  {
    std::string checksumtype = attr->Get("user.eos.blockchecksum");
    std::string blocksize = attr->Get("user.eos.blocksize");

    if ((checksumtype == "") || (blocksize == ""))
    {
      fprintf(stderr, "error: the extended attributes are missing on the block checksum file!\n");
      exit(-1);
    }
    XrdOucString envstring = "eos.layout.blockchecksum=";
    envstring += checksumtype.c_str();
    XrdOucEnv env(envstring.c_str());
    unsigned long checksumType = eos::common::LayoutId::GetBlockChecksumFromEnv(env);

    int blockSize = atoi(blocksize.c_str());
    int blockSizeSymbol = eos::common::LayoutId::BlockSizeEnum(blockSize);


    unsigned int layoutid = eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain, eos::common::LayoutId::kNone, 0, blockSizeSymbol, checksumType);

    eos::fst::CheckSum *checksum = eos::fst::ChecksumPlugins::GetChecksumObject(layoutid, true);

    struct stat info;
    fstat(fd, &info);
    off_t maxfilesize = info.st_size;
    off_t offset = 0;
    char* buffer = (char*) malloc(blockSize);
    if (!buffer)
    {
      fprintf(stderr, "error: cannot allocate blockmemory of size %u\n", (unsigned int) blockSize);
      exit(-1);
    }

    if (checksum && checksum->OpenMap(pathXS.c_str(), maxfilesize, blockSize, true))
    {
      do
      {
        int nread = read(fd, buffer, blockSize);
        if (nread < 0)
        {
          fprintf(stderr, "error: failed to read block at offset %llu\n", (unsigned long long) offset);
          exit(-1);
        }
        if (nread < blockSize)
        {
          // 0 rest of the block
          memset(buffer + nread, 0, blockSize - nread);
        }
        checksum->Reset();
        if (!checksum->AddBlockSum(offset, buffer, blockSize))
        {
          fprintf(stderr, "block-XS error => offset %llu\n", (unsigned long long) offset);
          nerr++;
        }
        else
        {
          //          fprintf(stderr,"block-XS ok    => offset %llu\n", (unsigned long long)offset);
          ngood++;
        }
        if (nread < blockSize)
          break;
        offset += nread;
      }
      while (1);
    }
    else
    {
      fprintf(stderr, "error: unable to open block checksum map\n");
    }
    checksum->CloseMap();
    free(buffer);
  }
  else
  {
    fprintf(stderr, "error: no extended attributes on block checksum file!\n");
    exit(-1);
  }
  close(fd);
  close(fdxs);
  fprintf(stderr, "%s : tot: %u ok: %u error: %u\n", path.c_str(), ngood + nerr, ngood, nerr);
  if (nerr)
    exit(-1);
  exit(0);
}

