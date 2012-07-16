// ----------------------------------------------------------------------
// File: RaidIO.cc
// Author: Elvin-Alin Sindrilaru - CERN
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

/*----------------------------------------------------------------------------*/
#include <set>
#include <cmath>
#include <string>
/*----------------------------------------------------------------------------*/
#include "common/Timing.hh"
#include "fst/io/RaidIO.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
RaidIO::RaidIO(std::string algorithm, std::vector<std::string> stripeurl,
               unsigned int nparitystripes, bool storerecovery, off_t targetsize,
               std::string bookingopaque):
    storeRecovery(storerecovery),
    nParityStripes(nparitystripes),
    targetSize(targetsize),    
    algorithmType(algorithm),
    bookingOpaque(bookingopaque),
    stripeUrls(stripeurl)    
{
  stripeWidth = STRIPESIZE;
  nTotalStripes = stripeUrls.size();
  nDataStripes = nTotalStripes - nParityStripes;

  fdUrl = new int[nTotalStripes];
  hdUrl = new HeaderCRC[nTotalStripes];
  sizeHeader = hdUrl[0].getSize();
  
  for (unsigned int i = 0; i < nTotalStripes; i++){
    fdUrl[i] = -1;
  }

  isRW = false;
  isOpen = false;
  doTruncate = false;
  updateHeader = false;
  doneRecovery = false;
  fullDataBlocks = false;
  offsetGroupParity = 0;
}


/*----------------------------------------------------------------------------*/
RaidIO::~RaidIO()
{
  delete[] fdUrl;
  delete[] hdUrl;  
}


//------------------------------------------------------------------------------
int
RaidIO::open(int flags)
{
  if (nTotalStripes < 2) {
    eos_err("Failed to open raidDP layout - stripe size should be at least 2");
    fprintf(stdout, "Failed to open raidDP layout - stripe size should be at least 2.\n");
    return -1;
  }

  if (stripeWidth < 64) {
    eos_err("Failed to open raidDP layout - stripe width should be at least 64");
    fprintf(stdout, "Failed to open raidDP layout - stripe width should be at least 64.\n");
    return -1;
  }

  for (unsigned int i = 0; i < nTotalStripes; i++)
  {
    fprintf(stderr,"open %s\n", stripeUrls[i].c_str());
    if (!(flags | O_RDONLY)) {
      fdUrl[i] = XrdPosixXrootd::Open(stripeUrls[i].c_str(), O_RDONLY,
                                      kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or);
    }
    else if (flags & O_WRONLY) {
      isRW = true;
      fdUrl[i] = XrdPosixXrootd::Open(stripeUrls[i].c_str(),
                                      kXR_async | kXR_mkpath | kXR_open_updt | kXR_new,
                                      kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or);
    }
    else if (flags & O_RDWR) {
      isRW = true;
      fdUrl[i] = XrdPosixXrootd::Open(stripeUrls[i].c_str(), O_RDWR,
                                      kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or);
    }
  }
 
  for (unsigned int i = 0; i < nTotalStripes; i++) {
    if (hdUrl[i].readFromFile(fdUrl[i])) {
      mapUrl_Stripe.insert(std::pair<unsigned int, unsigned int>(i, hdUrl[i].getIdStripe()));
      mapStripe_Url.insert(std::pair<unsigned int, unsigned int>(hdUrl[i].getIdStripe(), i));
    }
    else {
      mapUrl_Stripe.insert(std::pair<int, int>(i, i));
      mapStripe_Url.insert(std::pair<int, int>(i, i));
    }
  }

  if (!validateHeader()) {
    eos_err("Header invalid - can not continue");
    fprintf(stdout, "Header invalid - can not continue.\n");
    return -1;
  }

  //get the size of the file
  if (hdUrl[0].getNoBlocks() == 0) {
    fileSize = 0;
  } else {
    fileSize = (hdUrl[0].getNoBlocks() - 1) * stripeWidth + hdUrl[0].getSizeLastBlock();
  }
  
  isOpen = true;
  eos_info("Returning SFS_OK");
  fprintf(stdout, "Returning SFS_OK with fileSize=%zu.\n", fileSize);
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
//recover in case the header is corrupted
bool
RaidIO::validateHeader()
{  
  bool newFile = true;
  bool allHdValid = true;
  vector<unsigned int> idUrlInvalid;
 
  for (unsigned int i = 0; i < nTotalStripes; i++){
    if (hdUrl[i].isValid()){
      newFile = false;
    } else {
      allHdValid = false;
      idUrlInvalid.push_back(i);
    }
  }
  
  if (newFile || allHdValid) {
    eos_debug("File is either new or there are no corruptions in the headers.");
    fprintf(stdout, "File is either new or there are no corruptions in the headers.\n");
    if (newFile) {
      for (unsigned int i = 0; i < nTotalStripes; i++) {
        hdUrl[i].setState(true);    //set valid header
        hdUrl[i].setNoBlocks(0);
        hdUrl[i].setSizeLastBlock(0);
      }      
    }    
    return true;
  }
  
  //can not recover from more than two corruptions
  if (idUrlInvalid.size() > nParityStripes) {
    eos_debug("Can not recover from more than %u corruptions.", nParityStripes);
    fprintf(stdout, "Can not recover from more than %u corruptions.\n", nParityStripes);
    return false;
  }
  
  //get stripe id's already used and a valid header
  unsigned int idHdValid = -1;
  std::set<unsigned int> usedStripes;
  for (unsigned int i = 0; i < nTotalStripes; i++){
    if (hdUrl[i].isValid()) {
      usedStripes.insert(mapUrl_Stripe[i]);
      idHdValid = i;
    }
    else {
      mapUrl_Stripe.erase(i);
    }
  }
  mapStripe_Url.clear();
  
  while (idUrlInvalid.size()){
    unsigned int idUrl = idUrlInvalid.back();
    idUrlInvalid.pop_back();
    
    for (unsigned int i = 0; i < nTotalStripes; i++)
    {
      if (find(usedStripes.begin(), usedStripes.end(), i) == usedStripes.end()) //not used
      {
        //add the new mapping
        eos_debug("Add new mapping: stripe: %u, fid: %u", i, idUrl);
        mapUrl_Stripe[idUrl] = i;
        usedStripes.insert(i);
        hdUrl[idUrl].setIdStripe(i);
        hdUrl[idUrl].setState(true);
        hdUrl[idUrl].setNoBlocks(hdUrl[idHdValid].getNoBlocks());
        hdUrl[idUrl].setSizeLastBlock(hdUrl[idHdValid].getSizeLastBlock());
        if (storeRecovery) {
          XrdPosixXrootd::Close(fdUrl[idUrl]);
          //open stripe for writing
          fdUrl[i] = XrdPosixXrootd::Open(stripeUrls[i].c_str(),
                                          kXR_async | kXR_mkpath | kXR_open_updt | kXR_new,
                                          kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or);
          hdUrl[idUrl].writeToFile(fdUrl[idUrl]);
        }
        break;
      }
    }
  }
  
  usedStripes.clear();
  
  //populate the stripe url map
  for (unsigned int i = 0; i< nTotalStripes; i++) {
    mapStripe_Url[mapUrl_Stripe[i]] = i;
  }
 
  return true;
}


/*----------------------------------------------------------------------------*/
int
RaidIO::read(off_t offset, char* buffer, size_t length)
{
  eos::common::Timing rt("read");
  COMMONTIMING("start", &rt);
  
  size_t aread = 0;
  size_t nread = 0;
  size_t readLength = 0;
  off_t offsetLocal = 0;
  unsigned int stripeId;

  if (offset > (off_t)fileSize) {
    eos_err("error=offset is larger then file size");
    return 0;
  }

  if (offset + length > fileSize) {
    eos_warning("Read range larger than file, resizing the read length");
    length = fileSize - offset;    
  }      

  if ((offset < 0) && (isRW)) {  //recover file mode
    offset = 0;
    char *dummyBuf = (char*) calloc(stripeWidth, sizeof(char));

    while (length) {
      nread = (length > stripeWidth) ? stripeWidth : length;
      if((offset % sizeGroupBlocks == 0) && (!recoverBlock(dummyBuf, offset, nread)))
      {
        free(dummyBuf);
        eos_err("error=failed recovery of stripe");
        return -1;
      }
      length -= nread;
      offset += nread;
      readLength += nread;
    }
    //free memory
    free(dummyBuf);
  }
  else{ //normal reading mode
    bool doRecovery;
    doRecovery = false;
    while (length)
    {
      stripeId = (offset / stripeWidth) % nDataStripes;
      nread = (length > stripeWidth) ? stripeWidth : length;
      offsetLocal = ((offset / (nDataStripes * stripeWidth)) * stripeWidth) + (offset %  stripeWidth);

      size_t lread = nread;
      do {
        COMMONTIMING("read remote in", &rt);
	//        fprintf(stdout, "File descriptor is: %i \n", fdUrl[mapStripe_Url[stripeId]]);
        if (fdUrl[mapStripe_Url[stripeId]] >= 0) {
          aread = XrdPosixXrootd::Pread(fdUrl[mapStripe_Url[stripeId]], buffer, lread, offsetLocal + sizeHeader);
        } else {
          aread = 0;
          fprintf(stdout, "Setting aread = %zu\n", aread);
        }
        COMMONTIMING("read remote out in", &rt);
        if (aread > 0) {
          if (aread != lread) {
            lread -= aread;
            offsetLocal += lread;
          } else {
            break;
          }
        } else {
          eos_warning("Read returned %ld instead of %ld bytes", aread, lread);
          fprintf(stdout, "Read returned %ld instead of %ld bytes. \n", aread, lread);
          doRecovery = true;
          break;
        }
      } while (lread);

      COMMONTIMING("read recovery", &rt);
      if (doRecovery) {
        if (!recoverBlock(buffer, offset, nread)) { 
          eos_err("error=read recovery failed");
          return -1;
        }
      }

      length -= nread;
      offset += nread;
      buffer += nread;
      readLength += nread;
    }
  }

  COMMONTIMING("read return", &rt);
  //  rt.Print();
  return readLength; 
}


/*----------------------------------------------------------------------------*/
int
RaidIO::write(off_t offset, char* buffer, size_t length)
{
  eos::common::Timing wt("write");
  COMMONTIMING("start", &wt);

  int rc;
  size_t nwrite;
  size_t writeLength = 0;
  off_t offsetLocal;
  off_t offsetStart;
  off_t offsetEnd;
  unsigned int stripeId = -1;

  offsetStart = offset;
  offsetEnd = offset + length;

  while (length) {
    stripeId = (offset / stripeWidth) % nDataStripes;
    nwrite = (length < stripeWidth) ? length : stripeWidth;
    offsetLocal = ((offset / (nDataStripes * stripeWidth)) * stripeWidth) + (offset % stripeWidth);

    COMMONTIMING("write remote", &wt);
    eos_info("Write stripe=%u offset=%llu size=%u", stripeId, offsetLocal + sizeHeader, nwrite);
    rc = XrdPosixXrootd::Pwrite(fdUrl[mapStripe_Url[stripeId]], buffer, nwrite, offsetLocal + sizeHeader);
    if (rc != (int)nwrite) {
      eos_err("Write failed offset=%zu, length=%zu", offset, length);
      return -1;
    }

    //add data to the dataBlocks array and compute parity if enough information
    addDataBlock(offset, buffer, nwrite);
    
    offset += nwrite;
    length -= nwrite;
    buffer += nwrite;
    writeLength += nwrite;
  }

  if (offsetEnd > (off_t)fileSize) {
    fileSize = offsetEnd;
    doTruncate = true;
  }

  COMMONTIMING("end", &wt);
  //  wt.Print();
  return writeLength;
}

/*----------------------------------------------------------------------------*/
int
RaidIO::sync()
{
  int rc = SFS_OK;
  if (isOpen)
  {
    for (unsigned int i = 0; i < nTotalStripes; i++)
    {
      rc -= XrdPosixXrootd::Fsync(fdUrl[i]);
    }
  } else {
    eos_err("sync error=file is not opened");
    return -1;
  }
  return rc;
}


/*----------------------------------------------------------------------------*/
off_t
RaidIO::size()
{
  if (isOpen) 
    return fileSize;
  else {
    eos_err("size error=file is not opened");
    return -1;
  }
}


/*----------------------------------------------------------------------------*/
int
RaidIO::remove()
{
  int rc = SFS_OK;
  
  for (unsigned int i = 0; i <nTotalStripes; i++) {
    rc -= XrdPosixXrootd::Unlink(stripeUrls[i].c_str());
  }

  return rc;
}


/*----------------------------------------------------------------------------*/
int
RaidIO::stat(struct stat *buf)
{
  int rc = 0;
  rc = XrdPosixXrootd::Fstat(fdUrl[0], buf);
  if (rc) {
    eos_err("stat error=error in stat");
    return -1;
  }
  
  buf->st_size = fileSize;
  return rc;
}


/*----------------------------------------------------------------------------*/
int
RaidIO::close()
{
  eos::common::Timing ct("close");
  COMMONTIMING("start", &ct);

  int rc = SFS_OK;

  if (isOpen)
  {
    if (offsetGroupParity != 0 && offsetGroupParity < (off_t)fileSize) {
      computeDataBlocksParity(offsetGroupParity);
    }

    //update the header information and write it to all stripes
    long int nblocks = ceil((fileSize * 1.0) / stripeWidth);
    size_t sizelastblock = fileSize % stripeWidth;

    for (unsigned int i = 0; i < nTotalStripes; i++)
    {
      if (nblocks != hdUrl[i].getNoBlocks()) {
        hdUrl[i].setNoBlocks(nblocks);
        updateHeader = true;
      }
      if (sizelastblock != hdUrl[i].getSizeLastBlock()) {
        hdUrl[i].setSizeLastBlock(sizelastblock);
        updateHeader =  true;
      }
    }

    COMMONTIMING("updateheader", &ct);
    if (updateHeader){
      for (unsigned int i = 0; i < nTotalStripes; i++){  //fstid's
        eos_info("Write Stripe Header local");
         hdUrl[i].setIdStripe(mapUrl_Stripe[i]);
        if (hdUrl[i].writeToFile(fdUrl[i])) {
          eos_err("error=write header to file failed for stripe:%i", i);
          return -1;
        }
      }
      updateHeader = false;
    }
    
    if (doneRecovery || doTruncate) {
      doTruncate = false;
      doneRecovery = false;
      eos_info("Close: truncating after done a recovery or at end of write");
      truncate(fileSize);
    }
    
    for (unsigned int i = 0; i < nTotalStripes; i++) {
      rc -= XrdPosixXrootd::Close(fdUrl[i]);
    }
  }
  else {
    eos_err("error=file is not opened");
    return -1;
  }

  isOpen = false;
  return rc;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END
