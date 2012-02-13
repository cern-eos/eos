// ----------------------------------------------------------------------
// File: RaidDPLayout.cc
// Author: Elvin Sindrilaru - CERN
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
#include "fst/layout/RaidDPLayout.hh"
#include "fst/XrdFstOfs.hh"
#include "common/Timing.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"
/*----------------------------------------------------------------------------*/
#include <cmath>
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

extern XrdOssSys  *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

typedef long v2do __attribute__((vector_size(VECTOR_SIZE)));

//------------------------------------------------------------------------------
RaidDPLayout::RaidDPLayout(XrdFstOfsFile* thisFile, int lid, XrdOucErrInfo *outerror) 
  : Layout(thisFile, "raidDP", lid, outerror)
{
  nStripes = eos::common::LayoutId::GetStripeNumber(lid) - 1;    //TODO: *** fix this!!!! ***
  stripeWidth = (XrdSfsXferSize) eos::common::LayoutId::GetBlocksize(lid);// kb units
  isOpen = false;

  eos_info("Created layout with stripes=%u width=%u\n", nStripes, stripeWidth);
  nFiles = nStripes + 2;                      //data files + parity files
  nBlocks = (int)(pow(nStripes, 2));          
  nTotalBlocks = nBlocks + 2 * nStripes;      

  hd =  new HeaderCRC();
  headerSize = hd->GetHeaderSize();
  
  stripeClient = new XrdClient*[nFiles];
  stripeUrl = new XrdOucString[nFiles];

  for (unsigned int i = 0; i < nFiles; i++){
    stripeClient[i] = 0;
    stripeUrl[i] = "";
  }

  //allocate memory for blocks
  for (unsigned int i=0; i < nTotalBlocks; i++)
    dataBlock.push_back(new char[stripeWidth]);

  fileSize = 0;    
  indexStripe = -1;
  updateHeader = false;
  doneRecovery = false;
  isEntryServer = false;
}

//------------------------------------------------------------------------------
int
RaidDPLayout::open(const char           *path,
                   XrdSfsFileOpenMode    open_mode,
                   mode_t                create_mode,
                   const XrdSecEntity   *client,
                   const char           *opaque)
{
  int rc = 0;

  eos_info("Opening  layout with path=%s open_mode=%x create_mode=%x stripes=%u width=%u\n", path, open_mode,create_mode,nStripes, stripeWidth);
  if (nStripes < 2) {
    eos_err("Failed to open raidDP layout - stripe size should be at least 2");
    return gOFS.Emsg("RaidDPOpen",*error, EREMOTEIO, "open stripes - stripe size must be at least 2");
  }

  if (stripeWidth < 64) {
    eos_err("Failed to open raidDP layout - stripe width should be at least 64");
    return gOFS.Emsg("RaidDPOpen",*error, EREMOTEIO, "open stripes - stripe width must be at least 64");
  }

  int nmissing = 0;
  // assign stripe urls
  for (unsigned int i = 0; i < nFiles; i++) {
    XrdOucString stripetag = "mgm.url"; stripetag += (int) i;
    const char* stripe = ofsFile->capOpaque->Get(stripetag.c_str());
    if ((ofsFile->isRW && ( !stripe )) || ((nmissing > 0) && (!stripe))) {
      eos_err("Failed to open stripes - missing url for stripe %s", stripetag.c_str());
      return gOFS.Emsg("RaidDPOpen",*error, EINVAL, "open stripes - missing url for stripe ", stripetag.c_str());
    }
    if (!stripe) {
      nmissing++;
      stripeUrl[i] = "";
    } else {
      stripeUrl[i] = stripe;
    }
  }

  if (nmissing){
    eos_err("Failed to open raidDP layout - stripes are missing.");
    return gOFS.Emsg("RaidDPOpen",*error, EREMOTEIO, "open stripes - stripes are missing.");
  }

  const char* index = ofsFile->openOpaque->Get("mgm.replicaindex");
  if (index >= 0) {  
    indexStripe = atoi(index);
    if ((indexStripe < 0) || (indexStripe > eos::common::LayoutId::kSixteenStripe)) {
      eos_err("Illegal stripe index %d", indexStripe);
      return gOFS.Emsg("RaidDPOpen",*error, EINVAL, "open stripes - illegal stripe index found", index);
    }
  }

  const char* head = ofsFile->openOpaque->Get("mgm.replicahead");
  if (head >= 0) {
    stripeHead = atoi(head);
    if ((stripeHead < 0) || (stripeHead>eos::common::LayoutId::kSixteenStripe)) {
      eos_err("Illegal stripe head %d", stripeHead);
      return gOFS.Emsg("RaidDPOpen",*error, EINVAL, "open stripes - illegal stripe head found", head);
    }
  } 
  else {
    eos_err("Stripe head missing");
    return gOFS.Emsg("RaidDPOpen",*error, EINVAL, "open stripes - no stripe head defined");
  }
  
  //doing local operation for current stripe
  rc = ofsFile->openofs(path, open_mode, create_mode, client, opaque, true, layOutId);
  eos_info("openofs gave rc=%d", rc);
  if (rc) {
    eos_info("openofs failed for path=%s open_mode=%x create_mode=%x", path, open_mode, create_mode);
    //if file does not exist then we create it
    if (!ofsFile->isRW)
      gOFS.Emsg("RaidDPOpen",*error, EIO, "open stripes - local open failed in read mode");
    open_mode |= SFS_O_CREAT;
    create_mode |= SFS_O_MKPTH;
    rc = ofsFile->openofs(path, open_mode, create_mode, client, opaque, true, layOutId);
    if (rc)
      return gOFS.Emsg("RaidDPOpen",*error, EIO, "open stripes - local open failed");
    eos_info("openofs with create flag ok for path=%s open_mode=%x create_mode=%x", path, open_mode, create_mode);
  }

  //operations done only at the entry server
  if (indexStripe == stripeHead){
    eos_info("We are the entry server");
    isEntryServer = true;

    bool *hdValid = new bool[nFiles];
    for (unsigned int i = 0; i < nFiles;  i++){
      hdValid[i] = false;
    }
    
    if (hd->ReadFromFile(ofsFile)){
      mapFst_Stripe.insert(std::pair<unsigned int, unsigned int>(indexStripe, hd->GetIdStripe()));
      mapStripe_Fst.insert(std::pair<unsigned int, unsigned int>(hd->GetIdStripe(), indexStripe));
      fileSize = (hd->GetNoBlocks() - 1) * stripeWidth + hd->GetSizeLastBlock();
      hdValid[indexStripe] = true;
    }
    else{
      mapFst_Stripe.insert(std::pair<unsigned int, unsigned int>(indexStripe, indexStripe));
      mapStripe_Fst.insert(std::pair<unsigned int, unsigned int>(indexStripe, indexStripe));
      hd->SetIdStripe(indexStripe);
      hd->SetSizeLastBlock(0);
      hd->SetNoBlocks(0);
      fileSize = 0;
    }

    HeaderCRC* tmpHd = new HeaderCRC();
    for (unsigned int i = 0; i < nFiles; i++) {
      // open all other stripes available
      if (i != indexStripe) {
        const char* val;
        int envlen;
        XrdOucString remoteOpenOpaque = ofsFile->openOpaque->Env(envlen);

        // create the opaque information for the next stripe file
        if ((val = ofsFile->openOpaque->Get("mgm.replicaindex"))) {
          XrdOucString oldindex = "mgm.replicaindex=";
          XrdOucString newindex = "mgm.replicaindex=";
          oldindex += val;
          newindex +=(int) i;
          remoteOpenOpaque.replace(oldindex.c_str(),newindex.c_str());
        } 
        else {
          remoteOpenOpaque += "&mgm.replicaindex=";
          remoteOpenOpaque += (int) i;
        }

        stripeUrl[i] += "?";
        stripeUrl[i] += remoteOpenOpaque;
        stripeClient[i] = new XrdClient(stripeUrl[i].c_str());
	// create the r/w cache and read ahead
	stripeClient[i]->SetCacheParameters(20*1024*1024,4*1024*1024,-1);
	stripeClient[i]->UseCache(true);

        if (ofsFile->isRW) {
	  eos_info("Opening write remote url %s\n", stripeUrl[i].c_str());	  
          // write case
          if (!stripeClient[i]->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | 
                                     kXR_open_updt | kXR_new, false)) {
            eos_err("Failed to open stripes - remote open write failed on %s ", stripeUrl[i].c_str());
            return gOFS.Emsg("RaidDPOpen",*error, EREMOTEIO, "open stripes - remote open failed ", stripeUrl[i].c_str());
          }           
        } 
        else {	 
	  eos_info("Opening read remote url %s\n", stripeUrl[i].c_str());
          // read case
          if (!stripeClient[i]->Open(0,0 , false)) {
            eos_warning("Failed to open stripe - remote open read failed on %s ", stripeUrl[i].c_str());
          }
        }

	if (stripeClient[i]->IsOpen()) {
	  eos_info("Reading Remote header from %s\n", stripeUrl[i].c_str());
	  //read the header information of the opened stripe
	  if (tmpHd->ReadFromFile(stripeClient[i])){
	    mapFst_Stripe.insert(std::pair<int, int>(i, tmpHd->GetIdStripe()));
	    mapStripe_Fst.insert(std::pair<int, int>(tmpHd->GetIdStripe(), i));
	    hdValid[i] = true;
	  } else {
	    mapFst_Stripe.insert(std::pair<int, int>(i, i));
	    mapStripe_Fst.insert(std::pair<int, int>(i, i));
	  }
	} else {
	  mapFst_Stripe.insert(std::pair<int, int>(i, i));
	  mapStripe_Fst.insert(std::pair<int, int>(i, i));
	}
      }
    }

    if (!validateHeader(hd, hdValid, mapStripe_Fst, mapFst_Stripe)){
      delete[] hdValid;
      delete tmpHd;
      return gOFS.Emsg("RaidDPOpen",*error, EIO, "open stripes - header invalid");
    }

    delete[] hdValid;
    delete tmpHd;
  }

  eos_info("Returning SFS_OK\n");    
  isOpen = true;
  return SFS_OK;
}



//------------------------------------------------------------------------------
//recover in case the header is corrupted
bool 
RaidDPLayout::validateHeader(HeaderCRC *&hd, bool *hdValid, std::map<unsigned int, unsigned int>  &mapSF, 
                             std::map<unsigned int, unsigned int> &mapFS)
{

  bool newFile = true;
  bool allHdValid = true;
  vector<unsigned int> idFsInvalid;

  for (unsigned int i = 0; i < nFiles; i++){
    if (hdValid[i]){
      newFile = false;
    } else {
      allHdValid = false;
      idFsInvalid.push_back(i);
    }
  }

  if (newFile || allHdValid) {
    eos_debug("File is either new or there are no corruptions in the headers.");
    return true;
  }

  //can not recover from more than two corruptions
  if (idFsInvalid.size() > 2) {
    eos_debug("Can not recover from more than two corruptions.");
    return false;
  }

  //if not in writing mode then can not recover
  if (!ofsFile->isRW){
    eos_warning("Will not rewrite header after recovery if not in writing mode");
  }

  //read a valid header
  HeaderCRC *tmpHd = new HeaderCRC();
  for (unsigned int i = 0; i< nFiles; i++){
    if (hdValid[i]){
      if (i == indexStripe)
        tmpHd->ReadFromFile(ofsFile);
      else
        tmpHd->ReadFromFile(stripeClient[i]);
      break;
    }
  }

  //get stripe id's already used
  std::set<unsigned int> usedStripes;
  for (unsigned int i = 0; i < nFiles; i++){
    if (hdValid[i])
      usedStripes.insert(mapFS[i]);
    else
      mapFS.erase(i);
  }
  mapSF.clear();
  
  while (idFsInvalid.size()){
    unsigned int idFs = idFsInvalid.back();
    idFsInvalid.pop_back();
        
    for (unsigned int i = 0; i < nFiles; i++){
      if (find(usedStripes.begin(), usedStripes.end(), i) == usedStripes.end()){ //no used
        //add the new mapping
        eos_debug("Add new mapping: stripe: %u, fid: %u", i, idFs);
        mapFS[idFs] = i;
        usedStripes.insert(i);
        tmpHd->SetIdStripe(i);
        hdValid[idFs] = true;

        if (idFs == indexStripe){
	  if (ofsFile->isRW) {
	    tmpHd->WriteToFile(ofsFile);
	  }
          hd->SetIdStripe(i);
          hd->SetNoBlocks(tmpHd->GetNoBlocks());
          hd->SetSizeLastBlock(tmpHd->GetSizeLastBlock());
          fileSize = ((hd->GetNoBlocks() - 1) * stripeWidth) + hd->GetSizeLastBlock();
        } else {
          if (stripeClient[idFs]->IsOpen()) tmpHd->WriteToFile(stripeClient[idFs]);
	}
        break;
      }
    }
  }
  usedStripes.clear();

  //populate the stripe_fst map
  for (unsigned int i = 0; i< nFiles; i++)
    mapSF[mapFS[i]] = i;

  delete tmpHd;
  return true;
}


//------------------------------------------------------------------------------
//destructor
RaidDPLayout::~RaidDPLayout() 
{
  dataBlock.clear();
  mapFst_Stripe.clear();
  mapStripe_Fst.clear();

  //free memory
  for (unsigned int i=0; i< nFiles; i++) {
    if (stripeClient[i]) 
      delete stripeClient[i];    
  }
  delete[] stripeClient;
  delete[] stripeUrl;
  delete hd;
}


//------------------------------------------------------------------------------
//compute the parity and double parity blocks
void 
RaidDPLayout::computeParity()
{
  int indexPBlock;
  int currentBlock;
 
  //compute simple parity
  for (unsigned int i = 0; i < nStripes; i++) {
    indexPBlock = (i + 1) * nStripes + 2 * i;
    currentBlock = i * (nStripes + 2);     //beginning of current line
    operationXOR(dataBlock[currentBlock], dataBlock[currentBlock + 1], stripeWidth, dataBlock[indexPBlock]);
    currentBlock += 2;

    while (currentBlock < indexPBlock) {
      operationXOR(dataBlock[indexPBlock], dataBlock[currentBlock], stripeWidth, dataBlock[indexPBlock]);
      currentBlock++;
    }
  }

  //compute double parity
  int auxBlock;
  int nextBlock;
  int indexDPBlock;
  int jumpBlocks = nFiles + 1;
  vector<int> usedBlocks;
  
  //add the DP block's index to the used list
  for (unsigned int i = 0; i < nStripes; i++) {
    indexDPBlock = (i + 1) * (nStripes + 1) +  i;
    usedBlocks.push_back(indexDPBlock);
  }

  for (unsigned int i = 0; i < nStripes; i++) {
    indexDPBlock = (i + 1) * (nStripes + 1) +  i;
    nextBlock = i + jumpBlocks;
    operationXOR(dataBlock[i], dataBlock[nextBlock], stripeWidth, dataBlock[indexDPBlock]);
    usedBlocks.push_back(i);
    usedBlocks.push_back(nextBlock);

    for (unsigned int j = 0; j < nStripes - 2; j++) {
      auxBlock = nextBlock + jumpBlocks;

      if ((auxBlock < (int) nTotalBlocks) && (find(usedBlocks.begin(), usedBlocks.end(), auxBlock) == usedBlocks.end()))
        nextBlock = auxBlock;
      else {
        nextBlock++;
        while (find(usedBlocks.begin(), usedBlocks.end(), nextBlock) != usedBlocks.end())
          nextBlock++;
      }

      operationXOR(dataBlock[indexDPBlock], dataBlock[nextBlock], stripeWidth, dataBlock[indexDPBlock]);
      usedBlocks.push_back(nextBlock);
    }
  }
}


//------------------------------------------------------------------------------
//XOR the two stripes using 128 bits and return the result
//usually the totalBytes value should be a multiple of 128
void 
RaidDPLayout::operationXOR(char *stripe1, char *stripe2, XrdSfsXferSize totalBytes, char* result)
{
  v2do *xor_res;
  v2do *idx1;
  v2do *idx2;
  char *byte_res;
  char *byte_idx1;
  char *byte_idx2;
  long int noPices = -1;

  idx1 = (v2do*) stripe1;
  idx2 = (v2do*) stripe2;
  xor_res = (v2do*) result;

  noPices = totalBytes / sizeof(v2do);

  for (long int i = 0; i < noPices; idx1++, idx2++, xor_res++, i++) {
    *xor_res = *idx1 ^ *idx2;
  }

  //if the block does not devide perfectly to 128!
  if (totalBytes % sizeof(v2do) != 0) {
    byte_res = (char*) xor_res;
    byte_idx1 = (char*) idx1;
    byte_idx2 = (char*) idx2;
    for (int i = noPices * sizeof(v2do); i < totalBytes; byte_res++, byte_idx1++, byte_idx2++, i++)
      *byte_res = *byte_idx1 ^ *byte_idx2;
  }
}


//------------------------------------------------------------------------------
int
RaidDPLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  eos::common::Timing rt("read");
  TIMING("start", &rt);
  int aread = 0;
  unsigned int nclient;
  XrdSfsXferSize nread;
  XrdSfsFileOffset offsetLocal;
  XrdSfsFileOffset readLength = 0;

  if (isEntryServer) {
    if ((offset<0) && (ofsFile->isRW)) {  //recover file
      offset = 0;
      char *dummyBuf = (char*) calloc(stripeWidth, sizeof(char));
      
      //try to recover block using parity information
      while (length) {
        nread = (length > ((XrdSfsXferSize)stripeWidth)) ? stripeWidth : length;
        if((offset % (nBlocks * stripeWidth) == 0) && (!recoverBlock(dummyBuf, offset, nread, true))){
          free(dummyBuf);
          return gOFS.Emsg("ReedSRead",*error, EREMOTEIO, "recover stripe - recover failed ");
        }
             
        length -= nread;
        offset += nread;
        readLength += nread;
      }
      //free memory
      free(dummyBuf);
    }
    else{ //normal reading mode
      while (length) {
	bool doRecovery;
	doRecovery = false;
        nclient = (offset / stripeWidth) % nStripes;
        nread = (length > ((XrdSfsXferSize)stripeWidth)) ? stripeWidth : length;
        offsetLocal = ((offset / ( nStripes * stripeWidth)) * stripeWidth) + (offset %  stripeWidth);
     
        if (nclient == mapFst_Stripe[indexStripe]){  //read from local file
	  TIMING("read local in", &rt);
          if ((!(aread = ofsFile->readofs(offsetLocal + headerSize, buffer, nread))) || (aread != nread)) {
            //save info about the file corrupted
            //!!!
	    doRecovery = true;
          }
	  TIMING("read local out", &rt);
        } else {
	  int lread = nread;
	  if (stripeClient[mapStripe_Fst[nclient]]->IsOpen()) {
	    do {
	      TIMING("read remote in", &rt);
	      aread = stripeClient[mapStripe_Fst[nclient]]->Read(buffer, offsetLocal + headerSize, lread);
	      TIMING("read remote out in", &rt);
	      if (aread > 0) {
		if (aread != lread) {
		  lread -= aread;
		  offsetLocal += lread;
		} else {
		  break;
		}
	      } else { 
		eos_warning("Read returned %ld instead of %ld bytes", aread, lread);
		doRecovery = true;
		break;
	      }
	    } while ( lread );
	  } else {
	    doRecovery = true;
	  }
	}
	TIMING("read recovery", &rt);
	if (doRecovery) {
	  if (!recoverBlock(buffer, offset, nread, false)) {
	    return gOFS.Emsg("RaidDPRead",*error, EREMOTEIO, "read stripe - read failed after recovery has been tried ") ;     
	  }
	}

        length -= nread;
        offset += nread;
        buffer += nread;
        readLength += nread;
      }
    }
  } else { //read from one of the stripes, not entry server
    TIMING("read local", &rt);
    aread = ofsFile->readofs(offset, buffer, length);
    if (aread != length) {
      eos_crit("read of offset=%llu length=%lu gave retc=%lu", offset, length, aread);
      return gOFS.Emsg("RaidDPRead",*error, EREMOTEIO, "read stripe - read failed, local file ");
    }
    readLength = aread;
  }
  TIMING("read return", &rt);
  rt.Print();
  return readLength;
}

//------------------------------------------------------------------------------
int 
RaidDPLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  eos::common::Timing wt("write");
  TIMING("start", &wt);
  int rc1;
  size_t nwrite;
  unsigned int nclient;
  XrdSfsFileOffset offsetLocal;
  XrdSfsFileOffset offsetStart;
  XrdSfsFileOffset offsetEnd;
  XrdSfsFileOffset writeLength = 0;

  if (isEntryServer){
    offsetStart = offset;
    offsetEnd = offset + length;

    while (length) {
      nclient = (offset / stripeWidth) % nStripes; 
      nwrite = (length < stripeWidth) ? length : stripeWidth;
      offsetLocal = ((offset / ( nStripes * stripeWidth)) * stripeWidth) + (offset % stripeWidth);

      if (nclient == mapFst_Stripe[indexStripe]){  //local file
	TIMING("write local", &wt);
	eos_info("Write local offset=%llu size=%u", offsetLocal + headerSize, nwrite);
        rc1 = ofsFile->writeofs(offsetLocal + headerSize, buffer, nwrite);
        if (rc1 < 0) {
          return gOFS.Emsg("RaidDPWrite",*error, EIO, "write local stripe - write failed");
        }
      }
      else {
	TIMING("write remote", &wt);
	eos_info("Write remote offset=%llu size=%u", offsetLocal + headerSize, nwrite);
        if (!stripeClient[mapStripe_Fst[nclient]]->Write(buffer, offsetLocal + headerSize, nwrite)) {
          return gOFS.Emsg("RaidDPWrite",*error, EREMOTEIO, "write stripe - write failed ", 
                           stripeUrl[mapStripe_Fst[nclient]].c_str());
        }
      }

      offset += nwrite;
      length -= nwrite;
      buffer += nwrite;
      writeLength += nwrite;
    }

    TIMING("truncate", &wt);
    //update the size of the file if needed
    if (offsetEnd > fileSize)
      fileSize = offsetEnd;

    //truncate the files to the new size
    if ((fileSize % (nBlocks *stripeWidth)) != 0){
      XrdSfsFileOffset truncateOffset = ((fileSize / (nBlocks * stripeWidth)) + 1) * (nBlocks * stripeWidth) ;
      truncate(truncateOffset);
      eos_info("Truncate local  offset=%llu", truncateOffset);      
    } else {
      truncate(fileSize);
      eos_info("Truncate local  offset=%llu", fileSize);
    }


    TIMING("updateparity", &wt);
    //update parity blocks
    updateParityForGroups(offsetStart, offsetEnd);
    
    //update the header information and write it to all stripes
    if (ceil((fileSize * 1.0) / stripeWidth) != hd->GetNoBlocks()){
      hd->SetNoBlocks(ceil((fileSize * 1.0) / stripeWidth));
      updateHeader = true;
    }
                      
    if ((fileSize % stripeWidth) != hd->GetSizeLastBlock()){
      hd->SetSizeLastBlock(fileSize % stripeWidth);
      updateHeader =  true;
    }

    TIMING("updateheader", &wt);    
    if (updateHeader){
      for (unsigned int i = 0; i < nFiles; i++){  //fstid's
        if (i != indexStripe){
	  TIMING("updateheader remote", &wt);    
	  eos_info("Write Stripe Header remote %d",i);
          hd->SetIdStripe(mapFst_Stripe[i]);
          if (hd->WriteToFile(stripeClient[i]))
            return gOFS.Emsg("RaidDPWrite",*error, EIO, "write header failed ", stripeUrl[i].c_str());
        }
        else{
	  TIMING("updateheader local", &wt);    
	  eos_info("Write Stripe Header local");
          hd->SetIdStripe(mapFst_Stripe[i]);
          if (hd->WriteToFile(ofsFile))
            return gOFS.Emsg("RaidDPWrite",*error, EIO, "write header failed ");
        }
      }
      updateHeader = false;
    }
  }
  else {  //at one of the other stripes, not entry point
    TIMING("write local", &wt);    
    eos_info("Write local offset=%llu size=%u", offset, length);
    rc1 = ofsFile->writeofs(offset, buffer, length);
    if (rc1 < 0) {
      return gOFS.Emsg("RaidDPWrite",*error, EIO, "write local stripe - write failed");
    }
    writeLength += length;
  }

  TIMING("end", &wt);    
  wt.Print();
  return writeLength;
}


//------------------------------------------------------------------------------
//try to recover the block at the current offset
bool 
RaidDPLayout::recoverBlock(char *buffer, XrdSfsFileOffset offset, XrdSfsXferSize length, bool storeRecovery)
{
  
  bool ret = false;

  //use double parity to check(recover) also diagonal parity blocks
  if (doubleParityRecover(buffer, offset, length, storeRecovery)) {
    //Block was successfully reconstructed using double parity
    ret = true;
  } else {
    //Block could not be reconstructed using double parity
    ret = false;
  }

  if (ret) doneRecovery = true;
  return ret;
}


//------------------------------------------------------------------------------
//use simple parity to recover the stripe, return true if successfully reconstruted
bool 
RaidDPLayout::simpleParityRecover(char* buffer, XrdSfsFileOffset offset, XrdSfsXferSize length, int &blocksCorrupted)
{

  int aread;
  int idBlockCorrupted = -1;
  XrdSfsFileOffset offsetLocal = (offset / (nStripes * stripeWidth)) * stripeWidth;

  blocksCorrupted = 0;
  for (unsigned int i = 0; i < nFiles;  i++){
    if (i == mapFst_Stripe[indexStripe]){  //read from local file
      if ((!(aread = ofsFile->readofs(offsetLocal + headerSize, dataBlock[i], stripeWidth))) || (aread != stripeWidth)) {
        gOFS.Emsg("RaidDPSRecovery",*error, EIO, "read stripe - read failed, local file ");
        idBlockCorrupted = i; // [0 - nStripes]
        blocksCorrupted++;
      }
    }
    else {
      if (stripeClient[mapStripe_Fst[i]]->IsOpen()) {
	if ((!(aread = stripeClient[mapStripe_Fst[i]]->Read(dataBlock[i], offsetLocal + headerSize, stripeWidth))) || 
	    (aread != stripeWidth)) 
	  {
	    gOFS.Emsg("RaidDPSRecovery",*error, EREMOTEIO, "read stripe - too many corrupted blocks ", stripeUrl[mapStripe_Fst[i]].c_str());
	    idBlockCorrupted = i; // [0 - nStripes]
	    blocksCorrupted++;
	  }
      } else {
	idBlockCorrupted = i;
	blocksCorrupted++;
      }
    }
  }
  
  if (blocksCorrupted == 0)
    return true;
  else if (blocksCorrupted >= 2)
    return false;
  
  //use simple parity to recover
  operationXOR(dataBlock[(idBlockCorrupted + 1) % (nStripes + 1)],dataBlock[(idBlockCorrupted + 2) % (nStripes + 1)] , stripeWidth, dataBlock[idBlockCorrupted]);
  for (unsigned int i = 3, index = (idBlockCorrupted + i) % (nStripes + 1); i < (nStripes + 1) ; i++, index = (idBlockCorrupted + i) % (nStripes +1)){
    operationXOR(dataBlock[idBlockCorrupted], dataBlock[index], stripeWidth, dataBlock[idBlockCorrupted]);
  }
  
  //return recovered block and also write it to the file
  int rc1;
  int idReadBlock;
  unsigned int nclient;
  XrdSfsFileOffset offsetBlock;

  idReadBlock = (offset % (nStripes * stripeWidth)) / stripeWidth;  // [0-3]
  offsetBlock = (offset / (nStripes * stripeWidth)) * (nStripes * stripeWidth) + idReadBlock * stripeWidth;
  nclient = (offsetBlock / stripeWidth) % nStripes;
  offsetLocal = ((offsetBlock / ( nStripes * stripeWidth)) * stripeWidth);
  
  if (nclient == mapFst_Stripe[indexStripe]){  //local file
    rc1 = ofsFile->writeofs(offsetLocal + headerSize, dataBlock[idBlockCorrupted], stripeWidth);
    if (rc1 < 0) {
      return gOFS.Emsg("RaidDPSRecovery",*error, EIO, "write local stripe - write failed");
    }
  }
  else {
    if (!stripeClient[mapStripe_Fst[nclient]]->Write(dataBlock[idBlockCorrupted], offsetLocal + headerSize, stripeWidth)) 
      {
        return gOFS.Emsg("RaidDPSRecovery",*error, EREMOTEIO, "write stripe - write failed ", 
                         stripeUrl[mapStripe_Fst[nclient]].c_str());
      }
  }
  
  //write the correct block to the reading buffer
  memcpy(buffer, dataBlock[idReadBlock] + (offset % stripeWidth), length);    
  return true; 
}


//------------------------------------------------------------------------------
//use double parity to recover the stripe, return true if successfully reconstruted
bool 
RaidDPLayout::doubleParityRecover(char* buffer, XrdSfsFileOffset offset, XrdSfsXferSize length, bool storeRecovery)
{
  int aread;
  bool* statusBlock;
  unsigned int idStripe;
  vector<int> corruptId;
  vector<int> excludeId;
  XrdSfsFileOffset offsetLocal;
  XrdSfsFileOffset offsetGroup = (offset / (nBlocks *  stripeWidth)) * (nBlocks * stripeWidth);

  vector<unsigned int> simpleParityIndx = getSimpleParityIndices();
  vector<unsigned int> doubleParityIndx = getDoubleParityIndices();

  statusBlock = (bool*) calloc(nTotalBlocks, sizeof(bool));

  for (unsigned int i = 0; i < nTotalBlocks; i++){
    statusBlock[i] = true;
    offsetLocal = (offsetGroup / (nStripes * stripeWidth)) *  stripeWidth + ((i / nFiles) * stripeWidth);
    idStripe = i % nFiles;
    //memset(dataBlock[i], 0, stripeWidth);

    if (idStripe == mapFst_Stripe[indexStripe]){  //read from local file
      if ((!(aread = ofsFile->readofs(offsetLocal + headerSize, dataBlock[i], stripeWidth))) || (aread != stripeWidth)) {
        gOFS.Emsg("RaidDPDRecovery",*error, EIO, "read stripe - read failed, local file ");
        statusBlock[i] = false;
        corruptId.push_back(i);
      }
    } else {
      int lread = stripeWidth;
      do {
	aread = stripeClient[mapStripe_Fst[idStripe]]->Read(dataBlock[i], offsetLocal + headerSize, lread);
	if (aread > 0) {
	  if (aread != lread) {
	    lread -= aread;
	    offsetLocal += lread;
	  } else {
	    break;
	  }
	} else { 
          gOFS.Emsg("RaidDPDRecovery",*error, EREMOTEIO, "read stripe - read failed ", stripeUrl[mapStripe_Fst[idStripe]].c_str());
          statusBlock[i] = false;
          corruptId.push_back(i);
	  break;
	}
      } while ( lread );
    }
  }
  
  //recovery algorithm
  int rc1;
  unsigned int nclient;
  unsigned int  idBlockCorrupted;
  
  vector<unsigned int> horizontalStripe;
  vector<unsigned int> diagonalStripe;
 
  while (!corruptId.empty()) {

    idBlockCorrupted = corruptId.back();
    corruptId.pop_back();

    if (validHorizStripe(horizontalStripe, statusBlock, idBlockCorrupted)) {
      //try to recover using simple parity
      memset(dataBlock[idBlockCorrupted], 0, stripeWidth);
      for (unsigned int ind = 0;  ind < horizontalStripe.size(); ind++){
        if (horizontalStripe[ind] != idBlockCorrupted)
          operationXOR(dataBlock[idBlockCorrupted], dataBlock[horizontalStripe[ind]], stripeWidth, dataBlock[idBlockCorrupted]);
      }

      //return recovered block and also write it to the file
      nclient = idBlockCorrupted % nFiles;
      offsetLocal = ((offsetGroup / (nStripes * stripeWidth)) * stripeWidth) + ((idBlockCorrupted / nFiles) * stripeWidth);
  
      if (nclient == mapFst_Stripe[indexStripe]){  //local file
	if (storeRecovery) {
	  rc1 = ofsFile->writeofs(offsetLocal + headerSize, dataBlock[idBlockCorrupted], stripeWidth);
	  if (rc1 < 0) {
	    free(statusBlock);
	    return gOFS.Emsg("RaidDPDRecovery",*error, EIO, "write local stripe - write failed");
	  }
	}
      }
      else {  //remote file
	if (storeRecovery) {
	  if (!stripeClient[mapStripe_Fst[nclient]]->Write(dataBlock[idBlockCorrupted], offsetLocal + headerSize, stripeWidth)) {
	    free(statusBlock);
	    return gOFS.Emsg("RaidDPDRecovery",*error, EREMOTEIO, "write stripe - write failed ", stripeUrl[mapStripe_Fst[nclient]].c_str());
	  }
	}
      }
  
      //if not SP or DP, maybe we have to return it
      if (find(simpleParityIndx.begin(), simpleParityIndx.end(), idBlockCorrupted) == simpleParityIndx.end() &&
          find(doubleParityIndx.begin(), doubleParityIndx.end(), idBlockCorrupted) == doubleParityIndx.end()){
        
        if ((offset >= offsetGroup + mapBigToSmallBlock(idBlockCorrupted) * stripeWidth) && 
            (offset < offsetGroup + (mapBigToSmallBlock(idBlockCorrupted) + 1) * stripeWidth)) 
          {
            memcpy(buffer, dataBlock[idBlockCorrupted] + (offset % stripeWidth), length);    
          }
      }
      
      //copy the unrecoverd blocks back in the queue
      if (!excludeId.empty()) {
	corruptId.insert(corruptId.end(), excludeId.begin(), excludeId.end());
	excludeId.clear();
      }

      //update the status of the block recovered
      statusBlock[idBlockCorrupted] = true;

    } else {  
      //try to recover using double parity
      if (validDiagStripe(diagonalStripe, statusBlock, idBlockCorrupted)) {
	//reconstruct current block and write it back to file
        memset(dataBlock[idBlockCorrupted], 0, stripeWidth);
        for (unsigned int ind = 0;  ind < diagonalStripe.size(); ind++){
          if (diagonalStripe[ind] != idBlockCorrupted){
            operationXOR(dataBlock[idBlockCorrupted], dataBlock[diagonalStripe[ind]], stripeWidth, dataBlock[idBlockCorrupted]);
          }
        }

        //return recovered block and also write it to the file
        nclient = idBlockCorrupted % nFiles;
        offsetLocal = ((offsetGroup / (nStripes * stripeWidth)) * stripeWidth) + ((idBlockCorrupted / nFiles) * stripeWidth);
  
	if (storeRecovery) {
	  if (nclient == mapFst_Stripe[indexStripe]){  //local file
	    rc1 = ofsFile->writeofs(offsetLocal + headerSize, dataBlock[idBlockCorrupted], stripeWidth);
	    if (rc1 < 0) {
	      free(statusBlock);
	      return gOFS.Emsg("RaidDPDRecovery",*error, errno, "write local stripe - write failed");
	    }
	  } else {
	    if (!stripeClient[mapStripe_Fst[nclient]]->Write(dataBlock[idBlockCorrupted], offsetLocal + headerSize, stripeWidth)) {
	      free(statusBlock);
	      return gOFS.Emsg("RaidDPDRecovery",*error, EREMOTEIO, "write stripe - write failed ", stripeUrl[mapStripe_Fst[nclient]].c_str());
	    }
	  }
	}
  
        //if not sp or dp, maybe we have to return it
        if (find(simpleParityIndx.begin(), simpleParityIndx.end(), idBlockCorrupted) == simpleParityIndx.end() &&
            find(doubleParityIndx.begin(), doubleParityIndx.end(), idBlockCorrupted) == doubleParityIndx.end()){
        
          if ((offset >= offsetGroup + mapBigToSmallBlock(idBlockCorrupted) * stripeWidth) && 
              (offset < offsetGroup + (mapBigToSmallBlock(idBlockCorrupted) + 1) * stripeWidth)) {
            memcpy(buffer, dataBlock[idBlockCorrupted] + (offset % stripeWidth), length);    
          }
        }

	//copy the unrecoverd blocks back in the queue
	if (!excludeId.empty()) {
	  corruptId.insert(corruptId.end(), excludeId.begin(), excludeId.end());
	  excludeId.clear();
	}
        statusBlock[idBlockCorrupted] = true;
      }
      else {
	//current block can not be recoverd in this configuration
	excludeId.push_back(idBlockCorrupted);
      }
    }
  }
 
  //free memory
  free(statusBlock);

  if (corruptId.empty() && !excludeId.empty()) 
    return false;
  
  return true;
}


//------------------------------------------------------------------------------
//recompute and write to files the parity blocks of the groups between the two limits
int 
RaidDPLayout::updateParityForGroups(XrdSfsFileOffset offsetStart, XrdSfsFileOffset offsetEnd)
{
  XrdSfsFileOffset offsetGroup;
  XrdSfsFileOffset offsetBlock;

  eos::common::Timing up("parity");

  for (unsigned int i = (offsetStart / (nBlocks * stripeWidth));  i < ceil((offsetEnd * 1.0 ) / (nBlocks * stripeWidth)); i++){
    offsetGroup = i * (nBlocks * stripeWidth);
    for(unsigned int j = 0; j < nBlocks; j++){
      XrdOucString block = "block-"; block += (int)i;
      TIMING(block.c_str(),&up);
      
      offsetBlock = offsetGroup + j * stripeWidth;
      read(offsetBlock, dataBlock[mapSmallToBigBlock(j)], stripeWidth);        
      block += "-read";
      TIMING(block.c_str(),&up);
    }
    
    TIMING("Compute-In",&up);     
    //do computations of parity blocks
    computeParity();      
    TIMING("Compute-Out",&up);     
    
    //write parity blocks to files
    writeParityToFiles(offsetGroup);
    TIMING("WriteParity",&up);     
  }
  up.Print();
  return SFS_OK;
}


//------------------------------------------------------------------------------
//write the parity blocks from dataBlocks to the corresponding file stripes
int 
RaidDPLayout::writeParityToFiles(XrdSfsFileOffset offsetGroup)
{

  int rc1;
  unsigned int idPFile;
  unsigned int idDPFile;
  unsigned int indexPBlock;
  unsigned int indexDPBlock;
  XrdSfsFileOffset offsetParityLocal;

  idPFile = nFiles - 2;
  idDPFile = nFiles - 1;

  //write the blocks to the parity files
  for (unsigned int i = 0; i < nStripes; i++){
    indexPBlock = (i + 1) * nStripes + 2 * i;
    indexDPBlock = (i + 1) * (nStripes + 1) +  i;
    offsetParityLocal = (offsetGroup / nStripes) + (i * stripeWidth);
   
    //write simple parity    
    if (idPFile == indexStripe){  //local file
      rc1 = ofsFile->writeofs(offsetParityLocal + headerSize, dataBlock[indexPBlock], stripeWidth);
      if (rc1 < 0) {
        return gOFS.Emsg("RaidDPWriteParity",*error, EIO, "write local stripe - write failed");
      }
    }
    else if (!stripeClient[mapStripe_Fst[idPFile]]->Write(dataBlock[indexPBlock], offsetParityLocal + headerSize, stripeWidth)) 
      return gOFS.Emsg("RaidDPWriteParity",*error, EREMOTEIO, "write stripe - write failed ", 
                       stripeUrl[mapStripe_Fst[idPFile]].c_str());
     
    //write double parity
    if (idDPFile == indexStripe){  //local file
      rc1 = ofsFile->writeofs(offsetParityLocal + headerSize, dataBlock[indexDPBlock], stripeWidth);
      if (rc1 < 0) {
        return gOFS.Emsg("RaidDPWriteParity",*error, EIO, "write local stripe - write failed");
      }
    }
    else if (!stripeClient[mapStripe_Fst[idDPFile]]->Write(dataBlock[indexDPBlock], offsetParityLocal + headerSize, stripeWidth)) 
      return gOFS.Emsg("RaidDPWriteParity",*error, EREMOTEIO, "write stripe - write failed ", 
                       stripeUrl[mapStripe_Fst[idDPFile]].c_str());
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
//return the indices of the simple parity blocks from a big stripe
vector<unsigned int> 
RaidDPLayout::getSimpleParityIndices()
{
  unsigned int val = nStripes;
  vector<unsigned int> values;

  values.push_back(val);
  val++;

  for (unsigned int i = 1; i < nStripes; i++) {
    val += (nStripes + 1);
    values.push_back(val);
    val++;
  }

  return values;
}


//------------------------------------------------------------------------------
//return the indices of the double parity blocks from a big group
vector<unsigned int> 
RaidDPLayout::getDoubleParityIndices()
{
  unsigned int val = nStripes;
  vector<unsigned int> values;

  val++;
  values.push_back(val);

  for (unsigned int i = 1; i < nStripes; i++) {
    val += (nStripes + 1);
    val++;
    values.push_back(val);
  }

  return values;
}


//------------------------------------------------------------------------------
// check if the DIAGONAL stripe is valid in the sense that there is at most one
// corrupted block in the current stripe and this is not the ommited diagonal 
bool 
RaidDPLayout::validDiagStripe(vector<unsigned int> &stripe, bool* statusBlock, unsigned int blockId)
{
  int corrupted = 0;

  stripe.clear();
  stripe = getDiagonalStripe(blockId);

  if (stripe.size() == 0) return false;

  //the ommited diagonal contains the block with index nStripes
  if (find(stripe.begin(), stripe.end(), nStripes) != stripe.end())
    return false;

  for (vector<unsigned int>::iterator iter = stripe.begin(); iter != stripe.end(); ++iter) {
    if (statusBlock[*iter] == false)
      corrupted++;

    if (corrupted >= 2)
      return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// check if the HORIZONTAL stripe is valid in the sense that there is at
// most one corrupted block in the current stripe 
bool 
RaidDPLayout::validHorizStripe(vector<unsigned int> &stripe, bool* statusBlock, unsigned int blockId)
{
  int corrupted = 0;
  long int baseId = (blockId / nFiles) * nFiles;
  stripe.clear();

  //if double parity block then no horizontal stripes
  if (blockId == (baseId + nStripes + 1))
    return false;

  for (unsigned int i = 0; i < nFiles - 1; i++)
    stripe.push_back(baseId + i);

  //check if it is valid
  for (vector<unsigned int>::iterator iter = stripe.begin(); iter != stripe.end(); ++iter) {
    if (statusBlock[*iter] == false)
      corrupted++;

    if (corrupted >= 2)
      return false;
  }

  return true;
}


//------------------------------------------------------------------------------
//return the blocks corrsponding to the diagonal stripe of blockId
vector<unsigned int> 
RaidDPLayout::getDiagonalStripe(unsigned int blockId)
{
  bool dpAdded = false;
  vector<unsigned int> lastColumn;

  //get the indices for the last column (double parity)
  lastColumn = getDoubleParityIndices();
  
  unsigned int nextBlock;
  unsigned int jumpBlocks;
  unsigned int idLastBlock;
  unsigned int previousBlock;
  vector<unsigned int> stripe;

  //if we are on the ommited diagonal, return 
  if (blockId == nStripes){
    stripe.clear();
    return stripe;
  }

  //put the original block
  stripe.push_back(blockId);

  //if start with dp index, then construct in a special way the diagonal
  if (find(lastColumn.begin(), lastColumn.end(), blockId) != lastColumn.end()){
    blockId = blockId % (nStripes + 1);
    stripe.push_back(blockId);
    dpAdded = true;
  }

  previousBlock = blockId;
  jumpBlocks = nStripes + 3;
  idLastBlock = nTotalBlocks - 1;

  for (unsigned int i = 0 ; i < nStripes - 1; i++) {
    nextBlock = previousBlock + jumpBlocks;

    if (nextBlock > idLastBlock) {
      nextBlock %= idLastBlock;
      if (nextBlock >= nStripes + 1)
	nextBlock = (previousBlock + jumpBlocks) % jumpBlocks;
    } else if (find(lastColumn.begin(), lastColumn.end(), nextBlock) != lastColumn.end())
      nextBlock = previousBlock + 2;

    stripe.push_back(nextBlock);
    previousBlock = nextBlock;
    
    //if on the ommited diagonal return
    if (nextBlock == nStripes){
      eos_debug("Return empty vector - ommited diagonal");
      stripe.clear();
      return stripe;
    }
  }

  //add the index from the double parity block
  if (!dpAdded){
    nextBlock = getDParityBlockId(stripe);
    stripe.push_back(nextBlock);
  }

  return stripe;
}


//------------------------------------------------------------------------------
//return the id of stripe from a nTotalBlocks representation to a nBlocks representation
//in which we exclude the parity and double parity blocks
unsigned int 
RaidDPLayout::mapBigToSmallBlock(unsigned int IdBig)
{
  if (IdBig % (nStripes + 2) == nStripes  || IdBig % (nStripes + 2) == nStripes + 1)
    return -1;
  else
    return ((IdBig / (nStripes + 2)) * nStripes + (IdBig % (nStripes + 2)));
}


//------------------------------------------------------------------------------
//return the id of stripe from a nBlocks representation in a nTotalBlocks representation
unsigned int 
RaidDPLayout::mapSmallToBigBlock(unsigned int IdSmall)
{
  return (IdSmall / nStripes) *(nStripes + 2) + IdSmall % nStripes;
}


//------------------------------------------------------------------------------
//return the id (out of nTotalBlocks) for the parity block corresponding to the current block
unsigned int 
RaidDPLayout::getParityBlockId(unsigned int elemFromStripe)
{
  return (nStripes + (elemFromStripe / (nStripes + 2)) *(nStripes + 2));
}


//------------------------------------------------------------------------------
//return the id (out of nTotalBlocks) for the double parity block corresponding to the current block
unsigned int 
RaidDPLayout::getDParityBlockId(std::vector<unsigned int> stripe)
{
  int min = *(std::min_element(stripe.begin(), stripe.end()));
  return ((min + 1) *(nStripes + 1) + min);
}


//------------------------------------------------------------------------------
int 
RaidDPLayout::truncate(XrdSfsFileOffset offset) {

  int rc = SFS_OK;
  XrdSfsFileOffset truncValue = 0;

  if (!offset) return rc;

  if (isEntryServer){
    if (offset % (nBlocks * stripeWidth) != 0)
      truncValue = ((offset / (nBlocks * stripeWidth)) + 1) * (nStripes * stripeWidth);
    else truncValue = (offset / (nBlocks * stripeWidth)) *  (nStripes * stripeWidth);

    for (unsigned int i = 0; i < nFiles; i++) {
      if (i != indexStripe && stripeClient[i]) {
        if (!stripeClient[i]->Truncate(truncValue)) {
          return gOFS.Emsg("RaidDPTruncate",*error, EIO, "truncate stripe - truncate failed (1)");	
        }
      }
    }
  }
  else {
    truncValue = offset;
  }

  if ((rc = ofsFile->truncateofs(truncValue +  headerSize))) 
    return gOFS.Emsg("RaidDPTruncate",*error, EIO, "truncate stripe - truncate failed (0)");	

  return rc;
}


//------------------------------------------------------------------------------
int 
RaidDPLayout::sync() {

  int rc1 = SFS_OK;
  int rc2 = 1;

  if (isEntryServer){
    for (unsigned int i = 0; i < nFiles; i++) {
      if (i != indexStripe && stripeClient[i]) {
        if (!stripeClient[i]->Sync()) {
          eos_err("Failed to sync remote stripe - %s", stripeUrl[i].c_str());
          rc2 = 0;
        }
      }
    }
  }

  if (!rc2) {
    return gOFS.Emsg("RaidDPSync",*error, EREMOTEIO, "sync remote stripe");
  }

  rc1 = ofsFile->syncofs();
  if (rc1 < 0) {
    eos_err("Failed to sync local stripe");
    return gOFS.Emsg("RaidDPSync",*error, errno, "sync local stripe");
  }
  
  return rc1;
}



//------------------------------------------------------------------------------
int 
RaidDPLayout::stat(struct stat *buf) 
{

  int rc=0;

  if (XrdOfsOss->Stat(ofsFile->fstPath.c_str(), buf)) {
    rc = gOFS.Emsg("RaidDPStat", *error, EIO, "stat - cannot stat file to determine file size",ofsFile->Path.c_str());
  } 
  if (isOpen && (!isEntryServer)) {
    eos_info("reading filesize from header");
    if (hd->ReadFromFile(ofsFile)) {
      // read the file size from the header (which is necessary if we are not an entry server to allow stat after close
      fileSize = (hd->GetNoBlocks() - 1) * stripeWidth + hd->GetSizeLastBlock();
      eos_info("read filesize from header %llu", fileSize);
    }
  }


  buf->st_size = fileSize;
  return rc;
}


//------------------------------------------------------------------------------
int 
RaidDPLayout::close() 
{
  int rc1 = SFS_OK;
  int rc2 = SFS_OK;

  if (isEntryServer){
    //if recovered then we have to truncate once again to the right size 
    if (doneRecovery){
      doneRecovery = false;
      truncate(fileSize);
    }
    
    for (unsigned int i = 0; i < nFiles; i++) {
      if (i != indexStripe && stripeClient[i] && !stripeClient[i]->Close()) {
        rc2 |= gOFS.Emsg("RaidDPClose",*error, EREMOTEIO, "close stripe - close failed ", stripeUrl[i].c_str());	
      }
    }
  }

  //closing local file
  rc1 = ofsFile->closeofs();

  isOpen = false;
  return rc1 | rc2;
}

EOSFSTNAMESPACE_END

