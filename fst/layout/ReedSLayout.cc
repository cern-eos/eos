/*----------------------------------------------------------------------------*/
#include "fst/layout/ReedSLayout.hh"
#include "fst/XrdFstOfs.hh"
#include "XrdOss/XrdOssApi.hh"
/*----------------------------------------------------------------------------*/
#include <cmath>
#include <map>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "fst/zfec/fec.h"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

extern XrdOssSys  *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
ReedSLayout::ReedSLayout(XrdFstOfsFile* thisFile, int lid, XrdOucErrInfo *outerror) 
  : Layout(thisFile, "reedS", lid, outerror)
{

  extraBlocks = 2;                                      //extra error correction blocks
  nStripes = eos::common::LayoutId::GetStripeNumber(lid) - 1;   //TODO: *** fix this!!!! ****
  stripeWidth = (XrdSfsXferSize) eos::common::LayoutId::GetBlocksize(lid); //kb units
  nFiles = nStripes + extraBlocks;                      //data files + parity files
 
  hd =  new HeaderCRC();
  headerSize = hd->GetHeaderSize();
  
  stripeClient = new XrdClient*[nFiles];
  stripeUrl = new XrdOucString[nFiles];

  for (unsigned int i = 0; i < nFiles; i++){
    stripeClient[i] = 0;
    stripeUrl[i] = "";
  }

  //allocate memory for blocks
  for (unsigned int i = 0; i < nFiles; i++)
    dataBlock.push_back(new char[stripeWidth]);

  fileSize = 0;    
  indexStripe = -1;
  updateHeader = false;
  doneRecovery = false;
  isEntryServer = false;
}

//------------------------------------------------------------------------------                                                  
int 
ReedSLayout::open(const char           *path,
                  XrdSfsFileOpenMode    open_mode,
                  mode_t                create_mode,
                  const XrdSecEntity   *client,
                  const char           *opaque)
{
  int rc = 0;

  if (nStripes < 2) {
    eos_err("Failed to open raidDP layout - stripe size should be at least 2");
    return gOFS.Emsg("ReedSOpen",*error, EREMOTEIO, "open stripes - stripe size must be at least 2");
  }

  if (stripeWidth < 64) {
    eos_err("Failed to open raidDP layout - stripe width should be at least 64");
    return gOFS.Emsg("ReedSOpen",*error, EREMOTEIO, "open stripes - stripe width must be at least 64");
  }

  int nmissing = 0;
  //assign stripe urls
  for (unsigned int i = 0; i < nFiles; i++) {
    XrdOucString stripetag = "mgm.url"; stripetag += (int) i;
    const char* stripe = ofsFile->capOpaque->Get(stripetag.c_str());
    if ((ofsFile->isRW && ( !stripe )) || ((nmissing > 0 ) && (!stripe))) {
      eos_err("Failed to open stripes - missing url for stripe %s", stripetag.c_str());
      return gOFS.Emsg("ReedSOpen",*error, EINVAL, "open stripes - missing url for stripe ", stripetag.c_str());
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
    return gOFS.Emsg("ReedSOpen",*error, EREMOTEIO, "open stripes - stripes are missing.");
  }

  const char* index = ofsFile->openOpaque->Get("mgm.replicaindex");
  if (index >= 0) {  
    indexStripe = atoi(index);
    if ((indexStripe < 0) || (indexStripe > eos::common::LayoutId::kSixteenStripe)) {
      eos_err("Illegal stripe index %d", indexStripe);
      return gOFS.Emsg("ReedSOpen",*error, EINVAL, "open stripes - illegal stripe index found", index);
    }
  }

  const char* head = ofsFile->openOpaque->Get("mgm.replicahead");
  if (head >= 0) {
    stripeHead = atoi(head);
    if ((stripeHead < 0) || (stripeHead>eos::common::LayoutId::kSixteenStripe)) {
      eos_err("Illegal stripe head %d", stripeHead);
      return gOFS.Emsg("ReedSOpen",*error, EINVAL, "open stripes - illegal stripe head found", head);
    }
  } 
  else {
    eos_err("Stripe head missing");
    return gOFS.Emsg("ReedSOpen",*error, EINVAL, "open stripes - no stripe head defined");
  }
  
  //doing local operation for current stripe
  rc = ofsFile->openofs(path, open_mode, create_mode, client, opaque );
  if (rc) {
    //if file does not exist then we create it
    if (!ofsFile->isRW)
      gOFS.Emsg("ReedSOpen",*error, EIO, "open stripes - local open failed in read mode");
    open_mode |= SFS_O_CREAT;
    create_mode |= SFS_O_MKPTH;
    rc = ofsFile->openofs(path, open_mode, create_mode, client, opaque );
    if (rc)
      return gOFS.Emsg("ReedSOpen",*error, EIO, "open stripes - local open failed");
  }

  //operations done only at the entry server
  if (indexStripe == stripeHead){
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
        if (ofsFile->isRW) {
          // write case
          if (!stripeClient[i]->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | 
                                     kXR_open_updt | kXR_new, false)) {
            eos_err("Failed to open stripes - remote open write failed on %s ", stripeUrl[i].c_str());
            return gOFS.Emsg("ReedSOpen",*error, EREMOTEIO, "open stripes - remote open failed ", stripeUrl[i].c_str());
          }           
        } 
      
        else {
          // read case
          if (!stripeClient[i]->Open(0, 0, false)) {
            eos_err("Failed to open stripes - remote open read failed on %s ", stripeUrl[i].c_str());
            return gOFS.Emsg("ReedSOpen",*error, EREMOTEIO, "open stripes - remote open failed ", stripeUrl[i].c_str());
          }
        }
      
        //read the header information of the opened stripe
        if (tmpHd->ReadFromFile(stripeClient[i])){
          mapFst_Stripe.insert(std::pair<int, int>(i, tmpHd->GetIdStripe()));
          mapStripe_Fst.insert(std::pair<int, int>(tmpHd->GetIdStripe(), i));
          hdValid[i] = true;
        }
        else{
          mapFst_Stripe.insert(std::pair<int, int>(i, i));
          mapStripe_Fst.insert(std::pair<int, int>(i, i));
        }
      }
    }

    if (!validateHeader(hd, hdValid, mapStripe_Fst, mapFst_Stripe)){
      delete[] hdValid;
      delete tmpHd;
      return gOFS.Emsg("ReedSOpen",*error, EIO, "open stripes - header invalid");
    }
    
    for (std::map<unsigned int, unsigned int>::iterator iter = mapStripe_Fst.begin(); iter != mapStripe_Fst.end(); iter++){
      eos_debug("Mapping stripe: %u, fstid: %u ", (*iter).first, (*iter).second);
    }

    delete[] hdValid;
    delete tmpHd;
  }
    
  return SFS_OK;
}



//------------------------------------------------------------------------------
//recover in case the header is corrupted
bool 
ReedSLayout::validateHeader(HeaderCRC *&hd, 
                            bool *hdValid, 
                            std::map<unsigned int, unsigned int>  &mapSF, 
                            std::map<unsigned int, unsigned int> &mapFS)
{
  bool newFile = true;
  bool allHdValid = true;
  vector<unsigned int> idFsInvalid;

  for (unsigned int i = 0; i < nFiles; i++){
    if (hdValid[i]){
      eos_debug("Invalid header is for stripe: %i", i);
      newFile = false;
    }
    else {
      allHdValid = false;
      idFsInvalid.push_back(i);
    }
  }

  if (newFile || allHdValid) {
    eos_debug("File is either new or there are no corruptions in the headers.");
    return true;
  }

  //can not recover from more than two corruptions
  if (idFsInvalid.size() > extraBlocks) {                                  
    eos_debug("Can not recover from more than %i corruptions.", extraBlocks);
    return false;
  }

  //if not in writing mode then can not recover
  if (!ofsFile->isRW){
    eos_err("Can not recover header if not in writing mode.");
    return false;    
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
      if (find(usedStripes.begin(), usedStripes.end(), i) == usedStripes.end()){ //not used
        //add the new mapping
        eos_debug("New mapping stripe: %i, fstid: %i", i, idFs);
        mapFS[idFs] = i;
        usedStripes.insert(i);
        tmpHd->SetIdStripe(i);
        hdValid[idFs] = true;

        //write header to file
        if (idFs == indexStripe){
          tmpHd->WriteToFile(ofsFile);
          hd->SetIdStripe(i);
          hd->SetNoBlocks(tmpHd->GetNoBlocks());
          hd->SetSizeLastBlock(tmpHd->GetSizeLastBlock());
          fileSize = ((hd->GetNoBlocks() - 1) * stripeWidth) + hd->GetSizeLastBlock();
        }
        else
          tmpHd->WriteToFile(stripeClient[idFs]);
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
ReedSLayout::~ReedSLayout() 
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
//compute the error correction blocks
void 
ReedSLayout::computeParity()
{
  unsigned int block_nums[extraBlocks];
  unsigned char *outblocks[extraBlocks];
  const unsigned char *blocks[nStripes];

  for (unsigned int i = 0; i < nStripes; i++)
    blocks[i] = (const unsigned char*) dataBlock[i];

  for (unsigned int i = 0; i < extraBlocks; i++){
    block_nums[i] = nStripes + i;
    outblocks[i] = (unsigned char*) dataBlock[nStripes + i];
    memset(dataBlock[nStripes + i], 0, stripeWidth);
  }

  fec_t *const fec = fec_new(nStripes, nFiles);
  fec_encode(fec, blocks, outblocks, block_nums, extraBlocks, stripeWidth);
  
  //free memory
  fec_free(fec);
}


//------------------------------------------------------------------------------
int
ReedSLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  int aread = 0;
  unsigned int nclient;
  XrdSfsXferSize nread;
  XrdSfsFileOffset offsetLocal;
  XrdSfsFileOffset readLength = 0;

  if (isEntryServer){
    if ((offset<0) && (ofsFile->isRW)){ //recover file
      offset = 0;
      char *dummyBuf = (char*) calloc(stripeWidth, sizeof(char));
      
      //try to recover block using parity information
      while (length) {
        nread = (length > ((XrdSfsXferSize)stripeWidth)) ? stripeWidth : length;
        if((offset % (nStripes * stripeWidth) == 0) && (!recoverBlock(dummyBuf, offset, nread))){
          free(dummyBuf);
          return gOFS.Emsg("ReedSRead",*error, EIO, "recover stripe - recover failed ");
        }
             
        length -= nread;
        offset += nread;
        readLength += nread;
      }
      //free memory
      free(dummyBuf);
    }
    else { //normal reading of the file
      while (length) {
        nclient = (offset / stripeWidth) % nStripes;
        nread = (length > ((XrdSfsXferSize)stripeWidth)) ? stripeWidth : length;
        offsetLocal = ((offset / ( nStripes * stripeWidth)) * stripeWidth) + (offset %  stripeWidth);

        if (nclient == mapFst_Stripe[indexStripe]){  //read from local file
          if ((!(aread = ofsFile->readofs(offsetLocal + headerSize, buffer, nread))) || (aread != nread)) {
            //save info about the file corrupted
            return gOFS.Emsg("ReedSRead",*error, EIO, "read stripe - read failed, local file ");
          }
        }
        else {
          if ((!(aread = stripeClient[mapStripe_Fst[nclient]]->Read(buffer, offsetLocal + headerSize, nread))) 
              || (aread != nread)) {
            //save info about the file corrupted
            return gOFS.Emsg("ReedSRead",*error, EREMOTEIO, "read stripe - read failed ",
                             stripeUrl[mapStripe_Fst[nclient]].c_str());
          }
        }
      
        length -= nread;
        offset += nread;
        buffer += nread;
        readLength += nread;
      }
    }
  }
  else { //read from one of the stripes, not entry server
    if (!(aread = ofsFile->readofs(offset, buffer, length)) || (aread != length))
      return gOFS.Emsg("ReedSPRead",*error, EIO, "read stripe - read failed, local file ");
    readLength += aread;
  }

  return readLength;
}


//------------------------------------------------------------------------------
int 
ReedSLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
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
      offsetLocal = ((offset / ( nStripes * stripeWidth)) * stripeWidth) + (offset %  stripeWidth);

      if (nclient == mapFst_Stripe[indexStripe]){  //local file
        rc1 = ofsFile->writeofs(offsetLocal + headerSize, buffer, nwrite);
        if (rc1 < 0) {
          return gOFS.Emsg("ReedSWrite",*error, EIO, "write local stripe - write failed");
        }
      }
      else {
        if (!stripeClient[mapStripe_Fst[nclient]]->Write(buffer, offsetLocal + headerSize, nwrite)) {
          return gOFS.Emsg("ReedSWrite",*error, EREMOTEIO, "write stripe - write failed ", 
                           stripeUrl[mapStripe_Fst[nclient]].c_str());
        }
      }

      offset += nwrite;
      length -= nwrite;
      buffer += nwrite;
      writeLength += nwrite;
    }

    //update the size of the file if needed
    if (offsetEnd > fileSize)
      fileSize = offsetEnd;

    //truncate the files to the new size
    if ((fileSize % (nStripes *stripeWidth)) != 0){
      XrdSfsFileOffset truncateOffset = ((fileSize / (nStripes * stripeWidth)) + 1) * (nStripes * stripeWidth) ;
      truncate(truncateOffset);
    }
    else 
      truncate(fileSize);

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
    
    if (updateHeader){
      for (unsigned int i = 0; i < nFiles; i++){ //fstid's
        if (i != indexStripe){
          hd->SetIdStripe(mapFst_Stripe[i]);
          if (hd->WriteToFile(stripeClient[i]))
            return gOFS.Emsg("ReedSWrite",*error, EIO, "write header failed ", stripeUrl[i].c_str());
        }
        else{
          hd->SetIdStripe(mapFst_Stripe[i]);
          if (hd->WriteToFile(ofsFile))
            return gOFS.Emsg("ReedSWrite",*error, EIO, "write header failed ");
        }
      }
      updateHeader = false;
    }
  }
  else {  //at one of the other stripes, not entry point
    rc1 = ofsFile->writeofs(offset, buffer, length);
    if (rc1 < 0) {
      return gOFS.Emsg("ReedSWrite",*error, EIO, "write local stripe - write failed");
    }
    writeLength += length;
  }

  return writeLength;
}


//------------------------------------------------------------------------------
//try to recover the block at the current offset
bool 
ReedSLayout::recoverBlock(char *buffer, XrdSfsFileOffset offset, XrdSfsXferSize length)
{
  int aread;
  unsigned int blocksCorrupted;
  vector<unsigned int> validId;
  vector<unsigned int> invalidId;
  XrdSfsFileOffset offsetLocal = (offset / (nStripes * stripeWidth)) * stripeWidth;
  XrdSfsFileOffset offsetGroup = (offset / (nStripes * stripeWidth)) * (nStripes * stripeWidth);

  blocksCorrupted = 0;
  for (unsigned int i = 0; i < nFiles;  i++){
    if (i == mapFst_Stripe[indexStripe]){  //read from local file
      if ((!(aread = ofsFile->readofs(offsetLocal + headerSize, dataBlock[i], stripeWidth))) || (aread != stripeWidth)) {
        gOFS.Emsg("ReedSPRecovery",*error, EIO, "read stripe - read failed, local file ");
        invalidId.push_back(i);
        blocksCorrupted++;
      }
      else{
        validId.push_back(i);
      }
    }
    else {
      if ((!(aread = stripeClient[mapStripe_Fst[i]]->Read(dataBlock[i], offsetLocal + headerSize, stripeWidth))) 
          || (aread != stripeWidth)) {
        gOFS.Emsg("ReedSPRecovery",*error, EREMOTEIO, "read stripe - read failed ", stripeUrl[mapStripe_Fst[i]].c_str());
        invalidId.push_back(i);
        blocksCorrupted++;
      }
      else{
        validId.push_back(i);
      }
    }
  }
  
  if (blocksCorrupted == 0)
    return true;
  else if (blocksCorrupted > extraBlocks)
    return false;

  /* ******* DECODE *******/
  const unsigned char *inpkts[nFiles - blocksCorrupted];
  unsigned char *outpkts[extraBlocks];
  unsigned indexes[nStripes];
  bool found = false;

  //obtain a valid combination of blocks suitable for recovery
  backtracking(indexes, validId, 0);

  for (unsigned int i = 0; i < nStripes; i++){
    inpkts[i] = (const unsigned char*) dataBlock[indexes[i]];
  }

  //add the invalid data blocks to be recovered
  int countOut = 0;
  bool dataCorrupted = false;
  bool parityCorrupted = false;
  for (unsigned int i = 0; i < invalidId.size(); i++) {
    outpkts[i] = (unsigned char*) dataBlock[invalidId[i]];
    countOut++;
    if (invalidId[i] >= nStripes)
      parityCorrupted = true;
    else 
      dataCorrupted = true;
  }

  for (vector<unsigned int>::iterator iter = validId.begin(); iter != validId.end(); ++iter) {
    found = false;
    for (unsigned int i = 0; i < nStripes; i++){
      if (indexes[i] == *iter) {
        found = true;
        break;
      }
    }
    if (!found) {
      outpkts[countOut] = (unsigned char*) dataBlock[*iter];
      countOut++;
    }
  }

  //actual decoding - recover primary blocks
  if (dataCorrupted){
    fec_t *const fec = fec_new(nStripes, nFiles);
    fec_decode(fec, inpkts, outpkts, indexes, stripeWidth);
    fec_free(fec);  
  }
  
  //if there are also parity block corrupted then we encode again the blocks - recover secondary blocks
  if (parityCorrupted){
    computeParity();
  }

  //update the files in which we found invalid blocks
  int rc1;
  unsigned int nclient;
  for (vector<unsigned int>::iterator iter = invalidId.begin(); iter != invalidId.end(); ++iter) {
    nclient = *iter;
    eos_debug("Invalid index stripe: %i", nclient);

    if (nclient == mapFst_Stripe[indexStripe]){  //local file
      eos_debug("Writing to local file stripe: %i", nclient);
      rc1 = ofsFile->writeofs(offsetLocal + headerSize, dataBlock[nclient], stripeWidth);
      if (rc1 < 0) {
        return gOFS.Emsg("ReedSWrite",*error, EIO, "write local stripe - write failed");
      }
    }
    else {
      eos_debug("Writing to remote file stripe: %i, fstid: %i", nclient, mapStripe_Fst[nclient]);
      if (!stripeClient[mapStripe_Fst[nclient]]->Write(dataBlock[nclient], offsetLocal + headerSize, stripeWidth)) {
        return gOFS.Emsg("ReedSWrite",*error, EREMOTEIO, "write stripe - write failed ", 
                         stripeUrl[mapStripe_Fst[nclient]].c_str());
      }
    }

    //write the correct block to the reading buffer
    if (*iter < nStripes){  //if one of the data blocks
      if ((offset >= offsetGroup + (*iter) * stripeWidth) && 
          (offset < offsetGroup + ((*iter) + 1) * stripeWidth)) {
        memcpy(buffer, dataBlock[*iter] + (offset % stripeWidth), length);    
      }
    }
  }

  return true; 
}


//------------------------------------------------------------------------------
bool 
ReedSLayout::solutionBkt(unsigned int k, 
                         unsigned int *indexes, 
                         vector<unsigned int> validId)
{
  bool found = false;

  if (k != nStripes) return found;

  for (unsigned int i = nStripes; i < nFiles; i++) {
    if (find(validId.begin(), validId.end(), i) != validId.end()) {
      found = false;
      for (unsigned int j = 0; j <= k; j++) {
        if (indexes[j] == i) {
          found  = true;
          break;
        }
      }
      if (!found) break;
    }
  }

  return found;
}


//------------------------------------------------------------------------------
//validation function for backtracking
bool 
ReedSLayout::validBkt(unsigned int k, unsigned int *indexes, vector<unsigned int> validId)
{
  // Obs::condition from zfec implementation:
  // If a primary block, i, is present then it must be at index i. Secondary blocks can appear anywhere.
  if (find(validId.begin(), validId.end(), indexes[k]) == validId.end() ||
      (indexes[k] < nStripes && indexes[k] != k))
    return false;

  for (unsigned int i = 0; i < k; i++)
    if (indexes[i] == indexes[k] || (indexes[i] < nStripes && indexes[i] != i))
      return false;

  return true;
}


//------------------------------------------------------------------------------
//backtracking method to get the indices needed for recovery
bool 
ReedSLayout::backtracking(unsigned int *indexes, vector<unsigned int> validId, unsigned int k)
{
  if (this->solutionBkt(k, indexes, validId)) {
    return true;
  } else {
    for (indexes[k] = 0; indexes[k] < nFiles; indexes[k]++)
      if (this->validBkt(k, indexes, validId))
        if (this->backtracking(indexes, validId, k + 1))
          return true;

    return false;
  }
}


//------------------------------------------------------------------------------
//recompute and write to files the parity blocks of the groups between the two limits
int 
ReedSLayout::updateParityForGroups(XrdSfsFileOffset offsetStart, XrdSfsFileOffset offsetEnd)
{
  XrdSfsFileOffset offsetGroup;
  XrdSfsFileOffset offsetBlock;
  XrdSfsFileOffset lineWidth = nStripes * stripeWidth;

  for (unsigned int i = (offsetStart / lineWidth); i < ceil((offsetEnd * 1.0 ) / lineWidth); i++){
    offsetGroup = i * lineWidth;
    for(unsigned int j = 0; j < nStripes; j++){
      offsetBlock = offsetGroup + j * stripeWidth;
      read(offsetBlock, dataBlock[j], stripeWidth);        
    }
     
    //compute parity blocks and write to files
    computeParity();      
    writeParityToFiles(offsetGroup/nStripes);
  }
  
  return SFS_OK;
}

//------------------------------------------------------------------------------
//write the parity blocks from dataBlocks to the corresponding file stripes
int 
ReedSLayout::writeParityToFiles(XrdSfsFileOffset offsetParityLocal)
{
  int rc1;

  //write the blocks to the parity files
  for (unsigned int i = nStripes; i < nFiles; i++){
    if (i == indexStripe){  //local file
      rc1 = ofsFile->writeofs(offsetParityLocal + headerSize, dataBlock[i], stripeWidth);
      if (rc1 < 0) {
        return gOFS.Emsg("ReedSWrite",*error, errno, "write local stripe - write failed");
      }
    }
    else {
      if (!stripeClient[mapStripe_Fst[i]]->Write(dataBlock[i], offsetParityLocal + headerSize, stripeWidth)) 
        return gOFS.Emsg("ReedSWrite",*error, EREMOTEIO, "write stripe - write failed ", stripeUrl[mapStripe_Fst[i]].c_str());
    }
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
int 
ReedSLayout::truncate(XrdSfsFileOffset offset) {

  int rc = SFS_OK;
  XrdSfsFileOffset truncValue = 0;

  if (!offset) return rc;

  if (isEntryServer){
    if (offset % (nStripes * stripeWidth) != 0)
      truncValue = ((offset / (nStripes * stripeWidth)) + 1) * stripeWidth;
    else truncValue = (offset / (nStripes * stripeWidth)) *  stripeWidth;

    for (unsigned int i = 0; i < nFiles; i++) {
      if (i != indexStripe && stripeClient[i]) {
        if (!stripeClient[i]->Truncate(truncValue)) {
          return gOFS.Emsg("ReedSTruncate",*error, EREMOTEIO, "truncate stripe - truncate failed (1)");	
        }
      }
    }
  }
  else {
    truncValue = offset;
  }

  if ((rc = ofsFile->truncateofs(truncValue +  headerSize))) 
    return gOFS.Emsg("ReedSTruncate",*error, EIO, "truncate stripe - truncate failed (0)");	

  return rc;
}


//------------------------------------------------------------------------------
int 
ReedSLayout::sync() {

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
    return gOFS.Emsg("ReedSSync",*error, EREMOTEIO, "sync remote stripe");
  }

  rc1 = ofsFile->syncofs();
  if (rc1 < 0) {
    eos_err("Failed to sync local stripe");
    return gOFS.Emsg("ReedSSync",*error, errno, "sync local stripe");
  }
  
  return rc1;
}



//------------------------------------------------------------------------------
int 
ReedSLayout::stat(struct stat *buf) 
{

  int rc;

  if (!XrdOfsOss->Stat(ofsFile->fstPath.c_str(), buf))
    rc = gOFS.Emsg("ReedSStat", *error, EIO, "close - cannot stat closed file to determine file size",ofsFile->Path.c_str());
  
  buf->st_size = fileSize;
  return SFS_OK;
}


//------------------------------------------------------------------------------
int 
ReedSLayout::close() 
{
  int rc1 = SFS_OK;

  if (isEntryServer){
    //if recovered then we have to truncate once again to the right size 
    if (doneRecovery){
      doneRecovery = false;
      truncate(fileSize);
    }
    
    for (unsigned int i = 0; i < nFiles; i++) {
      if (i != indexStripe && stripeClient[i] && !stripeClient[i]->Close()) {
        return gOFS.Emsg("RaidDPClose",*error, EREMOTEIO, "close stripe - close failed ", stripeUrl[i].c_str());	
      }
    }
  }

  //closing local file
  rc1 = ofsFile->closeofs();
  return rc1;
}

EOSFSTNAMESPACE_END

