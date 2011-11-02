/*----------------------------------------------------------------------------*/
#include <unistd.h>
#include "common/LayoutId.hh"
#include "fst/layout/HeaderCRC.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN


/*----------------------------------------------------------------------------*/
//constructor
HeaderCRC::HeaderCRC()
{

  idStripe = -1;
  noBlocks = -1;
  sizeLastBlock = -1;
  sizeHeader = 4096;
}


/*----------------------------------------------------------------------------*/
//constructor
HeaderCRC::HeaderCRC(long int noblocks)
{
  strcpy(tag, HEADER);
  noBlocks = noblocks;
  sizeLastBlock = -1;
  idStripe = -1;
  sizeHeader = 4096;
}


/*----------------------------------------------------------------------------*/
//destructor
HeaderCRC::~HeaderCRC(){ }

/*----------------------------------------------------------------------------*/
//read the header from the file via XrdClient
bool HeaderCRC::ReadFromFile(XrdClient *stripeClient)
{
  int ret;
  long int offset = 0;
  char* buff =  (char*) calloc(sizeHeader, sizeof(char));

  eos_debug("ReadFromFile: XrdClient -  offset: %li, sizeHeader: %i \n", offset, sizeHeader);

  ret = stripeClient->Read(buff, offset, sizeHeader);
  if ((!ret) || (ret != sizeHeader)) {
    free(buff);
    return false;
  }
 
  memcpy(tag, buff, sizeof tag);
  if (strncmp(tag, HEADER, strlen(HEADER))){
    free(buff);
    return false;
  }

  offset += sizeof tag;
  memcpy(&idStripe, buff + offset, sizeof idStripe);
  offset += sizeof idStripe;
  memcpy(&noBlocks, buff + offset, sizeof noBlocks);
  offset += sizeof noBlocks;
  memcpy(&sizeLastBlock, buff + offset, sizeof sizeLastBlock);

  free(buff);
  return true;
}


/*----------------------------------------------------------------------------*/
//read the header from the file
bool HeaderCRC::ReadFromFile(XrdFstOfsFile *file)
{
  int ret;
  XrdSfsFileOffset offset = 0;
  char* buff = (char*) calloc(sizeHeader, sizeof(char));

  eos_debug("ReadFromFile: XrdFstOfsFile -  offset: %lli, sizeHeader: %i \n", offset, sizeHeader);

  ret = file->readofs(offset, buff, sizeHeader);
  if ((!ret) || (ret != sizeHeader)) {
    free(buff);
    return false;
  }
 
  memcpy(tag, buff, sizeof tag);
  if (strncmp(tag, HEADER, strlen(HEADER))){
    free(buff);
    return false;
  }

  offset += sizeof tag;
  memcpy(&idStripe, buff + offset, sizeof idStripe);
  offset += sizeof idStripe;
  memcpy(&noBlocks, buff + offset, sizeof noBlocks);
  offset += sizeof noBlocks;
  memcpy(&sizeLastBlock, buff + offset, sizeof sizeLastBlock);

  free(buff);
  return true;
}



/*----------------------------------------------------------------------------*/
//write the header to the file via XrdClient
int HeaderCRC::WriteToFile(XrdClient* stripeClient)
{
  int offset = 0;
  char* buff = (char*) calloc(sizeHeader, sizeof(char));

  memcpy(buff + offset, HEADER, sizeof tag);
  offset += sizeof tag;
  memcpy(buff + offset, &idStripe, sizeof idStripe);
  offset +=  sizeof idStripe;
  memcpy(buff + offset, &noBlocks, sizeof noBlocks);
  offset += sizeof noBlocks;
  memcpy(buff + offset, &sizeLastBlock, sizeof sizeLastBlock);
  offset += sizeof sizeLastBlock;
  memset(buff + offset, 0, sizeHeader - offset);
         
  if (!stripeClient->Write(buff, 0, sizeHeader)){
    free(buff);
    return 1;
  }
    
  free(buff);
  return SFS_OK;
}


/*----------------------------------------------------------------------------*/
//write the header to the file
int HeaderCRC::WriteToFile(XrdFstOfsFile* file)
{
  int offset = 0;
  char* buff = (char*) calloc(sizeHeader, sizeof(char));

  memcpy(buff + offset, HEADER, sizeof tag);
  offset += sizeof tag;
  memcpy(buff + offset, &idStripe, sizeof idStripe);
  offset +=  sizeof idStripe;
  memcpy(buff + offset, &noBlocks, sizeof noBlocks);
  offset += sizeof noBlocks;
  memcpy(buff + offset, &sizeLastBlock, sizeof sizeLastBlock);
  offset += sizeof sizeLastBlock;
  memset(buff + offset, 0, sizeHeader - offset);

  if (!file->writeofs(0, buff, sizeHeader)){
    free(buff);
    return 1;
  }
    
  free(buff);
  return SFS_OK;
}

EOSFSTNAMESPACE_END
