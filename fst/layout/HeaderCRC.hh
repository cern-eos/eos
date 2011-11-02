#ifndef __EOSFST_HEADERCRC_HH__
#define __EOSFST_HEADERCRC_HH__

/*----------------------------------------------------------------------------*/
#include <cstdio>
#include <sys/types.h>
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define HEADER ("_HEADER_RAIDDP_")

class HeaderCRC : public eos::common::LogId {

private:
 
  char tag[16];
  unsigned int idStripe;                                //index of the stripe to which the header belongs
  long int noBlocks;                           //total number of blocks of size PAGE_SIZE
  XrdSfsFileOffset sizeLastBlock;              //size of the last block of data
  char* checksum;                              //checksum for the information in the header

  int sizeHeader, lenChecksum;                 //total size of the header and length of the checksum

public:

  HeaderCRC();  
  HeaderCRC(long);  
  ~HeaderCRC();

  int WriteToFile(XrdClient*);                //write the current header to file and modify the file position
  int WriteToFile(XrdFstOfsFile*);            //write the current header to file and modify the file position

  bool ReadFromFile(XrdClient*);              //read the header from the file and modify the file position
  bool ReadFromFile(XrdFstOfsFile*);          //read the header from the file and modify the file position

  // char* ComputeCheckSum();                 //compute checksum from the information currently in the header
  void UpdateCheckSum();                      //update the value of the checksum based on the info in the header
  //bool ValidHeader();                       //check the integrity of the header by computing the check sum

  long int GetNoBlocks() { return noBlocks; };   
  void SetNoBlocks(long int nblocks){ noBlocks = nblocks; };

  XrdSfsFileOffset GetSizeLastBlock() { return sizeLastBlock; }; 
  void SetSizeLastBlock(XrdSfsFileOffset sizelastblock) { sizeLastBlock = sizelastblock; };  

  unsigned int GetIdStripe() { return idStripe; }; 
  void SetIdStripe(unsigned int idstripe) { idStripe = idstripe; }; 

  int GetLenChecksum() { return lenChecksum; };
  void SetLenChecksum(int lenchecksum) { lenChecksum = lenchecksum; };
  
  char* GetTag() { return tag; }; 
  const char* GetCheckSum() { return checksum; }; 

  int GetHeaderSize() { 
    return sizeHeader;
  };   
  
};

EOSFSTNAMESPACE_END

#endif
