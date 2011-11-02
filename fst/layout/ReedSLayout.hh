#ifndef __EOSFST_REEDSLAYOUT_HH__
#define __EOSFST_REEDSLAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/Layout.hh"
#include "fst/layout/HeaderCRC.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucString.hh"
#include "fst/layout/HeaderCRC.hh"
#include "XrdOfs/XrdOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class ReedSLayout : public Layout {

public:

  ReedSLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error);

  virtual int open(const char                *path,
		   XrdSfsFileOpenMode   open_mode,
		   mode_t               create_mode,
		   const XrdSecEntity        *client,
		   const char                *opaque);
  
  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length);
  virtual int truncate(XrdSfsFileOffset offset);
  virtual int stat(struct stat *buf);
  virtual int sync();
  virtual int close();
  
  virtual ~ReedSLayout();

private:

  HeaderCRC *hd;
 
  unsigned int extraBlocks;                //number of extra check blocks
  unsigned int indexStripe;                //fstid of current stripe
  unsigned int stripeHead;                 //fstid of stripe head
  unsigned int nFiles;                     //total number fo files used
  unsigned int nStripes;                   //number of data files used
  unsigned int headerSize;                 //size of the header 

  bool updateHeader;                       //mark if header updated
  bool doneRecovery;                       //mark if recovery done

  std::vector<char*> dataBlock;                         //nFiles data blocks
  std::map<unsigned int, unsigned int> mapFst_Stripe;   //map of fstid -> stripes
  std::map<unsigned int, unsigned int> mapStripe_Fst;   //map os stripes -> fstid

  XrdClient** stripeClient;                //xrd client objects
  XrdOucString* stripeUrl;                 //url's of stripe files
  XrdSfsXferSize stripeWidth;              //stripe width, usually 4k
  XrdSfsFileOffset fileSize;               //total size of current file

  //method to test and recover the header of stripe files
  bool validateHeader(HeaderCRC *&hd, bool *hdValid, 
                      std::map<unsigned int, unsigned int>  &mapSF, 
                      std::map<unsigned int, unsigned int> &mapFS);

  void computeParity();                                     //compute parity blocks
  int writeParityToFiles(XrdSfsFileOffset offsetGroup);     //write parity blocks 
  int updateParityForGroups(XrdSfsFileOffset offsetStart, XrdSfsFileOffset offsetEnd);

  //method that recovers a corrupted block
  bool recoverBlock(char *buffer, XrdSfsFileOffset offset, XrdSfsXferSize length);   
 
  //methods used for backtracking
  bool solutionBkt(unsigned int k, unsigned int *indexes, vector<unsigned int> validId);
  bool validBkt(unsigned int k, unsigned int *indexes, vector<unsigned int> validId);
  bool backtracking(unsigned int *indexes, vector<unsigned int> validId, unsigned int k);

};

EOSFSTNAMESPACE_END

#endif
