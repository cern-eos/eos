// ----------------------------------------------------------------------
// File: RaidDPLayout.hh
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

#ifndef __EOSFST_RAIDDPLAYOUT_HH__
#define __EOSFST_RAIDDPLAYOUT_HH__

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

#define VECTOR_SIZE 16    //used for computing XOR or 128 bits = 8 * 16
typedef uint32_t u32;

class RaidDPLayout : public Layout {

public:

  RaidDPLayout(XrdFstOfsFile* thisFile,int lid, XrdOucErrInfo *error);

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
  
  virtual ~RaidDPLayout();

private:

  HeaderCRC *hd;

  unsigned int headerSize;                 //size of the header  
  unsigned int indexStripe;                //fstid of current stripe
  unsigned int stripeHead;                 //fstid of stripe head
  unsigned int nStripes;                   //number of stripes used to generate the files for RAID DP
  unsigned int nFiles;                     //number fo files plus two more files for simple and double parity
  unsigned int nBlocks;                    //number of block in the stripe files per group = nStripes^2
  unsigned int nTotalBlocks;               //number of blocks used for data and parity

  bool updateHeader;                       //mark if header updated
  bool doneRecovery;                       //mark if recovery done
  bool isOpen;                             //makr if open

  std::vector<char*> dataBlock;                       //nTotalBlocks blocks
  std::map<unsigned int, unsigned int> mapFst_Stripe; //map of fstid to stripes
  std::map<unsigned int, unsigned int> mapStripe_Fst; //map os stripes to fstid

  XrdClient** stripeClient;                //xrd client objects
  XrdOucString* stripeUrl;                 //url's of stripe files
  XrdSfsXferSize stripeWidth;              //stripe with, usually 4k
  XrdSfsFileOffset fileSize;               //total size of current file

  bool validateHeader(HeaderCRC *&hd, bool *hdValid, std::map<unsigned int, unsigned int>  &mapSF, 
                      std::map<unsigned int, unsigned int> &mapFS);

  //block operations
  void computeParity();                                    //compute and write the simple and double parity blocks to files
  void operationXOR(char*, char*, XrdSfsXferSize, char*);  //compute the XOR result of two blocks of any size

  int writeParityToFiles(XrdSfsFileOffset offsetGroup);
  int updateParityForGroups(XrdSfsFileOffset offsetStart, XrdSfsFileOffset offsetEnd);

  bool recoverBlock(char *buffer, XrdSfsFileOffset offset, XrdSfsXferSize length, bool storeRecovery);   //method that recovers a corrupted block
  bool simpleParityRecover(char *buffer, XrdSfsFileOffset offset, XrdSfsXferSize length, int &blockCorrupted);  
  bool doubleParityRecover(char *buffer, XrdSfsFileOffset offset, XrdSfsXferSize lenfth, bool storeRecovery);                       

  std::vector<unsigned int> getDiagonalStripe(unsigned int);               //return diagonal stripe corresponding to current block

  bool validHorizStripe(std::vector<unsigned int> &, bool*, unsigned int); //check if horizontal stripe can be used for recovery
  bool validDiagStripe(std::vector<unsigned int> &, bool*, unsigned int);  //check if diagonal stripe can be used for recovery
 
  std::vector<unsigned int> getSimpleParityIndices();                      //return the indices of the simple parity blocks
  std::vector<unsigned int> getDoubleParityIndices();                      //return the indices of the double parity blocks

  unsigned int getParityBlockId(unsigned int);                             //return the SP block corresponding to current block
  unsigned int getDParityBlockId(std::vector<unsigned int>);               //return the DP block corresponding to current block
 
  unsigned int mapBigToSmallBlock(unsigned int);        //map current index from nTotalBlocks representation to nBlocks
  unsigned int mapSmallToBigBlock(unsigned int);        //map current index from nBlocks representation to nTotalBlocks

};

EOSFSTNAMESPACE_END

#endif

