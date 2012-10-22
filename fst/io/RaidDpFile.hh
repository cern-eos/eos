// ----------------------------------------------------------------------
// File: RaidDpFile.hh
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

#ifndef __EOSFST_RAIDDPFILE_HH__
#define __EOSFST_RAIDDPFILE_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/io/RaidIO.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define VECTOR_SIZE 16    //used for computing XOR or 128 bits = 8 * 16
typedef uint32_t u32;

class RaidDpFile : public eos::fst::RaidIO
{
public:

  RaidDpFile( std::vector<std::string> stripeurl, int nparitystripes, bool storerecovery,
              off_t targetsize = 0, std::string bookingopaque = "oss.size" );

  virtual int truncate( off_t offset );
  virtual ~RaidDpFile();

private:

  unsigned int nDataBlocks;          //no. data blocks in a group = nDataStripes^2
  unsigned int nTotalBlocks;         //no. data and parity blocks in a group

  //  virtual int updateParityForGroups(off_t offsetStart, off_t offsetEnd);
  virtual bool recoverBlock( char* buffer, off_t offset, size_t length );
  virtual void addDataBlock( off_t offset, char* buffer, size_t length );
  virtual void computeDataBlocksParity( off_t offsetGroup );

  void computeParity();                            //compute and write the simple and double parity blocks to files
  void operationXOR( char*, char*, char*, size_t ); //compute the XOR result of two blocks of any size

  int writeParityToFiles( off_t offsetGroup );
  bool simpleParityRecover( char* buffer, off_t offset, size_t length, int& blockCorrupted );
  bool doubleParityRecover( char* buffer, off_t offset, size_t lenfth );

  std::vector<unsigned int> getDiagonalStripe( unsigned int ); //return diagonal stripe corresponding to current block
  bool validHorizStripe( std::vector<unsigned int>&, bool*, unsigned int ); //validate horizontal stripe
  bool validDiagStripe( std::vector<unsigned int>&, bool*, unsigned int ); //validate diagonal stripe

  std::vector<unsigned int> getSimpleParityIndices();       //indices of the simple parity blocks
  std::vector<unsigned int> getDoubleParityIndices();       //indices of the double parity blocks

  unsigned int getParityBlockId( unsigned int );            //SP blocks corresponding to current block
  unsigned int getDParityBlockId( std::vector<unsigned int> ); //DP block corresponding to current block

  unsigned int mapBigToSmallBlock( unsigned int ); //map index from nTotalBlocks representation to nBlocks
  unsigned int mapSmallToBigBlock( unsigned int ); //map index from nBlocks representation to nTotalBlocks

};

EOSFSTNAMESPACE_END

#endif

