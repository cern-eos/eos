// ----------------------------------------------------------------------
// File: RaidIO.hh
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
#include <vector>
#include <string>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/io/HeaderCRC.hh"
#include "fst/io/AsyncRespHandler.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/

#ifndef __EOSFST_RAIDIO_HH__
#define __EOSFST_RAIDIO_HH__

EOSFSTNAMESPACE_BEGIN

#define STRIPESIZE 1024*1024      //size width of a block

using namespace XrdCl;

class RaidIO : public eos::common::LogId
{
public:

  RaidIO( std::string algorithm, std::vector<std::string> stripeurl, unsigned int nparitystripes,
          bool storerecovery, off_t targetsize = 0, std::string bookingopaque = "oss.size" );

  virtual int open( int flags );
  virtual int read( off_t offset, char* buffer, size_t length );
  virtual int write( off_t offset, char* buffer, size_t length );
  virtual int truncate( off_t offset ) = 0;
  virtual int remove();  // unlinks all connected pieces
  virtual int sync();
  virtual int close();
  virtual int stat( struct stat* buf );
  virtual off_t size(); // returns the total size of the file

  virtual ~RaidIO();

protected:

  File** xrdFile;                //! xrd clients corresponding to the stripes
  HeaderCRC* hdUrl;              //! array of header objects

  bool isRW;                     //! mark for writing
  bool isOpen;                   //! mark if open
  bool doTruncate;               //! mark if there is a need to truncate
  bool updateHeader;             //! mark if header updated
  bool doneRecovery;             //! mark if recovery done
  bool fullDataBlocks;           //! mark if we have all data blocks to compute parity
  bool storeRecovery;            //! set if recovery also triggers writing back to the files
  //this also means that all files must be available

  unsigned int nParityStripes;
  unsigned int nDataStripes;
  unsigned int nTotalStripes;

  off_t targetSize;
  off_t offsetGroupParity;       //offset of the last group for which we computed the parity blocks

  size_t sizeHeader;             //size of header = 4KB
  size_t stripeWidth;            //stripe with
  size_t fileSize;               //total size of current file
  size_t sizeGroupBlocks;        //eg. RAIDDP: group = nDataStripes^2 blocks

  std::string algorithmType;
  std::string bookingOpaque;
  std::vector<char*> dataBlocks;
  std::vector<std::string> stripeUrls;                 //! urls of the files
  std::vector<AsyncRespHandler*> vectRespHandler;  //! async response handlers for each stripe
  std::map<unsigned int, unsigned int> mapUrl_Stripe;  //! map of url to stripes
  std::map<unsigned int, unsigned int> mapStripe_Url;  //! map os stripes to url

  virtual bool validateHeader();
  virtual bool recoverBlock( char* buffer, std::map<off_t, size_t> &mapPieces, off_t offsetInit ) = 0;
  virtual void addDataBlock( off_t offset, char* buffer, size_t length ) = 0;
  virtual void computeDataBlocksParity( off_t offsetGroup ) = 0;
};

EOSFSTNAMESPACE_END

#endif
