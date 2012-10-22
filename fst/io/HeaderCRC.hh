// ----------------------------------------------------------------------
// File: HeaderCRC.cc
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

#ifndef __EOSFST_HEADERCRC_HH__
#define __EOSFST_HEADERCRC_HH__

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include <XrdCl/XrdClFile.hh>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

#define HEADER ("_HEADER_RAIDIO_")

class HeaderCRC : public eos::common::LogId
{

private:

  bool valid;
  char tag[16];
  long int noBlocks;                           //total number of blocks
  size_t sizeLastBlock;                        //size of the last block of data
  unsigned int idStripe;                       //index of the stripe the header belongs to

  static const size_t sizeHeader = 4 * 1024;  //size of header

public:

  HeaderCRC();
  HeaderCRC( long );
  ~HeaderCRC();

  int writeToFile( XrdCl::File* f );
  bool readFromFile( XrdCl::File* f );

  char*        getTag();
  int          getSize() const;
  size_t       getSizeLastBlock() const;
  long int     getNoBlocks() const;
  unsigned int getIdStripe() const;

  void setNoBlocks( long int nblocks );
  void setSizeLastBlock( size_t sizelastblock );
  void setIdStripe( unsigned int idstripe );

  bool isValid() const;
  void setState( bool state );
};

EOSFSTNAMESPACE_END

#endif
