// ----------------------------------------------------------------------
// File: ReedSFile.hh
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

#ifndef __EOSFST_REEDSFILE_HH__
#define __EOSFST_REEDSFILE_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/io/RaidIO.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class ReedSFile : public RaidIO
{
 public:

  ReedSFile(std::vector<std::string> stripeurl, int nparitystripes, bool storerecovery,
             off_t targetsize = 0, std::string bookingopaque="oss.size");

  virtual int truncate(off_t offset);
  virtual ~ReedSFile();

 private:

  void computeParity();                        
  int writeParityToFiles(off_t offsetGroup);
  
  virtual bool recoverBlock(char *buffer, off_t offset, size_t length);
  virtual void addDataBlock(off_t offset, char* buffer, size_t length);
  virtual void computeDataBlocksParity(off_t offsetGroup);
  //  virtual int updateParityForGroups(off_t offsetStart, off_t offsetEnd);
  
  //methods used for backtracking
  bool solutionBkt(unsigned int k, unsigned int *indexes, vector<unsigned int> validId);
  bool validBkt(unsigned int k, unsigned int *indexes, vector<unsigned int> validId);
  bool backtracking(unsigned int *indexes, vector<unsigned int> validId, unsigned int k);
};

EOSFSTNAMESPACE_END

#endif
