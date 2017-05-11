// ----------------------------------------------------------------------
// File: DrainTransfer.hh
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_DRAINTRANSFER_HH__
#define __EOSMGM_DRAINTRANSFER_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/FileId.hh"
#include "common/SecEntity.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClCopyProcess.hh"
#include "Xrd/XrdScheduler.hh"
#include <vector>
#include <string>
#include <deque>
#include <cstring>


/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/**
 * @file DrainTransferJob.hh
 * 
 * @brief Class implementing a third party copy transfer for drain
 * 
 */

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class implementing the third party copy transfer, takes as input the 
 *
 * source file id and the destination filesystem
 */
/*----------------------------------------------------------------------------*/
class DrainTransferJob : XrdJob
{
private:
  /// file id for the given file to transfer
  eos::common::FileId::fileid_t mFileId;
  /// destination fs
  eos::common::FileSystem::fsid_t mfsIdSource, mfsIdTarget;

  std::string mSourcePath; 

public:

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * @param fileId the file id
   */
  // ---------------------------------------------------------------------------

  DrainTransferJob (eos::common::FileId::fileid_t fileId, 
                              eos::common::FileSystem::fsid_t fsIdS,
                              eos::common::FileSystem::fsid_t fsIdT) {
    mFileId = fileId;
    mfsIdSource = fsIdS;
    mfsIdTarget = fsIdT;
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~DrainTransferJob (); 

  void DoIt();


};

EOSMGMNAMESPACE_END

#endif
