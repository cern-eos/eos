// ----------------------------------------------------------------------
// File: DynamicEC.hh
// Author: Andreas Stoeve - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @file DynamicEC.hh
//! @breif removing of old partision wich is not in use
//------------------------------------------------------------------------------




#ifndef __EOSMGM_GEOBALANCER__
#define __EOSMGM_GEOBALANCER__

/* -------------------------------------------------------------------------- */
#include "mgm/DynamicECFile.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/AssistedThread.hh"
#include "namespace/interface/IFileMD.hh"
/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* -------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <ctime>
#include <map>
#include <atomic>


//! Forward declaration
namespace eos
{
class IFileMD;
}

EOSMGMNAMESPACE_BEGIN
//thread for dynamic cleaning the system for old and not used files.


/// might be in the system in order to look for the constant update on this, bur for this it will be easy for the rest of the system to get the status of the thread.
/// other stuff for the status of the thread can be put in as well.
struct statusForSystem {
  uint64_t totalSize;
  uint64_t usedSize;
  uint64_t deletedSize;
  uint64_t undeletedSize; /// bytes that will have to be deleted.
};

class DynamicEC
{
private:
  AssistedThread mThread; /// thread for doing the clean up

  std::string
  mSpaceName; /// the space that the thread is running on // this have to be cheked on how it will have to run over

  std::string
  timeStore; /// some variable to store the time, to compare with the new time, can also be done dynamic from a function and like five years from now;

  std::atomic<double>
  minThresHold; /// Threshold on when to stop the deletion of files

  std::atomic<double>
  maxThresHold; /// ThresHold on when to delete part of different files

  std::atomic<uint64_t>
  timeFromWhenToDelete; /// time for how old the file have to be in order to be deleted.

  std::atomic<uint64_t>
  sizeMinForDeletion; /// the minimum size, that the file in the system will have to be in order to get deleted.



  uint64_t sizeToBeDeleted; /// the size that the system will have to delete in order to get under the minimum threshold.

public:

  uint64_t createdFileSize; /// the size of the created files in bytes

  uint64_t deletedFileSize; /// The deletion of files for this secion;

  //std::map<eos::IFileMD::id_t,std::shared_ptr<eos::IFileMD>> simulatedFiles;

  std::map<IFileMD::id_t, std::shared_ptr<DynamicECFile>> simulatedFiles;

  void setMinThresHold(double thres);

  double getMinThresHold();

  void setMaxThresHold(double thres);

  double getMaxThresHold();

  void setTimeFromWhenToDelete(uint64_t timeFrom);

  uint64_t getTimeFromWhenToDelete();

  void setMinForDeletion(uint64_t size);

  uint64_t getMinForDeletion();

  void fillFiles();

  void fillFiles(int newFiles);

  void fillSingleSmallFile(uint64_t time, uint64_t size, int partitions);

  std::string TimeStampCheck(std::string file);

  //high or low watermark, with some trigger.
  statusForSystem SpaceStatus();

  ///might be bool too tell if the file was deleted, or int is on how many copies were deleted.
  bool DeletionOfFileID(std::shared_ptr<DynamicECFile> file);



  //This is for a not modified file
  uint64_t GetSizeOfFile(std::shared_ptr<DynamicECFile> file);

  uint64_t GetSizeFactor1(std::shared_ptr<DynamicECFile> file);

  //Bool to check it is done or failed.
  void SingleDeletion(std::shared_ptr<DynamicECFile> file);

  void kQrainReduction(std::shared_ptr<DynamicECFile> file);

  int DummyFunction(int number);

  bool TrueForAllRequest();

  //DynamicEC();
//test
  //---------------------------------------------------------------------------------------------------
  //! Gets the age from now and how far back it will have to delete files from in seconds.
  //! Takes the size that will be the minimum for deletion as bytes.
  //! The theshold to start the thread for the system, as percentage of full storage
  //! The low threshold to stop the system as percentage of full storage
  //---------------------------------------------------------------------------------------------------

  DynamicEC(const char* spacename="default", uint64_t age=3600, uint64_t minsize=1024*1024, double maxThres=95.0, double minThres=90.0);

  ~DynamicEC();

  void Stop();

  void Cleanup() noexcept;

  void Run(ThreadAssistant& assistant)
  noexcept; /// no exceptions aloud, have to check for all the output combinations to return.
};



EOSMGMNAMESPACE_END
#endif
