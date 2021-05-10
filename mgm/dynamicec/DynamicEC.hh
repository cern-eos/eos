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




#ifndef __EOSMGM_DYNAMICEC__
#define __EOSMGM_DYNAMICEC__

/* -------------------------------------------------------------------------- */
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
#include "mgm/dynamicec/DynamicECFile.hh"
=======
#include "mgm/DynamicECFile.hh"
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/AssistedThread.hh"
#include "namespace/interface/IFileMD.hh"

/* -------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* -------------------------------------------------------------------------- */

#include "common/VirtualIdentity.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdOuc/XrdOucString.hh"
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/FileMD.hh"

#include "mgm/XrdMgmOfs.hh"

#include <memory>
#include <mutex>

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
<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
=======
//thread for dynamic cleaning the system for old and not used files.

//check for different nameing conventions
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

struct statusForSystem {
  int64_t totalSize;
  int64_t usedSize;
  int64_t deletedSize;
  int64_t undeletedSize;
};

struct Config {
  double min_threshold;
  double max_threshold;
  uint64_t min_age_for_deletion;
  uint64_t min_size_for_deletion;
  std::string spacename;
  bool onWork;
  int wait_time;
  bool test_enable;
  uint64_t mapMaxSize;
  uint64_t sleepWhenDone;
  uint64_t sleepWhenFull;
};

class DynamicEC
{
private:
  AssistedThread mThread; ///< thread for doing the clean up

  AssistedThread mThread2; ///< thread for creating files in the system for test

  AssistedThread mThread3; ///< thread for doing the checking for the files

  std::string
  mSpaceName; ///< the space that the thread is running on

  std::string
  mTimeStore; ///< some variable to store the time, to compare with the new time, can also be done dynamic from a function and like five years from now;



  std::atomic<bool>
  testEnabel; /// test needs this to speed up different productionscycles

  std::atomic<double>
  mMinThresHold; ///< Threshold on when to stop the deletion of files

  std::atomic<double>
  mMaxThresHold; ///< ThresHold on when to delete part of different files

  std::atomic<uint64_t>
  mTimeFromWhenToDelete; ///< time for how old the file have to be in order to be deleted.

  std::atomic<uint64_t>
  mSizeMinForDeletion; ///< the minimum size, that the file in the system will have to be in order to get deleted.

  std::atomic<uint64_t>
  mAge; ///< the age for old the files can be in order to be reduced.

  std::atomic<bool>
  mTestEnabel; ///< set the test to be online wich makes the system run faster but also use more resources.

  std::atomic<int>
  mWaitTime; ///< the time to wait between cycles in seconds.

  std::atomic<uint64_t>
  mSizeToBeDeleted; ///< the size that the system will have to delete in order to get under the minimum threshold.

  std::atomic<uint64_t>
  mSizeInMap; ///< the size of the files in the map

  std::atomic<uint64_t>
  mSleepWhenDone; ///< sleep when all the files in the system is finish

  std::atomic<uint64_t>
  mSleepWhenFull; ///< the time the system will sleep when there is enough in the map

  std::atomic<uint64_t>
  mSizeForMapMax; ///< this is the max size for the map

  std::atomic<bool>
  mOnWork; ///< used for running the unit tests

  std::map<uint64_t, std::map<std::string, uint64_t>>
      lastScanStats; ///< map for the last scan
  std::map<uint64_t, std::map<std::string, uint64_t>> currentScanStats; ///<
  std::map<std::string, std::set<uint64_t>> lastFaultyFiles;
  std::map<std::string, std::set<uint64_t>> currentFaultyFiles;

  std::atomic<double> scanned_percent; ///< scanned file percent

  std::mutex
  mMutexForStatusFilesMD; ///< mutex for the status files that can be removed
  std::map<uint64_t, std::shared_ptr<eos::QuarkFileMD>> statusFiles;
<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
  std::mutex mMutexForStatusFiles;
=======
  std::mutex mMutexForStatusFilesMD;
  //std::map<uint64_t, std::shared_ptr<eos::IFileMD>> statusFilesMD;
//std::shared_ptr<eos::IFileMD>
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  time_t timeCurrentScan;
  time_t timeLastScan;
  void Process(std::string& filepath);
  void Process(std::shared_ptr<eos::IFileMD> fmd);
  std::unique_ptr<qclient::QClient> mQcl;
  uint64_t nfiles;
  uint64_t ndirs;

  std::mutex mutexScanStats;

  int mTestNumber;

<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
  std::atomic<bool> mDynamicOn; ///< the bool to set the dynamicec on or off
=======
  std::atomic<bool> mDynamicOn;

  //bool mOnTest;
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  bool isIdInMap(uint64_t id);

public:

<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
  std::mutex mtx;

  std::condition_variable cv;

  void restartScan();

  std::map<uint64_t, std::shared_ptr<eos::IFileMD>> GetMap();

  std::map<uint64_t, std::shared_ptr<eos::IFileMD>> mStatusFilesMD; ///<
=======
  std::map<uint64_t, std::shared_ptr<eos::IFileMD>> GetMap();

  std::map<uint64_t, std::shared_ptr<eos::IFileMD>> statusFilesMD;
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  struct FailedToGetFileSize: public std::runtime_error {
    FailedToGetFileSize(const std::string& msg): std::runtime_error(msg) {}
  };

  uint64_t mCreatedFileSize; ///< the size of the created files in bytes this is used for testing

  std::atomic<uint64_t>
  mDeletedFileSize; ///< The deletion of files for this section;

  uint64_t mDeletedFileSizeInTotal; ///< The size that have been deletede though out the whole time of the systems time.

  std::map<IFileMD::id_t, std::shared_ptr<DynamicECFile>>
      mSimulatedFiles; ///< file for running tests in gtest

  Config getConfiguration();

  void setSizeForMap(uint64_t mapSize);

  uint64_t getSizeForMap();

  void setSleepWhenDone(uint64_t sleepWhenDone);

  uint64_t getSleepWhenDone();

  void setSleepWhenFull(uint64_t sleepWhenFull);

  uint64_t getSleepWhenFull();

  void setTest(bool onOff);

  bool getTest();

  void setDynamicEC(bool onOff);

  void testForSpaceCmd2();

  void testForSpaceCmd();

  void setWaitTime(int wait);

<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
  void createFilesOneTime();

  void createFilesOneTimeThread(ThreadAssistant& assistant);

=======
  void createFileForTest();

  void createFiles();

  void createFilesOneTime();

  void createFilesOneTimeThread(ThreadAssistant& assistant);

>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh
  void testForSingleFileWithkRaid5(int stripes, int redundancy, int excessstripes,
                                   uint64_t size);

  void testForSingleFileWithkRaidDP(int stripes, int redundancy,
                                    int excessstripes, uint64_t size);

  void testForSingleFileWithkArchive(int stripes, int redundancy,
                                     int excessstripes, uint64_t size);

  void testForSingleFileWithkReplica(int stripes, int redundancy,
                                     int excessstripes, uint64_t size);

  void testForSingleFileWithkPlain(int stripes, int redundancy, int excessstripes,
                                   uint64_t size);

  void testForSingleFileWithkQrain(int stripes, int redundancy, int excessstripes,
                                   uint64_t size);

  void testFilesBeignFilled(int stripes, int redundency, int excessstripes,
                            int number);

  void testFilesBeignFilledCompiledSize(int stripes, int redundancy,
                                        int excessstripes, int number, uint64_t size);

  void testForSingleFile(int stripes, int redundancy, int excessstripes,
                         uint64_t size);
<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
=======

  bool getTest();

  void setTestOn();

  void setTestOff();
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  int getWaitTime();

  void setMinThresHold(double thres);

  double getMinThresHold();

  void setMaxThresHold(double thres);

  double getMaxThresHold();

  void setAgeFromWhenToDelete(uint64_t timeFrom);

  uint64_t getAgeFromWhenToDelete();

  void setMinForDeletion(uint64_t size);

  uint64_t getMinForDeletion();

<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
=======
  void setSecurity(int security);

  int getSecurity();

  void fillSingleFile();

>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh
  void fillFiles();

  void fillFiles(int newFiles);

  void fillSingleSmallFile(uint64_t time, uint64_t size, int partitions);

  std::string TimeStampCheck(std::string file);

  statusForSystem SpaceStatus();

  bool DeletionOfFileID(std::shared_ptr<DynamicECFile> file, uint64_t ageOld);

  bool DeletionOfFileIDMD(std::shared_ptr<eos::IFileMD>, uint64_t ageOld);

<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
  uint64_t GetSizeOfFile(std::shared_ptr<DynamicECFile> file);

  long double TotalSizeInSystemMD(std::shared_ptr<eos::IFileMD> file);
=======
  //This is the new one for the fileMD
  bool DeletionOfFileIDMD(std::shared_ptr<eos::IFileMD>, uint64_t ageOld);

  //This is for a not modified file
  uint64_t GetSizeOfFile(std::shared_ptr<DynamicECFile> file);

  long double TotalSizeInSystem(std::shared_ptr<eos::QuarkFileMD> file);

  long double TotalSizeInSystemMD(std::shared_ptr<eos::IFileMD> file);

  static double GetRealSizeFactor(std::shared_ptr<eos::QuarkFileMD> file);

  static double GetRealSizeFactorMD(std::shared_ptr<eos::IFileMD> file);

  //Bool to check it is done or failed.
  void SingleDeletion(std::shared_ptr<DynamicECFile> file);

  void kQrainReduction(std::shared_ptr<DynamicECFile> file);
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  static double GetRealSizeFactorMD(std::shared_ptr<eos::IFileMD> file);

<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
  void kReduceMD(std::shared_ptr<eos::IFileMD> file);
=======
  void kRaid6T(std::shared_ptr<eos::DynamicECFile> file);

  void kReduce(std::shared_ptr<eos::QuarkFileMD> file);

  void kReduceMD(std::shared_ptr<eos::IFileMD> file);

  std::uint64_t getFileSizeBytes(const IFileMD::id_t fid);
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  void printAll();

//test
  //---------------------------------------------------------------------------------------------------
  //! Gets the age from now and how far back it will have to delete files from in seconds.
  //! Takes the size that will be the minimum for deletion as bytes.
  //! The theshold to start the thread for the system, as percentage of full storage
  //! The low threshold to stop the system as percentage of full storage
  //---------------------------------------------------------------------------------------------------
  //DynamicEC(const char* spacename="default", uint64_t age=3600, uint64_t minsize=1024*1024, double maxThres=95.0, double minThres=90.0);

  DynamicEC(const char* spacename = "default", uint64_t age = 3600,
            uint64_t minsize = 1024 * 1024,
            double maxThres = 98.0, double minThres = 95.0, bool OnWork = true,
<<<<<<< HEAD:mgm/dynamicec/DynamicEC.hh
            int wait = 30, uint64_t mapMaxSize = 10000000000000,
            uint64_t sleepWhenDone = 28800, uint64_t sleepWhenFull = 600);
=======
            int wait = 30, int securityNew = 1);
>>>>>>> 29f5f43... Udate for test:mgm/DynamicEC.hh

  ~DynamicEC();

  void Stop();

  void CleanupMD() noexcept;

  void CleanupMD() noexcept;

  void Run(ThreadAssistant& assistant)
  noexcept; ///< no exceptions aloud, have to check for all the output combinations to return.

  struct Options {
    bool enabled;                  ///< Is FileInspector even enabled?
    std::chrono::seconds
    interval; ///< Run FileInsepctor cleanup every this many seconds
  };

  Options getOptions();

  void performCycleQDBMD(ThreadAssistant& assistant) noexcept;

  void performCycleQDBMD(ThreadAssistant& assistant) noexcept;

  void RunScan(ThreadAssistant& assistant) noexcept;




};



EOSMGMNAMESPACE_END
#endif
