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
#include "mgm/DynamicECFile.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/AssistedThread.hh"
#include "namespace/interface/IFileMD.hh"
#include "mgm/DynamicCreator.hh"
#include "mgm/DynamicScanner.hh"
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
//thread for dynamic cleaning the system for old and not used files.

//check for different nameing conventions

/// might be in the system in order to look for the constant update on this, bur for this it will be easy for the rest of the system to get the status of the thread.
/// other stuff for the status of the thread can be put in as well.
struct statusForSystem {
  int64_t totalSize;
  int64_t usedSize;
  int64_t deletedSize;
  int64_t undeletedSize; /// bytes that will have to be deleted.
};

class DynamicEC
{
private:
  AssistedThread mThread; /// thread for doing the clean up

  AssistedThread mThread2; /// thread for doing the clean up

  AssistedThread mThread3; /// thread for doing the checking for the files

  AssistedThread TestThread; ///Thread for testign stuff.

  DynamicScanner mScanner;

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

  std::atomic<uint64_t>
  age;

  std::atomic<int>
  security;

  std::atomic<bool>
  mOnWork;

  std::atomic<bool>
  mTestEnabel;

  int waitTime;

  uint64_t sizeToBeDeleted; /// the size that the system will have to delete in order to get under the minimum threshold.



  //DynamicCreator mCreator;

  ////////This is for the scanner
  bool enabled()
  {
    return (mEnabled.load()) ? true : false;
  }
  bool disable()
  {
    if (!enabled()) {
      return false;
    } else {
      mEnabled.store(0, std::memory_order_seq_cst);
      return true;
    }
  }
  bool enable()
  {
    if (enabled()) {
      return false;
    } else {
      mEnabled.store(1, std::memory_order_seq_cst);
      return true;
    }
  }

  std::map<uint64_t, std::map<std::string, uint64_t>> lastScanStats;
  std::map<uint64_t, std::map<std::string, uint64_t>> currentScanStats;
  std::map<std::string, std::set<uint64_t>> lastFaultyFiles;
  std::map<std::string, std::set<uint64_t>> currentFaultyFiles;

  std::atomic<double> scanned_percent;

  std::atomic<int> mEnabled;

  //uint64_t test;
  //std::atomic<std::map<uint64_t, std::shared_ptr<eos::QuarkFileMD>>> statusFiles2;

  std::mutex mMutexForStatusFiles;
  std::map<uint64_t, std::shared_ptr<eos::QuarkFileMD>> statusFiles;
  std::mutex mMutexForStatusFilesMD;
  //std::map<uint64_t, std::shared_ptr<eos::IFileMD>> statusFilesMD;
//std::shared_ptr<eos::IFileMD>

  time_t timeCurrentScan;
  time_t timeLastScan;
  void Process(std::string& filepath);
  void Process(std::shared_ptr<eos::IFileMD> fmd);
  //AssistedThread mThread; ///< thread id of the creation background tracker
  std::unique_ptr<qclient::QClient> mQcl;
  uint64_t nfiles;
  uint64_t ndirs;

  std::mutex mutexScanStats;

  int mTestNumber;

  std::atomic<bool> mDynamicOn;

  //bool mOnTest;

  /// The XRootD OFS plugin implementing the metadata handling of EOS
  // use gOFS it is the same here
  //XrdMgmOfs &m_ofs;

public:

  std::map<uint64_t, std::shared_ptr<eos::IFileMD>> GetMap();

  std::map<uint64_t, std::shared_ptr<eos::IFileMD>> statusFilesMD;

  struct FailedToGetFileSize: public std::runtime_error {
    FailedToGetFileSize(const std::string& msg): std::runtime_error(msg) {}
  };

  void TestFunction();

  uint64_t createdFileSize; /// the size of the created files in bytes

  uint64_t deletedFileSize; /// The deletion of files for this section;

  uint64_t deletedFileSizeInTotal; /// The size that have been deletede though out the whole time of the systems time.

  //std::map<eos::IFileMD::id_t,std::shared_ptr<eos::IFileMD>> simulatedFiles;

  std::map<IFileMD::id_t, std::shared_ptr<DynamicECFile>> simulatedFiles;

  //void performCycleQDB(ThreadAssistant& assistant) noexcept;

  void setTestOn();

  void setTestOff();

  bool getTest();

  void turnDynamicECOn();

  void turnDynamicECOff();

  void testForSpaceCmd2();

  void testForSpaceCmd();

  void setWaitTime(int wait);

  void createFileForTest();

  void createFiles();

  void createFilesOneTime();

  void createFilesOneTimeThread(ThreadAssistant& assistant);

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

  int getWaitTime();

  void setMinThresHold(double thres);

  double getMinThresHold();

  void setMaxThresHold(double thres);

  double getMaxThresHold();

  void setAgeFromWhenToDelete(uint64_t timeFrom);

  uint64_t getAgeFromWhenToDelete();

  void setMinForDeletion(uint64_t size);

  uint64_t getMinForDeletion();

  void setSecurity(int security);

  int getSecurity();

  void fillSingleFile();

  void fillFiles();

  void fillFiles(int newFiles);

  void fillSingleSmallFile(uint64_t time, uint64_t size, int partitions);

  std::string TimeStampCheck(std::string file);

  //high or low watermark, with some trigger.
  statusForSystem SpaceStatus();

  ///might be bool too tell if the file was deleted, or int is on how many copies were deleted.
  bool DeletionOfFileID(std::shared_ptr<DynamicECFile> file, uint64_t ageOld);

  //This is for the system in order to make it for alle the files and not only for the special file made for this purpose
  bool DeletionOfFileIDForGenerelFile(std::shared_ptr<eos::QuarkFileMD> file,
                                      uint64_t ageOld);

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

  void kRaid6(std::shared_ptr<eos::QuarkFileMD> file);

  void kRaid6T(std::shared_ptr<eos::DynamicECFile> file);

  void kReduce(std::shared_ptr<eos::QuarkFileMD> file);

  void kReduceMD(std::shared_ptr<eos::IFileMD> file);

  std::uint64_t getFileSizeBytes(const IFileMD::id_t fid);

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
            int wait = 30, int securityNew = 1);

  ~DynamicEC();

  void Stop();

  void Cleanup() noexcept;

  void CleanupMD() noexcept;

  void Run(ThreadAssistant& assistant)
  noexcept; /// no exceptions aloud, have to check for all the output combinations to return.

  struct Options {
    bool enabled;                  //< Is FileInspector even enabled?
    std::chrono::seconds
    interval; //< Run FileInsepctor cleanup every this many seconds
  };

  Options getOptions();

  void performCycleQDB(ThreadAssistant& assistant) noexcept;

  void performCycleQDBMD(ThreadAssistant& assistant) noexcept;

  void RunScan(ThreadAssistant& assistant) noexcept;




};



EOSMGMNAMESPACE_END
#endif
