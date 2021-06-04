// ----------------------------------------------------------------------
// File: DynamicEC.cc
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

#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "mgm/DynamicECFile.hh"
#include "mgm/DynamicEC.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/Master.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"
#include "XrdCl/XrdClDefaultEnv.hh"

#include <random>
#include <cmath>
#include <map>
#include <iterator>
#include <atomic>
#include <chrono>
#include <list>

#include "common/Path.hh"
#include "common/IntervalStopwatch.hh"
#include "common/Timing.hh"
#include "common/ParseUtils.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/Resolver.hh"
#include "namespace/Prefetcher.hh"
#include <qclient/QClient.hh>

#include "namespace/Prefetcher.hh"

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 300 ///< seconds


/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN
DynamicEC::DynamicEC(const char* spacename, uint64_t ageNew,  uint64_t size,
                     double maxThres,
                     double minThres, bool OnWork, int wait, uint64_t mapMaxSize,
                     uint64_t sleepWhenDone, uint64_t sleepWhenFull)
{
  fprintf(stderr, "Constructor \n");
  eos_static_info("%s", "Constructor is running now \n");
  mAge = ageNew;

  if (OnWork) {
    mThread.reset(&DynamicEC::Run, this);
  }

  if (OnWork) {
    //mThread2.reset(&DynamicEC::createFilesOneTimeThread, this);
  }

  mTestNumber = 0;
  mSpaceName = spacename;
  mSimulatedFiles.clear();
  mDeletedFileSize = 0;
  mSizeMinForDeletion = size;
  mMaxThresHold = maxThres;
  mMinThresHold = minThres;
  mCreatedFileSize = 0;
  mSizeToBeDeleted = 0;
  mTestEnabel = false;
  mWaitTime = wait;
  mMutexForStatusFiles.lock();
  statusFiles.clear();
  mMutexForStatusFiles.unlock();
  mMutexForStatusFilesMD.lock();
  mStatusFilesMD.clear();
  mMutexForStatusFilesMD.unlock();
  timeCurrentScan = 0;
  timeLastScan = 0;
  mDeletedFileSizeInTotal = 0;
  ndirs = 0;
  nfiles = 0;
  mDynamicOn = true;
  mSizeForMapMax = mapMaxSize;
  mSleepWhenDone = sleepWhenDone;
  mSleepWhenFull = sleepWhenFull;
  mSizeInMap = 0;
  ready = false;

  if (OnWork) {
    mThread3.reset(&DynamicEC::RunScan, this);
  }
}

void
DynamicEC::restartScan()
{
  {
    std::unique_lock<std::mutex> lck(mtx);
    ready = true;
    cv.notify_all();
    eos_static_info("function done");
  }
}

std::map<uint64_t, std::shared_ptr<eos::IFileMD>>
    DynamicEC::GetMap()
{
  return mStatusFilesMD;
}

//--------------------------------------------------------------------------------------
//! Stops the system and shuts down the threads
//!
//--------------------------------------------------------------------------------------
void
DynamicEC::Stop()
{
  eos_static_info("stop");
  mThread.join();
  mThread2.join();
  mThread3.join();
}

void
DynamicEC::createFilesOneTimeThread(ThreadAssistant& assistant)
{
  //Wait in order to have the system booted
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  //This is for creating a few files in the eos system and it can be used for what ever number if it is changed
  //It will be run in a seperated thread for the system in order to make this work fast for the start purpose of the system
  assistant.wait_for(std::chrono::seconds(mWaitTime.load()));
  eos_static_debug("starting the creation of files.");
  createFilesOneTime();
}

void
DynamicEC::createFilesOneTime()
{
  //This is for creating a few files in the eos system and it can be used for what ever number if it is changed
  //It will be run in a seperated thread for the system in order to make this work fast for the start purpose of the system.
  for (int i = 0; i < 10; i++) {
    XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
    XrdCl::File file;
    XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update |
                                          XrdCl::OpenFlags::Delete;
    XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                               XrdCl::Access::UX;
    std::string helperUrl = "root://localhost//eos/testarea/dynec/rawfile";
    std::string url = helperUrl + std::to_string(i) + ".xrdcl";
    //the last number is for how many seconds we wait after each creation, have to be 5 normally.
    XrdCl::XRootDStatus status = file.Open(url, targetFlags, mode, 1);

    if (!status.IsOK()) {
      eos_static_info("error=%s", status.ToStr().c_str());
      eos_static_info("exit 1");
      eos_static_info("Here it is %d", status.IsOK());
    } else {
      std::string diskserverurl;
      file.GetProperty("LastURL", diskserverurl);
      std::cout << "[ diskserver ] : " << diskserverurl << std::endl;
      char buffer[2];
      buffer[0] = 1;
      buffer[1] = 2;
      //change the offset in order to change the size of the file, when this is needed for testing the system.
      off_t offset = 9999998;
      size_t length = 2;
      // write 2 bytes with 5s timeout - synchronous !!!
      status = file.Write(offset, length, buffer, 5);

      if (!status.IsOK()) {
        eos_static_info("exit 2");
      }

      //assistant.wait_for(std::chrono::seconds(2));
      status = file.Close(12);

      if (!status.IsOK()) {
        eos_static_info("exit 3");
      }
    }
  }
}

//--------------------------------------------------------------------------------------
//! Destructor
//!
//--------------------------------------------------------------------------------------
DynamicEC::~DynamicEC()
{
  Stop();
}

//--------------------------------------------------------------------------------------
//! Set the max size for the map to grow to
//!
//! @param mSizeForMapMax the size that can be put into to map before it will stop adding to the map
//--------------------------------------------------------------------------------------
void
DynamicEC::setSizeForMap(uint64_t mapSize)
{
  mSizeForMapMax = mapSize;
}

//--------------------------------------------------------------------------------------
//! Get the max size for the maps files to grow to, the map is the reduction map
//!
//! @param mSizeForMapMax the size that can be put into to map before it will stop adding to the map
//--------------------------------------------------------------------------------------
uint64_t
DynamicEC::getSizeForMap()
{
  return mSizeForMapMax.load();
}

//--------------------------------------------------------------------------------------
//! Set the sleep time when all data have been scanned
//!
//! @param mSleepWhenDone the time in seconds that the scanning will sleep, when the whole systems data have been scanned
//--------------------------------------------------------------------------------------
void
DynamicEC::setSleepWhenDone(uint64_t sleepWhenDone)
{
  //test
  //mThread3.AssistedThread.interrupt();
  mSleepWhenDone = sleepWhenDone;
}

//--------------------------------------------------------------------------------------
//! Get the sleep time when all data have been scanned
//!
//! @param mSleepWhenDone the time in seconds that the scanning will sleep, when the whole systems data have been scanned
//--------------------------------------------------------------------------------------
uint64_t
DynamicEC::getSleepWhenDone()
{
  return mSleepWhenDone.load();
}

//--------------------------------------------------------------------------------------
//! Set the sleep time when the cleanup map is full
//!
//! @param mSleepWhenFull the sleep time in seconds when there is enough file size in the map
//--------------------------------------------------------------------------------------
void
DynamicEC::setSleepWhenFull(uint64_t sleepWhenFull)
{
  mSleepWhenFull = sleepWhenFull;
}

//--------------------------------------------------------------------------------------
//! Get the sleep time when the cleanup map is full
//!
//! @param mSleepWhenFull the sleep time in seconds when there is enough file size in the map
//--------------------------------------------------------------------------------------
uint64_t
DynamicEC::getSleepWhenFull()
{
  return mSleepWhenFull.load();
}

//--------------------------------------------------------------------------------------
//! For turning the test off, the system will stop the test
//!
//! @param mTestEnabel the bool to set the test to off.
//--------------------------------------------------------------------------------------
void
DynamicEC::setTest(bool onOff)
{
  mTestEnabel = onOff;
}

//---------------------------------------------------------------------------------------
//! In order to see if the test is on
//!
//! @param mTestEnabel gives the mTestEnabel back in order to see if it is on.
//---------------------------------------------------------------------------------------
bool
DynamicEC::getTest()
{
  return mTestEnabel;
}

//---------------------------------------------------------------------------------------
//! Sets the time for the cleanup to wait each time
//!
//! @param mWaitTime set the time in seconds for the scans and cleanups to wait in seconds
//---------------------------------------------------------------------------------------
void
DynamicEC::setWaitTime(int wait)
{
  if (wait >= 0) {
    mWaitTime = wait;
  }
}

//----------------------------------------------------------------------------------------
//! Returns the wiattime in second
//!
//! @param mWaitTime return the wait time in second
//----------------------------------------------------------------------------------------
int
DynamicEC::getWaitTime()
{
  return mWaitTime.load();
}

//-----------------------------------------------------------------------------------------
//! Setup the min threshold for how much the system will try cleanup from the store
//!
//! @param mMaxThreshold the threshold for when the cleanup starts.
//! This have to be above  the new min or equal
//! @param mMinThreshold the threshold for how much of the data wanted to be reduced.
//-----------------------------------------------------------------------------------------
void
DynamicEC::setMinThresHold(double thres)
{
  if (thres > 0)
    if (thres <= mMaxThresHold) {
      mMinThresHold = thres;
    }
}

//------------------------------------------------------------------------------------------
//! return min threshold
//!
//! @param mMinThreshold the theshold for how much the system need to reduce of space.
//------------------------------------------------------------------------------------------
double
DynamicEC::getMinThresHold()
{
  return mMinThresHold;
}

//-------------------------------------------------------------------------------------------
//! Set the max threshold for when the system will start to clean up
//!
//! @param mMinThreshold the threshold for when is the threshold for how much the system will try to reduce the storage too.
//! @param mMaxThreshold is from the the system starts to reduce the space, and the minimum have to be the same or lower
//-------------------------------------------------------------------------------------------
void
DynamicEC::setMaxThresHold(double thres)
{
  if (thres < 100) {
    if (thres >= mMinThresHold) {
      mMaxThresHold = thres;
    }
  }
}

//-------------------------------------------------------------------------------------------
//! Return the max threshold
//!
//! @param mMaxThreshold returns the max threshold in order to when the system starts to cleanup in the space.
//-------------------------------------------------------------------------------------------
double
DynamicEC::getMaxThresHold()
{
  return mMaxThresHold;
}

//-------------------------------------------------------------------------------------------
//! Set the time for how old a file can be in order to be reduced
//!
//! @param mAge the age for how old the files will have to be in order to be reduced.
//-------------------------------------------------------------------------------------------
void
DynamicEC::setAgeFromWhenToDelete(uint64_t timeFrom)
{
  mAge = timeFrom;
}

//-------------------------------------------------------------------------------------------
//! Return the age for the cleanuped files
//!
//! @param mAge the age for how old the files will have to be in order to be reduced
//-------------------------------------------------------------------------------------------
uint64_t
DynamicEC::getAgeFromWhenToDelete()
{
  return mAge;
}

//-------------------------------------------------------------------------------------------
//! Set the size for when the cleanup will reduce the file,
//! if the size of the file is below, it will be skipped
//!
//! @param the size for for when the file will be reduced.
//-------------------------------------------------------------------------------------------
void
DynamicEC::setMinForDeletion(uint64_t size)
{
  mSizeMinForDeletion = size;
}

//-------------------------------------------------------------------------------------------
//! Return the size for the minimum for deletion
//!
//! @param mSizeMinForDeletion the size for for when the file will be reduced.
//-------------------------------------------------------------------------------------------
uint64_t
DynamicEC::getMinForDeletion()
{
  return mSizeMinForDeletion;
}

//-------------------------------------------------------------------------------------------
//! Test files in order to make GTests
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::fillFiles()
{
  fprintf(stderr, "Filled 100,000 files \n");
  eos_static_info("%s", "Filled 100,000 files \n");
  srand(1);

  for (int i = 0;  i < 100000; i++) {
    auto file = std::make_shared<DynamicECFile>(i);
    eos::IFileMD::ctime_t timeFile;
    timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
    file->setCTime(timeFile);
    file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kQrain,
                      eos::common::LayoutId::kAdler, 10, eos::common::LayoutId::k1M,
                      eos::common::LayoutId::kNone, 0, 0));

    for (int i = 0; i < 10; i++) {
      file->addLocation(i);
    }

    uint64_t size = (rand() % 49000000000 + 1000000000);
    file->setSize(size);
    mCreatedFileSize += GetSizeOfFile(file);
    mSimulatedFiles[file->getId()] = file;
  }
}

//-------------------------------------------------------------------------------------------
//! Test files in order to make GTests
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::fillFiles(int newFiles)
{
  fprintf(stderr, "Filled %d files up \n", newFiles);
  eos_static_info("%s", "Filled %d files up \n", newFiles);
  srand(0);

  for (int i = 0;  i <  newFiles; i++) {
    auto file = std::make_shared<DynamicECFile>(i + newFiles);
    eos::IFileMD::ctime_t timeFile;
    timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
    file->setCTime(timeFile);
    file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kQrain,
                      eos::common::LayoutId::kAdler, 10, eos::common::LayoutId::k1M,
                      eos::common::LayoutId::kNone, 0, 0));

    for (int i = 0; i < 10; i++) {
      file->addLocation(i);
    }

    uint64_t size = (rand() % 49000000000 + 1000000000);
    file->setSize(size);
    mCreatedFileSize += GetSizeOfFile(file);
    mSimulatedFiles[file->getId()] = file;
  }
}

//-------------------------------------------------------------------------------------------
//! Turns the dynamicec off
//!
//! @param mDynamicOn if it is off the dynamicec is running
//-------------------------------------------------------------------------------------------
Config
DynamicEC::getConfiguration()
{
  Config configForSystem;
  configForSystem.spacename = mSpaceName;
  configForSystem.min_threshold = mMinThresHold;
  configForSystem.max_threshold = mMaxThresHold;
  configForSystem.min_age_for_deletion = mAge;
  configForSystem.min_size_for_deletion = mSizeMinForDeletion;
  configForSystem.onWork = mDynamicOn;
  configForSystem.wait_time = mWaitTime;
  configForSystem.test_enable = mTestEnabel;
  return configForSystem;
}

//-------------------------------------------------------------------------------------------
//! Turns the dynamicec on or off
//!
//! @param mDynamicOn if it is off the dynamicec is running, and false is off
//-------------------------------------------------------------------------------------------
void
DynamicEC::setDynamicEC(bool onOff)
{
  mDynamicOn = onOff;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kRaid5 layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFileWithkRaid5(int stripes, int redundancy,
                                       int excessstripes, uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kRaidDP,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kRaidDP layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFileWithkRaidDP(int stripes, int redundancy,
                                        int excessstripes, uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kRaidDP,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kArchive layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFileWithkArchive(int stripes, int redundancy,
    int excessstripes, uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kReplica,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kReplica layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFileWithkReplica(int stripes, int redundancy,
    int excessstripes, uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kReplica,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kPlain layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFileWithkPlain(int stripes, int redundancy,
                                       int excessstripes, uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kQrain layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFileWithkQrain(int stripes, int redundancy,
                                       int excessstripes, uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kQrain,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kRaid5 layout
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testForSingleFile(int stripes, int redundancy, int excessstripes,
                             uint64_t size)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeFile;
  timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kRaid6,
                    eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, excessstripes, redundancy));

  for (int i = 0; i < stripes + excessstripes; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mStatusFilesMD[file->getId()] = file;
  //This is in the test
}

//-------------------------------------------------------------------------------------------
//! Filling in files for the gtest to test on
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param number the number of files wanted
//! the size is random
//-------------------------------------------------------------------------------------------
void
DynamicEC::testFilesBeignFilled(int stripes, int redundency, int excessstripes,
                                int number)
{
  for (int i = 0;  i <  number; i += 1) {
    mMutexForStatusFilesMD.lock();
//      fprintf(stderr,"This is number file %d \n",i);
    auto file = std::make_shared<DynamicECFile>(i + number);
    eos::IFileMD::ctime_t timeFile;
    timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
    file->setCTime(timeFile);
    file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kRaid6,
                      eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                      eos::common::LayoutId::kNone, excessstripes, redundency));

    for (int i = 0; i < stripes + excessstripes; i++) {
      file->addLocation(i);
    }

//      fprintf(stderr,"This is number for id %" PRId64 "  \n", file->getId());
    uint64_t size = (rand() % 49000000000 + 1000000000);
    file->setSize(size);
    mCreatedFileSize += GetSizeOfFile(file);
    mStatusFilesMD[file->getId()] = file;
    //fprintf(stderr,"This is something :%u the same: %i \n", mStatusFilesMD.size(), mStatusFilesMD.size());
    mMutexForStatusFilesMD.unlock();
  }
}

//-------------------------------------------------------------------------------------------
//! Put in files in order to use for the gtest
//!
//! @param stripes stripes for the file
//! @param redundancy the redundancy stripes
//! @param excessstripes the excessstripes
//! @param number number of files created for the system
//! @param size the size of the file
//-------------------------------------------------------------------------------------------
void
DynamicEC::testFilesBeignFilledCompiledSize(int stripes, int redundancy,
    int excessstripes, int number, uint64_t size)
{
  for (int i = 0;  i <  number; i += 1) {
    mMutexForStatusFilesMD.lock();
//      fprintf(stderr,"This is number file %d \n",i);
    auto file = std::make_shared<DynamicECFile>(i + number);
    eos::IFileMD::ctime_t timeFile;
    timeFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
    file->setCTime(timeFile);
    file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kRaid6,
                      eos::common::LayoutId::kAdler, stripes, eos::common::LayoutId::k1M,
                      eos::common::LayoutId::kNone, excessstripes, redundancy));

    for (int i = 0; i < stripes + excessstripes; i++) {
      file->addLocation(i);
    }

//      fprintf(stderr,"This is number for id %" PRId64 "  \n", file->getId());
    //uint64_t size = size;
    file->setSize(size);
    mCreatedFileSize += GetSizeOfFile(file);
    mStatusFilesMD[file->getId()] = file;
    //fprintf(stderr,"This is something :%u the same: %i \n", mStatusFilesMD.size(), mStatusFilesMD.size());
    mMutexForStatusFilesMD.unlock();
  }
}


void
DynamicEC::fillSingleSmallFile(uint64_t time, uint64_t size, int partitions)
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeForFile;
  timeForFile.tv_sec = time;
  file->setCTime(timeForFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kQrain,
                    eos::common::LayoutId::kAdler, partitions, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, 0, 0));

  for (int i = 0; i < partitions; i++) {
    file->addLocation(i);
  }

  file->setSize(size);
  mCreatedFileSize += GetSizeOfFile(file);
  mSimulatedFiles[file->getId()] = file;
  fprintf(stderr, "Constructor \n");
  eos_static_info("%s", "Constructor from fill single small file\n");
  fprintf(stderr,
          "There is a file with %d: time in seconds, %d: as size in bytes and %d: partitions.\n",
          time, size, partitions);
  eos_static_info("%s",
                  "There is a file with %d: time in seconds, %d: as size in bytes and %d: partitions.\n",
                  time, size, partitions);
}

std::string
DynamicEC::TimeStampCheck(std::string file)
{
  std::string text = "nothing";
  return text;
}

//-------------------------------------------------------------------------------------------
//! Set in file with kRaid5 layout
//!
//! @param mDynamicOn  is set to true as default and false if it is in teststage.
//-------------------------------------------------------------------------------------------
statusForSystem
DynamicEC::SpaceStatus()
{
  //This will return a status for the system, it gives different statuses for the file system
  statusForSystem status;
  eos_static_info("This is the mDynamic %B", mDynamicOn.load());

  if (mDynamicOn.load()) {
    eos_static_info("this is in space status %llu",
                    FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity",
                        false))

    while (FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity",
           false) <= 0) {
      eos_static_info("the space status fails");
      std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }

    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    status.totalSize =
      FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity",
          false);
    status.usedSize = status.totalSize -
                      FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw",
                          false);
    status.deletedSize = mDeletedFileSizeInTotal;

    if ((status.totalSize -
         FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw",
             false)) > ((status.totalSize * mMaxThresHold) / 100)) {
      status.undeletedSize = (status.usedSize - (((status.totalSize *
                              mMinThresHold.load()) /
                              100)))  ;
      eos_static_info("This is the status totalSize %lld", status.totalSize);
      eos_static_info("This is the min threshold %lld", mMinThresHold.load());
      eos_static_info("This is the deleted file size", mDeletedFileSize.load());
    } else {
      status.undeletedSize = 0;
      eos_static_info("This is from the static the undeleted file siye is now 0. ");
    }

    eos_static_info("Status:\i %llu: total size, %llu: used size, %llu: deleted size, %llu: undeleted size.\n",
                    FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity",
                        false),
                    status.usedSize, status.deletedSize, status.undeletedSize);
    return status;
  } else {
    // this have to be updated in order to make some of the new
    status.totalSize = mCreatedFileSize;
    status.usedSize = mCreatedFileSize - mDeletedFileSizeInTotal;
    status.deletedSize = mDeletedFileSize.load();
    //fprintf(stderr,"this is createdFileSize %"PRId64" deletedFileSize %"PRId64" ", createdFileSize, deletedFileSize);

    if ((mCreatedFileSize - mDeletedFileSizeInTotal) > ((mCreatedFileSize*
        mMaxThresHold) /
        100)) {
      status.undeletedSize = ((mCreatedFileSize - mDeletedFileSizeInTotal) - ((((
                                mCreatedFileSize) * mMinThresHold.load()) /
                              100)))  ;
    } else {
      status.undeletedSize = 0;
    }

    fprintf(stderr, "Status:\i %" PRId64 ": total size, %" PRId64 ": used size, %"
            PRId64 ": deleted size, %" PRId64 ": undeleted size.\n", status.totalSize,
            status.usedSize, status.deletedSize, status.undeletedSize);
    return status;
  }
}

//-------------------------------------------------------------------------------------------
//! Return true if the file will have to be reduced
//!
//! @param file the file from the test system to be checked on
//! @param ageOld the age for how old a file has to be in order to be
//-------------------------------------------------------------------------------------------
bool
DynamicEC::DeletionOfFileID(std::shared_ptr<DynamicECFile> file,
                            uint64_t ageOld)
{
  //In this function there will be the function to check if the file is big enough for the system to handle, or to small
  eos::IFileMD::ctime_t time;
  file->getCTime(time);

  if (time.tv_sec < ageOld) {
    if (GetSizeOfFile(file) > mSizeMinForDeletion) {
      return true;
    }
  }

  return false;
}

//-------------------------------------------------------------------------------------------
//! Return true if the file will have to be reduced
//!
//! @param file the file from the test system to be checked on
//! @param ageOld the age for how old a file has to be in order to be
//-------------------------------------------------------------------------------------------
bool
DynamicEC::DeletionOfFileIDMD(std::shared_ptr<eos::IFileMD> file,
                              uint64_t ageOld)
{
  //Checks if the file is big enough to be handled in the system, or if it is to small
  eos::IFileMD::ctime_t time;
  file->getCTime(time);

  if (time.tv_sec < ageOld) {
    if (file->getSize() > mSizeMinForDeletion) {
      return true;
    }
  }

  return false;
}

//-------------------------------------------------------------------------------------------
//! Get the size of the file
//!
//! @param file the file from the test system to be checked on
//-------------------------------------------------------------------------------------------

uint64_t
DynamicEC::GetSizeOfFile(std::shared_ptr<DynamicECFile> file)
{
  return file->getSize() * file->getActualSizeFactor();
}

//-------------------------------------------------------------------------------------------
//! Get the size of the file
//!
//! @param file the file from the test system to be checked on
//-------------------------------------------------------------------------------------------
long double
DynamicEC::TotalSizeInSystemMD(std::shared_ptr<eos::IFileMD> file)
{
  return (file->getSize() * eos::common::LayoutId::GetSizeFactor(
            file->getLayoutId()));
}

//-------------------------------------------------------------------------------------------
//! Get the size of the file this is better for manipulated files and are used in the production code
//!
//! @param file the file from the test system to be checked on
//-------------------------------------------------------------------------------------------
double
DynamicEC::GetRealSizeFactorMD(std::shared_ptr<eos::IFileMD> file)
{
  eos_static_info("This is the top part %lf",
                  1.0 * (eos::common::LayoutId::GetStripeNumber(file->getLayoutId()) + 1 -
                         eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())));
  eos_static_info("This is the next part %lf",
                  1.0 * (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                           file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                           file->getLayoutId()))));
  eos_static_info("This is the last part %lf",
                  1.0 * (eos::common::LayoutId::GetStripeNumber(
                           file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                           file->getLayoutId())));
  return 1.0 * ((((1.0 * eos::common::LayoutId::GetStripeNumber(
                     file->getLayoutId()) + 1) -
                  eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())) +
                 (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                       file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                       file->getLayoutId())))) / (eos::common::LayoutId::GetStripeNumber(
                             file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                             file->getLayoutId())));
}

//-------------------------------------------------------------------------------------------
//! Reducing any file with the neccesary parameters. This function will remove the excessstripes
//!
//! @param file the file from the system that have a potentiel for being reduced
//-------------------------------------------------------------------------------------------
void
DynamicEC::kReduceMD(std::shared_ptr<eos::IFileMD> file)
{
  uint64_t beforeSize = TotalSizeInSystemMD(file);
  double beforeScale = GetRealSizeFactorMD(file);

  while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
      file->getLayoutId()) + 1))) {
    auto fileId = file->getId();
    file->unlinkLocation(file->getLocations().back());

    //eos_static_info("Locatgion after unlinking %lld", file->getLocations().size());
    //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (gOFS) {
      fprintf(stderr, "This is from print f");
      gOFS->eosView->updateFileStore(file.get());
    }

    //file->removeLocation(file->getLocations().back());
    //gOFS->eosView->updateFileStore(file.get());
    // After update we might have to get the new address
    // file = gOFS->eosFileService->getFileMD(fileId);
    //file = gOFS->eosFileService->getFileMD(fileId);
  }

  eos_static_info("This works for locations %lu", file->getLocations().size());
  mDeletedFileSize += file->getSize() * (beforeScale - GetRealSizeFactorMD(file));
  eos_static_info("\n \n Deleted file size: %lld", mDeletedFileSize.load());
  //printAll();
}

//-------------------------------------------------------------------------------------------
//! The cleanup function in order do the clean up for the files in the reduction map, this will
//! remove the excessstripes in order to reduce the footprint in the system
//!
//! This will also remove the files from the reduction list so they will not be checked again
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::CleanupMD()
{
  if (mDynamicOn) {
    eos_static_info("CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    mSizeToBeDeleted = status.undeletedSize;
    mTimeFromWhenToDelete = time(0);
    eos_static_info(
      "This is where the status have been ran delete to be size is : %lld ",
      status.undeletedSize);
    //if (mDynamicOn)
    {
      status.totalSize =
        FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity",
            false);
      status.usedSize = status.totalSize -
                        FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw",
                            false);
      {
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                                __LINE__, __FILE__);
        eos_static_debug("\n this is the number of files in the system: %lld \n",
                         gOFS->eosFileService->getNumFiles());
      }
    }
    eos_static_info("This is the size to be deleted %lld", mSizeToBeDeleted.load());

    if (mSizeToBeDeleted.load() > 0) {
      std::list<uint64_t> deletionlist;
      mMutexForStatusFilesMD.lock();

      for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
             mStatusFilesMD.begin(); it != mStatusFilesMD.end(); ++it) {
        mSizeInMap -= it->second->getSize();
        eos_static_info("This is the size in the map %lld \n", mSizeInMap.load());

        if (DeletionOfFileIDMD(it->second, mTimeFromWhenToDelete)) {
          {
            kReduceMD(it->second);
            deletionlist.push_front(it->first);
          }
        }

        if (mDeletedFileSize.load() >= mSizeToBeDeleted.load()) {
          eos_static_info("%s",
                          "CleanUp ended with success there was deleted. \n \n \n \n"
                          "There is no stuff left and we went under the limit.");
          break;
        }
      }

      mMutexForStatusFilesMD.unlock();
      std::list<uint64_t>::iterator it;

      for (auto it = deletionlist.begin(); it != deletionlist.end(); ++it) {
        eos_static_info("CleanUp have just been done and now we move the will be removed file : %lld.",
                        mStatusFilesMD.size());
        uint64_t id = *it;
        mMutexForStatusFilesMD.lock();
        mStatusFilesMD.erase(id);
        mMutexForStatusFilesMD.unlock();
        eos_static_info("CleanUp have just been done and now we move the file : %lld.",
                        mStatusFilesMD.size());
      }
    }

    eos_static_info("CleanUp ended without success there was deleted:  %lld bytes, but there should have been deleted :  %llu bytes  \n",
                    mDeletedFileSize.load(), mSizeToBeDeleted.load());
  }

  mDeletedFileSizeInTotal += mDeletedFileSize.load();
  eos_static_info("This is the deleted file size for the system: %lld, and this is for this run: %lld",
                  mDeletedFileSizeInTotal, mDeletedFileSize.load());
  mDeletedFileSize = 0;
}

//-------------------------------------------------------------------------------------------
//! This is the thread which run the cleanup and wait for the wait time, when the cleanup has been done
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::Run(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(mWaitTime.load()));

  while (!assistant.terminationRequested())  {
    if (mDynamicOn.load()) {
      CleanupMD();
    }

wait:
    assistant.wait_for(std::chrono::seconds(mWaitTime.load()));

    if (assistant.terminationRequested()) {
      return;
    }
  }

  eos_static_info("closing the thread");
}

//-------------------------------------------------------------------------------------------
//! This returns the options for the system for the scan
//!
//-------------------------------------------------------------------------------------------
DynamicEC::Options DynamicEC::getOptions()
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  Options opts;
  // Default options
  opts.enabled = false;
  opts.interval = std::chrono::minutes(4 * 60);

  if (FsView::gFsView.mSpaceView.count("default")) {
    if (FsView::gFsView.mSpaceView["default"]->GetConfigMember("inspector") ==
        "on") {
      opts.enabled = true;
    }

    int64_t intv = 0;
    std::string interval =
      FsView::gFsView.mSpaceView["default"]->GetConfigMember("inspector.interval");

    if (!interval.empty()) {
      common::ParseInt64(interval, intv);

      if (intv) {
        opts.interval = std::chrono::seconds(intv);
      }
    }
  }

  return opts;
}




//-------------------------------------------------------------------------------------------
//! This runs the scan from a thread and will put possible files for deletion into a map,
//! where the clean up will reduce it and remove them from the map again
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::performCycleQDBMD(ThreadAssistant& assistant) noexcept
{
  //The start of performCycleQDB, this is code in order to look though the system where this will tjeck the system for potentiel files to be deleted.
  eos_static_info("The fact is that we are looking for the performance scan.");

  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  std::string member = gOFS->mQdbContactDetails.members.toString();
  eos_static_info("member:=%s", member.c_str());
  unsigned long long nfiles_processed;
  nfiles = ndirs = nfiles_processed = 0;
  time_t s_time = time(NULL);
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    nfiles = (unsigned long long) gOFS->eosFileService->getNumFiles();
    ndirs = (unsigned long long) gOFS->eosDirectoryService->getNumContainers();
  }
  Options opts = getOptions();
  uint64_t interval = opts.interval.count();
  FileScanner scanner(*(mQcl.get()));
  time_t c_time = s_time;
  eos_static_debug("This is the scanner valid %d ", scanner.valid());

  while (scanner.valid()) {
    eos_static_debug("runs the scan");
    scanner.next();
    std::string err;
    eos::ns::FileMdProto item;

    if (scanner.getItem(item)) {
      eos_static_info("This is the map that scans");

      if (mTestEnabel) {
        interval = 1;
      }

      std::shared_ptr<eos::QuarkFileMD> fmd = std::make_shared<eos::QuarkFileMD>();
      fmd->initialize(std::move(item));
      fmd->setFileMDSvc(gOFS->eosFileService);
      Process(fmd);
      nfiles_processed++;
      scanned_percent.store(100.0 * nfiles_processed / nfiles,
                            std::memory_order_seq_cst);
      time_t target_time = (1.0 * nfiles_processed / nfiles) * interval;
      time_t is_time = time(NULL) - s_time;
      auto hasTape = fmd->hasLocation(EOS_TAPE_FSID);
      long num2 = fmd->getNumLocation();
      num2 -= hasTape;
      num2 -= (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1);

      if (num2 > 0) {
        eos_static_info("start of map \n \n \n");
        mMutexForStatusFilesMD.lock();
        mStatusFilesMD[fmd->getId()] = fmd;
        mMutexForStatusFilesMD.unlock();
        mSizeInMap += fmd->getSize();
        eos_static_info("This is the size in the map %lld \n", mSizeInMap.load());
        eos_static_info("This is the map %ld \n \n ", mStatusFilesMD.size());
      }

      if (target_time > is_time) {
        uint64_t p_time = target_time - is_time;

        if (p_time > 5) {
          p_time = 5;
        }

        eos_static_debug("is:%lu target:%lu is_t:%lu target_t:%lu interval:%lu - pausing for %lu seconds\n",
                         nfiles_processed, nfiles, is_time, target_time, interval, p_time);
        std::this_thread::sleep_for(std::chrono::seconds(p_time));
      }

      if (assistant.terminationRequested()) {
        return;
      }

      if ((time(NULL) - c_time) > 60) {
        c_time = time(NULL);
        Options opts = getOptions();
        interval = opts.interval.count();

        if (!opts.enabled) {
          // interrupt the scan
          break;
        }

        if (!gOFS->mMaster->IsMaster()) {
          // interrupt the scan
          break;
        }
      }

      //This might have to be in the top to check.
      //if(mSizeToBeDeleted.load() <= mDeletedFileSize.load())
      if (mSizeInMap.load() > mSizeForMapMax) {
        sleep(mSleepWhenFull);
      }
    } else {
      eos_static_info("This is the end, everything has been scanned now");
      {
        std::unique_lock<std::mutex> lck(mtx);

        while (cv.wait_for(lck, std::chrono::seconds(mSleepWhenDone)) ==
               std::cv_status::timeout) {
          eos_static_info("the timer went");
          break;
        }
      }
      //sleep(mSleepWhenDone);
      mSizeInMap = 0;
      mStatusFilesMD.clear();
    }

    if (scanner.hasError(err)) {
      eos_static_err("msg=\"QDB scanner error - interrupting scan\" error=\"%s\"",
                     err.c_str());
      break;
    }
  }

  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> sMutex(mutexScanStats);
  lastScanStats = currentScanStats;
  lastFaultyFiles = currentFaultyFiles;
  timeLastScan = timeCurrentScan;
}

//-------------------------------------------------------------------------------------------
//! This runs the scan from a thread and will put possible files for deletion into a map,
//! where the clean up will reduce it and remove them from the map again
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::RunScan(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(mWaitTime.load()));
  eos_static_info("starting the scan for files");

  while (!assistant.terminationRequested())  {
    if (mDynamicOn.load()) {
      eos_static_info("This is the start of the scan \n \n \n");
      performCycleQDBMD(assistant);
    }

wait:
    assistant.wait_for(std::chrono::seconds(mWaitTime.load()));

    if (assistant.terminationRequested()) {
      return;
    }
  }

  eos_static_info("closing the thread for scanning");
}

//-------------------------------------------------------------------------------------------
//! This will print all the stats from the system
//!
//-------------------------------------------------------------------------------------------
void
DynamicEC::printAll()
{
  eos_static_info("This system has created: %lld bytes, and deletion in to tal is: %lld bytes",
                  mCreatedFileSize, mDeletedFileSizeInTotal);
  eos_static_info("Files is: %lld and directories: %lld", nfiles, ndirs);
  eos_static_info("This it the scanned_percent: %f",
                  scanned_percent.load());
  eos_static_info("Wait time is: %d, on work is: %B",
                  mWaitTime.load(), mDynamicOn.load());
  eos_static_info("age for the files is %lld, size for deletion %lld, time from when to delete %lld",
                  mAge.load(), mSizeMinForDeletion.load(), mTimeFromWhenToDelete.load());
  eos_static_info("max threshold %f, min threshold %f, time is %s, name is %s",
                  mMaxThresHold.load(), mMinThresHold.load(), mTimeStore.c_str(),
                  mSpaceName.c_str());
  eos_static_info("The systems map max size: %lld, the sleep when the cleanup has run though %lld, the sleep when the system is full %lld",
                  mSizeForMapMax.load(), mSleepWhenDone.load(), mSleepWhenFull.load());
}

void
DynamicEC::Process(std::string& filepath)
{
}

void
DynamicEC::Process(std::shared_ptr<eos::IFileMD> fmd)
{
}




EOSMGMNAMESPACE_END

