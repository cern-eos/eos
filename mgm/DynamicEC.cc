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
//test
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
#include "mgm/DynamicCreator.hh"
//#include "mgm/DynamicScanner.hh"
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

#define CACHE_LIFE_TIME 300 // seconds


/*----------------------------------------------------------------------------*/
//EOSMGMNAMESPACE_BEGIN
EOSMGMNAMESPACE_BEGIN
DynamicEC::DynamicEC(const char* spacename, uint64_t ageNew,  uint64_t size,
                     double maxThres,
                     double minThres, bool OnWork, int wait, int securityNew)//bool OnWork
{
  fprintf(stderr, "Constructor \n");
  eos_static_info("%s", "Constructor is running now \n");
  //Thread need to be activated
  age = ageNew;
  mOnWork = OnWork;
  security = securityNew;

  if (OnWork) {
    mThread.reset(&DynamicEC::Run, this);
  }

  if (OnWork) {
    //mThread2.reset(&DynamicEC::createFilesOneTimeThread, this);
  }

  mTestNumber = 0;
  mSpaceName = spacename;
  simulatedFiles.clear();
  deletedFileSize = 0;
  sizeMinForDeletion = size;
  maxThresHold = maxThres;
  minThresHold = minThres;
  createdFileSize = 0;
  sizeToBeDeleted = 0;
  testEnabel = false;
  waitTime = wait;
  mMutexForStatusFiles.lock();
  statusFiles.clear();
  mMutexForStatusFiles.unlock();
  mMutexForStatusFilesMD.lock();
  statusFilesMD.clear();
  mMutexForStatusFilesMD.unlock();
  timeCurrentScan = 0;
  timeLastScan = 0;
  deletedFileSizeInTotal = 0;
  ndirs = 0;
  nfiles = 0;
  mDynamicOn = true;

  if (OnWork) {
    mThread3.reset(&DynamicEC::RunScan, this);
  }
}

std::map<uint64_t, std::shared_ptr<eos::IFileMD>>
    DynamicEC::GetMap()
{
  return statusFilesMD;
}

void
DynamicEC::TestFunction()
{
  printf("this is from test \n");
  //createFilesOneTime();
}

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
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  //This is for creating a few files in the eos system and it can be used for what ever number if it is changed
  //It will be run in a seperated thread for the system in order to make this work fast for the start purpose of the system.
  assistant.wait_for(std::chrono::seconds(waitTime));
  eos_static_debug("starting the creation of files.");
  createFilesOneTime();
}

void
DynamicEC::createFileForTest()
{
}



void
DynamicEC::createFilesOneTime()
{
  //this will be changed in order to make it work for different test and to declass it.
  //--gOFS->WaitUntilNamespaceIsBooted(assistant);
  //This is for creating a few files in the eos system and it can be used for what ever number if it is changed
  //It will be run in a seperated thread for the system in order to make this work fast for the start purpose of the system.
  //--assistant.wait_for(std::chrono::seconds(waitTime));
  //--eos_static_debug("starting the creation of files.");
  for (int i = 0; i < 10; i++) {
    XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
    XrdCl::File file;
    XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update |
                                          XrdCl::OpenFlags::Delete;
    XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                               XrdCl::Access::UX;
    std::string helperUrl = "root://localhost//eos/testarea/dynec/rawfile";
    std::string url = helperUrl + std::to_string(i) + ".xrdcl";
    //the last number is for how many seconds we wait have to be 5 normally.
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


void
DynamicEC::createFiles()
{
  eos_static_info("start creating the file");
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  XrdCl::File file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update |
                                        XrdCl::OpenFlags::Delete;
  // default modes - user can rwx
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                             XrdCl::Access::UX;
  std::string url = "root://localhost//eos/testarea/dynec/rawfile1.xrdcl" ;
  //std::string url = "root://"
  //[root://localhost] |/eos/testarea/dynec/
  // timeout 5s
  XrdCl::XRootDStatus status = file.Open(url, targetFlags, mode, 5);

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
    off_t offset = 999999;
    size_t length = 2;
    status = file.Write(offset, length, buffer, 5);

    if (!status.IsOK()) {
      eos_static_info("exit 2");
    }

    status = file.Close(5);

    if (!status.IsOK()) {
      eos_static_info("exit 3");
    }
  }
}



DynamicEC::~DynamicEC()
{
  Stop();
}

void
DynamicEC::setTestOn()
{
  testEnabel = true;
}

void
DynamicEC::setTestOff()
{
  testEnabel = false;
}

bool
DynamicEC::getTest()
{
  return testEnabel;
}

void
DynamicEC::setWaitTime(int wait)
{
  if (wait >= 0) {
    waitTime = wait;
  }
}

int
DynamicEC::getWaitTime()
{
  return waitTime;
}

void
DynamicEC::setMinThresHold(double thres)
{
  if (thres > 0)
    if (thres <= maxThresHold) {
      minThresHold = thres;
    }
}

double
DynamicEC::getMinThresHold()
{
  return minThresHold;
}

void
DynamicEC::setMaxThresHold(double thres)
{
  if (thres < 100) {
    if (thres >= minThresHold) {
      maxThresHold = thres;
    }
  }
}


double
DynamicEC::getMaxThresHold()
{
  return maxThresHold;
}


void
DynamicEC::setAgeFromWhenToDelete(uint64_t timeFrom)
{
  age = timeFrom;
}

uint64_t
DynamicEC::getAgeFromWhenToDelete()
{
  return age;
}


void
DynamicEC::setMinForDeletion(uint64_t size)
{
  sizeMinForDeletion = size;
}

uint64_t
DynamicEC::getMinForDeletion()
{
  return sizeMinForDeletion;
}

void
DynamicEC::setSecurity(int sec)
{
  security = sec;
}

int
DynamicEC::getSecurity()
{
  return security;
}

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
    createdFileSize += GetSizeOfFile(file);
    simulatedFiles[file->getId()] = file;
  }
}

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
    createdFileSize += GetSizeOfFile(file);
    simulatedFiles[file->getId()] = file;
  }
}

void
DynamicEC::testForSpaceCmd()
{
  mDynamicOn = true;
  eos_static_info("This is for the test in order to work %d", mDynamicOn.load());
}

void
DynamicEC::testForSpaceCmd2()
{
  mDynamicOn = false;
  eos_static_info("This is for the test 2 %d", mDynamicOn.load());
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
}

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
  createdFileSize += GetSizeOfFile(file);
  statusFilesMD[file->getId()] = file;
  //This is in the test
}

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
    createdFileSize += GetSizeOfFile(file);
    statusFilesMD[file->getId()] = file;
    //fprintf(stderr,"This is something :%u the same: %i \n", statusFilesMD.size(), statusFilesMD.size());
    mMutexForStatusFilesMD.unlock();
  }
}

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
    createdFileSize += GetSizeOfFile(file);
    statusFilesMD[file->getId()] = file;
    //fprintf(stderr,"This is something :%u the same: %i \n", statusFilesMD.size(), statusFilesMD.size());
    mMutexForStatusFilesMD.unlock();
  }
}

void
DynamicEC::fillSingleFile()
{
  auto file = std::make_shared<DynamicECFile>(0);
  eos::IFileMD::ctime_t timeForFile;
  timeForFile.tv_sec = (rand() % 31556926 + (time(0) - 31556926));
  file->setCTime(timeForFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kRaid6,
                    eos::common::LayoutId::kAdler, 8, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, 2, 2));

  for (int i = 0; i < 10; i++) {
    file->addLocation(i);
  }

  uint64_t size = (rand() % 49000000000 + 1000000000);
  file->setSize(size);
  createdFileSize += GetSizeOfFile(file);
  simulatedFiles[file->getId()] = file;
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
  createdFileSize += GetSizeOfFile(file);
  simulatedFiles[file->getId()] = file;
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

statusForSystem
DynamicEC::SpaceStatus()
{
  statusForSystem status;

  if (mOnWork) {
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
    status.deletedSize = deletedFileSizeInTotal;

    if ((status.totalSize -
         FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw",
             false)) > ((status.totalSize * maxThresHold) / 100)) {
      status.undeletedSize = (status.usedSize - (((status.totalSize *
                              minThresHold.load()) /
                              100)))  ;
      eos_static_info("This is the status totalSize %lld", status.totalSize);
      eos_static_info("This is the min threshold %lld", minThresHold.load());
      eos_static_info("This is the deleted file size", deletedFileSize);
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
    status.totalSize = createdFileSize;
    status.usedSize = createdFileSize - deletedFileSizeInTotal;
    status.deletedSize = deletedFileSize;
    //fprintf(stderr,"this is createdFileSize %"PRId64" deletedFileSize %"PRId64" ", createdFileSize, deletedFileSize);

    if ((createdFileSize - deletedFileSizeInTotal) > ((createdFileSize *
        maxThresHold) /
        100)) {
      status.undeletedSize = ((createdFileSize - deletedFileSizeInTotal) - ((((
                                createdFileSize) * minThresHold.load()) /
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

bool
DynamicEC::DeletionOfFileID(std::shared_ptr<DynamicECFile> file,
                            uint64_t ageOld)
{
  //In this function there will be the function to check if the file is big enough for the system to handle, or to small
  eos::IFileMD::ctime_t time;
  file->getCTime(time);

  if (time.tv_sec < ageOld) {
    if (GetSizeOfFile(file) > sizeMinForDeletion) {
      return true;
    }
  }

  return false;
}

bool
DynamicEC::DeletionOfFileIDForGenerelFile(std::shared_ptr<eos::QuarkFileMD>
    file, uint64_t ageOld)
{
  eos::IFileMD::ctime_t time;
  file->getCTime(time);
  //eos_static_info("This is the time %lld", ageOld);
  //eos_static_info("This is the other time %lld", time.tv_sec);

  if (time.tv_sec < ageOld) {
    {
      return true;
    }
  }

  return false;
}

bool
DynamicEC::DeletionOfFileIDMD(std::shared_ptr<eos::IFileMD> file,
                              uint64_t ageOld)
{
  eos::IFileMD::ctime_t time;
  file->getCTime(time);
  //eos_static_info("This is the time %lld", ageOld);
  //eos_static_info("This is the other time %lld", time.tv_sec);

  if (time.tv_sec < ageOld) {
    // This is in order to use the min size for what to delete.
    if (file->getSize() > sizeMinForDeletion) {
      return true;
    }
  }

  return false;
}


uint64_t
DynamicEC::GetSizeOfFile(std::shared_ptr<DynamicECFile> file)
{
  return file->getSize() * file->getActualSizeFactor();
}



long double
DynamicEC::TotalSizeInSystem(std::shared_ptr<eos::QuarkFileMD> file)
{
  return (file->getSize() * eos::common::LayoutId::GetSizeFactor(
            file->getLayoutId()));
}

long double
DynamicEC::TotalSizeInSystemMD(std::shared_ptr<eos::IFileMD> file)
{
  return (file->getSize() * eos::common::LayoutId::GetSizeFactor(
            file->getLayoutId()));
}

double
DynamicEC::GetRealSizeFactor(std::shared_ptr<eos::QuarkFileMD> file)
{
  eos_static_info("This is the top part %lf \n",
                  1.0 * (eos::common::LayoutId::GetStripeNumber(file->getLayoutId()) + 1 -
                         eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())));
  eos_static_info("This is the next part %lf \n",
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

double
DynamicEC::GetRealSizeFactorMD(std::shared_ptr<eos::IFileMD> file)
{
  /*
  eos_static_info("This is stripenumber %lf",1.0 * eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));
  eos_static_info("This is redundancystripenumber %lf",1.0 * eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
  eos_static_info("This is the layout %lf ", 1.0 * eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
  eos_static_info("This is the redundency %lf", 1.0 * eos::common::LayoutId::GetRedundancy(file->getLayoutId(),file->getLocations().size()));
    eos_static_info("This is excessStripeNumber %lu", eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
  eos_static_info("This works for locations %lu",file->getLocations().size());
  */
  //static unsigned long
  //GetRedundancy(unsigned long layout, unsigned long locations)
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
  /*
            fprintf(stderr,"This is stripenumber %lf \n",1.0 * eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));
    fprintf(stderr,"This is redundancystripenumber %lf \n",1.0 * eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
    fprintf(stderr,"This is the layout %lf \n", 1.0 * eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
    fprintf(stderr,"This is the redundency %lf \n", 1.0 * eos::common::LayoutId::GetRedundancy(file->getLayoutId(),file->getLocations().size()));
    fprintf(stderr,"This is excessStripeNumber %lu \n", eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
    fprintf(stderr,"This works for locations %lu \n",file->getLocations().size());
  */
  /*
    fprintf(stderr,"This is the top part %lf \n",
                    1.0 * (eos::common::LayoutId::GetStripeNumber(file->getLayoutId()) + 1 -
                           eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())));
    fprintf(stderr,"This is the next part %lf \n",
                    1.0 * (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                             file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                             file->getLayoutId()))));

    fprintf(stderr,"This is the last part %lf \n",
                    1.0 * (eos::common::LayoutId::GetStripeNumber(
                             file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                             file->getLayoutId())));
  */
  return 1.0 * ((((1.0 * eos::common::LayoutId::GetStripeNumber(
                     file->getLayoutId()) + 1) -
                  eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())) +
                 (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                       file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                       file->getLayoutId())))) / (eos::common::LayoutId::GetStripeNumber(
                             file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                             file->getLayoutId())));
}


void
DynamicEC::SingleDeletion(std::shared_ptr<DynamicECFile> file)
{
  simulatedFiles.erase(file->getId());
}

void
DynamicEC::kRaid6(std::shared_ptr<eos::QuarkFileMD>file)
{
  uint64_t beforeSize = TotalSizeInSystem(file);
  double beforeScale = GetRealSizeFactor(file);
  eos_static_info("This is the sizefactor after: %lf", GetRealSizeFactor(file));
  eos_static_info("This works for the layout %lu",
                  eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
  eos_static_info("This works for locations %lu", file->getLocations().size());
  eos_static_info("This is reduncancyStripenumber %lu",
                  eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
  eos_static_info("This is excessStripeNumber %lu",
                  eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
  eos_static_info("This is the stripenumber %lld",
                  eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));

  while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
      file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(
                                          file->getLayoutId()) + security)) {
    eos_static_info("This is from deletion");
    auto fileId = file->getId();
    //eos_static_info("This is the sizefactor before: %f",GetRealSizeFactor(file));
    //eos_static_info("This is the size %lld", file->getSize());
    //eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
    file->unlinkLocation(file->getLocations().back());
    //eos_static_info("Locatgion after unlinking %lld", file->getLocations().size());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (gOFS) {
      eos_static_info("this is from gofs");
      //gOFS->eosView->updateFileStore(file.get());
    }

    gOFS->eosView->updateFileStore(file.get());
    // After update we might have to get the new address
    // file = gOFS->eosFileService->getFileMD(fileId);
//file = gOFS->eosFileService->getFileMD(fileId);
  }

  eos_static_info("This works for locations %lu", file->getLocations().size());
  deletedFileSize += file->getSize() * (beforeScale - GetRealSizeFactor(file));
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
  //printAll();
}

void
DynamicEC::kReduce(std::shared_ptr<eos::QuarkFileMD> file)
{
  uint64_t beforeSize = TotalSizeInSystem(file);
  double beforeScale = GetRealSizeFactor(file);

  while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
      file->getLayoutId()) + 1))) {
    //eos_
    //eos_static_info("This is the sizefactor before: %f",GetRealSizeFactor(file));
    //eos_static_info("This is the size %lld", file->getSize());
    //eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
    file->unlinkLocation(file->getLocations().back());
    //eos_static_info("Locatgion after unlinking %lld", file->getLocations().size());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (gOFS) {
      gOFS->eosView->updateFileStore(file.get());
    }

    eos_static_info("This is the sizefactor after: %lf", GetRealSizeFactor(file));
    eos_static_info("This works for the layout %lu",
                    eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
    eos_static_info("This works for locations %lu", file->getLocations().size());
    eos_static_info("This is reduncancyStripenumber %lu",
                    eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
    eos_static_info("This is excessStripeNumber %lu",
                    eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
    eos_static_info("This is the stripenumber %lld",
                    eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));
  }

  deletedFileSize += file->getSize() * (beforeScale - GetRealSizeFactor(file));
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
}

void
DynamicEC::kRaid6T(std::shared_ptr<DynamicECFile> file)
{
  //put this into work again in order to make the system work for test.
  //uint64_t beforeSize = TotalSizeInSystem(file);
  //double beforeScale = GetRealSizeFactor(file);
  while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
      file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(
                                          file->getLayoutId()) + security)) {
    //eos_static_info("This is the sizefactor before: %f",GetRealSizeFactor(file));
    //eos_static_info("This is the size %lld", file->getSize());
    //eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
    file->unlinkLocation(file->getLocations().back());
    //eos_static_info("Locatgion after unlinking %lld", file->getLocations().size());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (gOFS) {
      gOFS->eosView->updateFileStore(file.get());
    }

    //eos_static_info("This is the sizefactor after: %lf", GetRealSizeFactor(file));
    //eos_static_info("This works for the layout %lu",eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
    //eos_static_info("This works for locations %lu",file->getLocations().size());
    //eos_static_info("This is reduncancyStripenumber %lu", eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
    //eos_static_info("This is excessStripeNumber %lu", eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
  }

  //deletedFileSize += file->getSize() * (beforeScale - GetRealSizeFactor(file));
  deletedFileSize = 13;
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
  //printAll();
}


void
DynamicEC::kQrainReduction(std::shared_ptr<DynamicECFile> file)
{
  uint64_t beforeSize = GetSizeOfFile(file);
  {
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex, __FUNCTION__,
                                        __LINE__, __FILE__);

    while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
        file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(
                                            file->getLayoutId()) + 2)) {
      file->unlinkLocation(file->getLocations().back());

      if (gOFS) {
        gOFS->eosView->updateFileStore(file.get());
      }
    }
  }
  deletedFileSize += (beforeSize - GetSizeOfFile(file));
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
}


void
DynamicEC::kReduceMD(std::shared_ptr<eos::IFileMD> file)
{
  uint64_t beforeSize = TotalSizeInSystemMD(file);
  double beforeScale = GetRealSizeFactorMD(file);

  /*
      //eos_static_info("This is the sizefactor after: %lf", GetRealSizeFactorMD(file));
      eos_static_info("This works for the layout %lu",eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
      eos_static_info("This works for locations %lu",file->getLocations().size());
      eos_static_info("This is reduncancyStripenumber %lu", eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
      eos_static_info("This is excessStripeNumber %lu", eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
      eos_static_info("This is the stripenumber %lld", eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));

      fprintf(stderr,"This is the sizefactor after: %lf \n", GetRealSizeFactorMD(file));
      fprintf(stderr,"This works for the layout %lu \n",eos::common::LayoutId::GetLayoutType(file->getLayoutId()));
      fprintf(stderr,"This works for locations %lu \n",file->getLocations().size());
      fprintf(stderr,"This is reduncancyStripenumber %lu \n", eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
      fprintf(stderr,"This is excessStripeNumber %lu \n", eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));
      fprintf(stderr,"This is the stripenumber %lld \n", eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));
  */

  while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
      file->getLayoutId()) + 1))) {
    //eos_static_info("This is from deletion");
    auto fileId = file->getId();
    //eos_static_info("This is the sizefactor before: %f",GetRealSizeFactor(file));
    //eos_static_info("This is the size %lld", file->getSize());
    //eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
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
  deletedFileSize += file->getSize() * (beforeScale - GetRealSizeFactorMD(file));
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
  //printAll();
}


std::uint64_t
DynamicEC::getFileSizeBytes(const IFileMD::id_t fid)
{
  try {
    // Prefetch before taking lock because metadata may not be in memory
    Prefetcher::prefetchFileMDAndWait(gOFS->eosView, fid);
  } catch (std::exception& ex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid << ": prefetchFileMDAndWait() failed: "
        << ex.what();
    throw FailedToGetFileSize(msg.str());
  } catch (...) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid <<
        ": prefetchFileMDAndWait() failed: Unknown exception";
    throw FailedToGetFileSize(msg.str());
  }

  common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  std::shared_ptr<eos::IFileMD> fmd;

  try {
    fmd = gOFS->eosFileService->getFileMD(fid);
  } catch (std::exception& ex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid << ": getFileMD() failed: " << ex.what();
    throw FailedToGetFileSize(msg.str());
  } catch (...) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid <<
        ": getFileMD() failed: Unknown exception";
    throw FailedToGetFileSize(msg.str());
  }

  if (nullptr == fmd) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid << ": getFileMD() returned nullptr";
    throw FailedToGetFileSize(msg.str());
  }

  std::uint64_t fileSizeBytes = 0;

  try {
    fileSizeBytes = fmd->getSize();
  } catch (std::exception& ex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid << ": getSize() failed: " << ex.what();
    throw FailedToGetFileSize(msg.str());
  } catch (...) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid <<
        ": getSize() failed: Unknown exception";
    throw FailedToGetFileSize(msg.str());
  }

  IContainerMD::id_t containerId = 0;

  try {
    containerId = fmd->getContainerId();
  } catch (std::exception& ex) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid << ": getContainerId() failed: " <<
        ex.what();
    throw FailedToGetFileSize(msg.str());
  } catch (...) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid <<
        ": getContainerId() failed: Unknown exception";
    throw FailedToGetFileSize(msg.str());
  }

  // A file scheduled for deletion has a container ID of 0
  if (0 == containerId) {
    std::ostringstream msg;
    msg << __FUNCTION__ << ": fid=" << fid <<
        ": File has been scheduled for deletion";
    throw FailedToGetFileSize(msg.str());
  }

  return fileSizeBytes;
}

void
DynamicEC::CleanupMD()
{
  //if (mOnWork)
  {
    eos_static_info("CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    sizeToBeDeleted = status.undeletedSize;
    //fprintf(stderr, "This is for the size to be deleted %"PRId64" in bytes", sizeToBeDeleted);
    timeFromWhenToDelete = time(0);
    eos_static_info(
      "This is where the status have been ran delete to be size is : %lld ",
      status.undeletedSize);

    if (mOnWork) {
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

    eos_static_info("This is the size to be deleted %lld", sizeToBeDeleted);

    if (sizeToBeDeleted > 0) {
      std::list<uint64_t> deletionlist;
      mMutexForStatusFilesMD.lock();

      for (std::map<uint64_t, std::shared_ptr<eos::IFileMD>>::iterator it =
             statusFilesMD.begin(); it != statusFilesMD.end(); ++it) {
        if (DeletionOfFileIDMD(it->second, timeFromWhenToDelete)) {
          //if (eos::common::LayoutId::GetLayoutType(it->second->getLayoutId()) ==
          //eos::common::LayoutId::kRaid6)
          {
            kReduceMD(it->second);
            deletionlist.push_front(it->first);
          }
        }

        //    fprintf(stderr,"This is the size to be deleted %"PRId64" \n", sizeToBeDeleted);
        //  fprintf(stderr,"This is the deleted size       %"PRId64" \n", deletedFileSize );
        if (deletedFileSize >= sizeToBeDeleted) {
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
                        statusFilesMD.size());
        uint64_t id = *it;
        mMutexForStatusFilesMD.lock();
        statusFilesMD.erase(id);
        mMutexForStatusFilesMD.unlock();
        eos_static_info("CleanUp have just been done and now we move the file : %lld.",
                        statusFilesMD.size());
      }
    }

    eos_static_info("CleanUp ended without success there was deleted:  %lld bytes, but there should have been deleted :  %llu bytes  \n",
                    deletedFileSize, sizeToBeDeleted);
  }
  deletedFileSizeInTotal += deletedFileSize;
  eos_static_info("This is the deleted file size for the system: %lld, and this is for this run: %lld",
                  deletedFileSizeInTotal, deletedFileSize);
  deletedFileSize = 0;
}

void
DynamicEC::Cleanup()
{
  if (mOnWork) {
    eos_static_info("CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    sizeToBeDeleted = status.undeletedSize;
    timeFromWhenToDelete = time(0);
    eos_static_info(
      "This is where the status have been ran delete to be size is : %lld ",
      status.undeletedSize);
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
    eos_static_info("This is the size to be deleted %lld", sizeToBeDeleted);

    if (sizeToBeDeleted > 0) {
      std::list<uint64_t> deletionlist;
      mMutexForStatusFiles.lock();

      for (std::map<uint64_t, std::shared_ptr<eos::QuarkFileMD>>::iterator it =
             statusFiles.begin(); it != statusFiles.end(); ++it) {
        if (DeletionOfFileIDForGenerelFile(it->second, timeFromWhenToDelete)) {
          if (eos::common::LayoutId::GetLayoutType(it->second->getLayoutId()) ==
              eos::common::LayoutId::kRaid6) {
            kRaid6(it->second);
            //kReduceMD(it->second);
            deletionlist.push_front(it->first);
          }
        }

        if (deletedFileSize > sizeToBeDeleted) {
          eos_static_info("%s",
                          "CleanUp ended with success there was deleted. \n \n \n \n"
                          "There is no stuff left and we went under the limit.");
          break;
        }
      }

      mMutexForStatusFiles.unlock();
      std::list<uint64_t>::iterator it;

      for (auto it = deletionlist.begin(); it != deletionlist.end(); ++it) {
        eos_static_info("CleanUp have just been done and now we move the will be removed file : %lld.",
                        statusFiles.size());
        uint64_t id = *it;
        mMutexForStatusFiles.lock();
        statusFiles.erase(id);
        mMutexForStatusFiles.unlock();
        eos_static_info("CleanUp have just been done and now we move the file : %lld.",
                        statusFiles.size());
      }
    }

    eos_static_info("CleanUp ended without success there was deleted:  %lld bytes, but there should have been deleted :  %llu bytes  \n",
                    deletedFileSize, sizeToBeDeleted);
  } else {
    fprintf(stderr, "CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    sizeToBeDeleted = status.undeletedSize;
    timeFromWhenToDelete =  time(0) - age;

    if (sizeToBeDeleted > 0) {
      for (int i = 0;  i < simulatedFiles.size(); i++) {
        auto file = simulatedFiles[i];

        if (DeletionOfFileID(simulatedFiles[i], timeFromWhenToDelete)) {
          if (eos::common::LayoutId::GetLayoutType(simulatedFiles[i]->getLayoutId()) ==
              eos::common::LayoutId::kQrain) {
            kQrainReduction(simulatedFiles[i]);
          }

          if (eos::common::LayoutId::GetLayoutType(simulatedFiles[i]->getLayoutId()) ==
              eos::common::LayoutId::kRaid6) {
            kRaid6T(simulatedFiles[i]);
          }
        }

        if (deletedFileSize > sizeToBeDeleted) {
          fprintf(stderr, "CleanUp ended with success there was deleted:  %" PRId64
                  "  \n", deletedFileSize);
          eos_static_info("%s", "CleanUp ended with success there was deleted:  %" PRId64
                          "  \n", deletedFileSize);
          return;
        }
      }
    }

    /*
    fprintf(stderr, "CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    eos_static_info("\n \n This is from the broken constructer \n \n ");
    sizeToBeDeleted = status.undeletedSize;
    timeFromWhenToDelete =  time(0) - age;
    eos_static_info(
      "This is where the status have been ran delete to be size is : %d ",
      status.undeletedSize);

    if (sizeToBeDeleted > 0) {
      for (int i = 0;  i < simulatedFiles.size(); i++) {
        auto file = simulatedFiles[i];

        if (DeletionOfFileID(simulatedFiles[i], timeFromWhenToDelete)) {
          if (eos::common::LayoutId::GetLayoutType(simulatedFiles[i]->getLayoutId()) ==
              eos::common::LayoutId::kQrain) {
            kQrainReduction(simulatedFiles[i]);
          }
        }

        if (deletedFileSize > sizeToBeDeleted) {
          fprintf(stderr, "CleanUp ended with success there was deleted:  %" PRId64
                  "  \n", deletedFileSize);
          eos_static_info("%s", "CleanUp ended with success there was deleted:  %" PRId64
                          "  \n", deletedFileSize);
          return;
        }
      }
    }

    fprintf(stderr, "CleanUp ended without success there was deleted:  %" PRId64
            " bytes, but there should have been deleted :  %" PRId64 " bytes  \n",
            deletedFileSize, sizeToBeDeleted);
    */
  }

  deletedFileSizeInTotal += deletedFileSize;
  eos_static_info("This is the deleted file size for the system: %lld, and this is for this run: %lld",
                  deletedFileSizeInTotal, deletedFileSize);
  deletedFileSize = 0;
}

void
DynamicEC::Run(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(waitTime));
  assistant.wait_for(std::chrono::seconds(200));

  while (!assistant.terminationRequested())  {
    //if(mDynamicOn.load())
    {
      CleanupMD();
    }
wait:
    //let time pass for a notification
    assistant.wait_for(std::chrono::seconds(waitTime));

    if (assistant.terminationRequested()) {
      return;
    }
  }

  eos_static_info("closing the thread");
}


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

  if (opts.enabled) {
    enable();
    eos_static_debug("file inspector is enabled - interval = %ld seconds",
                     opts.interval.count());
  } else {
    disable();
  }

  return opts;
}



void DynamicEC::performCycleQDB(ThreadAssistant& assistant) noexcept
{
  //The start of performCycleQDB, this is code in order to look though the system where this will tjeck the system for potentiel files to be deleted.
  eos_static_info("The fact is that we are looking for the performance scan.");

  //----------------------------------------------------------------------------
  // Initialize qclient..
  //----------------------------------------------------------------------------
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  std::string member = gOFS->mQdbContactDetails.members.toString();
  eos_static_info("member:=%s", member.c_str());
  //----------------------------------------------------------------------------
  // Start scanning files
  //----------------------------------------------------------------------------
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
      eos_static_debug("This is a new file that comes into the scanning now");
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
        mMutexForStatusFiles.lock();
        statusFiles[fmd->getId()] = fmd;
        mMutexForStatusFiles.unlock();
        eos_static_info("This is the map %ld \n \n ", statusFiles.size());
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
          // interrupt the scan f
          break;
        }
      }
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

void
DynamicEC::performCycleQDBMD(ThreadAssistant& assistant) noexcept
{
  //The start of performCycleQDB, this is code in order to look though the system where this will tjeck the system for potentiel files to be deleted.
  eos_static_info("The fact is that we are looking for the performance scan.");

  //----------------------------------------------------------------------------
  // Initialize qclient..
  //----------------------------------------------------------------------------
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  std::string member = gOFS->mQdbContactDetails.members.toString();
  eos_static_info("member:=%s", member.c_str());
  //----------------------------------------------------------------------------
  // Start scanning files
  //----------------------------------------------------------------------------
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
      //test in order to speed the scan up in the live tests.
      if (testEnabel) {
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
        mMutexForStatusFilesMD.lock();
        statusFilesMD[fmd->getId()] = fmd;
        mMutexForStatusFilesMD.unlock();
        eos_static_info("This is the map %ld \n \n ", statusFilesMD.size());
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
          // interrupt the scan f
          break;
        }
      }
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

void
DynamicEC::RunScan(ThreadAssistant& assistant) noexcept
{
  //waiting for the system to boot.
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(waitTime));
  eos_static_info("starting the scan for files");

  while (!assistant.terminationRequested())  {
    //if(mDynamicOn.load())
    {
      eos_static_info("This is the start of the scan \n \n \n");
      performCycleQDBMD(assistant);
    }
wait:
    //let time pass for a notification
    assistant.wait_for(std::chrono::seconds(waitTime));

    if (assistant.terminationRequested()) {
      return;
    }
  }

  eos_static_info("closing the thread for scanning");
}

void
DynamicEC::printAll()
{
  eos_static_info("This system has created: %lld bytes, and deletion in to tal is: %lld bytes",
                  createdFileSize, deletedFileSizeInTotal);
  eos_static_info("Files is: %lld and directories: %lld", nfiles, ndirs);
  eos_static_info("This it the scanned_percent: %f, enabled: %u",
                  scanned_percent.load(), mEnabled.load());
  eos_static_info("Wait time is: %d, on work is: %B, security stribes %d",
                  waitTime, mOnWork.load(), security.load());
  eos_static_info("age for the files is %lld, size for deletion %lld, time from when to delete %lld",
                  age.load(), sizeMinForDeletion.load(), timeFromWhenToDelete.load());
  eos_static_info("max threshold %f, min threshold %f, time is %s, name is %s",
                  maxThresHold.load(), minThresHold.load(), timeStore.c_str(),
                  mSpaceName.c_str());
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

