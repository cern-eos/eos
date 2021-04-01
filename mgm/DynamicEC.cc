// ----------------------------------------------------------------------
// File: DynamicEC.cc
// Author: Andreas Stoeve - CERN
// ----------------------------------------------------------------------
/////
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
                     double minThres, bool OnWork, int wait)//bool OnWork
{
  fprintf(stderr, "Constructor \n");
  eos_static_info("%s", "Constructor is running now \n");
  //Thread need to be activated
  age = ageNew;
  mOnWork = OnWork;

  if (OnWork) {
    //mThread.reset(&DynamicEC::Run, this);
  }

  if (OnWork) {
    //mThread2.reset(&DynamicEC::createFilesOneTime, this);
  }

  //mThread.stop();
  mSpaceName = spacename;
  simulatedFiles.clear();
  deletedFileSize = 0;
  //time_t seconds;
  //timeFromWhenToDelete =  time(0) - age;
  sizeMinForDeletion = size;
  maxThresHold = maxThres;
  minThresHold = minThres;
  createdFileSize = 0;
  sizeToBeDeleted = 0;
  waitTime = wait;
//mCreator.createFiles();
// DynamicCreator CreateTus();
  //CreateTus.createFiles();
  //mThread2.reset(&CreateTus::Run, this);
  /////This is for the scanner
  statusFiles.clear();
  timeCurrentScan = 0;
  timeLastScan = 0;
  //mThread.reset(&DynamicScanner::performCycleQDB, this);
  ndirs = 0;
  nfiles = 0;

  if (OnWork) {
    mThread3.reset(&DynamicEC::RunScan, this);
  }

  // 0 eos_static_info("And gate %d", (true && false));
  // 0 eos_static_info("And gate %d", (false && true));
  // 1 eos_static_info("And gate %d", (true && true));
  // 0 eos_static_info("And gate %d", (false && false));
  // 1 eos_static_info("And gate %d", (true || false));
  // 1 eos_static_info("And gate %d", (false || true));
  // 1 eos_static_info("And gate %d", (true || true));
  // 0 eos_static_info("And gate %d", (false || false));
  // 1 eos_static_info("And gate %d", (true));
  // 0 eos_static_info("And gate %d", (false));
}



void DynamicEC::TestThreadFunction(ThreadAssistant& assistant) noexcept
{
}

//old constructor
/*
DynamicEC::DynamicEC()
{
  fprintf(stderr,"constructor\n");
  eos_static_info("%s","Constructor");
  mThread.reset(&DynamicEC::CleanUp, this);
  mSpaceName = "";
  simulatedFiles.clear();

  //can't make this problem for  tomorrow
  //auto file = std::make_shared<DynamicECFile>(2);
  //simulatedFiles[0] = file;
}
*/

void
DynamicEC::Stop()
{
  eos_static_info("stop");
  mThread.join();
  mThread2.join();
  mThread3.join();
}

void
DynamicEC::createFilesOneTime(ThreadAssistant& assistant)
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  //This is for creating a few files in the eos system and it can be used for what ever number if it is changed, but this is
  //It will be run in a seperated thread for the system in order to make this work fast for the start purpose of the system.
  assistant.wait_for(std::chrono::seconds(waitTime));
  eos_static_debug("starting the creation of files.");

  for (int i = 0; i < 10; i++) {
    XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
    XrdCl::File file;
    XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update |
                                          XrdCl::OpenFlags::Delete;
    XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                               XrdCl::Access::UX;
    std::string helperUrl = "root://localhost//eos/testarea/dynec/rawfile";
    std::string url = helperUrl + std::to_string(i) + ".xrdcl";
    XrdCl::XRootDStatus status = file.Open(url, targetFlags, mode, 5);

    if (!status.IsOK()) {
      eos_static_info("error=%s", status.ToStr().c_str());
      eos_static_info("exit 1");
      eos_static_info("Here it is %d", status.IsOK());
      /*
       *
       *   eos_static_info("%s",
                        "There is a file with %d: time in seconds, %d: as size in bytes and %d: partitions.\n",
                        time, size, partitions);
       *
       *
       */
    } else {
      std::string diskserverurl;
      file.GetProperty("LastURL", diskserverurl);
      std::cout << "[ diskserver ] : " << diskserverurl << std::endl;
      char buffer[2];
      buffer[0] = 1;
      buffer[1] = 2;
      off_t offset = 0;
      size_t length = 2;
      // write 2 bytes with 5s timeout - synchronous !!!
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
}

void
DynamicEC::createFiles()
{
  // switch timeout resolution to 1s
  eos_static_info("start creating the file");
  //might be put out
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  XrdCl::File file;
  // replace existing file
  //might be put out
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update |
                                        XrdCl::OpenFlags::Delete;
  // default modes - user can rwx
  //might be put out
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                             XrdCl::Access::UX;
  //std::string url="root://home/rawfile.xrdcl" ;
  //std::string url="root://eoshome-a.cern.ch//eos/user/a/astoeve/rawfile.xrdcl" ;
  std::string url = "root://localhost//eos/testarea/dynec/rawfile1.xrdcl" ;
  //std::string url = "root://"
  //[root://localhost] |/eos/testarea/dynec/
  // timeout 5s
  //might be put out
  XrdCl::XRootDStatus status = file.Open(url, targetFlags, mode, 5);

  if (!status.IsOK()) {
    // too bad
    //exit(-1);
    eos_static_info("error=%s", status.ToStr().c_str());
    eos_static_info("exit 1");
    eos_static_info("Here it is %d", status.IsOK());
    /*
     *
     *   eos_static_info("%s",
                      "There is a file with %d: time in seconds, %d: as size in bytes and %d: partitions.\n",
                      time, size, partitions);
     *
     *
     */
  } else {
    std::string diskserverurl;
    file.GetProperty("LastURL", diskserverurl);
    std::cout << "[ diskserver ] : " << diskserverurl << std::endl;
    char buffer[2];
    buffer[0] = 1;
    buffer[1] = 2;
    off_t offset = 0;
    size_t length = 2;
    // write 2 bytes with 5s timeout - synchronous !!!
    status = file.Write(offset, length, buffer, 5);

    if (!status.IsOK()) {
      // too bad again
      //exit(-2);
      eos_static_info("exit 2");
    }

    // give 5s to close the file
    status = file.Close(5);

    if (!status.IsOK()) {
      // too bad again
      //exit(-3);
      eos_static_info("exit 3");
    }
  }
}



DynamicEC::~DynamicEC()
{
  Stop();
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

int
DynamicEC::DummyFunction(int number)
{
  return number;
}

bool
DynamicEC::TrueForAllRequest()
{
  return true;
}

void
DynamicEC::setMinThresHold(double thres)
{
  //make an error message, and can both be the same thress?
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
  //make an error message, and can both thress be the same??
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
  //Find a way of not to get a minus for this one as it will result in another big number
  //Here everything is okay
  sizeMinForDeletion = size;
}

uint64_t
DynamicEC::getMinForDeletion()
{
  return sizeMinForDeletion;
}

void
DynamicEC::fillFiles()
{
  fprintf(stderr, "Filled 100,000 files \n");
  eos_static_info("%s", "Filled 100,000 files \n");
  //fprintf(stderr,"testing for error");
  //This is for some rear logging
  //eos_static_info("Stop");
  //eos_static_debug("No geotags over the average!");
  //eos_static_err("Stop this");
  srand(1);

  for (int i = 0;  i < 100000; i++) {
    auto file = std::make_shared<DynamicECFile>(i);
    //max for the set function is 281.474.976.710.656
    //file->setSize(rand() % 2000000000000 + 1);
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
    //std::cerr << "Loop number" << i << std::endl;
    //std::pair<eos::IFileMD::id_t,std::shared_ptr<eos::IFileMD>> file_forInsert (file->id_t,file);
    //std::pair<eos::DynamicECFile::id_t,std::shared_ptr<eos::IFileMD>> file_forInsert (file->id_t,file);
    //std::pair<const unsigned long int &, std::shared_ptr<eos::DynamicECFile> &> file_forInsert (file->id_t,file);
    //std::pair<std::shared_ptr<eos::DynamicECFile::id_t> &,std::shared_ptr<eos::DynamicECFile> &> file_For_Insert (file->id_t,file);
    //simulatedFiles.in
  }

  // Test for what the current time is;
  //simulatedFiles[1]->setCTimeNow();
  //This is a comment
}

void
DynamicEC::fillFiles(int newFiles)
{
  //fprintf(stderr,"filled 100,000 files");
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
DynamicEC::fillSingleSmallFile(uint64_t time, uint64_t size, int partitions)
{
  auto file = std::make_shared<DynamicECFile>(0);
  //max for the set function is 281.474.976.710.656
  //file->setSize(rand() % 2000000000000 + 1);
  eos::IFileMD::ctime_t timeForFile;
  timeForFile.tv_sec = time;
  file->setCTime(timeForFile);
  file->setLayoutId(eos::common::LayoutId::GetId(eos::common::LayoutId::kQrain,
                    eos::common::LayoutId::kAdler, partitions, eos::common::LayoutId::k1M,
                    eos::common::LayoutId::kNone, 0, 0));

  for (int i = 0; i < partitions; i++) {
    file->addLocation(i);
  }

  //uint64_t size = ( 5000000 );
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


//Look up what timestamp is needed for.
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

  /// this have to be updated in order to
  // If gtest.
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
    status.deletedSize = deletedFileSize;

    //this might have to be - deletedFileSize like in the other one.
    if ((status.totalSize -
         FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw",
             false)) > ((status.totalSize * maxThresHold) / 100)) {
    } else {
      status.undeletedSize = 0;
    }

    eos_static_info("Status:\i %llu: total size, %llu: used size, %llu: deleted size, %llu: undeleted size.\n",
                    FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity",
                        false),
                    status.usedSize, status.deletedSize, status.undeletedSize);
    return status;
  } else {
    status.totalSize = createdFileSize;
    /// have to be updated in order to be the used size from the system and not just the crated minus the deleted.
    status.usedSize = createdFileSize - deletedFileSize;
    status.deletedSize = deletedFileSize;

    /// this is if the used file size is bigger than what is a loud from the total file size in percentage
    if ((createdFileSize - deletedFileSize) > ((createdFileSize * maxThresHold) /
        100)) {
      status.undeletedSize = (createdFileSize - (((createdFileSize * minThresHold) /
                              100)) - deletedFileSize)  ;
    } else {
      status.undeletedSize = 0;
    }

    fprintf(stderr, "Status:\i %" PRId64 ": total size, %" PRId64 ": used size, %"
            PRId64 ": deleted size, %" PRId64 ": undeleted size.\n", status.totalSize,
            status.usedSize, status.deletedSize, status.undeletedSize);
    /*
     *    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    std::uint64_t number = FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw", false);
    eos_static_info("This is some bytes %llu",number);
    std::uint64_t number_another = FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity", false);
    eos_static_info("This is some bytes %llu",number_another);
    eos_static_info("This is after bytes");
     *
     */
    return status;
  }
}

bool
DynamicEC::DeletionOfFileID(std::shared_ptr<DynamicECFile> file,
                            uint64_t ageOld)
{
  //Something with relative timing, for a dynamic time frame.
  //This can depend on the current time minus a year or two years.
  //In this function there will be the function to check if the file is big enough for the system to handle, or to small
  eos::IFileMD::ctime_t time;
  file->getCTime(time);

  //if(time.tv_sec < 161500000000)
  if (time.tv_sec < ageOld) {
    //change this to something with the new function to get the actual and the one for the old one
    // this will have to give the right time for the file.
    if (GetSizeOfFile(file) > sizeMinForDeletion) {
      return true;
    }
  }

  return false;
  /*
  eos::IFileMD::ctime_t time;
  file->getCTime(time);
  if(time.tv_sec < 1613015000000)
  {
    return true;
  }

  return false;
  */
}

bool
DynamicEC::DeletionOfFileIDForGenerelFile(std::shared_ptr<eos::QuarkFileMD>
    file, uint64_t ageOld)
{
  eos::IFileMD::ctime_t time;
  file->getCTime(time);
  eos_static_info("This is the time %lld", ageOld);
  eos_static_info("This is the other time %lld", time.tv_sec);

  if (time.tv_sec < ageOld) {
    ///will have to be put in anyway ----------------------------------------------------------
    //if(GetSizeOfFileForGenerelFile(file) > sizeMinForDeletion)
    {
      return true;
    }
  }

  return false;
}

uint64_t
DynamicEC::GetSizeOfFileForGenerelFile(std::shared_ptr<eos::QuarkFileMD> file)
{
  //this is done but it can be in order to have the size and partisions as well
  return file->getSize();
}

uint64_t
DynamicEC::GetSizeOfFile(std::shared_ptr<DynamicECFile> file)
{
  return file->getSize() * file->getActualSizeFactor();
}

uint64_t
DynamicEC::GetSizeFactor1(std::shared_ptr<DynamicECFile> file)
{
  //Need of the right GetSizeFactor()
  return file->getSize() * eos::common::LayoutId::GetSizeFactor(
           file->getLayoutId());
  ///file->
}

long double
DynamicEC::TotalSizeInSystem(std::shared_ptr<eos::QuarkFileMD> file)
{
  return (file->getSize() * eos::common::LayoutId::GetSizeFactor(
            file->getLayoutId()));
}

double
DynamicEC::GetRealSizeFactor(std::shared_ptr<eos::QuarkFileMD> file)
{
  /* this is for another type of memory with excess stripes being something else.
  if (eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()) ==
      (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                                        file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                                        file->getLayoutId())))) {
    return 1.0 * (((1.0 * (eos::common::LayoutId::GetStripeNumber(
                             file->getLayoutId()) + 1)) /
                   (eos::common::LayoutId::GetStripeNumber(file->getLayoutId()) + 1 -
                    eos::common::LayoutId::GetRedundancyStripeNumber(
                      file->getLayoutId()))) + eos::common::LayoutId::GetExcessStripeNumber(
                    file->getLayoutId()));
  } else if (eos::common::LayoutId::GetExcessStripeNumber(
               file->getLayoutId()) >= (file->getLocations().size() -
                                        (eos::common::LayoutId::GetStripeNumber(file->getLayoutId() + 1 -
                                            eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())))) &&
             (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                   file->getLayoutId() + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                     file->getLayoutId())))) != 0) {
    return 5.0;
  } else if ((file->getLocations().size() -
              (eos::common::LayoutId::GetStripeNumber(file->getLayoutId() + 1 -
                  eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())))) >= 0) {
    return 10.0;
  } else {
    //crap something is wrong :(
    return 0.0;
  }
  */
  return 1.0 * ((eos::common::LayoutId::GetStripeNumber(file->getLayoutId()) + 1 -
                 eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId())) +
                (file->getLocations().size() - (eos::common::LayoutId::GetStripeNumber(
                      file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                      file->getLayoutId()))) / (eos::common::LayoutId::GetStripeNumber(
                            file->getLayoutId()) + 1 - eos::common::LayoutId::GetRedundancyStripeNumber(
                            file->getLayoutId())));
}

void
DynamicEC::SingleDeletion(std::shared_ptr<DynamicECFile> file)
{
  simulatedFiles.erase(file->getId());
  /// delete the file, where the only need to be deleted one instance.
}

void
DynamicEC::kRaid6(std::shared_ptr<eos::QuarkFileMD>file)
{
  //eos_static_info("This is from the deletion of the kRaid6");
  uint64_t beforeSize = TotalSizeInSystem(file);
  //eos_static_info("This is the size before %lf", (file->getSize() * eos::common::LayoutId::GetSizeFactor(
  //         file->getLayoutId())));
  eos_static_info("The file id %x", file->getLayoutId());
  eos_static_info("This is data for the file");
  eos_static_info("this is the stripenumber %lld",
                  eos::common::LayoutId::GetStripeNumber(file->getLayoutId()));
  eos_static_info("This is the redundancynumber %lld",
                  eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()));
  eos_static_info("This is the excessStripeNumber %lld",
                  eos::common::LayoutId::GetExcessStripeNumber(file->getLayoutId()));

  //eos_static_info("This is another strope %lld", eos::common::LayoutId::get)
  if (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
                                        file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(
                                       file->getLayoutId()) + 2)) {
    ///For the final test removing of unlinked locations is needed.
    eos_static_info("This is the sizefactor before: %f",
                    eos::common::LayoutId::GetSizeFactor(file->getLayoutId()));
    eos_static_info("This is the size %lld", file->getSize());
    eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
    file->unlinkLocation(file->getLocations().back());
    eos_static_info("Locatgion after unlinking %lld", file->getLocations().size());
    //remove is not for production
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    //look this up for test
    //eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
    //file->removeLocation(file->getLocations().back());
    //eos_static_info("Locatgion before unlinking %lld", file->getLocations().size());
    if (gOFS) {
      //eos_static_info("\n \n \n This is from the gofs");
      gOFS->eosView->updateFileStore(file.get());
    }

    eos_static_info("This is the sizefactor after: %lf",
                    eos::common::LayoutId::GetSizeFactor(file->getLayoutId()))
  } else {
    eos_static_info("Too few locations %ldd", file->getLocations().size());
  }

  eos_static_info("The file id %x", file->getLayoutId());
  //eos_static_info("This is the size after %lf", (file->getSize() * eos::common::LayoutId::GetSizeFactor(
  //         file->getLayoutId())));
  eos_static_info("This is the amount of locations %lld",
                  file->getLocations().size());
  //file->unlinkLocation(file->getLocations().back());
  //eos_static_info("This is the amount of locations %lld", file->getLocations().size());
  //file->removeLocation(file->getLocations().back());
  //eos_static_info("This is the amount of locations %lld", file->getLocations().size());
  eos_static_info("This is the sizefactor before: %f",
                  eos::common::LayoutId::GetSizeFactor(file->getLayoutId()));
  eos_static_info("This is the size %lld", file->getSize());
  eos_static_info("This is the size before %lld", beforeSize);
  eos_static_info("This is the size after %llf", TotalSizeInSystem(file));
  deletedFileSize += (beforeSize - TotalSizeInSystem(file));
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
  //eos_static_info("This is the amount of locations %lld", file->getLocations().size());
}


void
DynamicEC::kQrainReduction(std::shared_ptr<DynamicECFile> file)
{
  uint64_t beforeSize = GetSizeOfFile(file);
  //int size = file->getLocations().size();
  //Something about stride and RedundancyStripe numbers.
  // delete pari from back.
  //this 7 will have to be something like one less than what there is.
  // This will give you only two redundancystripes left out of four originally.
  //(eos::common::LayoutId::GetStripeNumber(file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(file->getLayoutId()) + 2;
  //eos_static_debug("Couldn't choose any FID to schedule: failedgeotag=%s",
  //               (*over_it).c_str());
  //eos_static_debug("sdf", (*over_it).c_str());
  //try for logging
  //eos_err("Stop");
  {
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex, __FUNCTION__,
                                        __LINE__, __FILE__);

    while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
        file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(
                                            file->getLayoutId()) + 2)) {
      ///For the final test removing of unlinked locations is needed.
      file->unlinkLocation(file->getLocations().back());
      //remove is not for production
      //look this up for test
      //file->removeLocation(file->getLocations().back());

      //rem
      if (gOFS) {
        gOFS->eosView->updateFileStore(file.get());
      }
    }
  }
  deletedFileSize += (beforeSize - GetSizeOfFile(file));
  eos_static_info("\n \n Deleted file size: %lld", deletedFileSize);
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
DynamicEC::Cleanup()
{
  if (mOnWork) {
    eos_static_info("CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    sizeToBeDeleted = status.undeletedSize;
    //this is for some part of testing change it change it to timeFromWhenToDelete = time(0) - age, this is because we look at some of the test.
    //timeFromWhenToDelete =  time(0) - age;
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
    //FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.", lock, subset)
    {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                              __LINE__, __FILE__);
      eos_static_debug("\n this is the number of files in the system: %lld \n",
                       gOFS->eosFileService->getNumFiles());
    }
    //this will have to be turned on to see if there have to be deleted anything
    //if (sizeToBeDeleted > 0)
    {
      for (std::map<uint64_t, std::shared_ptr<eos::QuarkFileMD>>::iterator it =
             statusFiles.begin(); it != statusFiles.end(); ++it) {
        //eos_static_info("this is for the fileId %lld", it->second->getId());
        //eos_static_info("this is the deletion of file id: %d", DeletionOfFileIDForGenerelFile(it->second, timeFromWhenToDelete));
        //eos_static_info("\n \n This is the size for the new file %lld", getFileSizeBytes(it->second->getId()));
        //eos_static_info("this is the other file for the size %lld", it->second->getSize());
        //eos_static_info("Test for true %d", DeletionOfFileIDForGenerelFile(it->second, timeFromWhenToDelete));
        //eos_static_info("Test for true %d", true);
        //eos_static_info("Test for false %d", false);
        if (DeletionOfFileIDForGenerelFile(it->second, timeFromWhenToDelete)) {
          if (eos::common::LayoutId::GetLayoutType(it->second->getLayoutId()) ==
              eos::common::LayoutId::kRaid6) {
            //eos_static_info("This is where we have to reduce the file");
            kRaid6(it->second);
          }
        }
      }

      /*
      for(int i = 0; statusFiles.size(); i++)
      {
        //auto file = statusFiles.begin();
            if (DeletionOfFileID(simulatedFiles[i], timeFromWhenToDelete)) {
              if (eos::common::LayoutId::GetLayoutType(simulatedFiles[i]->getLayoutId()) ==
                  eos::common::LayoutId::kQrain) {
                kQrainReduction(simulatedFiles[i]);
              }
            }


      }
      */
    }
    eos_static_info("CleanUp ended without success there was deleted:  %lld bytes, but there should have been deleted :  %llu bytes  \n",
                    deletedFileSize, sizeToBeDeleted);
  } else {
    //mCreator.createFiles();
    fprintf(stderr, "CleanUp started \n");
    //eos_static_info("%s", "CleanUp started \n");
    statusForSystem status;
    status = SpaceStatus();
    eos_static_info("\n \n This is from the broken constructer \n \n ");
    //file->
    sizeToBeDeleted = status.undeletedSize;
    timeFromWhenToDelete =  time(0) - age;
    eos_static_info(
      "This is where the status have been ran delete to be size is : %d ",
      status.undeletedSize);

    //fprintf(stderr, " what has to be deleted %" PRId64 "\n", sizeToBeDeleted );
    if (sizeToBeDeleted > 0) {
      //fprintf(stderr, "There will be deleted files \n");
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
  }

  //put this to work again
  //test for the writing from the system
  /*
  eos_static_info("this is another try to print %llu", status.undeletedSize);
  eos_static_info("%s","there will be a few tries by now");
  eos_static_info("this is %llu", status.undeletedSize);
  eos_static_info("this is %llu", sizeToBeDeleted);
  eos_static_info("this is %llu", (long long)status.undeletedSize);
  eos_static_info("this is %llu", (long)sizeToBeDeleted);
  */
  /*
  eos_static_info("%s", "CleanUp ended without success there was deleted:  %llu bytes, but there should have been deleted :  %llu bytes  \n",
                  deletedFileSize, sizeToBeDeleted);
  */
  //This have to do something if it deletes and there is more filesize to be deleted
}

void
DynamicEC::Run(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(waitTime));
  //assistant.wait_for(std::chrono::seconds(10));
  eos_static_debug("starting");
  //assistant.wait_for(std::chrono::seconds(30));
  //eos_static_info("Runs the create files again \n");
  //createFiles();
  //eos_static_info("%d this is true",!assistant.terminationRequested());
  //eos_static_info("before while loop");

  //this will tell where to put some of the files if it will have to be here
  //can be put here in order to make it before we try to put it toghter.

  while (!assistant.terminationRequested())  {
    //eos_static_info("will wait 30 seconds");
    //assistant.wait_for(std::chrono::seconds(30));
    //eos_static_info("Runs the create files again \n");
    //createFiles();
    /*
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    std::uint64_t number = FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw", false);
    eos_static_info("This is some bytes %llu",number);
    std::uint64_t number_another = FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity", false);
    eos_static_info("This is some bytes %llu",number_another);
    eos_static_info("This is after bytes");
    */
    Cleanup();
    //std::uint64_t number = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->SumLongLong("stat.statfs.freebytes?configstatus@rw", false);
    //eos_static_info("This is some bytes %llu",number);
    /// What to do when it runs
    ///can have a lock for a timeout, then needs to know where it started.
    // somehow taking different files, and go though them, whis is where the deletion of file id is running
    // if it is need to be deleted, there can be the single deletion, or other deletions depending on the way this file saved, wich can be predicted from the layout
wait:
    //let time pass for a notification
    assistant.wait_for(std::chrono::seconds(waitTime));

    //gOFS->getStats(buff, blen);
    //gOFS->getStats(buff, blen)
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
  //eos_static_info("msg=\"start FileInspector scan on QDB\"");
  //eos_static_info("The start of performCycleQDB");

  //----------------------------------------------------------------------------
  // Initialize qclient..
  //----------------------------------------------------------------------------
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  std::string member = gOFS->mQdbContactDetails.members.toString();
  //eos_static_info(member.c_str());
  eos_static_info("member:=%s", member.c_str());
  //eos_static_info("member:=%s",gOFS->mQdbContactDetails.members.toString());
  //eos_static_info("member:=%s",gOFS->mQdbContactDetails.members.toString().c_str());
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
  //eos_static_info("This is the scanner before validation");
  //eos_static_info("Test for true %d", true);
  //eos_static_info("Test for false %d", false);
  eos_static_debug("This is the scanner valid %d", scanner.valid());

  while (scanner.valid()) {
    eos_static_debug("runs the scan");
    scanner.next();
    std::string err;
    eos::ns::FileMdProto item;
    //eos::common::namespace::FileMD item;
    //FileMD item

    //eos::ns::FileMD item;
    if (scanner.getItem(item)) {
      std::shared_ptr<eos::QuarkFileMD> fmd = std::make_shared<eos::QuarkFileMD>();
      fmd->initialize(std::move(item));
      fmd->setFileMDSvc(gOFS->eosFileService);
      Process(fmd);
      nfiles_processed++;
      scanned_percent.store(100.0 * nfiles_processed / nfiles,
                            std::memory_order_seq_cst);
      time_t target_time = (1.0 * nfiles_processed / nfiles) * interval;
      time_t is_time = time(NULL) - s_time;
      //eos_static_info("This works for the layout %lu",eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()));
      //--eos_static_info("This works for locations %lu",fmd->getLocations().size());
      //--eos_static_info("This works in redundancy %lu", eos::common::LayoutId::GetRedundancy(eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()), fmd->getLocations().size()));
      //--eos_static_info("This is reduncancyStripenumber %lu", eos::common::LayoutId::GetRedundancyStripeNumber(fmd->getLayoutId()));
      //--eos_static_info("This is excessStripeNumber %lu", eos::common::LayoutId::GetExcessStripeNumber(fmd->getLayoutId()));
      //eos_static_info("This is numlocation, %lu", fmd->getNumLocation());
      //auto stripenumber = eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId());
      /*
      signed long numbers = eos::common::LayoutId::GetRedundancy(eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()), fmd->getLocations().size()) - fmd->getLocations().size();
      eos_static_info("This is number for stripenumber %lu", eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()));
      */
      //eos_static_info("This is the extra stribes for security %lu", eos::common::LayoutId::GetRedundancy(eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()), (fmd->getLocations().size()) +1) - eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()));
      auto hasTape = fmd->hasLocation(EOS_TAPE_FSID);
      //eos_static_info("this is test2");
      //still just test to look at it and how to compile a system for the calculation on how to move the see if the file can be reduced.
      //--eos_static_info("This is the location %d", hasTape);
      //eos_static_info("This is a test for the location %d true", true);
      //eos_static_info("This is a test for the location %d false", false);
      //eos_static_info("This is the next stripenumber for extra is %lu", fmd->getNumLocation() - hasTape - eos::common::LayoutId::GetExcessStripeNumber(fmd->getLayoutId()));
      //long numberfortest2 = fmd->getNumLocation();
      //eos_static_info("This is the start of number for test %ld", numberfortest2);
      //numberfortest2 =- hasTape;
      //eos_static_info("This is the start of number for test %ld", numberfortest2);
      //numberfortest2 =- eos::common::LayoutId::GetExcessStripeNumber(fmd->getLayoutId());
      //eos_static_info("This is the start of number for test %ld", numberfortest2);
      //auto numberfortest = fmd->getNumLocation() - hasTape - eos::common::LayoutId::GetExcessStripeNumber(fmd->getLayoutId());
      //eos_static_info("This is the next stripenumber for extra is %lu", numberfortest);
      //eos_static_info("This is a test for numlocation %lu", fmd->getNumLocation());
      //eos_static_info("This is excessstripenumber %lu",eos::common::LayoutId::GetExcessStripeNumber(fmd->getLayoutId()));
      //test for the right one
      //long num = fmd->getNumLocation() - hasTape - eos::common::LayoutId::GetExcessStripeNumber(fmd->getLayoutId());
      //eos_static_info("This is the number for locations at final test %ld", num);
      //eos_static_info("this is test3");
      //eos_static_info("This is the size before %lf", GetSizeOfFileForGenerelFile(fmd));
      long num2 = fmd->getNumLocation();
      num2 -= hasTape;
      num2 -= (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1);

      //--eos_static_info("Stipe number %ld", eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()));
      //    eos_static_info("this is test3");
      if (num2 > 0) {
        statusFiles[fmd->getId()] = fmd;
        eos_static_debug("this is the layoutid %ld", fmd->getId());
        //test += 1;
        eos_static_debug("this is the map %ld", statusFiles.size());
      }

      //eos_static_info("this is the map %ld", statusFiles.size());

      /*
      //here we have the difference
      eos_static_info("Got an  item");
      eos_static_info("Layout number: %i", eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()));
      if (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
          eos::common::LayoutId::kRaid6) {
        statusFiles[fmd->getId()] = fmd;
        eos_static_info("Hello from kRaid6");
        //kQrain is the one prepared for
      }

      if (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
          eos::common::LayoutId::kQrain) {
        statusFiles[fmd->getId()] = fmd;
        eos_static_info("Hello from kQrain in the system");
        //kQrain is the one prepared for
      }
      */

      if (target_time > is_time) {
        uint64_t p_time = target_time - is_time;

        if (p_time > 5) {
          p_time = 5;
        }

        eos_static_debug("is:%lu target:%lu is_t:%lu target_t:%lu interval:%lu - pausing for %lu seconds\n",
                         nfiles_processed, nfiles, is_time, target_time, interval, p_time);
        // eos_static_info("is:%lu target:%lu is_t:%lu target_t:%lu interval:%lu - pausing for %lu seconds\n",
        //                nfiles_processed, nfiles, is_time, target_time, interval, p_time);
        // pause for the diff ...
        std::this_thread::sleep_for(std::chrono::seconds(p_time));
      }

      //Get something in for this on how to terminate in the middle.

// This might have to be for somesthing wierd here in order to run it.
      //This is a
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

  //This is currently not used
  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> sMutex(mutexScanStats);
  lastScanStats = currentScanStats;
  lastFaultyFiles = currentFaultyFiles;
  timeLastScan = timeCurrentScan;
}

void
DynamicEC::RunScan(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(waitTime));
  //assistant.wait_for(std::chrono::seconds(waitTime));
  //assistant.wait_for(std::chrono::seconds(waitTime));
  //assistant.wait_for(std::chrono::seconds(waitTime));
  //assistant.wait_for(std::chrono::seconds(10));
  eos_static_info("starting the scan for files");
  //assistant.wait_for(std::chrono::seconds(30));
  //eos_static_info("Runs the create files again \n");
  //createFiles();
  //eos_static_info("%d this is true",!assistant.terminationRequested());
  //eos_static_info("before while loop");

  //this will tell where to put some of the files if it will have to be here
  //can be put here in order to make it before we try to put it toghter.

  while (!assistant.terminationRequested())  {
    //eos_static_info("will wait 30 seconds");
    //assistant.wait_for(std::chrono::seconds(30));
    //eos_static_info("Runs the create files again \n");
    //createFiles();
    /*
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    std::uint64_t number = FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.freebytes?configstatus@rw", false);
    eos_static_info("This is some bytes %llu",number);
    std::uint64_t number_another = FsView::gFsView.mSpaceView[mSpaceName]->SumLongLong("stat.statfs.capacity", false);
    eos_static_info("This is some bytes %llu",number_another);
    eos_static_info("This is after bytes");
    */
    performCycleQDB(assistant);
    //std::uint64_t number = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->SumLongLong("stat.statfs.freebytes?configstatus@rw", false);
    //eos_static_info("This is some bytes %llu",number);
    /// What to do when it runs
    ///can have a lock for a timeout, then needs to know where it started.
    // somehow taking different files, and go though them, whis is where the deletion of file id is running
    // if it is need to be deleted, there can be the single deletion, or other deletions depending on the way this file saved, wich can be predicted from the layout
wait:
    //let time pass for a notification
    assistant.wait_for(std::chrono::seconds(waitTime));

    //gOFS->getStats(buff, blen);
    //gOFS->getStats(buff, blen)
    if (assistant.terminationRequested()) {
      return;
    }
  }

  eos_static_info("closing the thread for scanning");
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










