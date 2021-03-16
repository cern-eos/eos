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
#include <random>
#include <cmath>
#include <map>
#include <iterator>
#include <atomic>

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 300 // seconds


/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

DynamicEC::DynamicEC(const char* spacename, uint64_t ageNew,  uint64_t size,
                     double maxThres,
                     double minThres, bool OnWork)//bool OnWork
{
  fprintf(stderr, "Constructor \n");
  eos_static_info("%s", "Constructor \n");
  //Thread need to be activated
  age = ageNew;

  if (OnWork) {
    mThread.reset(&DynamicEC::Run, this);
  }

  if (OnWork) {
    //mThread2.reset(&DynamicCreator::Run, this);
  }

  mThread.stop();
  mSpaceName = "";
  simulatedFiles.clear();
  deletedFileSize = 0;
  //time_t seconds;
  //timeFromWhenToDelete =  time(0) - age;
  sizeMinForDeletion = size;
  maxThresHold = maxThres;
  minThresHold = minThres;
  createdFileSize = 0;
  sizeToBeDeleted = 0;
  waitTime = 10;
//mCreator.createFiles();
// DynamicCreator CreateTus();
  //CreateTus.createFiles();
  //mThread2.reset(&CreateTus::Run, this);
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
}

void
DynamicEC::createFiles()
{
  // switch timeout resolution to 1s                                                               \
  \
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  XrdCl::File file;
  // replace existing file                                                                         \
  \
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update |
                                        XrdCl::OpenFlags::Delete;
  // default modes - user can rwx                                                                  \
  \
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                             XrdCl::Access::UX;
  //std::string url="root://home/rawfile.xrdcl" ;
  //std::string url="root://eoshome-a.cern.ch//eos/user/a/astoeve/rawfile.xrdcl" ;
  std::string url = "root://localhost//eos/testarea/dynec/rawfile1.xrdcl" ;
  //[root://localhost] |/eos/testarea/dynec/                                                       \
  \
  // timeout 5s                                                                                    \
  \
  XrdCl::XRootDStatus status = file.Open(url, targetFlags, mode, 5);

  if (!status.IsOK()) {
    // too bad                                                                                     \
    \
    exit(-1);
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
      // too bad again                                                                             \
      \
      exit(-2);
    }

    // give 5s to close the file                                                                   \
    \
    status = file.Close(5);

    if (!status.IsOK()) {
      // too bad again
      exit(-3);
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
  eos_static_info("%s", "Constructor \n");
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
  eos_static_info("%s", "Status:\i %" PRId64 ": total size, %" PRId64
                  ": used size, %"
                  PRId64 ": deleted size, %" PRId64 ": undeleted size.\n", status.totalSize,
                  status.usedSize, status.deletedSize, status.undeletedSize);
  return status;
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

void
DynamicEC::SingleDeletion(std::shared_ptr<DynamicECFile> file)
{
  simulatedFiles.erase(file->getId());
  /// delete the file, where the only need to be deleted one instance.
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
  while (file->getLocations().size() > ((eos::common::LayoutId::GetStripeNumber(
      file->getLayoutId()) + 1) - eos::common::LayoutId::GetRedundancyStripeNumber(
                                          file->getLayoutId()) + 2)) {
    ///For the final test removing of unlinked locations is needed.
    file->unlinkLocation(file->getLocations().back());
    //remove is not for production
    //look this up for test
    file->removeLocation(file->getLocations().back());

    //rem
    if (gOFS) {
      gOFS->eosView->updateFileStore(file.get());
    }
  }

  deletedFileSize += (beforeSize - GetSizeOfFile(file));
}

void
DynamicEC::Cleanup()
{
  //mCreator.createFiles();
  fprintf(stderr, "CleanUp started \n");
  eos_static_info("%s", "CleanUp started \n");
  statusForSystem status;
  status = SpaceStatus();
  sizeToBeDeleted = status.undeletedSize;
  timeFromWhenToDelete =  time(0) - age;

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
  eos_static_info("%s", "CleanUp ended without success there was deleted:  %"
                  PRId64 " bytes, but there should have been deleted :  %" PRId64 " bytes  \n",
                  deletedFileSize, sizeToBeDeleted);
  //This have to do something if it deletes and there is more filesize to be deleted
}

void
DynamicEC::Run(ThreadAssistant& assistant) noexcept
{
  //gOFS->WaitUntilNamespaceIsBooted(assistant);
  //assistant.wait_for(std::chrono::seconds(10));
  eos_static_info("starting");

  while (!assistant.terminationRequested())
    ///Assisting variables can be written here
  {
    Cleanup();
    createFiles();
    /// What to do when it runs
    ///can have a lock for a timeout, then needs to know where it started.
    // somehow taking different files, and go though them, whis is where the deletion of file id is running
    // if it is need to be deleted, there can be the single deletion, or other deletions depending on the way this file saved, wich can be predicted from the layout
wait:
    //let time pass for a notification
    assistant.wait_for(std::chrono::seconds(waitTime));

    if (assistant.terminationRequested()) {
      return;
    }
  }
}




EOSMGMNAMESPACE_END
