#include "VarPartitionMonitorTest.hh"

#include <vector>

#include <unistd.h>
#include <sys/statvfs.h>


#include <errno.h>

CPPUNIT_TEST_SUITE_REGISTRATION(VarPartitionMonitorTest);

void* VarPartitionMonitorTest::StartFstPartitionMonitor(void* pp){
  VarPartitionMonitorTest* storage = (VarPartitionMonitorTest*) pp;
  storage->monitor.Monitor(storage->fileSystemsVector, storage->fsMutex);
  return 0;
}


void VarPartitionMonitorTest::setUp(void)
{
  // Init partition
  system("mkdir -p /mnt/var_test");
  system("mount -t tmpfs -o size=100m tmpfs /mnt/var_test/");

  // Add few fileSystems in vector
  this->fileSystemsVector.push_back(new FileSystemTest());
  this->fileSystemsVector.push_back(new FileSystemTest());
  this->fileSystemsVector.push_back(new FileSystemTest());
  this->fileSystemsVector.push_back(new FileSystemTest());

  fill.open("/mnt/var_test/fill.temp");
  // Start monitoring.
  this->monitor_thread= std::thread(VarPartitionMonitorTest::StartFstPartitionMonitor, this);
}


void VarPartitionMonitorTest::VarMonitorTest(){

  // Fill partition to more than 90%
  std::string megabyte_line(1024*1024, 'a');

  for(int i = 0; i < 91; ++i){
    fill << megabyte_line << std::endl;
  }

  //wait 3 seconds and check
  usleep(3 * 1000 * 1000);
  fsMutex.LockRead();

  for(auto fs : fileSystemsVector)
    CPPUNIT_ASSERT(fs->GetConfigStatus() == eos::common::FileSystem::kRO);

  fsMutex.UnLockRead();

  // Setting status of filesystems to RW.
  fsMutex.LockWrite();

  for(auto fs : fileSystemsVector)
    fs->SetConfigStatus(eos::common::FileSystem::kRW);

  fsMutex.UnLockWrite();

  // Check if status is returned to readonly, after 3s
  usleep(3 * 1000 * 1000);

  fsMutex.LockRead();

  for(auto fs : fileSystemsVector)
    CPPUNIT_ASSERT(fs->GetConfigStatus() == eos::common::FileSystem::kRO);

  fsMutex.UnLockRead();

  // Close and delete file, 
  fill.close();
  system("rm /mnt/var_test/fill.temp");

  // Setting status of filesystems to RW
  fsMutex.LockWrite();

  for(auto fs : fileSystemsVector)
    fs->SetConfigStatus(eos::common::FileSystem::kRW);

  fsMutex.UnLockWrite();

  // Check if status is returned to readonly, after 3s
  usleep(3 * 1000 * 1000);

  fsMutex.LockRead();

  for(auto fs : fileSystemsVector)
    CPPUNIT_ASSERT(fs->GetConfigStatus() == eos::common::FileSystem::kRW);

  fsMutex.UnLockRead();

 
}

void VarPartitionMonitorTest::tearDown(void)
{
  // Cleaning
  delete this->fileSystemsVector[0];
  delete this->fileSystemsVector[1];
  delete this->fileSystemsVector[2];
  delete this->fileSystemsVector[3];

  // Terminate monitoring
  this->monitor.StopMonitoring();
  this->monitor_thread.join();

  system("umount /mnt/var_test/");
}

