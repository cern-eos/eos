#ifndef __FSTTESTSVARPARTITIONMONITORTEST__HH__
#define __FSTTESTSVARPARTITIONMONITORTEST__HH__

#include <cppunit/extensions/HelperMacros.h>
#include <vector>
#include <thread>
#include <fstream>

#include "common/FileSystem.hh"
#include "common/RWMutex.hh"

#include "fst/storage/MonitorVarPartition.hh"

typedef eos::common::FileSystem::eConfigStatus fsstatus_t;

// Mock class, implementing only few methods related to 
// testing unit.
struct FileSystemTest {
  fsstatus_t status;

  FileSystemTest() : status(eos::common::FileSystem::kRW){}

  void SetConfigStatus (fsstatus_t status){
    this->status = status;
  }

  fsstatus_t GetConfigStatus (bool cached = false){
    return this->status;
  }
};

typedef eos::fst::MonitorVarPartition<std::vector<FileSystemTest*>> VarMonitor;

class VarPartitionMonitorTest : public CppUnit::TestCase{
  CPPUNIT_TEST_SUITE(VarPartitionMonitorTest);
    CPPUNIT_TEST(VarMonitorTest);
  CPPUNIT_TEST_SUITE_END();

  eos::common::RWMutex fsMutex;
  std::vector<FileSystemTest*> fileSystemsVector;
  VarMonitor monitor;
  std::thread monitor_thread;
  std::ofstream fill;

public:

  VarPartitionMonitorTest() : monitor(10.,3, "/mnt/var_test/") {}

  // CPPUNIT required methods
  void setUp(void);
  void tearDown(void);

  // Testing unit
  static void* StartFstPartitionMonitor(void* pp);

  // Method implementing test
  void VarMonitorTest();

};


#endif //__FSTTESTSVARPARTITIONMONITORTEST__HH__
