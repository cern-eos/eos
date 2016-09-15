#ifndef __FSTSTORAGEMONITORVARPARTITION__HH__
#define __FSTSTORAGEMONITORVARPARTITION__HH__

#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"

#include "fst/Namespace.hh"

#include <unistd.h>
#include <sys/statvfs.h>

#include <string>
#include <errno.h>


EOSFSTNAMESPACE_BEGIN

template<class FSs>
class MonitorVarPartition : public eos::common::LogId {

  double m_space_treshold;
  int m_time_interval_in_us;
  std::string m_path;
  bool m_running;

public:

  MonitorVarPartition(double treshold, int time, std::string path) 
    : m_space_treshold(treshold), m_time_interval_in_us(time*1000*1000), 
      m_path(path), m_running(true)
    {}

  void Monitor(FSs& fss, eos::common::RWMutex& mtx){
    eos_static_info("FST Partition Monitor activated ...");

    struct statvfs buf;
    char buffer[256];

    while(m_running){

      // get info about filesystem where /var is located
      if(statvfs(m_path.c_str(), &buf) == -1){
        char* errorMessage = strerror_r(errno, buffer, 256);
        eos_err("statvfs failed! Error: %s ", errorMessage);
        continue;
      }

      // Calculating precentage of free space left
      // ignoring fragment size as doesn't matter in calculating percentage
      double free_percentage = ( (buf.f_bfree * 1.) / buf.f_blocks) * 100.;

      if(free_percentage < m_space_treshold){
        mtx.LockRead();

        for(auto fs : fss){

          //Check if filesystem is already in readonly mode
          if(fs->GetConfigStatus() != eos::common::FileSystem::eConfigStatus::kRO)
            fs->SetConfigStatus(eos::common::FileSystem::eConfigStatus::kRO);
        }

        mtx.UnLockRead();
      }
      usleep(m_time_interval_in_us);

    }
  }

  void StopMonitoring(){ m_running = false; }
};

EOSFSTNAMESPACE_END
#endif // __FSTSTORAGEMONITORVARPARTITION__HH__
