// ----------------------------------------------------------------------
// File: Health.hh
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSFST_HEALTH_HH__
#define __EOSFST_HEALTH_HH__

#include <fst/Namespace.hh>
#include <fst/storage/FileSystem.hh>
#include <mutex>

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class DiskHealth
{
public:
  std::map<std::string, std::string> getHealth(const char* devpath);

  bool Measure();

private:
  std::map<std::string, std::map<std::string, std::string>> smartctl_results;
  std::mutex mutex;

  /* Parse /proc/mdstat to obtain raid health. Existing indicator shows rebuild in progress. */
  std::map<std::string, std::string> parse_mdstat(const char* device);

  /* Obtain health of a single locally attached storage device by evaluating S.M.A.R.T values */
  std::string smartctl(const char* device);
};


class Health
{
private:
  bool skip;
  pthread_t tid;
  std::mutex Mutex;
  DiskHealth fDiskHealth;
  unsigned int interval;

public:

  Health(unsigned int ival_minutes = 15)
  {
    tid = 0;
    interval = ival_minutes;
    if (interval == 0) {
      interval = 1;
    }
    skip = false;
  }

  bool
  Monitor()
  {
    int rc = 0;

    if ((rc = XrdSysThread::Run(&tid, Health::StartHealthThread, static_cast<void*> (this),
                                XRDSYSTHREAD_HOLD, "Health-Monitor"))) {
      return false;
    } else {
      return true;
    }
  }

  virtual
  ~Health()
  {
    if (tid) {
      XrdSysThread::Cancel(tid);
      XrdSysThread::Join(tid, 0);
      tid = 0;
    }
  };

  void
  Measure()
  {
    while (1) {
      XrdSysThread::SetCancelOff();
      if (!fDiskHealth.Measure()) {
        fprintf(stderr, "error: cannot get disk health statistic\n");
      }
      XrdSysThread::SetCancelOn();
      for (unsigned int i = 0; i < interval; i++) {
        sleep(60);
        std::lock_guard<std::mutex> lock(Mutex);
        if (skip) {
          skip = false;
          break;
        }
      }
    }
  }

  std::map<std::string, std::string> getDiskHealth(const char* devpath)
  {
    auto result = fDiskHealth.getHealth(devpath);

    /* If we don't have any result, don't wait for interval timeout to measure. */
    if (result.empty()) {
      std::lock_guard<std::mutex> lock(Mutex);
      skip = true;
    }
    return result;
  };

  static void*
  StartHealthThread(void* pp)
  {
    Health* health = (Health*) pp;
    health->Measure();
    return 0;
  }
};

EOSFSTNAMESPACE_END


#endif
