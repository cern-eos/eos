//------------------------------------------------------------------------------
// File: Load.cc
// Author: Andreas-Joachim Peters <apeters@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "fst/Load.hh"
#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <sys/stat.h>
#include "XrdOuc/XrdOucString.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                              Load Class
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Load::Load(unsigned int ival):
  mTid(0)
{
  mInterval = ival;

  if (mInterval == 0) {
    mInterval = 1;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Load::~Load()
{
  if (mTid) {
    XrdSysThread::Cancel(mTid);
    XrdSysThread::Join(mTid, 0);
    mTid = 0;
  }
}

//------------------------------------------------------------------------------
// Get device name mounted at the given path
//-----------------------------------------------------------------------------
const std::string
Load::DevMap(const std::string& dev_path)
{
  static time_t loadtime = 0;
  static std::map<std::string, std::string> dev_map;
  static XrdSysMutex mutex_map; // Protect access to the dev_map
  std::string mapped_dev;

  if (!dev_path.empty() && dev_path[0] == '/') {
    struct stat stbuf;
    XrdSysMutexHelper scope_lock(&mutex_map);

    if (!(stat("/etc/mtab", &stbuf))) {
      if (stbuf.st_mtime != loadtime) {
        loadtime = stbuf.st_mtime;
        dev_map.clear();
        FILE* fd = fopen("/etc/mtab", "r");
        char line[1025];
        char val[6][1024];
        line[0] = 0;

        while (fd && fgets(line, 1024, fd)) {
          if ((sscanf(line, "%1023s %1023s %1023s %1023s %1023s %1023s\n",
                      val[0], val[1], val[2], val[3], val[4], val[5])) == 6) {
            std::string sdev = val[0];
            std::string spath = val[1];

            // fprintf(stderr,"%s => %s\n", sdev.c_str(), spath.c_str());
            if (sdev.find("/dev/") == 0) {
              sdev.erase(0, 5);
              dev_map[sdev] = spath;
            }
          }
        }

        if (fd) {
          fclose(fd);
        }
      }
    }

    std::string path = "";

    for (auto it = dev_map.begin(); it != dev_map.end(); ++it) {
      std::string dpath = it->second.c_str();
      std::string match = dev_path;

      if (dpath.length() <= match.length()) {
        match.erase(dpath.length());
      }

      // fprintf(stderr,"%s <=> %s\n",match.c_str(),dpath.c_str());
      if (match == dpath) {
        if (dpath.length() > path.length()) {
          mapped_dev = it->first.c_str();
          path = dpath;
        }
      }
    }
  }

  return mapped_dev;
}

//------------------------------------------------------------------------------
// Get disk rate type for a particular device
//------------------------------------------------------------------------------
double
Load::GetDiskRate(const char* dev_path, const char* tag)
{
  std::string dev = DevMap(dev_path);
  //fprintf(stderr,"**** Device is %s\n", dev);
  double val = fDiskStat.GetRate(dev.c_str(), tag);
  return val;
}

//------------------------------------------------------------------------------
// Get net rate type for a particular device
//------------------------------------------------------------------------------
double
Load::GetNetRate(const char* dev, const char* tag)
{
  double val = fNetStat.GetRate(dev, tag);
  return val;
}

//------------------------------------------------------------------------------
// Method run by scrubber thread to  measurement both disk and network values
// on regular intervals.
//------------------------------------------------------------------------------
void
Load::Measure()
{
  while (true) {
    XrdSysThread::SetCancelOff();

    if (!fDiskStat.Measure()) {
      fprintf(stderr, "error: cannot get disk IO statistic\n");
    }

    if (!fNetStat.Measure()) {
      fprintf(stderr, "error: cannot get network IO statistic\n");
    }

    XrdSysThread::SetCancelOn();
    sleep(mInterval);
  }
}

//------------------------------------------------------------------------------
// Static method used to start the scrubber thread
//------------------------------------------------------------------------------
void*
Load::StartLoadThread(void* pp)
{
  Load* load = (Load*) pp;
  load->Measure();
  return 0;
}

//------------------------------------------------------------------------------
// Start scrubber thread
//------------------------------------------------------------------------------
bool
Load::Monitor()
{
  int rc = 0;

  if ((rc = XrdSysThread::Run(&mTid, Load::StartLoadThread,
                              static_cast<void*>(this),
                              XRDSYSTHREAD_HOLD, "Scrubber"))) {
    return false;
  } else {
    return true;
  }
}


//------------------------------------------------------------------------------
//                              DiskStat
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
DiskStat::DiskStat()
{
  mTags.push_back("type");
  mTags.push_back("number");
  mTags.push_back("device");
  mTags.push_back("readReq");
  mTags.push_back("mergedReadReq");
  mTags.push_back("readSectors");
  mTags.push_back("millisRead");
  mTags.push_back("writeReqs");
  mTags.push_back("mergedWriteReq");
  mTags.push_back("writeSectors");
  mTags.push_back("millisWrite");
  mTags.push_back("concurrentIO");
  mTags.push_back("millisIO");
  mTags.push_back("weightedMillisIO");
  t1.tv_sec = 0;
  t2.tv_sec = 0;
  t1.tv_nsec = 0;
  t2.tv_nsec = 0;
}

//------------------------------------------------------------------------------
// Get rate type for device
//------------------------------------------------------------------------------
double
DiskStat::GetRate(const char* dev, const char* key)
{
  std::string tag = key;
  std::string device = dev;
  XrdSysRWLockHelper rd_lock(&mMutexRW, true);

  if (mRates.find(device) != mRates.end()) {
    return mRates[device][tag];
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get disk measurement values
//------------------------------------------------------------------------------
bool
DiskStat::Measure()
{
  FILE* fd = fopen("/proc/diskstats", "r");

  if (fd != 0) {
    XrdSysRWLockHelper wr_lock(&mMutexRW, false);
    int items = 0;
    char val[14][1024];
    bool scanned = false;

    do {
      items = fscanf(fd, "%1023s %1023s %1023s %1023s %1023s %1023s %1023s "
                     "%1023s %1023s %1023s %1023s %1023s %1023s %1023s\n",
                     val[0], val[1], val[2], val[3], val[4], val[5], val[6],
                     val[7], val[8], val[9], val[10], val[11], val[12], val[13]);

      if (items == 14) {
        scanned = true;
#ifdef __APPLE__
        struct timeval tv;
        gettimeofday(&tv, 0);
        t2.tv_sec = tv.tv_sec;
        t2.tv_nsec = tv.tv_usec * 1000;
#else
        clock_gettime(CLOCK_REALTIME, &t2);
#endif
        std::string dev_name = val[2];

        for (unsigned int i = 3; i < mTags.size(); i++) {
          values_t2[dev_name][mTags[i]] = val[i];
        }

        if (t1.tv_sec != 0) {
          float tdif = ((t2.tv_sec - t1.tv_sec) * 1000.0) +
                       ((t2.tv_nsec - t1.tv_nsec) / 1000000.0);

          for (unsigned int i = 3; i < mTags.size(); i++) {
            if (tdif > 0) {
              mRates[dev_name][mTags[i]] =
                1000.0 * (strtoll(values_t2[dev_name][mTags[i]].c_str(), 0, 10) -
                          strtoll(values_t1[dev_name][mTags[i]].c_str(), 0, 10)) / tdif;
            } else {
              mRates[dev_name][mTags[i]] = 0.0;
            }
          }

          for (unsigned int i = 3; i < mTags.size(); i++) {
            values_t1[dev_name][mTags[i]] = values_t2[dev_name][mTags[i]];
          }
        } else {
          for (auto it = mTags.begin(); it != mTags.end(); it++) {
            mRates[dev_name][*it] = 0.0;
          }

          for (unsigned int i = 3; i < mTags.size(); i++) {
            values_t1[dev_name][mTags[i]] = values_t2[dev_name][mTags[i]];
          }
        }

        continue;
      }

      fclose(fd);

      if (scanned) {
        t1.tv_sec = t2.tv_sec;
        t1.tv_nsec = t2.tv_nsec;
        return true;
      } else {
        return false;
      }
    } while (1);
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
//                                 NetStat Class
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
NetStat::NetStat()
{
  mTags.push_back("face");
  mTags.push_back("rxbytes");
  mTags.push_back("rxpackets");
  mTags.push_back("rxerrs");
  mTags.push_back("rxdrop");
  mTags.push_back("rxfifo");
  mTags.push_back("rxframe");
  mTags.push_back("rxcompressed");
  mTags.push_back("rxmulticast");
  mTags.push_back("txbytes");
  mTags.push_back("txpackets");
  mTags.push_back("txerrs");
  mTags.push_back("txdrop");
  mTags.push_back("txfifo");
  mTags.push_back("txframe");
  mTags.push_back("txcompressed");
  mTags.push_back("txrmulticast");
  t1.tv_sec = 0;
  t2.tv_sec = 0;
  t1.tv_nsec = 0;
  t2.tv_nsec = 0;
}

//------------------------------------------------------------------------------
// Get rate type for device
//------------------------------------------------------------------------------
double
NetStat::GetRate(const char* dev, const char* key)
{
  std::string tag = key;
  std::string device = dev;
  XrdSysRWLockHelper rd_lock(&mMutexRW, true);

  if (mRates.find(device) != mRates.end()) {
    return mRates[device][tag];
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Get network measurement values
//------------------------------------------------------------------------------
bool
NetStat::Measure()
{
  FILE* fd = fopen("/proc/net/dev", "r");

  if (fd != 0) {
    XrdSysRWLockHelper wr_lock(&mMutexRW, false);
    int items = 0;
    char val[18][1024];
    char garbage[4096];
    errno = 0;
    int n = 0;

    do {
      garbage[0] = 0;

      if (fgets(garbage, 1024, fd)) {
        char* dpos = 0;

        if ((dpos = strchr(garbage, ':'))) {
          *dpos = ' ';
        }
      }

      if (n >= 2) {
        items = sscanf(garbage, "%1023s %1023s %1023s %1023s %1023s %1023s "
                       "%1023s %1023s %1023s %1023s %1023s %1023s %1023s "
                       "%1023s %1023s %1023s %1023s\n",
                       val[0], val[1], val[2], val[3], val[4], val[5], val[6],
                       val[7], val[8], val[9], val[10], val[11], val[12],
                       val[13], val[14], val[15], val[16]);
      } else {
        items = 0;
      }

      if (items == 17) {
	size_t dev_len = strlen(val[0]);
	if (dev_len && (val[0][dev_len-1] == ':')) {
	  // newer kernel use <device-name>: in this field ... sigh ...
	  val[0][dev_len-1] = 0;
	}

#ifdef __APPLE__
        struct timeval tv;
        gettimeofday(&tv, 0);
        t2.tv_sec = tv.tv_sec;
        t2.tv_nsec = tv.tv_usec * 1000;
#else
        clock_gettime(CLOCK_REALTIME, &t2);
#endif
        std::string dev_name = val[0];

        for (unsigned int i = 1; i < mTags.size(); i++) {
          values_t2[dev_name][mTags[i]] = val[i];
        }

        if (t1.tv_sec != 0) {
          float tdif = ((t2.tv_sec - t1.tv_sec) * 1000.0) +
                       ((t2.tv_nsec - t1.tv_nsec) / 1000000.0);

          for (unsigned int i = 1; i < mTags.size(); i++) {
            if (tdif > 0) {
              mRates[dev_name][mTags[i]] =
                1000.0 * (strtoll(values_t2[dev_name][mTags[i]].c_str(), 0, 10) -
                          strtoll(values_t1[dev_name][mTags[i]].c_str(), 0, 10)) / tdif;
            } else {
              mRates[dev_name][mTags[i]] = 0.0;
            }
          }

          for (unsigned int i = 1; i < mTags.size(); i++) {
            values_t1[dev_name][mTags[i]] = values_t2[dev_name][mTags[i]];
          }
        } else {
          for (auto it = mTags.begin(); it != mTags.end(); it++) {
            mRates[dev_name][*it] = 0.0;
          }

          for (unsigned int i = 1; i < mTags.size(); i++) {
            values_t1[dev_name][mTags[i]] = values_t2[dev_name][mTags[i]];
          }
        }
      } else {
        if (items < 0) {
          fclose(fd);
          t1.tv_sec = t2.tv_sec;
          t1.tv_nsec = t2.tv_nsec;
          return true;
        }
      }

      n++;
    } while (1);
  } else {
    return false;
  }
}

EOSFSTNAMESPACE_END
