// ----------------------------------------------------------------------
// File: Load.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSFST_LOAD_HH__
#define __EOSFST_LOAD_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#include <sys/stat.h>
#include <vector>
#include <map>
#include <string>
#include <sys/time.h>

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class DiskStat
{
private:
  // map from device name to map of key/values
  std::map<std::string, std::map<std::string, std::string > > values_t2;
  std::map<std::string, std::map<std::string, std::string > > values_t1;
  std::map<std::string, std::map<std::string, double > > rates;

  struct timespec t1;
  struct timespec t2;

  std::vector<std::string> tags;
  std::vector<std::string>::const_iterator tagit;

  unsigned long long maxbandwidth;

public:

  void
  SetBandWidth (unsigned long long bw)
  {
    maxbandwidth = bw;
  }

  bool Measure ();

  double
  GetRate (const char* dev, const char* key)
  {
    std::string tag = key;
    std::string device = dev;
    return rates[device][tag];
  }

  DiskStat ()
  {
    maxbandwidth = 80 * 1024 * 1024;

    tags.push_back("type");
    tags.push_back("number");
    tags.push_back("device");
    tags.push_back("readReq");
    tags.push_back("mergedReadReq");
    tags.push_back("readSectors");
    tags.push_back("millisRead");
    tags.push_back("writeReqs");
    tags.push_back("mergedWriteReq");
    tags.push_back("writeSectors");
    tags.push_back("millisWrite");
    tags.push_back("concurrentIO");
    tags.push_back("millisIO");
    tags.push_back("weightedMillisIO");
    t1.tv_sec = 0;
    t2.tv_sec = 0;
    t1.tv_nsec = 0;
    t2.tv_nsec = 0;
  }

  virtual
  ~DiskStat () { }
};

class NetStat
{
private:
  // map from device name to map of key/values
  std::map<std::string, std::map<std::string, std::string > > values_t2;
  std::map<std::string, std::map<std::string, std::string > > values_t1;
  std::map<std::string, std::map<std::string, double > > rates;

  unsigned long long maxbandwidth;
  struct timespec t1;
  struct timespec t2;

  std::vector<std::string> tags;
  std::vector<std::string>::const_iterator tagit;

public:

  bool Measure ();

  double
  GetRate (const char* dev, const char* key)
  {
    std::string tag = key;
    std::string device = dev;
    return rates[device][tag];
  }

  void
  SetBandWidth (unsigned long long bw)
  {
    maxbandwidth = bw;
  }

  NetStat ()
  {
    maxbandwidth = 1 * 1024 * 1024 * 1024; // default is 1GBit ethernet
    tags.push_back("face");
    tags.push_back("rxbytes");
    tags.push_back("rxpackets");
    tags.push_back("rxerrs");
    tags.push_back("rxdrop");
    tags.push_back("rxfifo");
    tags.push_back("rxframe");
    tags.push_back("rxcompressed");
    tags.push_back("rxmulticast");
    tags.push_back("txbytes");
    tags.push_back("txpackets");
    tags.push_back("txerrs");
    tags.push_back("txdrop");
    tags.push_back("txfifo");
    tags.push_back("txframe");
    tags.push_back("txcompressed");
    tags.push_back("txrmulticast");

    t1.tv_sec = 0;
    t2.tv_sec = 0;
    t1.tv_nsec = 0;
    t2.tv_nsec = 0;
  }

  virtual
  ~NetStat () { };
};

class Load
{
private:
  bool zombie;
  pthread_t tid;
  XrdSysMutex Mutex;
  DiskStat fDiskStat;
  NetStat fNetStat;
  unsigned int interval;

public:

  void
  SetDiskBandWidth (unsigned long long bw)
  {
    fDiskStat.SetBandWidth(bw);
  }

  void
  SetNetBandWidth (unsigned long long bw)
  {
    fNetStat.SetBandWidth(bw);
  }

  Load (unsigned int ival = 15)
  {
    tid = 0;
    interval = ival;
    if (interval == 0)
      interval = 1;
    zombie = false;
  }

  bool
  Monitor ()
  {
    int rc = 0;

    if ((rc = XrdSysThread::Run(&tid, Load::StartLoadThread, static_cast<void *> (this),
                                XRDSYSTHREAD_HOLD, "Scrubber")))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

  virtual
  ~Load ()
  {
    if (tid)
    {
      XrdSysThread::Cancel(tid);
      XrdSysThread::Join(tid, 0);
      tid = 0;
    }
  };

  void
  Measure ()
  {
    while (1)
    {
      Mutex.Lock();
      XrdSysThread::SetCancelOff();
      if (!fDiskStat.Measure())
      {
        fprintf(stderr, "error: cannot get disk IO statistic\n");
      }

      if (!fNetStat.Measure())
      {
        fprintf(stderr, "error: cannot get network IO statistic\n");
      }
      Mutex.UnLock();
      XrdSysThread::SetCancelOn();
      sleep(interval);
    }
  }

  const char*
  DevMap (const char* devpath)
  {
    static time_t loadtime = 0;
    static std::map<std::string, std::string> devicemap;
    static XrdOucString mapdev;
    mapdev = devpath;
    static XrdOucString mappath;
    mappath = "";
    if (mapdev.beginswith("/"))
    {
      mapdev = "";
      struct stat stbuf;
      if (!(stat("/etc/mtab", &stbuf)))
      {
        devicemap.clear();
        if (stbuf.st_mtime != loadtime)
        {
          FILE* fd = fopen("/etc/mtab", "r");
          // reparse the mtab
          char line[1025];
          char val[6][1024];
          line[0] = 0;
          while (fd && fgets(line, 1024, fd))
          {
            if ((sscanf(line, "%s %s %s %s %s %s\n", val[0], val[1], val[2], val[3], val[4], val[5])) == 6)
            {
              XrdOucString sdev = val[0];
              XrdOucString spath = val[1];
              //fprintf(stderr,"%s => %s\n", sdev.c_str(), spath.c_str());
              if (sdev.beginswith("/dev/"))
              {
                sdev.erase(0, 5);
                devicemap[sdev.c_str()] = spath.c_str();
                //fprintf(stderr,"=> %s %s\n", sdev.c_str(),spath.c_str());
              }
            }
          }
          if (fd)
            fclose(fd);
        }
      }
      std::map<std::string, std::string>::const_iterator devicemapit;
      for (devicemapit = devicemap.begin(); devicemapit != devicemap.end(); devicemapit++)
      {
        XrdOucString match = devpath;
        XrdOucString itstr = devicemapit->second.c_str();
        match.erase(devicemapit->second.length());
        //        fprintf(stderr,"%s <=> %s\n",match.c_str(),itstr.c_str());
        if (match == itstr)
        {
          if ((int) devicemapit->second.length() > (int) mappath.length())
          {
            mapdev = devicemapit->first.c_str();
            mappath = devicemapit->second.c_str();
            //fprintf(stderr,"Setting up mapping %s=>%s\n", mapdev.c_str(), mappath.c_str());
          }
        }
      }
    }
    if (!mapdev.length())
    {
      mapdev = devpath;
    }
    //    printf("===> returning |%s|\n", mapdev.c_str());
    return mapdev.c_str();
  }

  double
  GetDiskRate (const char* devpath, const char* tag)
  {
    Mutex.Lock();
    const char* dev = DevMap(devpath);
    //fprintf(stderr,"**** Device is %s\n", dev);
    double val = fDiskStat.GetRate(dev, tag);
    Mutex.UnLock();
    return val;
  }

  double
  GetDiskUtilization ()
  {
    return 0;
  }

  double
  GetNetRate (const char* dev, const char* tag)
  {
    Mutex.Lock();
    double val = fNetStat.GetRate(dev, tag);
    Mutex.UnLock();
    return val;
  }

  static void*
  StartLoadThread (void *pp)
  {
    Load* load = (Load*) pp;
    load->Measure();
    return 0;
  }
};

EOSFSTNAMESPACE_END

#endif

