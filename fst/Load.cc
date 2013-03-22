// ----------------------------------------------------------------------
// File: Load.cc
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


/*----------------------------------------------------------------------------*/
#include "fst/Load.hh"
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

bool
DiskStat::Measure ()
{
  FILE* fd = fopen("/proc/diskstats", "r");
  if (fd != 0)
  {
    int items = 0;
    char val[14][1024];
    bool scanned = false;
    do
    {
      items = fscanf(fd, "%s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7], val[8], val[9], val[10], val[11], val[12], val[13]);

      if (items == 14)
      {
        scanned = true;
#ifdef __APPLE__
        struct timeval tv;
        gettimeofday(&tv, 0);
        t2.tv_sec = tv.tv_sec;
        t2.tv_nsec = tv.tv_usec * 1000;
#else
        clock_gettime(CLOCK_REALTIME, &t2);
#endif
        std::string devname = val[2];

        for (unsigned int i = 3; i < tags.size(); i++)
        {
          values_t2[devname][tags[i]] = val[i];
        }

        if (t1.tv_sec != 0)
        {
          float tdif = ((t2.tv_sec - t1.tv_sec)*1000.0) + ((t2.tv_nsec - t1.tv_nsec) / 1000000.0);
          for (unsigned int i = 3; i < tags.size(); i++)
          {
            if (tdif > 0)
            {
              rates[devname][tags[i]] = 1000.0 * (strtoll(values_t2[devname][tags[i]].c_str(), 0, 10) - strtoll(values_t1[devname][tags[i]].c_str(), 0, 10)) / tdif;
              //              if (!strncmp(devname.c_str(),"md2",3))
              //              fprintf(stderr,"%s %s %f\n", devname.c_str(), tags[i].c_str(),  rates[devname][tags[i]]);
            }
            else
            {
              rates[devname][tags[i]] = 0.0;
            }
          }
          for (unsigned int i = 3; i < tags.size(); i++)
          {
            values_t1[devname][tags[i]] = values_t2[devname][tags[i]];
          }
        }
        else
        {
          for (tagit = tags.begin(); tagit != tags.end(); tagit++)
          {
            rates[devname][*tagit] = 0.0;
          }
          for (unsigned int i = 3; i < tags.size(); i++)
          {
            values_t1[devname][tags[i]] = values_t2[devname][tags[i]];
          }
        }
        continue;
      }
      fclose(fd);
      if (scanned)
      {
        t1.tv_sec = t2.tv_sec;
        t1.tv_nsec = t2.tv_nsec;
        return true;
      }
      else
      {
        return false;
      }
    }
    while (1);

  }
  else
  {
    return false;
  }
}

bool
NetStat::Measure ()
{
  FILE* fd = fopen("/proc/net/dev", "r");
  if (fd != 0)
  {
    int items = 0;
    char val[18][1024];
    char garbage[4096];
    errno = 0;

    int n = 0;
    do
    {
      garbage[0] = 0;
      if (fgets(garbage, 1024, fd))
      {
        char* dpos = 0;
        if ((dpos = strchr(garbage, ':')))
        {
          *dpos = ' ';
        }
      }

      if (n >= 2)
      {
        items = sscanf(garbage, "%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7], val[8], val[9], val[10], val[11], val[12], val[13], val[14], val[15], val[16]);
      }
      else
      {
        items = 0;
      }

      if (items == 17)
      {
#ifdef __APPLE__
        struct timeval tv;
        gettimeofday(&tv, 0);
        t2.tv_sec = tv.tv_sec;
        t2.tv_nsec = tv.tv_usec * 1000;
#else
        clock_gettime(CLOCK_REALTIME, &t2);
#endif
        std::string devname = val[0];

        for (unsigned int i = 1; i < tags.size(); i++)
        {
          values_t2[devname][tags[i]] = val[i];
        }

        if (t1.tv_sec != 0)
        {
          float tdif = ((t2.tv_sec - t1.tv_sec)*1000.0) + ((t2.tv_nsec - t1.tv_nsec) / 1000000.0);
          for (unsigned int i = 1; i < tags.size(); i++)
          {
            if (tdif > 0)
            {
              rates[devname][tags[i]] = 1000.0 * (strtoll(values_t2[devname][tags[i]].c_str(), 0, 10) - strtoll(values_t1[devname][tags[i]].c_str(), 0, 10)) / tdif;
            }
            else
            {
              rates[devname][tags[i]] = 0.0;
            }
          }
          for (unsigned int i = 1; i < tags.size(); i++)
          {
            values_t1[devname][tags[i]] = values_t2[devname][tags[i]];
          }
        }
        else
        {
          for (tagit = tags.begin(); tagit != tags.end(); tagit++)
          {
            rates[devname][*tagit] = 0.0;
          }
          for (unsigned int i = 1; i < tags.size(); i++)
          {
            values_t1[devname][tags[i]] = values_t2[devname][tags[i]];
          }
        }
      }
      else
      {
        if (items < 0)
        {
          fclose(fd);
          t1.tv_sec = t2.tv_sec;
          t1.tv_nsec = t2.tv_nsec;
          return true;
        }
      }

      n++;
    }
    while (1);
  }
  else
  {
    return false;
  }
}

EOSFSTNAMESPACE_END
