// ----------------------------------------------------------------------
// File: VstView.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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
#include "mgm/VstView.hh"
#include "common/StringConversion.hh"
#include "XrdSys/XrdSysTimer.hh"

/*----------------------------------------------------------------------------*/
#include <math.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
VstView VstView::gVstView;

void
VstView::Print (std::string &out,
                std::string option,
                const char* selection)
{
  XrdSysMutexHelper vLock(ViewMutex);
  bool monitoring = false;
  bool ioformating = false;

  if (option == "m")
    monitoring = true;
  if (option == "io")
    ioformating = true;

  const char* format = "%s %-16s %-4s %-40s %-16s %-6s %-10s %-8s %12s %8s %5s %5s %7s %12s %12s %8s\n";
  const char* ioformat = "%s %-20s %-4s %12s %8s %12s %12s %8s %5s %5s %10s %10s %10s %10s %5s %5s\n";
  char line[4096];
  if (!monitoring)
  {
    if (ioformating)
      snprintf(line, sizeof (line) - 1, ioformat, "#", "instance", "age", "space", "used", "files", "directories", "clients", "ropen", "wopen", "diskr-MB/s", "diskw-MB/s", "ethi-MiB/s", "etho-MiB/s", "NsR/s","NsW/s");
    else
      snprintf(line, sizeof (line) - 1, format, "#", "instance", "age", "host", "ip", "mode", "version", "uptime", "space", "used", "n(fs)", "iops", "bw-MB/s", "files", "directories", "clients");
    out += "# _______________________________________________________________________________________________________________________________________________________________________________________\n";
    out += line;
    out += "# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
  }
  if (!monitoring)
  { 
    for (auto it = mView.begin(); it != mView.end(); ++it)
    {
      XrdOucString age1, age2;
      XrdOucString space;
      XrdOucString val1, val2, val3, val4, val5, val6, val7, val8, val9;
      char sused[64];
      long lage = time(NULL) - strtol(it->second["timestamp"].c_str(), 0, 10);
      long lup = time(NULL) - strtol(it->second["uptime"].c_str(), 0, 10);

      unsigned long long max_bytes = strtoull(it->second["maxbytes"].c_str(), 0, 10);
      unsigned long long free_bytes = strtoull(it->second["freebytes"].c_str(), 0, 10);
      unsigned long long diskin = strtoull(it->second["diskin"].c_str(), 0, 10);
      unsigned long long diskout = strtoull(it->second["diskout"].c_str(), 0, 10);
      unsigned long long ethin = strtoull(it->second["ethin"].c_str(), 0, 10);
      unsigned long long ethout = strtoull(it->second["ethout"].c_str(), 0, 10);
      unsigned long long ropen = strtoull(it->second["ropen"].c_str(), 0, 10);
      unsigned long long wopen = strtoull(it->second["wopen"].c_str(), 0, 10);
      unsigned long long rlock = strtoull(it->second["rlock"].c_str(), 0, 10);
      unsigned long long wlock = strtoull(it->second["wlock"].c_str(), 0, 10);
      unsigned long long nfsrw = strtoull(it->second["nfsrw"].c_str(), 0, 10);
      unsigned long long iops = strtoull(it->second["iops"].c_str(), 0, 10);
      unsigned long long bw = strtoull(it->second["bw"].c_str(), 0, 10);
      
      double used = 100.0 * (max_bytes - free_bytes) / max_bytes;
      if ((used < 0) || (used > 100))
        used = 100;

      if (lage < 0)
        lage = 0;

      if (lup < 0)
        lup = 0;

      if (max_bytes)
        snprintf(sused, sizeof (sused) - 1, "%.02f%%", used);
      else
        snprintf(sused, sizeof (sused) - 1, "unavail");

      std::string is = it->second["instance"];
      if (it->second["mode"]== "master")
        is+= "[W]";
      else
        is+= "[R]";
      
      if (ioformating)
        snprintf(line, sizeof (line) - 1, ioformat,
                 " ",
                 is.c_str(),
                 eos::common::StringConversion::GetReadableAgeString(age1, lage),
                 eos::common::StringConversion::GetReadableSizeString(space, max_bytes, "B"),
                 sused,
                 eos::common::StringConversion::GetSizeString(val9, nfsrw),
                 it->second["ns_files"].c_str(),
                 it->second["ns_container"].c_str(),
                 it->second["clients"].c_str(),
                 eos::common::StringConversion::GetSizeString(val5, ropen),
                 eos::common::StringConversion::GetSizeString(val6, wopen),
                 eos::common::StringConversion::GetSizeString(val1, diskout),
                 eos::common::StringConversion::GetSizeString(val2, diskin),
                 eos::common::StringConversion::GetSizeString(val3, ethout),
                 eos::common::StringConversion::GetSizeString(val4, ethin),
                 eos::common::StringConversion::GetSizeString(val7, rlock),
                 eos::common::StringConversion::GetSizeString(val8, wlock),
                 wlock
                 );
      else
        snprintf(line, sizeof (line) - 1, format,
                 " ",
                 it->second["instance"].c_str(),
                 eos::common::StringConversion::GetReadableAgeString(age1, lage),
                 it->second["host"].c_str(),
                 it->second["ip"].c_str(),
                 it->second["mode"].c_str(),
                 it->second["version"].c_str(),
                 eos::common::StringConversion::GetReadableAgeString(age2, lup),
                 eos::common::StringConversion::GetReadableSizeString(space, max_bytes, "B"),
                 sused,
                 eos::common::StringConversion::GetSizeString(val1, nfsrw),
                 eos::common::StringConversion::GetSizeString(val2, iops),
                 eos::common::StringConversion::GetSizeString(val3, bw),
                 it->second["ns_files"].c_str(),
                 it->second["ns_container"].c_str(),
                 it->second["clients"].c_str()
                 );

      out += line;
    }
  }
  else
  {
    for (auto it = mView.begin(); it != mView.end(); ++it)
    {
      if (it != mView.begin())
        out += "\n";
      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit)
      {
        if (sit != it->second.begin())
          out += " ";
        out += sit->first.c_str();
        out += "=";
        out += sit->second.c_str();
      }
    }
  }
  if (!monitoring)
  {
    out += "# ........................................................................................................................................................................................\n";
  }
}

EOSMGMNAMESPACE_END
