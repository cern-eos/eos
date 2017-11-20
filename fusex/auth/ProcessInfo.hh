// ----------------------------------------------------------------------
// File: ProcessInfo.hh
// Author: Georgios Bitzes - CERN
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

#ifndef __PROCESS_INFO_HH__
#define __PROCESS_INFO_HH__

#include <mutex>
#include <map>
#include <atomic>
#include "Utils.hh"
#include "auth/RmInfo.hh"

typedef int64_t Jiffies;

// Holds information about a specific process.
// Stat information (pid, ppid, sid, starttime) must be present for such an
// object to be considered non-empty.
class ProcessInfo
{
public:
  ProcessInfo() : empty(true), pid(0), ppid(0), pgrp(0), sid(0), startTime(-1),
    flags(0) {}

  // Fill stat information as obtained from /proc/<pid>/stat
  void fillStat(pid_t pid, pid_t ppid, pid_t pgrp, pid_t sid, Jiffies startTime,
                unsigned int flags)
  {
    if (!empty) {
      THROW("ProcessInfo stat information can only be filled once");
    }

    empty = false;
    this->pid = pid;
    this->ppid = ppid;
    this->pgrp = pgrp;
    this->sid = sid;
    this->startTime = startTime;
    this->flags = flags;
  }

  bool isSameProcess(const ProcessInfo& other)
  {
    if (pid != other.pid || startTime != other.startTime) {
      return false;
    }

    return true;
  }

  // Certain information can change over the lifetime of a process, such as ppid
  // (parent dying and PID 1 taking over), or sid.
  // This function updates the current object to the new information, if and
  // only if it can be guaranteed they both refer to the same process.
  // (ie same pid, same start time)
  // Return value: false if they're not the same process, true otherwise.
  bool updateIfSameProcess(const ProcessInfo& src)
  {
    if (empty || src.empty) {
      THROW("updateIfSameProcess can only be used on filled ProcessInfo objects.");
    }

    if (!isSameProcess(src)) {
      return false;
    }

    ppid = src.ppid;
    sid = src.sid;
    return true;
  }

  // Fill cmdline information as obtained from /proc/<pid>/cmdline
  void fillCmdline(const std::vector<std::string>& contents)
  {
    cmd = contents;
    cmdStr = join(cmd, " ");
  }

  void fillExecutablePath(const std::string& path)
  {
    executablePath = path;
  }

  void fillRmInfo()
  {
    rmInfo = RmInfo(executablePath, cmd);
  }

  bool isEmpty() const
  {
    return empty;
  }

  pid_t getPid() const
  {
    return pid;
  }

  pid_t getParentId() const
  {
    return ppid;
  }

  pid_t getGroupLeader() const
  {
    return pgrp;
  }

  pid_t getSid() const
  {
    return sid;
  }

  Jiffies getStartTime() const
  {
    return startTime;
  }

  const std::vector<std::string>& getCmd() const
  {
    return cmd;
  }

  std::string getCmdStr() const
  {
    return cmdStr;
  }

  unsigned int getFlags() const
  {
    return flags;
  }

  std::string getExecPath() const
  {
    return executablePath;
  }

  const struct RmInfo& getRmInfo() const {
    return rmInfo;
  }

private:
  bool empty;
  RmInfo rmInfo;

// TODO(gbitzes): Make these private once ProcessInfoProvider is implemented
public:
  // from /proc/<pid>/stat
  pid_t pid;
  pid_t ppid;
  pid_t pgrp;
  pid_t sid;
  Jiffies startTime;
  unsigned int flags;

  // from /proc/<pid>/cmdline
  std::vector<std::string> cmd;
  std::string cmdStr; // TODO(gbitzes): remove this eventually?
  std::string executablePath;
};

// Parses the contents of /proc/<pid>/stat, converting it to a ProcessInfo
class ProcessInfoProvider
{
public:
  void inject(pid_t pid, const ProcessInfo& info);

  // retrieves information about a process from the kernel.
  // Does not fill cmdline, thus only reading a single file.
  bool retrieveBasic(pid_t pid, ProcessInfo& ret);

  // retrieves information about a process from the kernel, including cmdline.
  bool retrieveFull(pid_t pid, ProcessInfo& ret);
  static bool fromString(const std::string& stat, const std::string& cmd,
                         ProcessInfo& ret);
private:
  std::mutex mtx;
  std::map<pid_t, ProcessInfo> injections;
  std::atomic<bool> useInjectedData {false};
  static bool parseStat(const std::string& stat, ProcessInfo& ret);
  static void parseCmdline(const std::string& cmdline, ProcessInfo& ret);
  static bool parseExec(pid_t pid, ProcessInfo& ret);
};

#endif
