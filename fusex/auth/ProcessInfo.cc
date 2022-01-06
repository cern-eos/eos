// ----------------------------------------------------------------------
// File: ProcessInfo.cc
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <unistd.h>
#include "ProcessInfo.hh"
#include "common/Logging.hh"

bool ProcessInfoProvider::fromString(const std::string& procstat,
                                     const std::string& cmdline, ProcessInfo& ret)
{
  if (!ret.isEmpty()) {
    THROW("The ProcessInfo object must be empty for this function to fill!");
  }

  if (!parseStat(procstat, ret)) {
    return false;
  }

  parseCmdline(cmdline, ret);
  return true;
}

// Reference:
// Table 1-4: Contents of the stat files (as of 2.6.30-rc7)
// ..............................................................................
//  Field          Content
//   pid           process id
//   tcomm         filename of the executable
//   state         state (R is running, S is sleeping, D is sleeping in an
//                 uninterruptible wait, Z is zombie, T is traced or stopped)
//   ppid          process id of the parent process
//   pgrp          pgrp of the process
//   sid           session id
//   tty_nr        tty the process uses
//   tty_pgrp      pgrp of the tty
//   flags         task flags
//   min_flt       number of minor faults
//   cmin_flt      number of minor faults with child's
//   maj_flt       number of major faults
//   cmaj_flt      number of major faults with child's
//   utime         user mode jiffies
//   stime         kernel mode jiffies
//   cutime        user mode jiffies with child's
//   cstime        kernel mode jiffies with child's
//   priority      priority level
//   nice          nice level
//   num_threads   number of threads
//   it_real_value  (obsolete, always 0)
//   start_time    time the process started after system boot
//   vsize         virtual memory size
//   rss           resident set memory size
//   rsslim        current limit in bytes on the rss
//   start_code    address above which program text can run
//   end_code      address below which program text can run
//   start_stack   address of the start of the main process stack
//   esp           current value of ESP
//   eip           current value of EIP
//   pending       bitmap of pending signals
//   blocked       bitmap of blocked signals
//   sigign        bitmap of ignored signals
//   sigcatch      bitmap of caught signals
//   0             (place holder, used to be the wchan address, use /proc/PID/wchan instead)
//   0             (place holder)
//   0             (place holder)
//   exit_signal   signal to send to parent thread on exit
//   task_cpu      which CPU the task is scheduled on
//   rt_priority   realtime priority
//   policy        scheduling policy (man sched_setscheduler)
//   blkio_ticks   time spent waiting for block IO
//   gtime         guest time of the task in jiffies
//   cgtime        guest time of the task children in jiffies
//   start_data    address above which program data+bss is placed
//   end_data      address below which program data+bss is placed
//   start_brk     address above which program heap can be expanded with brk()
//   arg_start     address above which program command line is placed
//   arg_end       address below which program command line is placed
//   env_start     address above which program environment is placed
//   env_end       address below which program environment is placed
//   exit_code     the thread's exit_code in the form reported by the waitpid system call
// ..............................................................................

bool ProcessInfoProvider::parseStat(const std::string& procstat,
                                    ProcessInfo& ret)
{
  if (!ret.isEmpty()) {
    THROW("The ProcessInfo object must be empty for this function to fill!");
  }

  // variables to assist with parsing
  bool inParenth = false;
  size_t tokenCount = 0;
  bool success = false;
  // variables in which to store results
  pid_t pid;
  pid_t ppid;
  pid_t pgrp;
  pid_t sid;
  Jiffies startTime;
  unsigned flags;

  // let's parse
  for (size_t i = 0; i < procstat.size(); i++) {
    // be careful, process names can have all kinds of combinations of () in it !
    // we will fail parsing if a process ends with a name like ') <X> ' where <X> = R|S|Z|D|T and there is a space of <X>
    if (procstat[i] == '(') {
      inParenth = true;
      continue;
    }

    if (procstat[i] == ')' &&
        procstat[i + 1] == ' ' &&
        procstat[i + 3] == ' ' &&
        ((procstat[i + 2] == 'R') ||
         (procstat[i + 2] == 'S') ||
         (procstat[i + 2] == 'Z') ||
         (procstat[i + 2] == 'D') ||
         (procstat[i + 2] == 'T'))) {
      if (!inParenth) {
        return false; // parse error
      }

      inParenth = false;
    }

    // start of a token, use scanf if we're interested in it
    if (!inParenth && (procstat[i] == ' ' || i == 0)) {
      switch (tokenCount) {
      case 0: {
        if (!sscanf(procstat.c_str() + i, "%u", &pid)) {
          return false;
        }

        break;
      }

      case 3: {
        if (!sscanf(procstat.c_str() + i, "%u", &ppid)) {
          return false;
        }

        break;
      }

      case 4: {
        if (!sscanf(procstat.c_str() + i, "%u", &pgrp)) {
          return false;
        }

        break;
      }

      case 5: {
        if (!sscanf(procstat.c_str() + i, "%u", &sid)) {
          return false;
        }

        break;
      }

      case 8: {
        if (!sscanf(procstat.c_str() + i, "%u", &flags)) {
          return false;
        }

        break;
      }

      case 21: {
        if (!sscanf(procstat.c_str() + i, "%" PRId64, &startTime)) {
          return false;
        }

        success = true;
        break;
      }
      }

      tokenCount++;
    }
  }

  if (!success) {
    return false;
  }

  ret.fillStat(pid, ppid, pgrp, sid, startTime, flags);
  return true;
}

void ProcessInfoProvider::parseCmdline(const std::string& cmdline,
                                       ProcessInfo& ret)
{
  if (cmdline.empty()) {
    return;
  }

  ret.fillCmdline(split_on_nullbyte(cmdline));
}

void ProcessInfoProvider::inject(pid_t pid, const ProcessInfo& info)
{
  std::lock_guard<std::mutex> lock(mtx);
  useInjectedData = true;
  injections[pid] = info;
}

bool ProcessInfoProvider::retrieveBasic(pid_t pid, ProcessInfo& ret)
{
  if (useInjectedData) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = injections.find(pid);

    if (it == injections.end()) {
      return false;
    }

    ret = it->second;
    // Keep the same behavior as when reading from /proc, don't give out the
    // cmdline even if the injection contains it
    ret.fillCmdline({});
    return true;
  }

  std::string procstat;

  if (!readFile(SSTR("/proc/" << pid << "/stat"), procstat)) {
    return false;
  }

  if (!parseStat(procstat, ret)) {
    return false;
  }

  if (pid != ret.getPid()) {
    eos_static_crit("Hell has frozen over, /proc/%d/stat contained information for a different pid: %d",
                    pid, ret.getPid());
    return false;
  }

  return true;
}

bool ProcessInfoProvider::retrieveFull(pid_t pid, ProcessInfo& ret)
{
  if (useInjectedData) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = injections.find(pid);

    if (it == injections.end()) {
      return false;
    }

    ret = it->second;
    ret.fillRmInfo();
    return true;
  }

  if (!retrieveBasic(pid, ret)) {
    return false;
  }

  std::string cmdline;

  if (!readFile(SSTR("/proc/" << pid << "/cmdline"), cmdline)) {
    // This is a valid case, if for example, the calling PID is actually
    // a kernel thread.
    return true;
  }

  parseCmdline(cmdline, ret);
  parseExec(pid, ret);
  ret.fillRmInfo();
  // Read path of /proc/<pid>/exe
  std::string exePath = SSTR("/proc/" << pid << "/exe");
  char buffer[1024];
  ssize_t outcome = readlink(exePath.c_str(), buffer, 1024);

  if (outcome > 0) {
    ret.exe = std::string(buffer, outcome);
  }

  return true;
}

bool ProcessInfoProvider::parseExec(pid_t pid, ProcessInfo& ret)
{
  const size_t BUFF_SIZE = 8096;
  char buffer[BUFF_SIZE];
  ssize_t len = readlink(SSTR("/proc/" << pid << "/exe").c_str(), buffer,
                         BUFF_SIZE - 2);

  if (len == -1) {
    return false;
  }

  ret.fillExecutablePath(std::string(buffer, len));
  return true;
}
