// ----------------------------------------------------------------------
// File: LinuxStat.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                  *
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

/**
 * @file   LinuxStat.hh
 * 
 * @brief  Class getting process statistics from the linux proc filesystem
 *  
 */

#ifndef __EOSCOMMON__LINUXSTAT__HH
#define __EOSCOMMON__LINUXSTAT__HH

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Static Class to measure memory consumption
//! 
//! Example: linux_stat_t st; GetStat(st);
//! 
/*----------------------------------------------------------------------------*/
class LinuxStat {
public:
  typedef struct {
    // TODO! in 2.6. kernels we see 3 more entries (unknown for the meanwhile)
    unsigned long long pid;
    char tcomm[PATH_MAX];
    char state;
    unsigned long long ppid;
    unsigned long long pgid;
    unsigned long long sid;
    unsigned long long tty_nr;
    unsigned long long tty_pgrp;
    unsigned long long flags;
    unsigned long long min_flt;

    unsigned long long cmin_flt;
    unsigned long long maj_flt;
    unsigned long long cmaj_flt;
    unsigned long long utime;
    unsigned long long stime;
    unsigned long long cutime;
    unsigned long long cstime;
    unsigned long long priority;
    unsigned long long nicev;
    unsigned long long threads;

    unsigned long long it_real_value;
    unsigned long long start_time;
    unsigned long long vsize;
    unsigned long long rss;
    unsigned long long rsslim;
    unsigned long long start_code;
    unsigned long long end_code;
    unsigned long long start_stack;
    unsigned long long esp;
    unsigned long long eip;

    unsigned long long pending;
    unsigned long long blocked;
    unsigned long long sigign;
    unsigned long long sigcatch;
    unsigned long long wchan;
    unsigned long long zero1;
    unsigned long long zero2;
    unsigned long long exit_signal;
    unsigned long long cpu;
    unsigned long long rt_priority;

    unsigned long long policy;

  } linux_stat_t;

  static bool GetStat(linux_stat_t& result)
  {
    const char* stat_path = "/proc/self/stat";
    result.tcomm[0]=0; result.state=0;
    result.pid=result.ppid=result.pgid=result.sid=result.tty_nr=result.tty_pgrp=result.flags=result.min_flt=result.cmin_flt=result.maj_flt=result.cmaj_flt=result.utime=result.stime=result.cutime=result.cstime=result.priority=result.nicev=result.threads=result.it_real_value=result.start_time=result.vsize=result.rss=result.rsslim=result.start_code=result.end_code=result.start_stack=result.esp=result.eip=result.pending=result.blocked=result.sigign=result.sigcatch=result.wchan=result.zero1=result.zero2=result.exit_signal=result.cpu=result.rt_priority=result.policy=0;


    FILE *f = fopen(stat_path,"r");
    if(!f){
      perror(stat_path);
      return false;
    }
    if(41 != fscanf(f,"%lld %s %c %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld\n",
		    &result.pid,result.tcomm,&result.state,&result.ppid,&result.pgid,&result.sid,&result.tty_nr,&result.tty_pgrp,&result.flags,&result.min_flt,&result.cmin_flt,&result.maj_flt,&result.cmaj_flt,&result.utime,&result.stime,&result.cutime,&result.cstime,&result.priority,&result.nicev,&result.threads,&result.it_real_value,&result.start_time,&result.vsize,&result.rss,&result.rsslim,&result.start_code,&result.end_code,&result.start_stack,&result.esp,&result.eip,&result.pending,&result.blocked,&result.sigign,&result.sigcatch,&result.wchan,&result.zero1,&result.zero2,&result.exit_signal,&result.cpu,&result.rt_priority,&result.policy))
      {
	perror(stat_path);
	return false;
      }
    fclose(f);
    return true;
  }
};

EOSCOMMONNAMESPACE_END
 
#endif
