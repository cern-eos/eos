// ----------------------------------------------------------------------
// File: Timing.hh
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

//         $Id: Timing.hh,v 1.1 2008/09/15 10:04:02 apeters Exp $

#ifndef __EOSCOMMON__TIMING__HH
#define __EOSCOMMON__TIMING__HH

#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "common/Namespace.hh"
#include <sys/time.h>

EOSCOMMONNAMESPACE_BEGIN

class Timing {
public:
  struct timeval tv;
  XrdOucString tag;
  XrdOucString maintag;
  Timing* next;
  Timing* ptr;

  Timing(const char* name, struct timeval &i_tv) {
    memcpy(&tv, &i_tv, sizeof(struct timeval));
    tag = name;
    next = 0;
    ptr  = this;
  }
  Timing(const char* i_maintag) {
    tag = "BEGIN";
    next = 0;
    ptr  = this;
    maintag = i_maintag;
  }

  void Print() {
    char msg[512];
    Timing* p = this->next;
    Timing* n; 
    cerr << std::endl;
    while ((n =p->next)) {

      sprintf(msg,"                                        [%12s] %12s<=>%-12s : %.03f\n",maintag.c_str(),p->tag.c_str(),n->tag.c_str(), (float)((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0);
      cerr << msg;
      p = n;
    }
    n = p;
    p = this->next;
    sprintf(msg,"                                        =%12s= %12s<=>%-12s : %.03f\n",maintag.c_str(),p->tag.c_str(), n->tag.c_str(), (float)((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0);
    cerr << msg;
  }

  virtual ~Timing(){Timing* n = next; if (n) delete n;};


  static void GetTimeSpec(struct timespec &ts) {
#ifdef __APPLE__
    struct timeval tv;
    gettimeofday(&tv, 0);
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
  }    
};

#define TIMING( __ID__,__LIST__)                                \
  do {								\
    struct timeval tp;						\
    struct timezone tz;						\
    gettimeofday(&tp, &tz);					\
    (__LIST__)->ptr->next=new eos::common::Timing(__ID__,tp);	\
    (__LIST__)->ptr = (__LIST__)->ptr->next;			\
  } while(0);							


EOSCOMMONNAMESPACE_END
 
#endif
