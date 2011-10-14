// ----------------------------------------------------------------------
// File: XrdMqTiming.hh
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

#ifndef __MQ__TIMING__HH__
#define __MQ__TIMING__HH__

#include <sys/time.h>

class XrdMqTiming {
public:
  struct timeval tv;
  XrdOucString tag;
  XrdOucString maintag;
  XrdMqTiming* next;
  XrdMqTiming* ptr;

  XrdMqTiming(const char* name, struct timeval &i_tv) {
    memcpy(&tv, &i_tv, sizeof(struct timeval));
    tag = name;
    next = 0;
    ptr  = this;
  }
  XrdMqTiming(const char* i_maintag) {
    tag = "BEGIN";
    next = 0;
    ptr  = this;
    maintag = i_maintag;
  }

  void Print() {
    char msg[512];
    XrdMqTiming* p = this->next;
    XrdMqTiming* n; 

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

  virtual ~XrdMqTiming(){XrdMqTiming* n = next; if (n) delete n;};
};

#define TIMING( __ID__,__LIST__)                                        \
  do {                                                                    \
    struct timeval tp;                                                 \
    struct timezone tz;                                                \
    gettimeofday(&tp, &tz);                                            \
    (__LIST__)->ptr->next=new XrdMqTiming(__ID__,tp);                  \
    (__LIST__)->ptr = (__LIST__)->ptr->next;                           \
  } while(0);                                                             \
 
#endif
