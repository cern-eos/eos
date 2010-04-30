//         $Id: XrdxCommonTiming.hh,v 1.1 2008/09/15 10:04:02 apeters Exp $

#ifndef __XRDCOMMON__TIMING__HH
#define __XRDCOMMON__TIMING__HH

#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include <sys/time.h>

class XrdxCommonTiming {
public:
  struct timeval tv;
  XrdOucString tag;
  XrdOucString maintag;
  XrdxCommonTiming* next;
  XrdxCommonTiming* ptr;

  XrdxCommonTiming(const char* name, struct timeval &i_tv) {
    memcpy(&tv, &i_tv, sizeof(struct timeval));
    tag = name;
    next = NULL;
    ptr  = this;
  }
  XrdxCommonTiming(const char* i_maintag) {
    tag = "BEGIN";
    next = NULL;
    ptr  = this;
    maintag = i_maintag;
  }

  void Print(XrdOucTrace &trace) {
    char msg[512];
    if (!(trace.What & 0x8000)) 
      return;
    XrdxCommonTiming* p = this->next;
    XrdxCommonTiming* n; 
    trace.Beg("Timing");

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
    trace.End();
  }

  virtual ~XrdxCommonTiming(){XrdxCommonTiming* n = next; if (n) delete n;};
};

#define TIMING(__trace__, __ID__,__LIST__)                              \
if (__trace__.What & TRACE_debug)                                       \
do {                                                                    \
     struct timeval tp;                                                 \
     struct timezone tz;                                                \
     gettimeofday(&tp, &tz);                                            \
     (__LIST__)->ptr->next=new XrdxCommonTiming(__ID__,tp);             \
     (__LIST__)->ptr = (__LIST__)->ptr->next;                           \
} while(0);                                                             \
 
#endif
