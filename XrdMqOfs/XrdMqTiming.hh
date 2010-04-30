#ifndef __MQ__TIMING__HH
#define __MQ__TIMING__HH

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
    next = NULL;
    ptr  = this;
  }
  XrdMqTiming(const char* i_maintag) {
    tag = "BEGIN";
    next = NULL;
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
