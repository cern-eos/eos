#include <time.h>

#ifdef __MACH__
#include <mach/mach_time.h>
#ifndef CLOCK_REALTIME
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
#define CLOCK_REALTIME_COARSE 0
#define clockid_t int
#define clock_gettime _clock_gettime
#endif 

int _clock_gettime(clockid_t clk_id, struct timespec *t);
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#else
#define _clock_gettime clock_gettime
#endif
