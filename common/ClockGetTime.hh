#ifdef __MACH__
#include <mach/mach_time.h>
#ifndef CLOCK_REALTIME
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
#define CLOCK_REALTIME_COARSE 0

int clock_gettime(int clk_id, struct timespec *t){
  mach_timebase_info_data_t timebase;
  mach_timebase_info(&timebase);
  uint64_t time;
  time = mach_absolute_time();
  double nseconds = ((double)time * (double)timebase.numer)/((double)timebase.denom);
  double seconds = ((double)time * (double)timebase.numer)/((double)timebase.denom * 1e9);
  t->tv_sec = seconds;
  t->tv_nsec = nseconds;
  return 0;
}
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#endif
#else
#include <time.h>
#endif
