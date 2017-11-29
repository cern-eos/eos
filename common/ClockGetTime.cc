#include "common/ClockGetTime.hh"

#ifdef __MACH__

int _clock_gettime(clockid_t clk_id, struct timespec *t) {
  if (clock_gettime == 0)
  {
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
  else
  {
    return clock_gettime(clk_id, t);
  }
}
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#endif
