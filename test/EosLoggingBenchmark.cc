// ----------------------------------------------------------------------
// File: EosLoggingBenchmark.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/wait.h>
/*-----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/StringConversion.hh"
/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
#include <string>
#include <iostream>
#include <thread>
#include <stdio.h>
/*-----------------------------------------------------------------------------*/

using namespace std;

// 1GB mem buffer

#define NTHREADS 1024
#define NMESSAGES 2000

double realtimes[NTHREADS][NMESSAGES];

int nosaturation = 0;

void threadlog(int id)
{
  std::string message;
  message = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_";
  message += std::to_string(id);

  double realtime = 0.0;
  for (size_t i=0; i< NMESSAGES; i++) {
    eos::common::Timing tm("Checksumming");
    COMMONTIMING("START", &tm);
    eos_static_info("%.4f %s", realtime, message.c_str());
    COMMONTIMING("STOP", &tm);
    realtime = tm.RealTime();
    realtimes[id][i] = realtime;
    if (nosaturation) {
      usleep(40000);
    }
  }
}

int main(int argc, char* argv[])
{
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.SetUnit("eoschecksumbenchmark@localhost");
  g_logging.gShortFormat = true;
  g_logging.SetLogPriority(LOG_DEBUG);

  if (argc==1) {
    fprintf(stdout, "#running in saturation mode\n");
  } else {
    nosaturation = true;
    fprintf(stdout, "#running in non-saturation mode\n");
  }

  FILE* fp = fopen("/var/tmp/eoslogbench.fan.log", "a+");

  if (fp) {
    g_logging.AddFanOut("#", fp);
  }


  FILE* fstderr = 0;

  if (!(fstderr = freopen("/var/tmp/eoslogbench.log", "a+", stderr))) {
    fprintf(stderr,"error: cannot open test log file /var/tmp/eoslogbench.log");
    exit(-1);
  }
  std::vector<std::thread*> threads;

  eos::common::Timing tm("Messaging");
  COMMONTIMING("START", &tm);

  for (size_t i= 0; i< NTHREADS; i++) {
    threads.push_back(new std::thread(threadlog, i));
  }

  for (size_t i =0 ; i< NTHREADS; i++) {
    threads[i]->join();
  }

  for (size_t i =0 ; i< NTHREADS; i++) {
    delete threads[i];
  }

  double min, max, avg;
  min = 1000000;
  max = 0;
  avg = 0;

  for (size_t i = 0; i< NTHREADS; i++) {
    for (size_t m = 0 ; m<NMESSAGES; m++) {
      if (realtimes[i][m] < min) {
        min = realtimes[i][m];
      }
      if (realtimes[i][m] > max) {
	max = realtimes[i][m];
      }
      avg += realtimes[i][m];
    }
  }
  avg /= (NTHREADS*NMESSAGES);

  COMMONTIMING("STOP", &tm);

  fprintf(stdout,"duration: %.02f [s] min: %.04f [ms] max: %.04f [ms] avg: %.04f [ms] nmsg: %d rate: %.02f [Hz] \n", tm.RealTime()/1000.0, min, max, avg, NTHREADS*NMESSAGES, NTHREADS*NMESSAGES / tm.RealTime()*1000);

  g_logging.shutDown(true);        /* gracefully, while files are still open */
  fclose(fstderr);
  fclose(fp);
}
