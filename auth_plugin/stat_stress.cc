// -----------------------------------------------------------------------------
// File: stat_stress.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
// -----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include <cstdio>
#include <iostream>
#include <string>
#include <sys/time.h>
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! The stat_stress program tries to do as many stat requests against a XRootD
//! server as possible, measuring the maximum rate.
//------------------------------------------------------------------------------
struct Params
{
  int index;
  int duration;
  std::string path;
  XrdCl::URL url;

  Params( int i, int d, std::string p, XrdCl::URL u):
      index(i),
      duration(d),
      path(p),
      url(u)
  { };
};


void* thread_function(void* arg)
{
  struct Params* param = static_cast<struct Params*>(arg);
  struct timeval begin;
  struct timeval current;
  gettimeofday(&begin, NULL);
  XrdCl::StatInfo* stat_resp = 0;
  XrdCl::XRootDStatus status;
  int count = 0;

  struct timeval stat_start, stat_end;
  unsigned long long diff;
  float sum_time = 0;
  
  while (1)
  {
    // Using synchrnous stat operation
    XrdCl::FileSystem fs(param->url);
    gettimeofday(&stat_start, NULL);
    status = fs.Stat(param->path, stat_resp, 5);
    gettimeofday(&stat_end, NULL);
    diff = (stat_end.tv_sec * 1000000 + stat_end.tv_usec) -
           (stat_start.tv_sec * 1000000 + stat_start.tv_usec);

    sum_time += diff;
    std::cout << "Stat time: " << diff << " microseconds" << std::endl;
    
    /*
    if (status.IsOK())
    {
      std::cout << "Id: " << stat_resp->GetId() << " size: " << stat_resp->GetSize()
                << " mod_time: " << stat_resp->GetModTimeAsString() << std::endl;
    }
    else
    {
      std::cout << "Stat failed" << std::endl;
    }
    */

    count++;
    
    if (count % 10 == 0)
    {
      gettimeofday(&current, NULL);

      if (current.tv_sec - begin.tv_sec > param->duration)
      {
        //std::cout << "Duration time expired ... exiting with count:"
        //          << count << std::endl;
        break;
      }     
    }    
  }

  if (stat_resp)
    delete stat_resp;

  std::cout << "Avg response time is:" << (sum_time/count) << std::endl;
  pthread_exit(new int(count));
}


//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  std::string location;
  int duration = 0;
  int num_threads = 0;
  
  if (argc != 4)
  {
    std::cerr << "Usage: ./stat_stress <xrd_path> <run_duration> <num_threads>" << std::endl;
    exit(1);    
  }

  location = argv[1];
  duration = atoi(argv[2]);
  num_threads = atoi(argv[3]);

  std::string address = location.substr(0, location.rfind("//"));
  std::string path = location.substr(location.rfind("//") + 1);

  if (address.empty() || path.empty())
  {
    std::cerr << "Xrd path has to be of the form: root://host.cern.ch//dir1/file1.dat" << std::endl;
    exit(1);    
  }
  
  std::cout << "Address is: " << address << " path is: " << path << std::endl;
  XrdCl::URL orig_url(address);

  if (!orig_url.IsValid())
  {
    std::cerr << "XRootD server address is invalid." << std::endl;
    exit(1);
  }

  // Set Xrdcl TimeoutResolution to 1
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt( "TimeoutResolution", 1 );
  
  std::vector<pthread_t> thread_ids;
  std::vector<struct Params*> vect_param;

  // Start all the threads
  for (int i = 0; i < num_threads; i++)
  {
    struct Params* thread_arg = new Params(i, duration, path, orig_url);
    vect_param.push_back(thread_arg);
    pthread_t tid;
    pthread_create(&tid, NULL, thread_function, static_cast<void*>(vect_param.back()));
    thread_ids.push_back(tid);
  }

  uint64_t total_count = 0;
  int* count = 0;
  
  // Collect the results and join all the threads
  for (auto it = thread_ids.begin(); it != thread_ids.end(); it++)
  {
    pthread_join(*it, (void**)(&count));
    total_count += *count;
    
    //std::cout << "Thread requests: " << (int)*count << " avg per thread: "
    //          << (float)(*count) / duration << " req/s" << std::endl;
    delete count;
  }

  std::cout << " Total requests: " << total_count
            << " avg total: " << (float)(total_count) / duration
            << " req/s" << std::endl;
}
  
