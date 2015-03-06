// ----------------------------------------------------------------------
// File: ProcCacheTest.cc
// Author: Geoffray Adde - CERN
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

#include "fuse/ProcCache.hh"

using namespace std;

int main ()
{
  ProcCache pc;
  //auto pid = getpid();
  // ********************
  // TEST ON ALL RUNNING PROCESSES
  // ********************
#if 0
  for (int pid = 1; pid < 32768; pid++)
  {
    //cout << " My pid is " << pid << endl;
    if (!pc.InsertEntry (pid))
    {
      //cout << "failed to insert the entry" << endl;
      //return 1;
    }
    else
    {
      auto entry = pc.GetEntry (pid);
      if (entry->HasError ())
      {
        //cout << "entry has error! error message is : " << entry->GetErrorMessage() <<endl;
        //return 1;
      }
      cout << "pid is : " << pid << endl;
      cout << "command line is : " << entry->GetArgsStr () << endl;
      cout << "-------------------" << endl;
//    cout << " environment is : " << endl;
//    std::map<std::string,std::string> env;
//    entry->GetEnv(env);
//    for(auto it = env.begin(); it != env.end(); it++)
//    {
//      cout << it->first << " = " << it->second << endl;
//    }
    }
  }
#endif

  const int niter = 1e6;

  std::cout << "// ********************" << std::endl;
  std::cout << "// BENCHMARK WITH STARTUP TIME" << std::endl;
  std::cout << "// ********************" << std::endl;
  auto pid = getpid ();
  {
    eos::common::Timing tm ("With timestamp");
    COMMONTIMING("START", &tm);
    for (int i = 0; i < niter; i++)
    {
      if (pc.InsertEntry (pid,false))
      {
        cout << "Failed to insert entry for self pid" << endl;
        return 1;
      }
      std::string PWD;
      if (!pc.GetEntry (pid)->GetEnv ("PWD", PWD))
      {
        cout << "Failed to get environment variable PWD" << endl;
        cout << " environment is : " << endl;
        std::map<std::string, std::string> env;
        pc.GetEntry (pid)->GetEnv (env);
        for (auto it = env.begin (); it != env.end (); it++)
        {
          cout << it->first << " = " << it->second << endl;
        }
        return 1;
      }
    }
    COMMONTIMING("STOP", &tm);
    tm.Print ();
    cout << "time per iteration : "
        << tm.GetTagTimelapse ("START", "STOP") * 1000.0 / niter << "us"
        << std::endl;
  }

  std::cout << "// ********************" << std::endl;
  std::cout << "// BENCHMARK WITHOUT STARTUP TIME" << std::endl;
  std::cout << "// ********************" << std::endl;
  {
    eos::common::Timing tm ("Without timestamp");
    COMMONTIMING("START", &tm);
    if (pc.InsertEntry (pid,false))
    {
      cout << "Failed to insert entry for self pid" << endl;
      return 1;
    }
    for (int i = 0; i < niter; i++)
    {
      std::string PWD;
      if (!pc.GetEntry (pid)->GetEnv ("PWD", PWD))
      {
        cout << "Failed to get environment variable PWD" << endl;
        cout << " environment is : " << endl;
        std::map<std::string, std::string> env;
        pc.GetEntry (pid)->GetEnv (env);
        for (auto it = env.begin (); it != env.end (); it++)
        {
          cout << it->first << " = " << it->second << endl;
        }
        return 1;
      }
    }
    COMMONTIMING("STOP", &tm);
    tm.Print ();
    cout << "time per iteration : "
        << tm.GetTagTimelapse ("START", "STOP") * 1000.0 / niter << "us"
        << std::endl;
  }

  return 0;
}
