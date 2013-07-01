//------------------------------------------------------------------------------
// Copyright (c) 2012 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
//------------------------------------------------------------------------------
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
#include <iostream>
#include "common/Timing.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "common/StringConversion.hh"
#include "common/RWMutex.hh"
#include "common/ulib/hash_align.h"

//------------------------------------------------------------------------------
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <google/dense_hash_map>
#include <string>
#include <map>

eos::common::RWMutex nslock;
XrdSysMutex nsmutex;

std::map<long long , long long> stdmap;
google::dense_hash_map<long long, long long> googlemap;
ulib::align_hash_map<long long, long long> ulibmap;

std::map<std::string, double> results;
std::map<std::string, long long> results_mem;

//------------------------------------------------------------------------------
// Print current status
//------------------------------------------------------------------------------
void PrintStatus( eos::common::LinuxStat::linux_stat_t &st1, eos::common::LinuxStat::linux_stat_t &st2, eos::common::LinuxMemConsumption::linux_mem_t &mem1, eos::common::LinuxMemConsumption::linux_mem_t &mem2, double &rate) {
  XrdOucString stdOut;
  XrdOucString sizestring;
  stdOut+="# ------------------------------------------------------------------------------------\n";
  stdOut+="# ------------------------------------------------------------------------------------\n";
  stdOut+="ALL      memory virtual                   ";stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long)mem2.vmsize,"B"); stdOut += "\n";
  stdOut+="ALL      memory resident                  ";stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long)mem2.resident,"B"); stdOut += "\n";
  stdOut+="ALL      memory share                     ";stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long)mem2.share,"B"); stdOut += "\n";
  stdOut+="ALL      memory growths                   ";stdOut += eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long)(st2.vsize-st1.vsize),"B"); stdOut += "\n";
  stdOut+="# ------------------------------------------------------------------------------------\n";
  stdOut+="ALL      rate                             "; char srate[256]; snprintf(srate,sizeof(srate)-1,"%.02f",rate); stdOut += srate; stdOut += "\n";
  stdOut+="# ------------------------------------------------------------------------------------\n";
  fprintf(stderr,"%s", stdOut.c_str());
}


class RThread {
public:
  RThread() {};
  RThread(size_t a, size_t b, size_t t, size_t nt, bool lock=false) { i = a; n_files = b; type = t; threads = nt; dolock=lock;}
  ~RThread() {};
  size_t i;
  size_t n_files;
  size_t type;
  size_t threads;
  bool dolock;
};


//----------------------------------------------------------------------------
// start hash consumer thread
//----------------------------------------------------------------------------

static void* RunReader(void* tconf)
{

  RThread* r = (RThread*) tconf;

  size_t i = r->i;
  size_t n_files = r->n_files;
  bool dolock = r->dolock;

  for (size_t n = 1+i; n<= n_files; n+=r->threads) {
    // if (dolock)nsmutex.Lock();
    if (dolock) nslock.LockRead();
    if (r->type == 0) {
      long long v = stdmap[n];
      v = 1;
    }
    if (r->type == 1) {
      long long v = googlemap[n];
      v = 1;
    }
    if (r->type == 2) {
      long long v = ulibmap[n];
      v = 1;
    }

    // if (dolock)nsmutex.UnLock();
    if (dolock) nslock.UnLockRead();
  }
    
  return 0;
}

int main( int argc, char **argv )
{
  googlemap.set_deleted_key( -1 );
  googlemap.set_empty_key( 0 );
  //----------------------------------------------------------------------------
  // Check up the commandline params
  //----------------------------------------------------------------------------
  if( argc != 3 )
  {
    std::cerr << "Usage:"                                << std::endl;
    std::cerr << "  eos-has-benchmark <entries> <threads>" << std::endl;
    return 1;
  };

  size_t n_files = atoi(argv[1]);
  size_t n_i = atoi(argv[2]);

  {
    std::cerr << "# **********************************************************************************" << std::endl;
    std::cerr << "[i] Initialize Hash STL ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("hash-start",&tm);

    for (size_t i = 1; i<= n_files; i++) {
      if (!(i%1000000)) {
	XrdOucString l = "level-"; l += (int)i;
	COMMONTIMING(l.c_str(), &tm);
      }
      // fill the hash
      stdmap[i] = i;
    }
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop",&tm);
    tm.Print();

    double rate = (n_files)/ tm.RealTime()*1000.0;

    results["001 Fill STL            Hash"]=rate;
    results_mem["001 Fill STL            Hash"]=st[1].vsize-st[0].vsize;
    PrintStatus(st[0],st[1],mem[0],mem[1],rate);
  }

  {
    std::cerr << "# **********************************************************************************" << std::endl;
    std::cerr << "[i] Initialize Hash GOOGLE DENSE ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("hash-start",&tm);

    for (size_t i = 1; i<= n_files; i++) {
      if (!(i%1000000)) {
	XrdOucString l = "level-"; l += (int)i;
	COMMONTIMING(l.c_str(), &tm);
      }
      // fill the hash
      googlemap[i] = i;
    }
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop",&tm);
    tm.Print();

    double rate = (n_files)/ tm.RealTime()*1000.0;
    results["002 Fill Google         Hash"]=rate;
    results_mem["002 Fill Google         Hash"]=st[1].vsize-st[0].vsize;
    PrintStatus(st[0],st[1],mem[0],mem[1],rate);
  }

  {
    std::cerr << "# **********************************************************************************" << std::endl;
    std::cerr << "[i] Initialize Hash ULIB..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("hash-start",&tm);

    for (size_t i = 1; i<= n_files; i++) {
      if (!(i%1000000)) {
	XrdOucString l = "level-"; l += (int)i;
	COMMONTIMING(l.c_str(), &tm);
      }
      // fill the hash
      ulibmap[i] = i;
    }
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop",&tm);
    tm.Print();

    double rate = (n_files)/ tm.RealTime()*1000.0;
    results["003 Fill Ulib           Hash"]=rate;
    results_mem["003 Fill Ulib           Hash"]=st[1].vsize-st[0].vsize;
    PrintStatus(st[0],st[1],mem[0],mem[1],rate);
  }

  //----------------------------------------------------------------------------
  // Run a parallel consumer thread benchmark without locking
  //----------------------------------------------------------------------------
  for (size_t t = 0; t < 3; t++ )
  {
    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Parallel reader benchmark without locking ";
    if (t == 0) 
      std::cerr << "STL-MAP"; 
    if (t == 1)
      std::cerr << "GOOGLE-DENSE";
    if (t == 2)
      std::cerr << "ULIB-MAP";
    std::cerr <<  std::endl;

    std::cerr << "# **********************************************************************************" << std::endl;    
    
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("reading");
    
    COMMONTIMING("read-start",&tm);
    
    pthread_t tid[1024];
    
    // fire threads
    for (size_t i = 0; i< n_i; i++) {
      fprintf(stderr,"# Level %02u\n", (unsigned int)i);
      RThread r(i,n_files, t, n_i);
      XrdSysThread::Run(&tid[i], RunReader, static_cast<void *>(&r),XRDSYSTHREAD_HOLD, "Reader Thread");
    }
    
    // join them
    for (size_t i = 0; i< n_i; i++) {
      XrdSysThread::Join(tid[i],NULL);
    }
    
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("read-stop",&tm);
    tm.Print();
    
    double rate = (n_files)/ tm.RealTime()*1000.0;
    if (t==0)
      results["003 Read no-lock STL    Hash"]=rate;
    if (t==1)
      results["004 Read no-lock Google Hash"]=rate;
    if (t==2)
      results["005 Read no-lock Ulib   Hash"]=rate;

    PrintStatus(st[0],st[1],mem[0],mem[1],rate);
  }

  //----------------------------------------------------------------------------
  // Run a parallel consumer thread benchmark with namespace locking
  //----------------------------------------------------------------------------
  for (size_t t = 0; t < 3; t++ )
  {
    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Parallel reader benchmark with locking ";
    if (t == 0) 
      std::cerr << "STL-MAP"; 
    if (t == 1)
      std::cerr << "GOOGLE-DENSE";
    if (t == 2)
      std::cerr << "ULIB-MAP";
    std::cerr <<  std::endl;

    std::cerr << "# **********************************************************************************" << std::endl;        
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("reading");
    
    COMMONTIMING("read-lock-start",&tm);
    
    pthread_t tid[1024];
    
    // fire threads
    for (size_t i = 0; i< n_i; i++) {
      fprintf(stderr,"# Level %02u\n", (unsigned int)i);
      RThread r(i,n_files, t, n_i, true);
      XrdSysThread::Run(&tid[i], RunReader, static_cast<void *>(&r),XRDSYSTHREAD_HOLD, "Reader Thread");
    }
    
    // join them
    for (size_t i = 0; i< n_i; i++) {
      XrdSysThread::Join(tid[i],NULL);
    }
    
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("read-lock-stop",&tm);
    tm.Print();
    
    double rate = (n_files )/ tm.RealTime()*1000.0;
    if (t==0)
      results["006 Read lock    STL    Hash"]=rate;
    if (t==1)
      results["007 Read lock    Google Hash"]=rate;
    if (t==2)
      results["008 Read lock    Ulib   Hash"]=rate ;
    
    PrintStatus(st[0],st[1],mem[0],mem[1],rate);
  }

  fprintf(stdout,"=====================================================================\n");
  fprintf(stdout,"--------------------- SUMMARY ---------------------------------------\n");
  fprintf(stdout,"=====================================================================\n");
  int i=0;
  for (auto it = results.begin(); it != results.end(); it++) {
    if (!(i%3)) {
      fprintf(stdout,"----------------------------------------------------\n");
    }
    if (i<3) {
      fprintf(stdout,"%s rate: %.02f MHz mem-overhead: %.02f %%\n", it->first.c_str(), it->second/1000000.0, 1.0*results_mem[it->first]/(n_files*16));
    } else {
      fprintf(stdout,"%s rate: %.02f MHz\n", it->first.c_str(), it->second/1000000.0);
    }
    i++;
  }
  fprintf(stdout,"====================================================\n");
}
