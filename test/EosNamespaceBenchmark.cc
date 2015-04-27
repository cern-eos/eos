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
#include "namespace/views/HierarchicalView.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/persistency/ChangeLogFileMDSvc.hh"
#include "common/Timing.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "common/StringConversion.hh"
#include "common/RWMutex.hh"
//------------------------------------------------------------------------------
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <string>


eos::common::RWMutex nslock;

//------------------------------------------------------------------------------
// File size mapping function
//------------------------------------------------------------------------------
static uint64_t mapSize( const eos::IFileMD *file )
{
  return 0;
}

//------------------------------------------------------------------------------
// Boot the namespace
//------------------------------------------------------------------------------
eos::IView *bootNamespace( const std::string &dirLog,
                           const std::string &fileLog )
  throw( eos::MDException )
{
  eos::IContainerMDSvc *contSvc = new eos::ChangeLogContainerMDSvc();
  eos::IFileMDSvc      *fileSvc = new eos::ChangeLogFileMDSvc();
  eos::IView           *view    = new eos::HierarchicalView();

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;
  std::map<std::string, std::string> settings;
  contSettings["changelog_path"] = dirLog;
  fileSettings["changelog_path"] = fileLog;

  fileSvc->configure( fileSettings );
  contSvc->configure( contSettings );

  ((eos::ChangeLogFileMDSvc*)fileSvc)->setContainerService( (eos::ChangeLogContainerMDSvc*)contSvc );

  view->setContainerMDSvc( contSvc );
  view->setFileMDSvc( fileSvc );

  
  view->configure( settings );
  view->getQuotaStats()->registerSizeMapper( mapSize );
  view->initialize();
  return view;
}

//------------------------------------------------------------------------------
// Close the namespace
//------------------------------------------------------------------------------
void closeNamespace( eos::IView *view ) throw( eos::MDException )
{
  eos::IContainerMDSvc *contSvc = view->getContainerMDSvc();
  eos::IFileMDSvc      *fileSvc = view->getFileMDSvc();
  view->finalize();
  delete view;
  delete contSvc;
  delete fileSvc;
}

//------------------------------------------------------------------------------
// Print current namespace status
//------------------------------------------------------------------------------
void PrintStatus( eos::IView *view, const char* f2, const char* f1, eos::common::LinuxStat::linux_stat_t &st1, eos::common::LinuxStat::linux_stat_t &st2, eos::common::LinuxMemConsumption::linux_mem_t &mem1, eos::common::LinuxMemConsumption::linux_mem_t &mem2, double &rate) {
  XrdOucString clfsize;
  XrdOucString cldsize;
  XrdOucString clfratio;
  XrdOucString cldratio;
  XrdOucString sizestring;
  struct stat statf;
  struct stat statd;
  XrdOucString stdOut;

  eos::IContainerMDSvc *contSvc = view->getContainerMDSvc();
  eos::IFileMDSvc      *fileSvc = view->getFileMDSvc();

  unsigned long long f = (unsigned long long)fileSvc->getNumFiles();
  unsigned long long d = (unsigned long long)contSvc->getNumContainers();

  // statistic for the changelog files
  if ( (!::stat(f1, &statf)) && (!::stat(f2, &statd)) ) {
    eos::common::StringConversion::GetReadableSizeString(clfsize,(unsigned long long)statf.st_size,"B");
    eos::common::StringConversion::GetReadableSizeString(cldsize,(unsigned long long)statd.st_size,"B");
    eos::common::StringConversion::GetReadableSizeString(clfratio,(unsigned long long) f?(1.0*statf.st_size)/f:0 ,"B");
    eos::common::StringConversion::GetReadableSizeString(cldratio,(unsigned long long) d?(1.0*statd.st_size)/d:0 ,"B");
  }

  char files[256]; snprintf(files,sizeof(files)-1,"%llu", f);
  char dirs[256]; snprintf(dirs,sizeof(dirs)-1,"%llu",d);
  stdOut+="# ------------------------------------------------------------------------------------\n";
          
  stdOut+="ALL      Files                            ";stdOut += files; stdOut += "\n";
          
  stdOut+="ALL      Directories                      ";stdOut += dirs;  stdOut+="\n";
  stdOut+="# ....................................................................................\n";
  stdOut+="ALL      File Changelog Size              ";stdOut += clfsize; stdOut += "\n";
  stdOut+="ALL      Dir  Changelog Size              ";stdOut += cldsize; stdOut += "\n";
  stdOut+="# ....................................................................................\n";
  stdOut+="ALL      avg. File Entry Size             ";stdOut += clfratio; stdOut += "\n";
  stdOut+="ALL      avg. Dir  Entry Size             ";stdOut += cldratio; stdOut += "\n";
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
  RThread(size_t a, size_t b, size_t c, size_t d, eos::IView *iview, bool lock=false) { i = a; n_j = b; n_k = c; n_files = d; view = iview;dolock=lock;}
  ~RThread() {};
  size_t i;
  size_t n_j;
  size_t n_k;
  size_t n_files;
  bool dolock;
  eos::IView* view;
};


//----------------------------------------------------------------------------
// start namespace consumer thread
//----------------------------------------------------------------------------

static void* RunReader(void* tconf)
{

  RThread* r = (RThread*) tconf;

  size_t i = r->i;
  size_t n_j = r->n_j;
  size_t n_k = r->n_k;
  size_t n_files = r->n_files;
  eos::IView *view = r->view;
  bool dolock = r->dolock;

  try 
  {
    for (size_t j = 0; j< n_j; j++) {
      for (size_t k = 0; k< n_k; k++) {
	for (size_t n = 0; n< n_files; n++) {
	  char s_file_path[1024];
	  snprintf(s_file_path,sizeof(s_file_path)-1,"/eos/nsbench/level_0_%08u/"
                   "level_1_%08u/level_2_%08u/file____________________%08u",\
                   (unsigned int)i,(unsigned int)j,(unsigned int)k, (unsigned int)n);
	  std::string file_path = s_file_path;
	  if (dolock) nslock.LockRead();
	  eos::IFileMD* fmd = view->getFile(file_path);
	  if (fmd) {
	    unsigned long long size = (unsigned long long) fmd->getSize();
	    if (size == 0 ) {
	      size = 1;
	    }
	  }
	  if (dolock) nslock.UnLockRead();
	}
      }
    }
  }   
  catch( eos::MDException &e )
  {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 0;
  }

    
  return 0;
}

int main( int argc, char **argv )
{
  //----------------------------------------------------------------------------
  // Check up the commandline params
  //----------------------------------------------------------------------------
  if( argc != 5 )
  {
    std::cerr << "Usage:"                                << std::endl;
    std::cerr << "  eos-namespace-benchmark directory.log file.log <level1-dirs> <level3-files> " << std::endl;
    return 1;
  };

  // remove ns
  unlink (argv[1]);
  unlink (argv[2]);

  size_t n_i = atoi(argv[3]);
  size_t n_j = 256;
  size_t n_k = 256;
  size_t n_files = atoi(argv[4]);
  

  //----------------------------------------------------------------------------
  // Create Namespace and populate dirs
  //----------------------------------------------------------------------------
  try
  {
    std::cerr << "# **********************************************************************************" << std::endl;
    std::cerr << "[i] Initialize Directory Namespace..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    
    eos::IView *view = bootNamespace( argv[1], argv[2] );

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("dir-start",&tm);

    for (size_t i = 0; i< n_i; i++) {
      fprintf(stderr,"# Level %02u\n", (unsigned int)i);
      XrdOucString l = "dir-level-"; l += (int)i;
      COMMONTIMING(l.c_str(), &tm);
      for (size_t j = 0; j< n_j; j++) {
	for (size_t k = 0; k< n_k; k++) {
	  char s_container_path[1024];
	  snprintf(s_container_path,sizeof(s_container_path)-1,"/eos/nsbench/level_0_%08u/level_1_%08u/level_2_%08u/",(unsigned int)i,(unsigned int)j,(unsigned int)k);
	  std::string container_path = s_container_path;
	  eos::IContainerMD* cont = view->createContainer( container_path, true );
	  cont->setAttribute("sys.forced.blocksize","4k");
	  cont->setAttribute("sys.forced.checksum","adler");
	  cont->setAttribute("sys.forced.layout","replica");
	  cont->setAttribute("sys.forced.nstripes","2");
	  cont->setAttribute("user.acl","u:atlas003:rw,egroup:atlas-comp-cern-storage-support:rw");
	  view->updateContainerStore(cont);
	}
      }
    }
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop",&tm);
    tm.Print();

    double rate = (n_i * n_j * n_k)/ tm.RealTime()*1000.0;

    PrintStatus(view, argv[1], argv[2], st[0],st[1],mem[0],mem[1],rate);
    closeNamespace( view );
  }
  catch( eos::MDException &e )
  {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  //----------------------------------------------------------------------------
  // Reboot Namespace from scratch
  //----------------------------------------------------------------------------
  try
  {

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("boot-start",&tm);
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Boot Directory namespace  ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    
    eos::IView *view = bootNamespace( argv[1], argv[2] );

    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("boot-stop",&tm);
    tm.Print();

    double rate = (n_i * n_j * n_k)/ tm.RealTime()*1000.0;

    PrintStatus(view, argv[1], argv[2], st[0],st[1],mem[0],mem[1],rate);
  }
  catch( eos::MDException &e )
  {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  //----------------------------------------------------------------------------
  // Fill namespace with files
  //----------------------------------------------------------------------------
  try
  {
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Initialize File Namespace ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    
    eos::IView *view = bootNamespace( argv[1], argv[2] );

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("dir-start",&tm);

    for (size_t i = 0; i< n_i; i++) {
      fprintf(stderr,"# Level %02u\n", (unsigned int)i);
      XrdOucString l = "dir-level-"; l += (int)i;
      COMMONTIMING(l.c_str(), &tm);
      for (size_t j = 0; j< n_j; j++) {
	for (size_t k = 0; k< n_k; k++) {
	  for (size_t n = 0; n< n_files; n++) {
	    char s_file_path[1024];
	    snprintf(s_file_path,sizeof(s_file_path)-1,"/eos/nsbench/level_0_%08u/"
                     "level_1_%08u/level_2_%08u/file____________________%08u",
                     (unsigned int)i,(unsigned int)j,(unsigned int)k, (unsigned int)n);
	    std::string file_path = s_file_path;
	    eos::IFileMD* fmd = view->createFile(file_path, 0,0);
	    // add two locations
	    fmd->addLocation(k);
	    fmd->addLocation(k+1);
	    /*	    fmd->addLocation(k+2);
	    fmd->addLocation(k+3);
	    fmd->addLocation(k+4);
	    fmd->addLocation(k+5);*/
	    fmd->setLayoutId(10);
	    view->updateFileStore(fmd);
	  }
	}
      }
    }
    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop",&tm);
    tm.Print();

    double rate = (n_files* n_i * n_j * n_k)/ tm.RealTime()*1000.0;

    PrintStatus(view, argv[1], argv[2], st[0],st[1],mem[0],mem[1],rate);
    closeNamespace( view );
  }
  catch( eos::MDException &e )
  {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  eos::IView* view = 0;

  //----------------------------------------------------------------------------
  // Reboot Namespace from scratch
  //----------------------------------------------------------------------------
  try
  {

    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 

    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");

    COMMONTIMING("boot-start",&tm);
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Boot File+Directory namespace  ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    
    view = bootNamespace( argv[1], argv[2] );

    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("boot-stop",&tm);
    tm.Print();

    double rate = (n_files * n_i * n_j * n_k)/ tm.RealTime()*1000.0;

    PrintStatus(view, argv[1], argv[2], st[0],st[1],mem[0],mem[1],rate);
  }
  catch( eos::MDException &e )
  {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  //----------------------------------------------------------------------------
  // Run a parallel consumer thread benchmark without locking
  //----------------------------------------------------------------------------
  {
    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Parallel reader benchmark without locking  ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;    
    
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("reading");
    
    COMMONTIMING("read-start",&tm);
    
    pthread_t tid[1024];
    
    // fire threads
    for (size_t i = 0; i< n_i; i++) {
      fprintf(stderr,"# Level %02u\n", (unsigned int)i);
      RThread r(i,n_j,n_k,n_files, view);
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
    
    double rate = (n_files* n_i * n_j * n_k)/ tm.RealTime()*1000.0;
    
    PrintStatus(view, argv[1], argv[2], st[0],st[1],mem[0],mem[1],rate);
  }

  //----------------------------------------------------------------------------
  // Run a parallel consumer thread benchmark with namespace locking
  //----------------------------------------------------------------------------
  {
    eos::common::LinuxStat::linux_stat_t st[10];; 
    eos::common::LinuxMemConsumption::linux_mem_t mem[10]; 
    std::cerr << "# **********************************************************************************" << std::endl;    
    std::cerr << "[i] Parallel reader benchmark with locking  ..." << std::endl;
    std::cerr << "# **********************************************************************************" << std::endl;        
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("reading");
    
    COMMONTIMING("read-lock-start",&tm);
    
    pthread_t tid[1024];
    
    // fire threads
    for (size_t i = 0; i< n_i; i++) {
      fprintf(stderr,"# Level %02u\n", (unsigned int)i);
      RThread r(i,n_j,n_k,n_files, view, true);
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
    
    double rate = (n_files* n_i * n_j * n_k)/ tm.RealTime()*1000.0;
    
    PrintStatus(view, argv[1], argv[2], st[0],st[1],mem[0],mem[1],rate);
  }
  return 0;
}
