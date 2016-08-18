/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "common/LinuxStat.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/RWMutex.hh"
#include "common/StringConversion.hh"
#include "common/Timing.hh"
#include "namespace/ns_on_redis/persistency/ContainerMDSvc.hh"
#include "namespace/ns_on_redis/persistency/FileMDSvc.hh"
#include "namespace/ns_on_redis/views/HierarchicalView.hh"
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

eos::common::RWMutex nslock;

//------------------------------------------------------------------------------
// File size mapping function
//------------------------------------------------------------------------------
static uint64_t
mapSize(const eos::IFileMD* /*file*/)
{
  return 0u;
}

//------------------------------------------------------------------------------
// Boot the namespace
//------------------------------------------------------------------------------
eos::IView*
bootNamespace(const std::map<std::string, std::string>& config)
{
  eos::IContainerMDSvc* contSvc = new eos::ContainerMDSvc();
  eos::IFileMDSvc* fileSvc = new eos::FileMDSvc();
  eos::IView* view = new eos::HierarchicalView();
  fileSvc->configure(config);
  contSvc->configure(config);
  fileSvc->setContMDService(contSvc);
  contSvc->setFileMDService(fileSvc);
  view->setContainerMDSvc(contSvc);
  view->setFileMDSvc(fileSvc);
  view->configure(config);
  view->getQuotaStats()->registerSizeMapper(mapSize);
  view->initialize();
  return view;
}

//------------------------------------------------------------------------------
// Close the namespace
//------------------------------------------------------------------------------
void
closeNamespace(eos::IView* view)
{
  eos::IContainerMDSvc* contSvc = view->getContainerMDSvc();
  eos::IFileMDSvc* fileSvc = view->getFileMDSvc();
  view->finalize();
  delete view;
  delete contSvc;
  delete fileSvc;
}

//------------------------------------------------------------------------------
// Print current namespace status
//------------------------------------------------------------------------------
void
PrintStatus(eos::IView* view, eos::common::LinuxStat::linux_stat_t* st1,
            eos::common::LinuxStat::linux_stat_t* st2,
            eos::common::LinuxMemConsumption::linux_mem_t* /*mem1*/,
            eos::common::LinuxMemConsumption::linux_mem_t* mem2,
            const double& rate, bool print_total = false)
{
  XrdOucString sizestring;
  XrdOucString stdOut;
  eos::IContainerMDSvc* contSvc = view->getContainerMDSvc();
  eos::IFileMDSvc* fileSvc = view->getFileMDSvc();
  unsigned long long f = 0, d = 0;

  if (print_total) {
    f = static_cast<unsigned long long>(fileSvc->getNumFiles());
    d = static_cast<unsigned long long>(contSvc->getNumContainers());
  }

  char files[256];
  snprintf(static_cast<char*>(files), sizeof(files) - 1, "%llu", f);
  char dirs[256];
  snprintf(static_cast<char*>(dirs), sizeof(dirs) - 1, "%llu", d);
  stdOut += "# -------------------------------------------------------------\n";
  stdOut += "ALL      Files                            ";
  stdOut += static_cast<char*>(files);
  stdOut += "\n";
  stdOut += "ALL      Directories                      ";
  stdOut += static_cast<char*>(dirs);
  stdOut += "\n";
  stdOut += "# -------------------------------------------------------------\n";
  stdOut += "ALL      memory virtual                   ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(
              sizestring, mem2->vmsize, "B");
  stdOut += "\n";
  stdOut += "ALL      memory resident                  ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(
              sizestring, mem2->resident, "B");
  stdOut += "\n";
  stdOut += "ALL      memory share                     ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(
              sizestring, mem2->share, "B");
  stdOut += "\n";
  stdOut += "ALL      memory growths                   ";
  stdOut += eos::common::StringConversion::GetReadableSizeString(
              sizestring, (st2->vsize - st1->vsize), "B");
  stdOut += "\n";
  stdOut += "# -------------------------------------------------------------\n";
  stdOut += "ALL      rate                             ";
  char srate[256];
  snprintf(static_cast<char*>(srate), sizeof(srate) - 1, "%.02f", rate);
  stdOut += static_cast<char*>(srate);
  stdOut += "\n";
  stdOut += "# -------------------------------------------------------------\n";
  fprintf(stderr, "%s", stdOut.c_str());
}

class RThread
{
public:
  RThread() = default;
  ~RThread() = default;
  RThread(size_t a, size_t b, size_t c, size_t d, eos::IView* iview,
          bool lock = false)
  {
    i = a;
    n_j = b;
    n_k = c;
    n_files = d;
    view = iview;
    dolock = lock;
  }

  size_t i;
  size_t n_j;
  size_t n_k;
  size_t n_files;
  bool dolock;
  eos::IView* view;
};

//----------------------------------------------------------------------------
// Start namespace consumer thread
//----------------------------------------------------------------------------
static void*
RunReader(void* tconf)
{
  RThread* r = static_cast<RThread*>(tconf);
  size_t i = r->i;
  size_t n_j = r->n_j;
  size_t n_k = r->n_k;
  size_t n_files = r->n_files;
  eos::IView* view = r->view;
  bool dolock = r->dolock;

  try {
    for (size_t j = 0; j < n_j; j++) {
      for (size_t k = 0; k < n_k; k++) {
        for (size_t n = 0; n < n_files; n++) {
          char s_file_path[1024];
          snprintf(static_cast<char*>(s_file_path), sizeof(s_file_path) - 1,
                   "/eos/nsbench/level_0_%08u/"
                   "level_1_%08u/level_2_%08u/file____________________%08u",
                   static_cast<unsigned int>(i), static_cast<unsigned int>(j),
                   static_cast<unsigned int>(k), static_cast<unsigned int>(n));
          std::string file_path = static_cast<char*>(s_file_path);

          if (dolock) {
            nslock.LockRead();
          }

          std::shared_ptr<eos::IFileMD> fmd = view->getFile(file_path);

          if (fmd) {
            unsigned long long size = fmd->getSize();
            (void) size;
          }

          if (dolock) {
            nslock.UnLockRead();
          }
        }
      }
    }
  } catch (eos::MDException& e) {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Main function
//----------------------------------------------------------------------------
int
main(int argc, char** argv)
{
  // Check up the commandline params
  if (argc != 5) {
    std::cerr << "Usage:" << std::endl;
    std::cerr << "  eos-namespace-benchmark <redis_host> <redis_port> "
              << "<level1-dirs> <level3-files> " << std::endl;
    return 1;
  }

  std::map<std::string, std::string> config = {{"redis_host", argv[1]},
    {"redis_port", argv[2]}
  };
  size_t n_i = std::stoi(argv[3]);
  size_t n_j = 64;
  size_t n_k = 64;
  size_t n_files = std::stoi(argv[4]);

  // Create Namespace and populate dirs
  try {
    std::cerr << "# ***********************************************************"
              << std::endl;
    std::cerr << "[i] Initialize Directory Namespace..." << std::endl;
    std::cerr << "# ***********************************************************"
              << std::endl;
    eos::IView* view = bootNamespace(config);
    eos::common::LinuxStat::linux_stat_t st[10];
    ;
    eos::common::LinuxMemConsumption::linux_mem_t mem[10];
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("directories");
    COMMONTIMING("dir-start", &tm);

    for (size_t i = 0; i < n_i; i++) {
      fprintf(stderr, "# Level %02u\n", static_cast<unsigned int>(i));
      XrdOucString l = "dir-level-";
      l += static_cast<int>(i);
      COMMONTIMING(l.c_str(), &tm);

      for (size_t j = 0; j < n_j; j++) {
        for (size_t k = 0; k < n_k; k++) {
          char s_container_path[1024];
          snprintf(static_cast<char*>(s_container_path), sizeof(s_container_path) - 1,
                   "/eos/nsbench/level_0_%08u/level_1_%08u/level_2_%08u/",
                   static_cast<unsigned int>(i), static_cast<unsigned int>(j),
                   static_cast<unsigned int>(k));
          std::string container_path = static_cast<char*>(s_container_path);
          std::shared_ptr<eos::IContainerMD> cont =
            view->createContainer(container_path, true);
          cont->setAttribute("sys.forced.blocksize", "4k");
          cont->setAttribute("sys.forced.checksum", "adler");
          cont->setAttribute("sys.forced.layout", "replica");
          cont->setAttribute("sys.forced.nstripes", "2");
          cont->setAttribute("user.acl",
                             "u:atlas003:rw,egroup:atlas-comp-cern-storage-support:rw");
          view->updateContainerStore(cont.get());
        }
      }
    }

    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop", &tm);
    tm.Print();
    double rate = (n_i * n_j * n_k) / tm.RealTime() * 1000.0;
    PrintStatus(view, &st[0], &st[1], &mem[0], &mem[1], rate);
    closeNamespace(view);
  } catch (eos::MDException& e) {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  // Fill namespace with files
  try {
    std::cerr << "# ***********************************************************"
              << std::endl;
    std::cerr << "[i] Initialize File Namespace ..." << std::endl;
    std::cerr << "# ***********************************************************"
              << std::endl;
    eos::IView* view = bootNamespace(config);
    eos::common::LinuxStat::linux_stat_t st[10];
    ;
    eos::common::LinuxMemConsumption::linux_mem_t mem[10];
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("files");
    COMMONTIMING("dir-start", &tm);

    for (size_t i = 0; i < n_i; i++) {
      fprintf(stderr, "# Level %02u\n", static_cast<unsigned int>(i));
      XrdOucString l = "dir-level-";
      l += static_cast<int>(i);
      COMMONTIMING(l.c_str(), &tm);

      for (size_t j = 0; j < n_j; j++) {
        for (size_t k = 0; k < n_k; k++) {
          for (size_t n = 0; n < n_files; n++) {
            char s_file_path[1024];
            snprintf(static_cast<char*>(s_file_path), sizeof(s_file_path) - 1,
                     "/eos/nsbench/level_0_%08u/"
                     "level_1_%08u/level_2_%08u/file____________________%08u",
                     static_cast<unsigned int>(i), static_cast<unsigned int>(j),
                     static_cast<unsigned int>(k), static_cast<unsigned int>(n));
            std::string file_path = static_cast<char*>(s_file_path);
            std::shared_ptr<eos::IFileMD> fmd =
              view->createFile(file_path, 0, 0);
            // add two locations
            fmd->addLocation(k);
            fmd->addLocation(k + 1);
            // fmd->addLocation(k+2);
            // fmd->addLocation(k+3);
            // fmd->addLocation(k+4);
            // fmd->addLocation(k+5);
            fmd->setLayoutId(10);
            view->updateFileStore(fmd.get());
          }
        }
      }
    }

    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("dir-stop", &tm);
    tm.Print();
    double rate = (n_files * n_i * n_j * n_k) / tm.RealTime() * 1000.0;
    PrintStatus(view, &st[0], &st[1], &mem[0], &mem[1], rate);
    closeNamespace(view);
  } catch (eos::MDException& e) {
    std::cerr << "[!] Error: " << e.getMessage().str() << std::endl;
    return 2;
  }

  eos::IView* view = nullptr;
  // Run a parallel consumer thread benchmark without locking
  {
    eos::common::LinuxStat::linux_stat_t st[10];
    ;
    eos::common::LinuxMemConsumption::linux_mem_t mem[10];
    std::cerr << "# ***********************************************************"
              << std::endl;
    std::cerr << "[i] Parallel reader benchmark without locking  ..."
              << std::endl;
    std::cerr << "# ***********************************************************"
              << std::endl;
    view = bootNamespace(config);
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("reading");
    COMMONTIMING("read-start", &tm);
    pthread_t tid[1024];

    // fire threads
    for (size_t i = 0; i < n_i; i++) {
      fprintf(stderr, "# Level %02u\n", static_cast<unsigned int>(i));
      RThread r(i, n_j, n_k, n_files, view);
      XrdSysThread::Run(&tid[i], RunReader, static_cast<void*>(&r),
                        XRDSYSTHREAD_HOLD, "Reader Thread");
    }

    // join them
    for (size_t i = 0; i < n_i; i++) {
      XrdSysThread::Join(tid[i], nullptr);
    }

    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("read-stop", &tm);
    tm.Print();
    double rate = (n_files * n_i * n_j * n_k) / tm.RealTime() * 1000.0;
    PrintStatus(view, &st[0], &st[1], &mem[0], &mem[1], rate);
  }
  // Run a parallel consumer thread benchmark with namespace locking
  {
    eos::common::LinuxStat::linux_stat_t st[10];
    ;
    eos::common::LinuxMemConsumption::linux_mem_t mem[10];
    std::cerr << "# ***********************************************************"
              << std::endl;
    std::cerr << "[i] Parallel reader benchmark with locking  ..." << std::endl;
    std::cerr << "# ***********************************************************"
              << std::endl;
    eos::common::LinuxStat::GetStat(st[0]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[0]);
    eos::common::Timing tm("reading");
    COMMONTIMING("read-lock-start", &tm);
    pthread_t tid[1024];

    // fire threads
    for (size_t i = 0; i < n_i; i++) {
      fprintf(stderr, "# Level %02u\n", static_cast<unsigned int>(i));
      RThread r(i, n_j, n_k, n_files, view, true);
      XrdSysThread::Run(&tid[i], RunReader, static_cast<void*>(&r),
                        XRDSYSTHREAD_HOLD, "Reader Thread");
    }

    // join them
    for (size_t i = 0; i < n_i; i++) {
      XrdSysThread::Join(tid[i], nullptr);
    }

    eos::common::LinuxStat::GetStat(st[1]);
    eos::common::LinuxMemConsumption::GetMemoryFootprint(mem[1]);
    COMMONTIMING("read-lock-stop", &tm);
    tm.Print();
    double rate = (n_files * n_i * n_j * n_k) / tm.RealTime() * 1000.0;
    PrintStatus(view, &st[0], &st[1], &mem[0], &mem[1], rate, true);
  }
  return 0;
}
