#pragma once

#include "common/VirtualIdentity.hh"
#include "common/AssistedThread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "XrdOuc/XrdOucString.hh"
#include "mgm/DynamicECFile.hh"
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include <atomic>
#include <memory>
#include <mutex>

namespace qclient
{
class QClient;
}


EOSMGMNAMESPACE_BEGIN

class DynamicScanner
{
public:

  struct Options {
    bool enabled;                  //< Is FileInspector even enabled?
    std::chrono::seconds
    interval; //< Run FileInsepctor cleanup every this many seconds
  };
  Options getOptions();
  DynamicScanner();
  virtual ~DynamicScanner();
  void Run(ThreadAssistant& assistant) noexcept;
  void Stop();
  void performCycleQDB(ThreadAssistant& assistant) noexcept;
private:

  bool enabled()
  {
    return (mEnabled.load()) ? true : false;
  }
  bool disable()
  {
    if (!enabled()) {
      return false;
    } else {
      mEnabled.store(0, std::memory_order_seq_cst);
      return true;
    }
  }
  bool enable()
  {
    if (enabled()) {
      return false;
    } else {
      mEnabled.store(1, std::memory_order_seq_cst);
      return true;
    }
  }


  std::map<uint64_t, std::map<std::string, uint64_t>> lastScanStats;
  std::map<uint64_t, std::map<std::string, uint64_t>> currentScanStats;
  std::map<std::string, std::set<uint64_t>> lastFaultyFiles;
  std::map<std::string, std::set<uint64_t>> currentFaultyFiles;

  std::atomic<double> scanned_percent;

  std::atomic<int> mEnabled;

  std::map<uint64_t, std::shared_ptr<eos::QuarkFileMD>> statusFiles;
  time_t timeCurrentScan;
  time_t timeLastScan;
  void Process(std::string& filepath);
  void Process(std::shared_ptr<eos::IFileMD> fmd);
  AssistedThread mThread; ///< thread id of the creation background tracker
  std::unique_ptr<qclient::QClient> mQcl;
  uint64_t nfiles;
  uint64_t ndirs;

  std::mutex mutexScanStats;
};

EOSMGMNAMESPACE_END
