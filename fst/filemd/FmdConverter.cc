#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"

#include "common/async/OpaqueFuture.hh"
#include "common/async/ExecutorMgr.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include "common/utils/XrdUtils.hh"
#include "fst/utils/FSPathHandler.hh"
#include "fst/utils/FTSWalkTree.hh"
#include "fst/utils/StdFSWalkTree.hh"

EOSFSTNAMESPACE_BEGIN

using eos::common::ExecutorType;
//------------------------------------------------------------------------------
// Check if file system was already converted
//------------------------------------------------------------------------------
bool
FileFSConversionDoneHandler::isFSConverted(std::string_view fstpath)
{
  return std::filesystem::exists(getDoneFilePath(fstpath));
}

//------------------------------------------------------------------------------
// Mark file system as converted
//------------------------------------------------------------------------------
bool
FileFSConversionDoneHandler::markFSConverted(std::string_view fstpath)
{
  std::ofstream f(getDoneFilePath(fstpath));
  return f.good();
}

//------------------------------------------------------------------------------
// Construct conversion done file path based on the mountpoint
//------------------------------------------------------------------------------
std::string
FileFSConversionDoneHandler::getDoneFilePath(std::string_view fstpath)
{
  std::string path {fstpath};

  if (!eos::common::endsWith(fstpath, "/")) {
    path += "/";
  }

  path += mConversionDoneFile;
  return path;
}


//------------------------------------------------------------------------------
// Class FmdConverter
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FmdConverter::FmdConverter(FmdHandler* src_handler,
                           FmdHandler* tgt_handler,
                           size_t per_disk_pool) :
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutorMgr(std::make_shared<common::ExecutorMgr>(ExecutorType::kThreadPool,
               per_disk_pool)),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{}

FmdConverter::FmdConverter(FmdHandler* src_handler, FmdHandler* tgt_handler,
                           size_t per_disk_pool, std::string_view executor_type):
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutorMgr(std::make_shared<common::ExecutorMgr>(executor_type,
               per_disk_pool)),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{
}

FmdConverter::FmdConverter(FmdHandler* src_handler, FmdHandler* tgt_handler,
                           std::shared_ptr<common::ExecutorMgr> executor_mgr):
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutorMgr(executor_mgr),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{
}

FmdConverter::~FmdConverter()
{
  eos_static_info("%s", "msg=\"calling FmdConverter destructor\"");
  // drain all pending tasks
  //mExecutor.reset();
  eos_static_info("%s", "msg=\"calling FmdConverter destructor done\"");
}

//------------------------------------------------------------------------------
// Conversion method
//------------------------------------------------------------------------------
bool
FmdConverter::Convert(eos::common::FileSystem::fsid_t fsid, std::string path)
{
  auto fid = eos::common::FileId::PathToFid(path.c_str());

  if (!fsid || !fid) {
    eos_static_info("msg=\"conversion failed invalid fid\" file=%s, fid=%08llx",
                    path.c_str(), fid);
    return false;
  }

  bool status = mTgtFmdHandler->ConvertFrom(fid, fsid, mSrcFmdHandler,
                                            true, &path);
  eos_static_info("msg=\"conversion done\" file=%s, fid=%08llx, status=%d",
                  path.data(), fid, status);
  return status;
}


//------------------------------------------------------------------------------
// Method converting files on a given file system
//------------------------------------------------------------------------------
void
FmdConverter::ConvertFS(std::string_view fspath,
                        eos::common::FileSystem::fsid_t fsid)
{
  if (mDoneHandler->isFSConverted(fspath)) {
    return;
  }

  if (!mSrcFmdHandler || !mTgtFmdHandler) {
    eos_static_crit("%s", "msg=\"failed fs conversion due to null handers\"");
    return;
  }

  LoadConfigFromEnv(fsid);

  using future_vector = std::vector<eos::common::OpaqueFuture<bool>>;
  future_vector futures;
  eos_static_info("msg=\"starting file system conversion\" fsid=%u", fsid);
  std::error_code ec;
  size_t success_count = 0;
  mConversionCounter.Init();
  stdfs::WalkFSTree(std::string(fspath),
      [this, fsid, &futures, &success_count](std::string path) {
    try {
      auto fut = mExecutorMgr->PushTask([this, fsid, path = std::move(path)]() {
        return this->Convert(fsid, std::move(path));
      });
      futures.emplace_back(std::move(fut));
      success_count += DrainFutures(futures, fsid);
    } catch (const std::exception& e) {
      eos_static_crit("msg=\"failed to push task\" fsid=%u err=%s", fsid,
                      e.what());
    }
  }, ec);

  success_count += DrainFutures(futures, fsid, true);
  if (ec) {
    eos_static_err("msg=\"walking fs tree ran into errors!\" err=%s",
                   ec.message().c_str());
    return;
  }

  eos_static_info("msg=\"conversion successful, set done marker\" count=%llu "
                  "success_count=%llu frequency=%0.02f kHz",
                  mTotalFiles, success_count,
                  mConversionCounter.GetFrequency()/1000.0);
  mDoneHandler->markFSConverted(fspath);
}

//------------------------------------------------------------------------------
// Helper method for file system conversion
//------------------------------------------------------------------------------
void
FmdConverter::ConvertFS(std::string_view fspath)
{
  ConvertFS(fspath, FSPathHandler::GetFsid(fspath));
}

uint64_t
FmdConverter::DrainFutures(std::vector<common::OpaqueFuture<bool>>& futures,
                           eos::common::FileSystem::fsid_t fsid,
                           bool force)
{
  uint64_t success_count = 0;

  if (force ||
      futures.size() > mPerDiskQueueSize) {
      for (auto && fut : futures) {
        try {
          success_count += fut.getValue();
          ++mTotalFiles;
        } catch (const std::exception& e) {
           eos_static_crit("msg=\"failed to get value\" err=%s, fsid=%u", e.what(), fsid);
        }
      }

    mConversionCounter.Increment(mTotalFiles);
    futures.clear();
    unsigned int wait_ctr {0};
    while (mExecutorMgr->GetQueueSize() > mGlobalQueueSize) {
      eos_static_info("msg=\"waiting for FmdConverter queue to drain\" "
                      "fsid=%u wait_ctr=%u", fsid, ++wait_ctr);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    if (wait_ctr || mTotalFiles % (5*mPerDiskQueueSize) == 0) {
      LogConversionProgress(fsid);
    }
   }

  return success_count;
}

void
FmdConverter::LoadConfigFromEnv(eos::common::FileSystem::fsid_t fsid)
{
  mPerDiskQueueSize = common::XrdUtils::GetEnv("EOS_FMD_PER_FS_QUEUE_SIZE",
                                       FMD_PER_FS_QUEUE_SIZE);
  mGlobalQueueSize = common::XrdUtils::GetEnv("EOS_FMD_GLOBAL_QUEUE_SIZE",
                                      FMD_GLOBAL_QUEUE_SIZE);
  eos_static_info("msg=\"loading FmdConverter config:\" "
                  "fsid=%u per_disk_queue_size=%lu global_queue_size=%lu",
                  fsid, mPerDiskQueueSize, mGlobalQueueSize);
}

void FmdConverter::LogConversionProgress(eos::common::FileSystem::fsid_t fsid)
{
  eos_static_info("msg=\"conversion frequency\" fsid=%u frequency=%0.02f kHz last_frequency=%0.02f kHz",
                  fsid, mConversionCounter.GetFrequency()/1000.0,
                  mConversionCounter.GetLastFrequency()/1000.0);
}

EOSFSTNAMESPACE_END
