#ifndef EOS_COMMON_NFS_LOG_SINK_HH
#define EOS_COMMON_NFS_LOG_SINK_HH

// ************************************************************************
// * EOS - the CERN Disk Storage System                                   *
// * Copyright (C) 2026 CERN/Switzerland                                  *
// ************************************************************************

#include "common/Logging.hh"

#include <spdlog/sinks/base_sink.h>

#include <mutex>
#include <string>

namespace eos::common {

//! spdlog sink forwarding cern-nfs lines into the EOS ZSTD log pipeline.
template<typename Mutex>
class NfsZstdLogSink : public spdlog::sinks::base_sink<Mutex>
{
public:
  explicit NfsZstdLogSink(std::string tag)
      : mTag(std::move(tag))
  {
  }

protected:
  void
  sink_it_(const spdlog::details::log_msg& msg) override
  {
    spdlog::memory_buf_t formatted;
    this->formatter_->format(msg, formatted);
    std::string line(formatted.data(), formatted.size());

    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
      line.pop_back();
    }

    Logging::GetInstance().WriteZstd(mTag.c_str(), line.c_str());
  }

  void
  flush_() override
  {
  }

private:
  std::string mTag;
};

using NfsZstdLogSink_mt = NfsZstdLogSink<std::mutex>;

} // namespace eos::common

#endif
