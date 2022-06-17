/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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
 ************************************************************************
 */


#ifndef EOS_FMDCONVERTER_HH
#define EOS_FMDCONVERTER_HH

#include "fst/filemd/FmdHandler.hh"
#include <folly/futures/Future.h>
#include <memory>

namespace folly {
class Executor;
}

namespace eos::fst {

struct FsidPathInfo {
  virtual std::string GetPath(eos::common::FileSystem::fsid_t fsid) = 0;
  static eos::common::FileSystem::fsid_t GetFsid(std::string_view path);
  virtual ~FsidPathInfo() = default;
};

class XrdOfsPathInfo final: public FsidPathInfo {
public:
  std::string GetPath(eos::common::FileSystem::fsid_t fsid) override;
};

class FmdConverter {
public:
  FmdConverter(FmdHandler * src_handler,
               FmdHandler * tgt_handler,
               size_t per_disk_pool);

  folly::Future<bool> Convert(std::string_view path, uint64_t count);
  void ConvertFS(std::string_view fspath);
private:
  FmdHandler * mSrcFmdHandler;
  FmdHandler * mTgtFmdHandler;
  std::unique_ptr<folly::Executor> mExecutor;
};


} // namespace eos::fst

#endif // EOS_FMDCONVERTER_HH
