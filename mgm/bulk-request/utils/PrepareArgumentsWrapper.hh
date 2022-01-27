//------------------------------------------------------------------------------
//! @file PrepareArgumentsWrapper.hh
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#ifndef EOS_PREPAREARGUMENTSWRAPPER_HH
#define EOS_PREPAREARGUMENTSWRAPPER_HH

#include "auth_plugin/ProtoUtils.hh"
#include "mgm/Namespace.hh"

EOSBULKNAMESPACE_BEGIN

class PrepareArgumentsWrapper {
public:
  PrepareArgumentsWrapper(const std::string& reqid, const int opts, const std::vector<std::string>& paths, const std::vector<std::string>& oinfos):mPargs(nullptr)   {
    mPargsProto.set_reqid(reqid);
    mPargsProto.set_opts(opts);
    for(auto & oinfo: oinfos) {
      mPargsProto.add_oinfo(oinfo);
    }
    for(auto & path: paths) {
      mPargsProto.add_paths(path);
    }
  }

  PrepareArgumentsWrapper(const std::string & reqid, const int opts):mPargs(nullptr) {
    mPargsProto.set_reqid(reqid);
    mPargsProto.set_opts(opts);
  }

  ~PrepareArgumentsWrapper(){
    if(mPargs != nullptr) {
      eos::auth::utils::DeleteXrdSfsPrep(mPargs);
      mPargs = nullptr;
    }
  }

  void addFile(const std::string & path, const std::string & opaqueInfos) {
    mPargsProto.add_paths(path);
    mPargsProto.add_oinfo(opaqueInfos);
  }

  uint64_t getNbFiles() {
    return mPargsProto.paths().size();
  }

  XrdSfsPrep * getPrepareArguments() {
    mPargs = eos::auth::utils::GetXrdSfsPrep(mPargsProto);
    return mPargs;
  }
private:
  eos::auth::XrdSfsPrepProto mPargsProto;
  XrdSfsPrep * mPargs;
};



EOSBULKNAMESPACE_END

#endif // EOS_PREPAREARGUMENTSWRAPPER_HH
