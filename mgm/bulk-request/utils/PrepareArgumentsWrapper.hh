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
  PrepareArgumentsWrapper(const std::string & reqid, const int opts, const std::vector<std::string> & oinfos, const std::vector<std::string> & paths) {
    eos::auth::XrdSfsPrepProto pargsProto;
    pargsProto.set_reqid(reqid);
    pargsProto.set_opts(opts);
    for(auto & oinfo: oinfos) {
      pargsProto.add_oinfo(oinfo);
    }
    for(auto & path: paths) {
      pargsProto.add_paths(path);
    }
    mPargs = eos::auth::utils::GetXrdSfsPrep(pargsProto);
  }

  PrepareArgumentsWrapper(const std::string & reqid, const int opts) {
    eos::auth::XrdSfsPrepProto pargsProto;
    pargsProto.set_reqid(reqid);
    pargsProto.set_opts(opts);
    mPargs = eos::auth::utils::GetXrdSfsPrep(pargsProto);
  }

  ~PrepareArgumentsWrapper(){
    eos::auth::utils::DeleteXrdSfsPrep(mPargs);
  }

  XrdSfsPrep * getPrepareArguments() {
    return mPargs;
  }
private:
  XrdSfsPrep * mPargs;
};



EOSBULKNAMESPACE_END

#endif // EOS_PREPAREARGUMENTSWRAPPER_HH
