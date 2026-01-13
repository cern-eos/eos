// ----------------------------------------------------------------------
// File: Workflow.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSMGM_WORKFLOW__HH__
#define __EOSMGM_WORKFLOW__HH__

#include "common/FileId.hh"
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IView.hh"
#include <sys/types.h>

EOSMGMNAMESPACE_BEGIN

class Workflow
{
public:
  Workflow():
    mAttr(nullptr), mPath(""), mFid(0), mEvent(""), mWorkflow(""), mAction("")
  {}

  ~Workflow() = default;

  void Init(eos::IContainerMD::XAttrMap* attr, std::string path = "",
            eos::common::FileId::fileid_t fid = 0)
  {
    mAttr = attr;
    mPath = std::move(path);
    mFid = fid;
  }

  void SetFile(const std::string& path = "",
               eos::common::FileId::fileid_t fid = 0)
  {
    if (path.length()) {
      mPath = path;
    }

    if (fid) {
      mFid = fid;
    }
  }

  int Trigger(const std::string& event, std::string workflow,
              eos::common::VirtualIdentity& vid,
              const char * const ininfo, std::string& errorMessage);

  std::string getCGICloseW(const std::string& workflow,
                           const eos::common::VirtualIdentity& vid);

  std::string getCGICloseR(const std::string& workflow);


  bool IsSync()
  {
    return (mEvent.substr(0, 6) == "sync::");
  }

  void Reset()
  {
    mPath = "";
    mFid = 0;
    mEvent = "";
    mWorkflow = "";
    mAttr = nullptr;
    mAction = "";
  }

  int Create(eos::common::VirtualIdentity& vid,
             const char * const ininfo, std::string& errorMessage);

  int ExceptionThrowingCreate(eos::common::VirtualIdentity& vid,
    const char * const ininfo, std::string& errorMessage);

  bool Attach(const char* path);

  bool Delete();

private:
  eos::IContainerMD::XAttrMap* mAttr;
  std::string mPath;
  eos::common::FileId::fileid_t mFid;
  std::string mEvent;
  std::string mWorkflow;
  std::string mAction;

  inline static bool WfeRecordingEnabled();

  inline static bool WfeEnabled();
};

EOSMGMNAMESPACE_END
#endif
