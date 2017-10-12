// ----------------------------------------------------------------------
// File: Workflow.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/StringConversion.hh"
#include "mgm/Workflow.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
#include <ctime>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/

int
Workflow::Trigger(std::string event, std::string workflow,
                  eos::common::Mapping::VirtualIdentity& vid)
{
  eos_static_info("event=\"%s\" workflow=\"%s\"", event.c_str(),
                  workflow.c_str());
  errno = 0;

  if ((event == "open")) {
    std::string key = "sys.workflow.";
    key += event;
    key += ".";
    key += workflow;

    if (mAttr && (*mAttr).count(key)) {
      eos_static_info("key=%s %d %d", key.c_str(), mAttr, (*mAttr).count(key));
      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      int retc = Create(vid);

      if (!retc) {
        if ((workflow == "enonet")) {
          std::string stallkey = key + ".stall";

          if ((*mAttr).count(stallkey)) {
            int stalltime = eos::common::StringConversion::GetSizeFromString((
                              *mAttr)[stallkey]);
            return stalltime;
          }
        }

        return 0;
      }

      errno = retc;
      return -1;
    } else {
      errno = ENOKEY;
    }
  } else {
    std::string key = "sys.workflow." + event + "." + workflow;

    if (mAttr && (*mAttr).count(key)) {
      eos_static_info("key=%s %d %d", key.c_str(), mAttr, (*mAttr).count(key));
      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      int retc = Create(vid);

      if (retc) {
        errno = retc;
        return -1;
      }

      return 0;
    } else {
      errno = ENOKEY;
    }
  }

  // not defined
  return -1;
}

/*----------------------------------------------------------------------------*/
std::string
Workflow::getCGICloseW(std::string workflow)
{
  std::string cgi;
  std::string key = "sys.workflow.closew." + workflow;

  if (mAttr && (*mAttr).count(key.c_str())) {
    cgi = "&mgm.event=close&mgm.workflow=";
    cgi += workflow;
  }

  return cgi;
}

/*----------------------------------------------------------------------------*/
std::string
Workflow::getCGICloseR(std::string workflow)
{
  std::string cgi;
  std::string key = "sys.workflow.closer." + workflow;

  if (mAttr && (*mAttr).count(key.c_str())) {
    cgi = "&mgm.event=close&mgm.workflow=";
    cgi += workflow;
  }

  return cgi;
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Attach(const char* path)
{
  return false;
}

/*----------------------------------------------------------------------------*/
int
Workflow::Create(eos::common::Mapping::VirtualIdentity& vid)
{
  int retc = 0;
  WFE::Job job(mFid, vid);
  time_t t = time(0);

  if (job.IsSync(mEvent)) {
    job.AddAction(mAction, mEvent, t, mWorkflow, "s");
    retc = job.Save("s", t);
  } else {
    job.AddAction(mAction, mEvent, t, mWorkflow, "q");
    retc = job.Save("q", t);
  }

  if (retc) {
    eos_static_err("failed to save");
    return retc;
  }

  if (job.IsSync()) {
    eos_static_info("running synchronous workflow");
    return job.DoIt(true);
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Delete()
{
  return false;
}
EOSMGMNAMESPACE_END
