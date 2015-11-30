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
Workflow::Trigger (std::string event, std::string workflow)
{
  eos_static_info("event=\"%s\" workflow=\"%s\"", event.c_str(), workflow.c_str());

  if ((event == "open") || (event == "prepare"))
  {
    std::string key = "sys.workflow.";
    key += event;
    key += ".";
    key += workflow;
    eos_static_info("key=%s %d %d", key.c_str(), mAttr, (*mAttr).count(key));
    if (mAttr && (*mAttr).count(key))
    {
      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      bool ok = Create();
      if (ok)
      {
        if ((workflow == "enonet"))
        {
          std::string stallkey = key + ".stall";

          if ((*mAttr).count(stallkey))
          {
            int stalltime = eos::common::StringConversion::GetSizeFromString((*mAttr)[stallkey]);
            return stalltime;
          }
        }
        return 0;
      }
      return -1;
    }
  }
  if (event == "closer")
  {
    std::string key = "sys.workflow.closer." + workflow;
    if (mAttr && (*mAttr).count(key))
    {
      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      return Create();
    }
  }
  if (event == "closew")
  {
    std::string key = "sys.workflow.closew." + workflow;
    if (mAttr && (*mAttr).count(key))
    {

      mEvent = event;
      mWorkflow = workflow;
      mAction = (*mAttr)[key];
      return Create();
    }
  }
  // not defined
  return -1;
}

/*----------------------------------------------------------------------------*/
std::string
Workflow::getCGICloseW (std::string workflow)
{
  std::string cgi;
  std::string key = "sys.workflow.closew." + workflow;
  if (mAttr && (*mAttr).count(key.c_str()))
  {

    cgi = "&mgm.event=close&mgm.workflow=";
    cgi += workflow;
  }
  return cgi;
}

/*----------------------------------------------------------------------------*/
std::string
Workflow::getCGICloseR (std::string workflow)
{
  std::string cgi;
  std::string key = "sys.workflow.closer." + workflow;
  if (mAttr && (*mAttr).count(key.c_str()))
  {

    cgi = "&mgm.event=close&mgm.workflow=";
    cgi += workflow;
  }
  return cgi;
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Attach (const char* path)
{

  return false;
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Create ()
{

  WFE::Job job(mFid);

  time_t t = time(0);
  job.AddAction(mAction, mEvent, t, mWorkflow);
  return job.Save("q");
}

/*----------------------------------------------------------------------------*/
bool
Workflow::Delete ()
{
  return false;
}
EOSMGMNAMESPACE_END
