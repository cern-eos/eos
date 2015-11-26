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
#include "mgm/Workflow.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/

int
Workflow::Trigger (std::string event, std::string workflow)
{
  eos_static_info("event=\"%s\"", event.c_str());

  if (event == "open")
  {
    std::string key = "sys.workflow.open." + workflow;
    if (mAttr && (*mAttr).count(key))
    {
      return 1;
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

EOSMGMNAMESPACE_END
