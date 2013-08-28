// ----------------------------------------------------------------------
// File: proc/user/Attr.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Attr ()
{
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString option = pOpaque->Get("mgm.option");

  const char* inpath = spath.c_str();

  NAMESPACEMAP;
  info = 0;
  if (info)info = 0; // for compiler happyness
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  spath = path;

  if ((!spath.length()) ||
      ((mSubCmd != "set") && (mSubCmd != "get") && (mSubCmd != "ls") && (mSubCmd != "rm")))
  {
    stdErr = "error: you have to give a path name to call 'attr' and one of the subcommands 'ls', 'get','rm','set' !";
    retc = EINVAL;
  }
  else
  {
    if (((mSubCmd == "set") && ((!pOpaque->Get("mgm.attr.key")) || ((!pOpaque->Get("mgm.attr.value"))))) ||
        ((mSubCmd == "get") && ((!pOpaque->Get("mgm.attr.key")))) ||
        ((mSubCmd == "rm") && ((!pOpaque->Get("mgm.attr.key")))))
    {

      stdErr = "error: you have to provide 'mgm.attr.key' for set,get,rm and 'mgm.attr.value' for set commands!";
      retc = EINVAL;
    }
    else
    {
      retc = 0;
      XrdOucString key = pOpaque->Get("mgm.attr.key");
      XrdOucString val = pOpaque->Get("mgm.attr.value");

      while (val.replace("\"", "")) {}
      // find everything to be modified
      std::map<std::string, std::set<std::string> > found;
      std::map<std::string, std::set<std::string> >::const_iterator foundit;
      std::set<std::string>::const_iterator fileit;

      if (option == "r")
      {
        if (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, found))
        {
          stdErr += "error: unable to search in path";
          retc = errno;
        }
      }
      else
      {
        // the single dir case
        found[spath.c_str()].size();
      }

      if (!retc)
      {
        // apply to  directories starting at the highest level
        for (foundit = found.begin(); foundit != found.end(); foundit++)
        {
          {
            eos::ContainerMD::XAttrMap map;
            if (mSubCmd == "ls")
            {
              XrdOucString partialStdOut = "";
              if (gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid, (const char*) 0, map))
              {
                stdErr += "error: unable to list attributes in directory ";
                stdErr += foundit->first.c_str();
                retc = errno;
              }
              else
              {
                eos::ContainerMD::XAttrMap::const_iterator it;
                if (option == "r")
                {
                  stdOut += foundit->first.c_str();
                  stdOut += ":\n";
                }

                for (it = map.begin(); it != map.end(); ++it)
                {
                  partialStdOut += (it->first).c_str();
                  partialStdOut += "=";
                  partialStdOut += "\"";
                  partialStdOut += (it->second).c_str();
                  partialStdOut += "\"";
                  partialStdOut += "\n";
                }
                XrdMqMessage::Sort(partialStdOut);
                stdOut += partialStdOut;
                if (option == "r")
                  stdOut += "\n";
              }
            }

            if (mSubCmd == "set")
            {
              if (gOFS->_attr_set(foundit->first.c_str(), *mError, *pVid, (const char*) 0, key.c_str(), val.c_str()))
              {
                stdErr += "error: unable to set attribute in directory ";
                stdErr += foundit->first.c_str();
                if (mError != 0)
                {
                  stdErr += ": ";
                  stdErr += mError->getErrText();
                }
                retc = errno;
              }
              else
              {
                stdOut += "success: set attribute ";
                stdOut += key;
                stdOut += "=\"";
                stdOut += val;
                stdOut += "\" in directory ";
                stdOut += foundit->first.c_str();
                stdOut += "\n";
              }
            }

            if (mSubCmd == "get")
            {
              if (gOFS->_attr_get(foundit->first.c_str(), *mError, *pVid, (const char*) 0, key.c_str(), val))
              {
                stdErr += "error: unable to get attribute ";
                stdErr += key;
                stdErr += " in directory ";
                stdErr += foundit->first.c_str();
                retc = errno;
              }
              else
              {
                stdOut += key;
                stdOut += "=";
                stdOut += val;
                stdOut += "\n";
              }
            }

            if (mSubCmd == "rm")
            {
              if (gOFS->_attr_rem(foundit->first.c_str(), *mError, *pVid, (const char*) 0, key.c_str()))
              {
                stdErr += "error: unable to remove attribute '";
                stdErr += key;
                stdErr += "' in directory ";
                stdErr += foundit->first.c_str();
		retc = errno;
              }
              else
              {
                stdOut += "success: removed attribute '";
                stdOut += key;
                stdOut += "' from directory ";
                stdOut += foundit->first.c_str();
                stdOut += "\n";
              }
            }
          }
        }
      }
    }
  }
  return SFS_OK;
  ;
}

EOSMGMNAMESPACE_END
