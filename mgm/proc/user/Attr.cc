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

  eos::common::Path cPath(path);
  spath = cPath.GetPath();

  if ((!spath.length()) ||
      ((mSubCmd != "set") && (mSubCmd != "get") && (mSubCmd != "ls") && (mSubCmd != "rm") && (mSubCmd != "fold")))
  {
    stdErr = "error: you have to give a path name to call 'attr' and one of the subcommands 'ls', 'get','rm','set', 'fold' !";
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
            eos::IContainerMD::XAttrMap map;
            eos::IContainerMD::XAttrMap linkmap;

            if ( (mSubCmd == "ls") )
            {
	      if (gOFS->_access(foundit->first.c_str(),R_OK|X_OK, *mError, *pVid,0))
	      {
                stdErr += "error: unable to browse directory ";
                stdErr += foundit->first.c_str();
                retc = errno;
		return SFS_OK;
	      }

              XrdOucString partialStdOut = "";
              if (gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid, (const char*) 0, map, true, true))
              {
                stdErr += "error: unable to list attributes in directory ";
                stdErr += foundit->first.c_str();
                retc = errno;
              }
              else
              {
                eos::IContainerMD::XAttrMap::const_iterator it;
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
	      if (key=="user.acl") {
		XrdOucString evalacl;
		// If someone wants to set a user.acl and the tag sys.eval.useracl
                // is not there, we return an error ...
		if ((pVid->uid != 0) && gOFS->_attr_get(foundit->first.c_str(),
                                                   *mError, *pVid,
                                                   (const char*) 0,
                                                   "sys.eval.useracl",evalacl))
		{
                  stdErr += "error: unable to set user.acl - the directory does not "
                      "evaluate user acls (sys.eval.useracl is undefined)!\n";
		  retc = EINVAL;
		  return SFS_OK;
		}
	      }

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
	      if (gOFS->_access(foundit->first.c_str(),R_OK, *mError, *pVid,0))
	      {
                stdErr += "error: unable to browse directory ";
                stdErr += foundit->first.c_str();
                retc = errno;
		return SFS_OK;
	      }

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
                stdOut += "=\"";
                stdOut += val;
                stdOut += "\"\n";
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
	    
	    if (mSubCmd == "fold")
	    {
	      int retc = gOFS->_attr_ls(foundit->first.c_str(), *mError, *pVid, (const char*) 0, map, true, false);
	      if ( (!retc) && map.count("sys.attr.link"))
		retc |= gOFS->_attr_ls(map["sys.attr.link"].c_str(), *mError, *pVid, (const char*) 0, linkmap, true, true);
	      
	      if (retc)
	      {
                stdErr += "error: unable to list attributes in directory ";
                stdErr += foundit->first.c_str();
                retc = errno;
              }
              else
              {
		XrdOucString partialStdOut;
                eos::IContainerMD::XAttrMap::const_iterator it;
                if (option == "r")
                {
                  stdOut += foundit->first.c_str();
                  stdOut += ":\n";
                }

                for (it = map.begin(); it != map.end(); ++it)
                {
		  if ( linkmap.count(it->first) &&
		       linkmap[it->first] == map[it->first] ) 
		  {
		    if (gOFS->_attr_rem(foundit->first.c_str(), *mError, *pVid, (const char*) 0, it->first.c_str()))
		    {
		      stdErr += "error [ attr fold ] : unable to remove local attribute ";
		      stdErr += it->first.c_str();
		      stdErr += "\n";
		      retc = errno;
		    }
		    else
		    {
		      stdOut += "info [ attr fold ] : removing local attribute ";
		      stdOut += (it->first).c_str();
		      stdOut += "=";
		      stdOut += "\"";
		      stdOut += (it->second).c_str();
		      stdOut += "\"";
		      stdOut += "\n";
		    }
		  }
                }
                if (option == "r")
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
