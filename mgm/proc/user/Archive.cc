//------------------------------------------------------------------------------
// File: Archive.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Archive()
{
  std::ostringstream sstr;

  // For listing we don't need an EOS path
  if (mSubCmd == "list") 
  {
    if (!pOpaque->Get("mgm.archive.type"))
    {
      stdErr = "error: need to provide the archive listing type";
      retc = EINVAL;
    }
    else
    {
      sstr << mSubCmd << " " << pOpaque->Get("mgm.archive.type");
    }
  }
  else
  {
    XrdOucString spath = pOpaque->Get("mgm.archive.path");
    const char* inpath = spath.c_str();
    NAMESPACEMAP;
    if (info) info = 0;
    PROC_BOUNCE_ILLEGAL_NAMES;
    PROC_BOUNCE_NOT_ALLOWED;
    eos::common::Path cPath(path);
    spath = cPath.GetPath();
    
    sstr << mSubCmd << " root://" << gOFS->ManagerId << "/" << spath.c_str();
    eos_static_info("[-1] retc=%i, stdOut=%s, stdErr=%s", retc, stdOut.c_str(), stdErr.c_str());
    
    if (mSubCmd == "create")
    {
      if ((!pOpaque->Get("mgm.archive.dst") || (!spath.length())))
      {
        stdErr = "error: need to provide 'mgm.archive.dst' and archive path for create";
        retc = EINVAL;
      }

      sstr << " " << pOpaque->Get("mgm.archive.dst");
    }
    else if ((mSubCmd == "migrate") || ( mSubCmd == "stage"))
    {
      if (!spath.length())
      {
        stdErr = "error: need to provide a path for archive stage or migrate";
        retc = EINVAL;
      }
      else
      { 
        // Check that the archive file exists 
        std::ostringstream arch_file;
        arch_file << spath.c_str() << "/" << XrdMgmOfs::msArchiveFname;
        struct stat buf;
        
        if (gOFS->stat(arch_file.str().c_str(), &buf, *mError))
        {
          stdErr = "error: no archive file in directory: ";
          stdErr += spath.c_str();
          retc = EINVAL;
        }
        else
        {
          sstr << "/" << XrdMgmOfs::msArchiveFname;
        }
      }
    }
    else
    {
      stdErr = "error: operation not supported, needs to be one of the following: "
        "create, migrate, stage or list";
      retc = EINVAL;
    }
  }

  // Send request to archiver process
  if (!retc)
  {
    int sock_linger = 0;
    int sock_timeout = 1000; // 1s
    zmq::context_t zmq_ctx;
    zmq::socket_t socket(zmq_ctx, ZMQ_REQ);
    socket.setsockopt(ZMQ_RCVTIMEO, &sock_timeout, sizeof(sock_timeout));
    socket.setsockopt(ZMQ_LINGER, &sock_linger, sizeof(sock_linger));
 
    try
    {
      socket.connect(XrdMgmOfs::msArchiveEndpoint.c_str());
    }
    catch (zmq::error_t& zmq_err)
    {
      eos_static_err("connect to archiver failed");
      stdErr = "error: connect to archiver failed";
      retc = EINVAL;
    }

    if (!retc)
    {
      zmq::message_t msg((void*)sstr.str().c_str(), sstr.str().length(), 0);

      try
      {
        if (!socket.send(msg))
        {
          stdErr = "error: send request to archiver";
          retc = EINVAL;
        }
        else if (!socket.recv(&msg))
        {
          stdErr = "error: get response from archiver";
          retc = EINVAL;
        }
        else 
        {
          stdOut = XrdOucString((const char*)msg.data(), (int)msg.size());

          //TODO: deal with errors returned by the archiver
          std::istringstream iss(stdOut.c_str());
        }
      }
      catch (zmq::error_t& zmq_err)
      {
        stdErr = "error: timeout getting response from archiver, msg: ";
        stdErr += zmq_err.what();
        retc = EINVAL;
      }
    }
  }

  eos_static_info("[2] retc=%i, stdOut=%s, stdErr=%s", retc, stdOut.c_str(), stdErr.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Create archive file.
//------------------------------------------------------------------------------
int
ProcCommand::ArchiveCreate(const XrdOucString& dir_path)
{
  // Get "find -d --fileinfo" command result for the archive directory
  ProcCommand Cmd;
  XrdOucString lStdOut = "";
  XrdOucString lStdErr = "";
  XrdOucString info = "&mgm.cmd=find&mgm.path=";
  info += dir_path.c_str();
  info += "&mgm.option=I";
  Cmd.open("/proc/user", info.c_str(), *pVid, mError);
  Cmd.AddOutput(lStdOut, lStdErr);
  Cmd.close();

  // Read from the find result file and construct the archive file
  
  int nread = 0;
  XrdSfsFileOffset offset = 0;
  XrdSfsXferSize size = 4096;
  char buf[size + 1];
  
  while ((nread = Cmd.read(offset, buf, size)))
  {
    buf[nread] = '\0';
    lStdOut += buf;
    offset += nread;
  }
  
  stdOut = lStdOut.c_str();
  stdErr = lStdErr.c_str();
 
  return 0;  
}

EOSMGMNAMESPACE_END
