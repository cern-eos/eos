//------------------------------------------------------------------------------
// File: EosAuthOfsDirectory.cc
// Author: Elvin-Alin Sindrilau <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#include "EosAuthOfsDirectory.hh"
#include "EosAuthOfs.hh"
#include "ProtoUtils.hh"
/*----------------------------------------------------------------------------*/
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
EosAuthOfsDirectory::EosAuthOfsDirectory(char* user, int MonID):
  XrdSfsDirectory(user, MonID),
  LogId(),
  mName("")
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
EosAuthOfsDirectory::~EosAuthOfsDirectory()
{
  // emtpy
}


//------------------------------------------------------------------------------
// Open a directory
//------------------------------------------------------------------------------
int
EosAuthOfsDirectory::open(const char* name,
                          const XrdSecClientName* client,
                          const char* opaque)
{
  int retc;
  eos_debug("dir open name=%s", name);
  mName = name;
  
  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  RequestProto* req_proto = utils::GetDirOpenRequest(sstr.str(), name, client,
                                   opaque, error.getErrUser(), error.getErrMid());

  if (!gOFS->SendProtoBufRequest(socket, req_proto))
  {
    eos_err("dir open - unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_open = static_cast<ResponseProto*>(gOFS->GetResponse(socket));
  retc = resp_open->response();
  eos_debug("got response for dir open request");

  delete resp_open;
  delete req_proto;
  
  // Put back the socket object in the pool
  gOFS->mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Get entry of an open directory
//------------------------------------------------------------------------------
const char*
EosAuthOfsDirectory::nextEntry()
{
  int retc;
  eos_debug("dir read name=%s", mName.c_str());

  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  RequestProto* req_proto = utils::GetDirReadRequest(sstr.str());

  if (!gOFS->SendProtoBufRequest(socket, req_proto))
  {
    eos_err("dir read - unable to send request");
    return static_cast<const char*>(0);
  }

  ResponseProto* resp_read = static_cast<ResponseProto*>(gOFS->GetResponse(socket));
  retc = resp_read->response();
  eos_debug("got response for dir read request");

  if (retc == SFS_ERROR)
  {
    eos_debug("no more entries or error on server side");
    return static_cast<const char*>(0);
  }
  else
  {
    eos_debug("next entry is: %s", resp_read->message().c_str());
    mNextEntry = resp_read->message();
  }

  delete resp_read;
  delete req_proto;
  
  // Put back the socket object in the pool
  gOFS->mPoolSocket.push(socket);
  return mNextEntry.c_str();
}


//------------------------------------------------------------------------------
// Close an open directory
//------------------------------------------------------------------------------
int
EosAuthOfsDirectory::close()
{
  int retc;
  eos_debug("dir close name=%s", mName.c_str());

  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  RequestProto* req_proto = utils::GetDirCloseRequest(sstr.str());

  if (!gOFS->SendProtoBufRequest(socket, req_proto))
  {
    eos_err("dir close - unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_close = static_cast<ResponseProto*>(gOFS->GetResponse(socket));
  retc = resp_close->response();
  eos_debug("got response dir close request");
  delete resp_close;
  delete req_proto;
  
  // Put back the socket object in the pool
  gOFS->mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Get name of an open directory
//------------------------------------------------------------------------------
const char*
EosAuthOfsDirectory::FName()
{
  int retc;
  eos_debug("dir fname");
  
  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  RequestProto* req_proto = utils::GetDirFnameRequest(sstr.str());

  if (!gOFS->SendProtoBufRequest(socket, req_proto))
  {
    eos_err("dir fname - unable to send request");
    return static_cast<const char*>(0);
  }

  ResponseProto* resp_fname = static_cast<ResponseProto*>(gOFS->GetResponse(socket));
  retc = resp_fname->response();
  eos_debug("got response for dirfname request");

  if (retc == SFS_ERROR)
  {
    eos_debug("dir fname not found or error on server side");
    return static_cast<const char*>(0);
  }
  else
  {
    eos_debug("dir fname is: %s", resp_fname->message().c_str());
    mName = resp_fname->message();
  }

  delete resp_fname;
  delete req_proto;
  
  // Put back the socket object in the pool
  gOFS->mPoolSocket.push(socket);
  return mName.c_str();
}

EOSAUTHNAMESPACE_END
