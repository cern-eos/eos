//------------------------------------------------------------------------------
// File: EosAuthOfsFile.cc
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
#include "EosAuthOfsFile.hh"
#include "EosAuthOfs.hh"
#include "ProtoUtils.hh"
/*----------------------------------------------------------------------------*/
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
EosAuthOfsFile::EosAuthOfsFile(char* user, int MonID):
  XrdSfsFile(user, MonID),
  eos::common::LogId(),
  mName("")
{
  // emtpy
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
EosAuthOfsFile::~EosAuthOfsFile()
{
  // empty
}


//------------------------------------------------------------------------------
// Open a file
//------------------------------------------------------------------------------
int
EosAuthOfsFile::open(const char* fileName,
                     XrdSfsFileOpenMode openMode,
                     mode_t createMode,
                     const XrdSecEntity* client,
                     const char* opaque)
{
  int retc = SFS_ERROR;
  eos_debug("file open name=%s opaque=%s", fileName, opaque);
  mName = fileName;
  
  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  
  // Save file pointer value which is used as a key on the MGM instance
  std::ostringstream sstr;
  sstr << this;
  RequestProto* req_proto = utils::GetFileOpenRequest(sstr.str(), fileName,
                                   openMode, createMode, client, opaque,
                                   error.getErrUser(), error.getErrMid());

  if (gOFS->SendProtoBufRequest(socket, req_proto))
  {
    ResponseProto* resp_open = static_cast<ResponseProto*>(gOFS->GetResponse(socket));

    if (resp_open)
    {
      retc = resp_open->response();
      eos_debug("got response for file open request: %i", retc);
      
      if (resp_open->has_error())
        error.setErrInfo(resp_open->error().code(), resp_open->error().message().c_str());
      
      delete resp_open;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Read function
//------------------------------------------------------------------------------
XrdSfsXferSize
EosAuthOfsFile::read(XrdSfsFileOffset offset,
                     char* buffer,
                     XrdSfsXferSize length)
{
  int retc = 0;  // this means read 0 bytes and NOT SFS_OK :)
  eos_debug("read off=%li len=%i", (long long)offset, (int)length);
  
  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  eos_debug("fptr=%s, off=%li, len=%i", sstr.str().c_str(), offset, length);
  RequestProto* req_proto = utils::GetFileReadRequest(sstr.str(), offset, length);

  if (gOFS->SendProtoBufRequest(socket, req_proto))
  {
    ResponseProto* resp_fread = static_cast<ResponseProto*>(gOFS->GetResponse(socket));

    if (resp_fread)
    {
      retc = resp_fread->response();

      if (retc && resp_fread->has_message())
      {
        buffer = static_cast<char*>(memcpy((void*)buffer,
                                           resp_fread->message().c_str(),
                                           resp_fread->message().length()));
      }
      
      delete resp_fread;
    }
  }
  
  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Write function
//------------------------------------------------------------------------------
XrdSfsXferSize
EosAuthOfsFile::write(XrdSfsFileOffset offset,
                      const char* buffer,
                      XrdSfsXferSize length)
{
  int retc = 0;  // this means written 0 bytes and NOT SFS_OK :)
  eos_debug("write off=%ll len=%i", offset, length);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  eos_debug("file pointer: %s", sstr.str().c_str());
  RequestProto* req_proto = utils::GetFileWriteRequest(sstr.str(), offset, buffer, length);

  if (gOFS->SendProtoBufRequest(socket, req_proto))
  {
    ResponseProto* resp_fwrite = static_cast<ResponseProto*>(gOFS->GetResponse(socket));

    if (resp_fwrite)
    {
      retc = resp_fwrite->response();
      eos_debug("got response for file write request");
      delete resp_fwrite;
    }
  }
 
  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Get name of file
//------------------------------------------------------------------------------
const char*
EosAuthOfsFile::FName()
{
  int retc = SFS_ERROR;
  eos_debug("file fname");

  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  eos_debug("file pointer: %s", sstr.str().c_str());
  RequestProto* req_proto = utils::GetFileFnameRequest(sstr.str());

  if (gOFS->SendProtoBufRequest(socket, req_proto))
  {
    ResponseProto* resp_fname = static_cast<ResponseProto*>(gOFS->GetResponse(socket));

    if (resp_fname)
    {
      retc = resp_fname->response();
      eos_debug("got response for filefname request");
      
      if (retc == SFS_OK)
      {
        eos_debug("file fname is: %s", resp_fname->message().c_str());
        mName = resp_fname->message();
      }
      else
      { 
        eos_debug("file fname not found or error on server side");
      }
      
      delete resp_fname;
    }
  }
 
  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return (retc ? static_cast<const char*>(0) : mName.c_str());
}


//------------------------------------------------------------------------------
// Stat function
//------------------------------------------------------------------------------
int
EosAuthOfsFile::stat(struct stat* buf)
{
  int retc = SFS_ERROR;
  eos_debug("stat file name=%s", mName.c_str());

  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  eos_debug("file pointer: %s", sstr.str().c_str());
  RequestProto* req_proto = utils::GetFileStatRequest(sstr.str());

  if (gOFS->SendProtoBufRequest(socket, req_proto))
  {
    ResponseProto* resp_fstat = static_cast<ResponseProto*>(gOFS->GetResponse(socket));

    if (resp_fstat)
    {
      retc = resp_fstat->response();
      buf = static_cast<struct stat*>(memcpy((void*)buf,
                                             resp_fstat->message().c_str(),
                                             sizeof(struct stat)));
      eos_debug("got response for fstat request: %i", retc);
      delete resp_fstat;
    }
  }
  else
  {
    eos_err("file stat - unable to send request");
    memset(buf, 0, sizeof(struct stat));
  }
  
  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
//! Close file
//------------------------------------------------------------------------------
int
EosAuthOfsFile::close()
{
  int retc = SFS_ERROR;
  eos_debug("close");

  // Get a socket object from the pool
  zmq::socket_t* socket;
  gOFS->mPoolSocket.wait_pop(socket);
  std::ostringstream sstr;
  sstr << this;
  eos_debug("file pointer: %s", sstr.str().c_str());
  RequestProto* req_proto = utils::GetFileCloseRequest(sstr.str());

  if (gOFS->SendProtoBufRequest(socket, req_proto))
  {
    ResponseProto* resp_close = static_cast<ResponseProto*>(gOFS->GetResponse(socket));

    if (resp_close)
    {
      retc = resp_close->response();
      eos_debug("got response for file close request: %i", retc);
      delete resp_close;
    }
  }
  
  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
//!!!!!!!!! THE FOLLOWING OPERATIONS ARE NOT SUPPORTED !!!!!!!!!
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// fctl fakes ok (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::fctl(int, const char*, XrdOucErrInfo&)
{
  return 0;
}


//------------------------------------------------------------------------------
// Return mmap address (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::getMmap(void** Addr, off_t& Size)
{
  if (Addr) Addr = 0;

  Size = 0;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// File pre-read fakes ok (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::read(XrdSfsFileOffset fileOffset, XrdSfsXferSize preread_sz)
{
  return SFS_OK;
}

//------------------------------------------------------------------------------
// File read in async mode (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::read(XrdSfsAio* aioparm)
{
  static const char* epname = "read";
  return Emsg(epname, error, EOPNOTSUPP, "read", mName.c_str());
}


//------------------------------------------------------------------------------
// File write in async mode (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::write(XrdSfsAio* aiop)
{
  static const char* epname = "write";
  return Emsg(epname, error, EOPNOTSUPP, "write", mName.c_str());
}


//------------------------------------------------------------------------------
// File sync (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::sync()
{
  static const char* epname = "sync";
  return Emsg(epname, error, EOPNOTSUPP, "sync", mName.c_str());
}


//------------------------------------------------------------------------------
// File async sync (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::sync(XrdSfsAio* aiop)
{
  static const char* epname = "sync";
  return Emsg(epname, error, EOPNOTSUPP, "sync", mName.c_str());
}


//------------------------------------------------------------------------------
// File truncate (not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::truncate(XrdSfsFileOffset flen)
{
  static const char* epname = "trunc";
  return Emsg(epname, error, EOPNOTSUPP, "truncate", mName.c_str());
}


//------------------------------------------------------------------------------
// Get checksum info (returns nothing - not supported)
//------------------------------------------------------------------------------
int
EosAuthOfsFile::getCXinfo(char cxtype[4], int& cxrsz)
{
  return cxrsz = 0;
}


//------------------------------------------------------------------------------
// Create error message
//------------------------------------------------------------------------------
int
EosAuthOfsFile::Emsg(const char* pfx,
                     XrdOucErrInfo& einfo,
                     int ecode,
                     const char* op,
                     const char* target)
{
  char* etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  if (ecode < 0) ecode = -ecode;

  if (!(etext = strerror(ecode)))
  {
    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);
  eos_err("Unable to %s %s; %s", op, target, etext);

  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}

EOSAUTHNAMESPACE_END
