//------------------------------------------------------------------------------
// File RainFile.cc
// Author Elvin Sindrilaru <esindril@cern.ch>
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
#include "RainFile.hh"
#include "fst/layout/RaidMetaLayout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RainFile::RainFile():
  mIsOpen(false)
{
  eos_debug("calling constructor");
  pFile = new XrdCl::File(false);
}
  
  
//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RainFile::~RainFile()
{
  eos_debug("calling destructor");
  delete pFile;
}
  

//------------------------------------------------------------------------------
// Open
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Open( const std::string& url,
                OpenFlags::Flags flags,
                Access::Mode mode,
                ResponseHandler* handler,
                uint16_t timeout )
{
  eos_debug("url=%s", url.c_str());
  XRootDStatus st = pFile->Open(url, flags, mode, timeout);

  if (!st.IsOK())
    return st;

  mIsOpen = true;
  XRootDStatus* ret_st = new XRootDStatus(st);
  handler->HandleResponse(ret_st, 0);
  return st;  
}


//------------------------------------------------------------------------------
// Close
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Close( ResponseHandler* handler,
                 uint16_t timeout )
{
  eos_debug("calling close");
  XRootDStatus status = pFile->Close(handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Stat
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Stat( bool force,
                ResponseHandler* handler,
                uint16_t timeout )
{
  eos_debug("calling stat");
  XRootDStatus status = pFile->Stat(force, handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Read( uint64_t offset,
                uint32_t size,
                void* buffer,
                ResponseHandler* handler,
                uint16_t timeout )
{
  eos_debug("offset=%ju, size=%ju", offset, size);
  XRootDStatus status = pFile->Read(offset, size, buffer, handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Write( uint64_t offset,
                 uint32_t size,
                 const void* buffer,
                 ResponseHandler* handler,
                 uint16_t timeout )
{
  eos_debug("offset=%ju, size=%ju", offset, size);
  XRootDStatus status = pFile->Write(offset, size, buffer, handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Sync
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Sync( ResponseHandler* handler,
                uint16_t timeout )
{
  eos_debug("callnig sync");
  XRootDStatus status = pFile->Sync(handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Truncate
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Truncate( uint64_t size,
                    ResponseHandler* handler,
                    uint16_t timeout )
{
  eos_debug("offset=%ju", size);
  XRootDStatus status = pFile->Truncate(size, handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// VectorRead
//------------------------------------------------------------------------------
XRootDStatus
RainFile::VectorRead( const ChunkList& chunks,
                      void* buffer,
                      ResponseHandler* handler,
                      uint16_t timeout )
{
  eos_debug("calling vread");
  XRootDStatus status = pFile->VectorRead(chunks, buffer, handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Fcntl
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Fcntl( const Buffer& arg,
                 ResponseHandler* handler,
                 uint16_t timeout )
{
  eos_debug("calling fcntl");
  XRootDStatus status = pFile->Fcntl(arg, handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// Visa
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Visa( ResponseHandler* handler,
                uint16_t timeout )
{
  eos_debug("calling visa");
  XRootDStatus status = pFile->Visa(handler, timeout);
  return status;  
}


//------------------------------------------------------------------------------
// IsOpen
//------------------------------------------------------------------------------
bool
RainFile::IsOpen() const
{
  return mIsOpen;
}


//------------------------------------------------------------------------------
// @see XrdCl::File::SetProperty
//------------------------------------------------------------------------------
bool
RainFile::SetProperty( const std::string &name,
                       const std::string &value )
{
  eos_debug("name=%s, value=%s", name.c_str(), value.c_str());
  return pFile->SetProperty(name, value);
}


//------------------------------------------------------------------------------
// @see XrdCl::File::GetProperty
//------------------------------------------------------------------------------
bool
RainFile::GetProperty( const std::string &name,
                       std::string &value ) const
{
  eos_debug("name=%s", name.c_str());
  return pFile->GetProperty(name, value);
}


//------------------------------------------------------------------------------
//! @see XrdCl::File::GetDataServer
//------------------------------------------------------------------------------
std::string
RainFile::GetDataServer() const
{
  eos_debug("get data server");
  return std::string("");
}


//------------------------------------------------------------------------------
//! @see XrdCl::File::GetLastURL
//------------------------------------------------------------------------------
URL
RainFile::GetLastURL() const
{
  eos_debug("get last URL");
  return std::string("");
}


EOSFSTNAMESPACE_END
