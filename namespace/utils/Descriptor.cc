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

//------------------------------------------------------------------------------
// Author: Lukasz Janyst <ljanyst@cern.ch>
// Date:   07.07.2010
// File:   Descriptor.cc
//------------------------------------------------------------------------------

#include "namespace/utils/Descriptor.hh"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <cerrno>
#include <cstring>
#include <cstdlib>

static void resolve(const char* address, sockaddr_in& addr)
{
  eos::DescriptorException ex;
  //--------------------------------------------------------------------------
  // Get the ip address
  //--------------------------------------------------------------------------
  hostent* hp;
  //--------------------------------------------------------------------------
  // Enlarge the buffer until the call succeeds
  //--------------------------------------------------------------------------
#ifdef __APPLE__
  hp = gethostbyname(address);

  if (!hp) {
    ex.getMessage() << "Socket: get host by name failed";
    throw ex;
  }

#else
  hostent  hostbuf;
  size_t   hstbuflen = 1024;
  int      herr;
  int      res;
  char*    tmphstbuf = (char*)malloc(hstbuflen);

  while ((res = gethostbyname_r(address, &hostbuf, tmphstbuf, hstbuflen,
                                &hp, &herr)) == ERANGE) {
    hstbuflen *= 2;
    char* ptr  = (char*)realloc(tmphstbuf, hstbuflen);

    if (ptr == nullptr) {
      std::abort();
    }

    tmphstbuf = ptr;
  }

  if (res || !hp) {
    free(tmphstbuf);
    ex.getMessage() << "Socket: get host by name failed";
    throw ex;
  }

#endif

  if (hp->h_addr_list == 0) {
    ex.getMessage() << "Socket: host unknown";
    throw ex;
  }

  if (hp->h_addr_list[0] == 0) {
    ex.getMessage() << "Socket: host unknown";
    throw ex;
  }

  memcpy(&addr.sin_addr.s_addr, hp->h_addr_list[0], sizeof(in_addr));
#ifndef __APPLE__
  free(tmphstbuf);
#endif
}

namespace eos
{
//----------------------------------------------------------------------------
// Initializer
//----------------------------------------------------------------------------
void Socket::init(Protocol proto)
{
  if (pFD != -1) {
    DescriptorException ex;
    ex.getMessage() << "Socket: socket is already initialized";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Create the socket
  //--------------------------------------------------------------------------
  int type = SOCK_STREAM;

  if (proto == UDP) {
    type = SOCK_DGRAM;
  }

  if ((pFD = socket(PF_INET, type, 0)) == -1) {
    DescriptorException ex;
    ex.getMessage() << "Socket: Unable to create socket: ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }
}

//----------------------------------------------------------------------------
// Connect the socket
//----------------------------------------------------------------------------
void Socket::connect(const char* address, unsigned port)
{
  DescriptorException ex;

  if (pFD == -1) {
    init(TCP);
  }

  //--------------------------------------------------------------------------
  //! Resolve the hostname
  //--------------------------------------------------------------------------
  sockaddr_in addr = {};
  resolve(address, addr);
  //--------------------------------------------------------------------------
  // Set up the port
  //--------------------------------------------------------------------------
  addr.sin_family = AF_INET;
  addr.sin_port   = htons((unsigned short)port);

  //--------------------------------------------------------------------------
  // Connect to the remote host
  //--------------------------------------------------------------------------
  if ((pFD < 0) || (::connect(pFD, (sockaddr*)&addr, sizeof(addr)) != 0)) {
    if (pFD > 0) {
      ::close(pFD);
    }

    ex.getMessage() << "Socket: Connection failed: ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }
}

//----------------------------------------------------------------------------
// Bind to the port
//----------------------------------------------------------------------------
void Socket::bind(const char* address, unsigned port)
{
  if (pFD == -1) {
    init(TCP);
  }

  //--------------------------------------------------------------------------
  // Resolve the address
  //--------------------------------------------------------------------------
  sockaddr_in         localAddr;
  DescriptorException ex;
  memset(&localAddr, 0, sizeof(localAddr));

  if (address) {
    resolve(address, localAddr);
  } else {
    localAddr.sin_addr.s_addr = INADDR_ANY;
  }

  localAddr.sin_family = AF_INET;
  localAddr.sin_port   = htons((unsigned short)port);

  //--------------------------------------------------------------------------
  // Bind the socket
  //--------------------------------------------------------------------------
  if ((pFD < 0) || (::bind(pFD, (sockaddr*)&localAddr,
                           sizeof(sockaddr_in)) == -1)) {
    if (pFD >= 0) {
      ::close(pFD);
    }

    ex.getMessage() << "Socket: Unable to bind to port: " << port << " ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }
}

//----------------------------------------------------------------------------
// Listen to the incomming connections
//----------------------------------------------------------------------------
void Socket::listen(unsigned queue)
{
  DescriptorException ex;

  //--------------------------------------------------------------------------
  // Listen to the incomming connections
  //--------------------------------------------------------------------------
  if (::listen(pFD, 20) == -1) {
    ex.getMessage() << "Socket: Unable to listen: " << strerror(errno);
    throw ex;
  }
}

Socket* Socket::accept()
{
  DescriptorException ex;
  //--------------------------------------------------------------------------
  // Accept the connection
  //--------------------------------------------------------------------------
  socklen_t   sinSize = sizeof(sockaddr_in);
  sockaddr_in remoteAddr;
  int newSock = ::accept(pFD, (sockaddr*)&remoteAddr, &sinSize);

  if (newSock == -1) {
    ex.getMessage() << "Socket: Error while accpeting connection: ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Create a new thread to handle this connection
  //--------------------------------------------------------------------------
  return new Socket(newSock);
}

//----------------------------------------------------------------------------
// Close the socket
//----------------------------------------------------------------------------
void Descriptor::close()
{
  if (pFD != -1) {
    ::close(pFD);
    pFD = -1;
  }
}

//----------------------------------------------------------------------------
// Read the buffer from the blocking descriptor (socket, pipe), it won't
// return untill all the requested data is read
//----------------------------------------------------------------------------
void Descriptor::readBlocking(char* buffer, unsigned len)
{
  if (len == 0) {
    return;
  }

  int   ret;
  int   left = len;
  char* ptr  = buffer;

  while (1) {
    ret = ::read(pFD, ptr, left);

    if (ret == -1 || ret == 0) {
      DescriptorException ex;
      ex.getMessage() << "Descriptor: Unable to read " << len << " bytes: ";
      ex.getMessage() << strerror(errno);
      throw ex;
    }

    left -= ret;

    if (!left) {
      return;
    }

    ptr += ret;
  }
}

//----------------------------------------------------------------------------
// Read the buffer from the non-blocking descriptor (file, block device),
// it won't return untill all the requested data is read.
//----------------------------------------------------------------------------
void Descriptor::readNonBlocking(char* buffer, unsigned len, unsigned poll)
{
  if (len == 0) {
    return;
  }

  int   ret;
  int   left = len;
  char* ptr  = buffer;

  while (1) {
    ret = ::read(pFD, ptr, left);

    if (ret == -1) {
      DescriptorException ex;
      ex.getMessage() << "Descriptor: Unable to read " << len << " bytes: ";
      ex.getMessage() << strerror(errno);
      throw ex;
    }

    if (ret == 0) {
      if (poll != 0) {
        usleep(poll);
      } else {
        DescriptorException ex;
        ex.getMessage() << "Descriptor: Not enough data to fulfill the request";
        throw ex;
      }
    }

    left -= ret;

    if (!left) {
      return;
    }

    ptr += ret;
  }
}

//----------------------------------------------------------------------------
// Read the buffer from the non-blocking descriptor (file, block device)
// at given offset, it won't return untill all the requested data is read.
//----------------------------------------------------------------------------
void Descriptor::offsetReadNonBlocking(char* buffer, unsigned len,
                                       off_t offset, unsigned poll)
{
  if (len == 0) {
    return;
  }

  int   ret;
  off_t off  = offset;
  int   left = len;
  char* ptr  = buffer;

  while (1) {
    ret = ::pread(pFD, ptr, left, off);

    if (ret == -1) {
      DescriptorException ex;
      ex.getMessage() << "Descriptor: Unable to read " << len << " bytes";
      ex.getMessage() << "at offset " << offset << ": ";
      ex.getMessage() << strerror(errno);
      throw ex;
    }

    if (ret == 0) {
      if (poll != 0) {
        usleep(poll);
      } else {
        DescriptorException ex;
        ex.getMessage() << "Descriptor: Not enough data to fulfill the request";
        throw ex;
      }
    }

    left -= ret;
    off  += ret;

    if (!left) {
      return;
    }

    ptr += ret;
  }
}

//----------------------------------------------------------------------------
// Try to read len bytes at offset
//----------------------------------------------------------------------------
unsigned Descriptor::tryRead(char* buffer, unsigned len, off_t offset)
{
  if (len == 0) {
    return 0;
  }

  int   ret;
  off_t off  = offset;
  int   left = len;
  char* ptr  = buffer;

  while (1) {
    ret = ::pread(pFD, ptr, left, off);

    if (ret == -1) {
      DescriptorException ex;
      ex.getMessage() << "Descriptor: Unable to read " << len << " bytes";
      ex.getMessage() << "at offset " << offset << ": ";
      ex.getMessage() << strerror(errno);
      throw ex;
    }

    if (ret == 0) {
      return len - left;
    }

    left -= ret;
    off  += ret;

    if (!left) {
      return len;
    }

    ptr += ret;
  }

  return len;
}

//----------------------------------------------------------------------------
// Write data to the descriptor
//----------------------------------------------------------------------------
void Descriptor::write(const char* buffer, unsigned len)
{
  if (len == 0) {
    return;
  }

  int         ret  = 0;
  int         left = len;
  const char* ptr  = buffer;

  while (1) {
    ret = ::write(pFD, ptr, left);

    if (ret == -1  || ret == 0) {
      DescriptorException ex;
      ex.getMessage() << "Descriptor: Unable to write " << len << " bytes: ";
      ex.getMessage() << strerror(errno);
      throw ex;
    }

    left -= ret;

    if (!left) {
      return;
    }

    ptr += ret;
  }
}

//----------------------------------------------------------------------------
// The same as the ones in the manual
//----------------------------------------------------------------------------
void Socket::setsockopt(int level, int name, void* value, socklen_t len)
{
  if (::setsockopt(pFD, level, name, value, len) == -1) {
    DescriptorException ex;
    ex.getMessage() << "Socket: Unable to set socket option ";
    ex.getMessage() << level << "-" << name << ": ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }
}

//----------------------------------------------------------------------------
// The same as the ones in the manual
//----------------------------------------------------------------------------
void Socket::getsockopt(int level, int name, void* value, socklen_t& len)
{
  if (::getsockopt(pFD, level, name, value, &len) == -1) {
    DescriptorException ex;
    ex.getMessage() << "Socket: Unable to set socket option";
    ex.getMessage() << level << "-" << name << ": ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }
}
}
