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

static void resolve( const char *address, sockaddr_in &addr )
  throw( eos::DescriptorException )
{
  eos::DescriptorException ex;

  //--------------------------------------------------------------------------
  // Get the ip address
  //--------------------------------------------------------------------------
  hostent *hp;

  //--------------------------------------------------------------------------
  // Enlarge the buffer until the call succeeds
  //--------------------------------------------------------------------------

#ifdef __APPLE__ 
  hp = gethostbyname( address );
  if (!hp) {
    ex.getMessage() << "Socket: get host by name failed";
    throw ex;
  }
#else
  hostent  hostbuf;
  size_t   hstbuflen = 1024;
  int      herr;
  int      res;
  char    *tmphstbuf = (char*)malloc( hstbuflen );

  while( (res = gethostbyname_r( address, &hostbuf, tmphstbuf, hstbuflen,
                                 &hp, &herr ) ) == ERANGE)
    {
      hstbuflen *= 2;
      tmphstbuf  = (char*)realloc( tmphstbuf, hstbuflen );
    }

  if( res || !hp )
    {
      free( tmphstbuf );
      ex.getMessage() << "Socket: get host by name failed";
      throw ex;
    }
#endif

  if( hp->h_addr_list == 0 )
    {
      ex.getMessage() << "Socket: host unknown";
      throw ex;
    }

  if( hp->h_addr_list[0] == 0 )
    {
      ex.getMessage() << "Socket: host unknown";
      throw ex;
    }

  memcpy( &addr.sin_addr.s_addr, hp->h_addr_list[0], sizeof( in_addr ) );
#ifndef __APPLE__
  free( tmphstbuf );
#endif
}

namespace eos
{
  //----------------------------------------------------------------------------
  // Initializer
  //----------------------------------------------------------------------------
  void Socket::init( Protocol proto ) throw( DescriptorException )
  {
    if( pFD != -1 )
      {
        DescriptorException ex;
        ex.getMessage() << "Socket: socket is already initialized";
        throw ex;
      }

    //--------------------------------------------------------------------------
    // Create the socket
    //--------------------------------------------------------------------------
    int type = SOCK_STREAM;
    if( proto == UDP )
      type = SOCK_DGRAM;

    if( (pFD = socket( PF_INET, type, 0 ) ) == -1 )
      {
        DescriptorException ex;
        ex.getMessage() << "Socket: Unable to create socket: ";
        ex.getMessage() << strerror( errno );
        throw ex;
      }
  }

  //----------------------------------------------------------------------------
  // Connect the socket
  //----------------------------------------------------------------------------
  void Socket::connect( const char *address, unsigned port )
    throw( DescriptorException )
  {
    DescriptorException ex;
    if( pFD == -1 )
      init( TCP );

    //--------------------------------------------------------------------------
    //! Resolve the hostname
    //--------------------------------------------------------------------------
    sockaddr_in addr;
    resolve( address, addr );

    //--------------------------------------------------------------------------
    // Set up the port
    //--------------------------------------------------------------------------
    addr.sin_family = AF_INET;
    addr.sin_port   = htons( (unsigned short)port );

    //--------------------------------------------------------------------------
    // Connect to the remote host
    //--------------------------------------------------------------------------
    if( (pFD<0) || ( ::connect( pFD, (sockaddr*)&addr, sizeof( addr ) ) != 0 ))
      {
        if (pFD>0) 
          ::close( pFD );
        ex.getMessage() << "Socket: Connection failed: ";
        ex.getMessage() << strerror( errno );
        throw ex;
      }
  }

  //----------------------------------------------------------------------------
  // Bind to the port
  //----------------------------------------------------------------------------
  void Socket::bind( const char *address, unsigned port )
    throw( DescriptorException )
  {
    if( pFD == -1 )
      init( TCP );

    //--------------------------------------------------------------------------
    // Resolve the address
    //--------------------------------------------------------------------------
    sockaddr_in         localAddr;
    DescriptorException ex;

    memset( &localAddr, 0, sizeof( localAddr ) );

    if( address )
      resolve( address, localAddr );
    else
      localAddr.sin_addr.s_addr = INADDR_ANY;

    localAddr.sin_family = AF_INET;
    localAddr.sin_port   = htons( (unsigned short)port );

    //--------------------------------------------------------------------------
    // Bind the socket
    //--------------------------------------------------------------------------
    if( (pFD<0) || (::bind( pFD, (sockaddr *)&localAddr, sizeof( sockaddr_in ) ) == -1 ))
      {
        if (pFD) 
          ::close( pFD );
        ex.getMessage() << "Socket: Unable to bind to port: " << port << " ";
        ex.getMessage() << strerror( errno );
        throw ex;
      }
  }

  //----------------------------------------------------------------------------
  // Listen to the incomming connections
  //----------------------------------------------------------------------------
  void Socket::listen( unsigned queue ) throw( DescriptorException )
  {
    DescriptorException ex;

    //--------------------------------------------------------------------------
    // Listen to the incomming connections
    //--------------------------------------------------------------------------
    if( ::listen( pFD, 20 ) == -1 )
      {
        ex.getMessage() << "Socket: Unable to listen: " << strerror( errno );
        throw ex;
      }
  }

  Socket *Socket::accept() throw( DescriptorException )
  {
    DescriptorException ex;

    //--------------------------------------------------------------------------
    // Accept the connection
    //--------------------------------------------------------------------------
    socklen_t   sinSize = sizeof( sockaddr_in );
    sockaddr_in remoteAddr;
    int newSock = ::accept( pFD, (sockaddr*)&remoteAddr, &sinSize );

    if( newSock == -1 )
      {
        ex.getMessage() << "Socket: Error while accpeting connection: ";
        ex.getMessage() << strerror(errno);
        throw ex;
      }

    //--------------------------------------------------------------------------
    // Create a new thread to handle this connection
    //--------------------------------------------------------------------------
    return new Socket( newSock );
  }

  //----------------------------------------------------------------------------
  // Close the socket
  //----------------------------------------------------------------------------
  void Descriptor::close()
  {
    if( pFD != -1 )
      {
        ::close( pFD );
        pFD = -1;
      }
  }

  //----------------------------------------------------------------------------
  // Read the buffer from the blocking descriptor (socket, pipe), it won't
  // return untill all the requested data is read
  //----------------------------------------------------------------------------
  void Descriptor::readBlocking( char *buffer, unsigned len )
    throw( DescriptorException )
  {
    if( len == 0 )
      return;

    int   ret;
    int   left = len;
    char *ptr  = buffer;
    while( 1 )
      {
        ret = ::read( pFD, ptr, left );
        if( ret == -1 || ret == 0 )
          {
            DescriptorException ex;
            ex.getMessage() << "Descriptor: Unable to read " << len << " bytes: ";
            ex.getMessage() << strerror( errno );
            throw ex;
          }

        left -= ret;
        if( !left )
          return;

        ptr += ret;
      }
  }

  //----------------------------------------------------------------------------
  // Read the buffer from the non-blocking descriptor (file, block device),
  // it won't return untill all the requested data is read.
  //----------------------------------------------------------------------------
  void Descriptor::readNonBlocking( char *buffer, unsigned len, unsigned poll )
    throw( DescriptorException )
  {
    if( len == 0 )
      return;

    int   ret;
    int   left = len;
    char *ptr  = buffer;
    while( 1 )
      {
        ret = ::read( pFD, ptr, left );
        if( ret == -1 )
          {
            DescriptorException ex;
            ex.getMessage() << "Descriptor: Unable to read " << len << " bytes: ";
            ex.getMessage() << strerror( errno );
            throw ex;
          }

        if( ret == 0 )
          {
            if( poll != 0 )
              usleep( poll );
            else
              {
                DescriptorException ex;
                ex.getMessage() << "Descriptor: Not enough data to fulfill the request";
                throw ex;
              }
          }

        left -= ret;
        if( !left )
          return;

        ptr += ret;
      }
  }

  //----------------------------------------------------------------------------
  // Read the buffer from the non-blocking descriptor (file, block device)
  // at given offset, it won't return untill all the requested data is read.
  //----------------------------------------------------------------------------
  void Descriptor::offsetReadNonBlocking( char *buffer, unsigned len,
                                          off_t offset, unsigned poll )
    throw( DescriptorException )
  {
    if( len == 0 )
      return;

    int   ret;
    off_t off  = offset;
    int   left = len;
    char *ptr  = buffer;
    while( 1 )
      {
        ret = ::pread( pFD, ptr, left, off );
        if( ret == -1 )
          {
            DescriptorException ex;
            ex.getMessage() << "Descriptor: Unable to read " << len << " bytes";
            ex.getMessage() << "at offset " << offset << ": ";
            ex.getMessage() << strerror( errno );
            throw ex;
          }

        if( ret == 0 )
          {
            if( poll != 0 )
              usleep( poll );
            else
              {
                DescriptorException ex;
                ex.getMessage() << "Descriptor: Not enough data to fulfill the request";
                throw ex;
              }
          }

        left -= ret;
        off  += ret;

        if( !left )
          return;

        ptr += ret;
      }
  }

  //----------------------------------------------------------------------------
  // Write data to the descriptor
  //----------------------------------------------------------------------------
  void Descriptor::write( const char *buffer, unsigned len )
    throw( DescriptorException )
  {
    if( len == 0 )
      return;

    int         ret  = 0;
    int         left = len;
    const char *ptr  = buffer;
    while( 1 )
      {
        ret = ::write( pFD, ptr, left );
        if( ret == -1  || ret == 0 )
          {
            DescriptorException ex;
            ex.getMessage() << "Descriptor: Unable to write " << len << " bytes: ";
            ex.getMessage() << strerror( errno );
            throw ex;
          }

        left -= ret;
        if( !left )
          return;

        ptr += ret;
      }
  }

  //----------------------------------------------------------------------------
  // The same as the ones in the manual
  //----------------------------------------------------------------------------
  void Socket::setsockopt( int level, int name, void *value, socklen_t len )
    throw( DescriptorException )
  {
    if( ::setsockopt( pFD, level, name, value, len ) == -1 )
      {
        DescriptorException ex;
        ex.getMessage() << "Socket: Unable to set socket option ";
        ex.getMessage() << level << "-" << name << ": ";
        ex.getMessage() << strerror( errno );
        throw ex;
      }
  }

  //----------------------------------------------------------------------------
  // The same as the ones in the manual
  //----------------------------------------------------------------------------
  void Socket::getsockopt( int level, int name, void *value, socklen_t &len )
    throw( DescriptorException )
  {
    if( ::getsockopt( pFD, level, name, value, &len ) == -1 )
      {
        DescriptorException ex;
        ex.getMessage() << "Socket: Unable to set socket option";
        ex.getMessage() << level << "-" << name << ": ";
        ex.getMessage() << strerror( errno );
        throw ex;
      }
  }
}
