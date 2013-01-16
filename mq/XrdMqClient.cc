// ----------------------------------------------------------------------
// File: XrdMqClient.cc
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

//          $Id: XrdMqClient.cc,v 1.00 2007/10/04 01:34:19 ajp Exp $

const char* XrdMqClientCVSID = "$Id: XrdMqClient.cc,v 1.0.0 2007/10/04 01:34:19 ajp Exp $";

#include <mq/XrdMqClient.hh>
#include <mq/XrdMqTiming.hh>

#include <XrdSys/XrdSysDNS.hh>
#include <XrdSys/XrdSysTimer.hh>
#include <XrdClient/XrdClientUrlSet.hh>
#include <XrdClient/XrdClientConn.hh>

#include <setjmp.h>
#include <signal.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

/******************************************************************************/
/*                        X r d M q C l i e n t                               */
/******************************************************************************/

XrdSysMutex XrdMqClient::Mutex;
//------------------------------------------------------------------------------
// Signal Handler for SIGBUS
//------------------------------------------------------------------------------

static sigjmp_buf xrdmqclient_sj_env;

static void xrdmqclient_sigbus_hdl( int sig, siginfo_t* siginfo, void* ptr )
{
  // to jump to execution point
  siglongjmp( xrdmqclient_sj_env, 1 );
}


//------------------------------------------------------------------------------
// Subscribe
//------------------------------------------------------------------------------
bool
XrdMqClient::Subscribe( const char* queue )
{
  if ( queue ) {
    // at the moment we support subscrition to a single queue only - queue has to be 0 !!!
    XrdMqMessage::Eroute.Emsg( "Subscribe", EINVAL, "subscribe to additional user specified queue" );
    return false;
  }

  for ( int i = 0; i < kBrokerN; i++ ) {
    XrdCl::OpenFlags::Flags flags_xrdcl = XrdCl::OpenFlags::Read;
    XrdCl::File* file = GetBrokerXrdClientReceiver( i );
    XrdOucString* url = kBrokerUrls.Find( GetBrokerId( i ).c_str() );

    if ( !file || !file->Open( url->c_str(), flags_xrdcl ).IsOK() ) {
      // open failed
      continue;
    } else {
      CheckBrokerXrdClientReceiver( i );
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Unsubscribe
//------------------------------------------------------------------------------
bool
XrdMqClient::Unsubscribe( const char* queue )
{
  if ( queue ) {
    XrdMqMessage::Eroute.Emsg( "Unubscribe", EINVAL, "unsubscribe from additional user specified queue" );
    return false;
  }

  for ( int i = 0; i < kBrokerN; i++ ) {
    XrdCl::File* file = GetBrokerXrdClientReceiver( i );

    if ( file && ( !file->Close().IsOK() ) ) {
      // open failed
      continue;
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// Disconnect
//------------------------------------------------------------------------------
void
XrdMqClient::Disconnect()
{
  for ( int i = 0; i < kBrokerN; i++ ) {
    delete GetBrokerXrdClientReceiver( i );
  }

  kBrokerN = 0;
  return;
}


//------------------------------------------------------------------------------
// SendMessage
//------------------------------------------------------------------------------
bool
XrdMqClient::SendMessage( XrdMqMessage& msg, const char* receiverid, bool sign, bool encrypt )
{
  XrdSysMutexHelper lock( Mutex );
  bool rc = true;
  int i = 0;
  // tag the sender
  msg.kMessageHeader.kSenderId = kClientId;
  // tag the send time
  XrdMqMessageHeader::GetTime( msg.kMessageHeader.kSenderTime_sec, msg.kMessageHeader.kSenderTime_nsec );

  // tag the receiver queue
  if ( !receiverid ) {
    msg.kMessageHeader.kReceiverQueue = kDefaultReceiverQueue;
  } else {
    msg.kMessageHeader.kReceiverQueue = receiverid;
  }

  if ( encrypt ) {
    msg.Sign( true );
  } else {
    if ( sign )
      msg.Sign( false );
    else
      msg.Encode();
  }

  XrdOucString message = msg.kMessageHeader.kReceiverQueue;
  message += "?";
  message += msg.GetMessageBuffer();

  if ( message.length() > ( 2 * 1000 * 1000 ) ) {
    fprintf( stderr, "XrdMqClient::SendMessage: error => trying to send message with size %d [limit is 2M]\n",
             message.length() );
    XrdMqMessage::Eroute.Emsg( "SendMessage", E2BIG, "The message exceeds the maximum size of 2M!" );
    return false;
  }

  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdCl::FileSystem* fs = 0;

  //  msg.Print();
  for ( i = 0 ; i < kBrokerN; i++ ) {
    XrdCl::URL url( GetBrokerUrl( i )->c_str() );

    if ( !url.IsValid() ) {
      fprintf( stderr, "error=URL is not valid: %s", GetBrokerUrl( i )->c_str() );
      XrdMqMessage::Eroute.Emsg( "SendMessage", EINVAL, "URL is not valid" );
      continue;
    }

    fs = new XrdCl::FileSystem( url );

    if ( !fs ) {
      fprintf( stderr, "error=failed to get new FS object" );
      XrdMqMessage::Eroute.Emsg( "SendMessage", EINVAL, "no broker available" );
      delete fs;
      continue;
    }

    arg.FromString( message.c_str() );
    status = fs->Query( XrdCl::QueryCode::OpaqueFile, arg, response );
    rc = status.IsOK();

    // we continue until any of the brokers accepts the message
    if ( !rc ) {
      XrdMqMessage::Eroute.Emsg( "SendMessage", status.errNo, status.GetErrorMessage().c_str() );
    }

    delete response;
    delete fs;
  }

  return true;
}


//------------------------------------------------------------------------------
// RecvMessage
//------------------------------------------------------------------------------
XrdMqMessage*
XrdMqClient::RecvFromInternalBuffer()
{
  if ( ( kMessageBuffer.length() - kInternalBufferPosition ) > 0 ) {
    // fprintf( stderr,"Message Buffer %ld\n", kMessageBuffer.length());
    //          there is still a message in the buffer
    int nextmessage;
    int firstmessage;
    // fprintf( stderr,"#### %ld Entering at position %ld %ld\n", time(NULL),
    //          kInternalBufferPosition, kMessageBuffer.length() );
    firstmessage = kMessageBuffer.find( XMQHEADER, kInternalBufferPosition );

    if ( firstmessage == STR_NPOS )
      return 0;
    else {
      if ( ( firstmessage > 0 ) && ( ( size_t )firstmessage > kInternalBufferPosition ) ) {
        kMessageBuffer.erase( 0, firstmessage );
        kInternalBufferPosition = 0;
      }
    }

    if ( ( kMessageBuffer.length() + kInternalBufferPosition ) < ( int )strlen( XMQHEADER ) )
      return 0;

    nextmessage = kMessageBuffer.find( XMQHEADER, kInternalBufferPosition + strlen( XMQHEADER ) );
    char savec = 0;

    if ( nextmessage != STR_NPOS ) {
      savec = kMessageBuffer.c_str()[nextmessage];
      ( ( char* )kMessageBuffer.c_str() )[nextmessage] = 0;
    }

    XrdMqMessage* message = XrdMqMessage::Create( kMessageBuffer.c_str() + kInternalBufferPosition );

    if ( !message ) {
      fprintf( stderr, "couldn't get any message\n" );
      return 0;
    }

    XrdMqMessageHeader::GetTime( message->kMessageHeader.kReceiverTime_sec,
                                 message->kMessageHeader.kReceiverTime_nsec );

    if ( nextmessage != STR_NPOS )( ( char* )kMessageBuffer.c_str() )[nextmessage] = savec;

    if ( nextmessage == STR_NPOS ) {
      // last message
      kMessageBuffer = "";
      kInternalBufferPosition = 0;
    } else {
      // move forward
      //kMessageBuffer.erase(0,nextmessage);
      kInternalBufferPosition = nextmessage;
    }

    return message;
  } else {
    kMessageBuffer = "";
    kInternalBufferPosition = 0;
  }

  return 0;
}


//------------------------------------------------------------------------------
// Receive message
//------------------------------------------------------------------------------
XrdMqMessage*
XrdMqClient::RecvMessage()
{
  if ( kBrokerN == 1 ) {
    // single broker case
    // try if there is still a buffered message
    XrdMqMessage* message;
    message = RecvFromInternalBuffer();

    if ( message ) return message;

    CheckBrokerXrdClientReceiver( 0 );
    XrdCl::File* file = GetBrokerXrdClientReceiver( 0 );

    if ( !file ) {
      // fatal no client
      XrdMqMessage::Eroute.Emsg( "RecvMessage", EINVAL, "receive message - no client present" );
      return 0;
    }

    XrdCl::StatInfo* stinfo = 0;

    while ( !file->Stat( true, stinfo ).IsOK() ) {
      ReNewBrokerXrdClientReceiver( 0 );
      file = GetBrokerXrdClientReceiver( 0 );
      XrdSysTimer sleeper;
      sleeper.Wait( 2000 );
      fprintf( stderr, "XrdMqClient::RecvMessage => Stat failed\n" );
    }

    if ( !stinfo->GetSize() ) {
      return 0;
    }

    // mantain a receiver buffer which fits the need
    if ( kRecvBufferAlloc < ( int ) stinfo->GetSize() ) {
      uint64_t allocsize = 1024 * 1024;

      if ( stinfo->GetSize() > allocsize ) {
        allocsize = stinfo->GetSize() + 1;
      }

      kRecvBuffer = static_cast<char*>( realloc( kRecvBuffer, allocsize ) );

      if ( !kRecvBuffer ) {
        // this is really fatal - we exit !
        exit( -1 );
      }

      kRecvBufferAlloc = allocsize;
    }

    // read all messages
    uint32_t nread = 0;
    XrdCl::XRootDStatus status = file->Read( 0, stinfo->GetSize(), kRecvBuffer, nread );

    if ( status.IsOK() && ( nread > 0 ) ) {
      kRecvBuffer[nread] = 0;
      // add to the internal message buffer
      kInternalBufferPosition = 0;
      kMessageBuffer = kRecvBuffer;
    }

    delete stinfo;
    return RecvFromInternalBuffer();
    // ...
  } else {
    // multiple broker case
    return 0;
  }

  return 0;
}


//------------------------------------------------------------------------------
// RegisterRecvCallback
//------------------------------------------------------------------------------
bool
XrdMqClient::RegisterRecvCallback( void ( *callback_func )( void* arg ) )
{
  return false;
}


//------------------------------------------------------------------------------
// GetBrokerUrl
//------------------------------------------------------------------------------
XrdOucString*
XrdMqClient::GetBrokerUrl( int i )
{
  static XrdSysMutex AliasMutex;
  static time_t AliasResolved = 0;
  XrdOucString n = "";
  n += i;
  // split url
  XrdOucString* s = kBrokerUrls.Find( n.c_str() );
  XrdOucString* a = kBrokerAliasUrls.Find( n.c_str() );
  XrdSysMutexHelper vMutex( AliasMutex );

  // if we didn't resolve the name yet or it is older than 5 seconds, resolve it ...
  if ( ( !a ) || ( ( time( NULL ) - AliasResolved ) > 5 ) ) {
    int hstart = s->find( "://" );
    int dstop  = s->find( ":", ( hstart > 2 ) ? ( hstart + 3 ) : 0 );
    int hstop  = s->find( "//", ( hstart > 2 ) ? ( hstart + 3 ) : 0 );
    XrdOucString hostname;
    XrdOucString queue;
    XrdOucString port;
    hostname.assign( *s, hstart + 3, ( dstop > 0 ) ? ( dstop - 1 ) : ( hstop - 1 ) );

    if ( dstop > 0 ) {
      port.assign( *s, dstop, hstop - 1 );
    }

    queue.assign( *s, hstop );
    //    fprintf(stdout,"hostname=%s port=%s queue=%s\n", hostname.c_str(), port.c_str(), queue.c_str());
    AliasResolved = time( NULL );
    // resolve the hostname to an ip, this is not IPV6 compatible
    struct hostent* hp;
    struct hostent* rhp;
    hp = gethostbyname( hostname.c_str() );
    hostname = "localhost";

    if ( hp ) {
      if ( hp->h_addrtype == AF_INET ) {
        if ( hp->h_addr_list[0] ) {
          hostname = inet_ntoa( *( struct in_addr* )hp->h_addr_list[0] );
          rhp = gethostbyaddr( hp->h_addr_list[0], sizeof( int ), AF_INET );

          if ( rhp ) {
            hostname = rhp->h_name;
          }
        }
      }
    }

    if ( hostname == "localhost" ) {
      hostname = "localhost.localdomain";
    }

    XrdOucString* alias = new XrdOucString();
    *alias = "root://";
    *alias += hostname;
    *alias += port;
    *alias += queue;
    kBrokerAliasUrls.Rep( n.c_str(), alias );
    //    fprintf(stderr,"alias=%s\n", alias->c_str());
  }

  return kBrokerAliasUrls.Find( n.c_str() );
}


//------------------------------------------------------------------------------
// GetBrokerId
//------------------------------------------------------------------------------
XrdOucString
XrdMqClient::GetBrokerId( int i )
{
  XrdOucString brokern;

  if ( i == 0 )
    brokern = "0";
  else
    brokern += kBrokerN;

  return brokern;
}


//------------------------------------------------------------------------------
// GetBrokerXrdClientReceiver
//------------------------------------------------------------------------------
XrdCl::File*
XrdMqClient::GetBrokerXrdClientReceiver( int i )
{
  return kBrokerXrdClientReceiver.Find( GetBrokerId( i ).c_str() );
}


//------------------------------------------------------------------------------
// GetBrokerXrdClientSender
//------------------------------------------------------------------------------
XrdCl::FileSystem*
XrdMqClient::GetBrokerXrdClientSender( int i )
{
  return kBrokerXrdClientSender.Find( GetBrokerId( i ).c_str() );
}


//------------------------------------------------------------------------------
// ReNewBrokerXrdClientReceiver
//------------------------------------------------------------------------------
void
XrdMqClient::ReNewBrokerXrdClientReceiver( int i )
{
  kBrokerXrdClientReceiver.Del( GetBrokerId( i ).c_str() );
  kBrokerXrdClientReceiver.Add( GetBrokerId( i ).c_str(), new XrdCl::File() );
  XrdCl::XRootDStatus status = GetBrokerXrdClientReceiver( i )->Open( GetBrokerUrl( i )->c_str(),
                               XrdCl::OpenFlags::Read );

  if ( !status.IsOK() ) {
    fprintf( stderr, "XrdMqClient::Reopening of new alias failed ...\n" );
  }
}


//------------------------------------------------------------------------------
// CheckBrokerXrdClientReceiver
//------------------------------------------------------------------------------
void
XrdMqClient::CheckBrokerXrdClientReceiver( int i )
{
  Mutex.Lock();

  // SIGBUS re-entry
  if ( sigsetjmp( xrdmqclient_sj_env, 1 ) ) {
    fprintf( stderr, "Fatal: recovered SIGBUS error - backoff for 5s\n" );
    XrdSysTimer sleeper;
    sleeper.Wait( 5000 );
  }

  XrdCl::File* file = GetBrokerXrdClientReceiver( i );

  if ( i < 256 ) {
    if ( kBrokerXrdClientReceiverAliasTimeStamp[i] &&
         ( ( time( NULL ) - kBrokerXrdClientReceiverAliasTimeStamp[i] ) < 10 ) ) {
      // do nothing
    } else {
      if ( file ) {
        //  fprintf(stderr,"Checking Broker\n");
        XrdOucString* bk = GetBrokerUrl( i );
        XrdOucString data_server = file->GetDataServer().c_str();

        if ( ( !kBrokerXrdClientReceiverAliasTimeStamp[i] ) ||
             ( ( bk ) && ( *bk != data_server ) &&
               ( bk->find( "localhost" ) == STR_NPOS ) ) ) {
          if ( *bk != data_server ) {
            fprintf( stderr, "XrdMqClient::CheckBrokerXrdClientReceiver => Broker alias changed from %s => %s\n",
                     data_server.c_str(), bk->c_str() );
          }

          ReNewBrokerXrdClientReceiver( i );
          // get the new client object
          GetBrokerXrdClientReceiver( i );
          // the alias has been switched, del the client and create a new one to connect to the new alias
        }

        kBrokerXrdClientReceiverAliasTimeStamp[i] = time( NULL );
      }
    }
  }

  Mutex.UnLock();
}


//------------------------------------------------------------------------------
// AddBroker
//------------------------------------------------------------------------------
bool XrdMqClient::AddBroker( const char* brokerurl,
                             bool        advisorystatus,
                             bool        advisoryquery )
{
  bool exists = false;

  if ( !brokerurl ) return false;

  XrdOucString newBrokerUrl = brokerurl;

  if ( ( newBrokerUrl.find( "?" ) ) == STR_NPOS ) {
    newBrokerUrl += "?";
  }

  newBrokerUrl += "&";
  newBrokerUrl += XMQCADVISORYSTATUS;
  newBrokerUrl += "=";
  newBrokerUrl += advisorystatus;
  newBrokerUrl += "&";
  newBrokerUrl += XMQCADVISORYQUERY;
  newBrokerUrl += "=";
  newBrokerUrl += advisoryquery;
  printf( "==> new Broker %s\n", newBrokerUrl.c_str() );

  for ( int i = 0; i < kBrokerN; i++ ) {
    XrdOucString* brk = GetBrokerUrl( i );

    if ( brk && ( ( *brk ) == newBrokerUrl ) ) exists = true;
  }

  if ( !exists ) {
    XrdOucString brokern = GetBrokerId( kBrokerN );
    kBrokerUrls.Add( brokern.c_str(), new XrdOucString( newBrokerUrl.c_str() ) );
    XrdCl::URL url( newBrokerUrl.c_str() );

    if ( !url.IsValid() ) {
      fprintf( stderr, "error=URL is not valid: %s", newBrokerUrl.c_str() );
      return exists;
    }

    XrdCl::FileSystem* fs = new XrdCl::FileSystem( url );

    if ( !fs ) {
      fprintf( stderr, "cannot create FS obj to %s\n", newBrokerUrl.c_str() );
      kBrokerUrls.Del( brokern.c_str() );
      XrdMqMessage::Eroute.Emsg( "AddBroker", EPERM, "add and connect to broker:", newBrokerUrl.c_str() );
      return false;
    }

    kBrokerXrdClientSender.Add( GetBrokerId( kBrokerN ).c_str(), fs );
    kBrokerXrdClientReceiver.Add( GetBrokerId( kBrokerN ).c_str(), new XrdCl::File() );
    kBrokerN++;
  }

  return ( !exists );
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqClient::XrdMqClient( const char* clientid,
                          const char* brokerurl,
                          const char* defaultreceiverid )
{
  kBrokerN = 0;
  kMessageBuffer = "";
  kRecvBuffer = 0;
  kRecvBufferAlloc = 0;
  // install sigbus signal handler
  struct sigaction act;
  memset( &act, 0, sizeof( act ) );
  act.sa_sigaction = xrdmqclient_sigbus_hdl;
  act.sa_flags = SA_SIGINFO;

  if ( sigaction( SIGBUS, &act, 0 ) ) {
    fprintf( stderr, "error: [XrdMqClient] cannot install SIGBUS handler\n" );
  }

  memset( kBrokerXrdClientReceiverAliasTimeStamp, 0, sizeof( int ) * 256 );
  memset( kBrokerXrdClientSenderAliasTimeStamp  , 0, sizeof( int ) * 256 );

  if ( brokerurl && ( !AddBroker( brokerurl ) ) ) {
    fprintf( stderr, "error: [XrdMqClient] cannot add broker %s\n", brokerurl );
  }

  if ( defaultreceiverid ) {
    kDefaultReceiverQueue = defaultreceiverid;
  } else {
    // default receiver is always a master
    kDefaultReceiverQueue = "/xmessage/*/master/*";
  }

  if ( clientid ) {
    kClientId = clientid;

    if ( kClientId.beginswith( "root://" ) ) {
      // truncate the URL away
      int pos = kClientId.find( "//", 7 );

      if ( pos != STR_NPOS ) {
        kClientId.erase( 0, pos + 1 );
      }
    }
  } else {
    // the default is to create the client id as /xmesssage/<domain>/<host>/
    int ppos = 0;
    char* cfull_name = XrdSysDNS::getHostName();
    XrdOucString FullName = cfull_name;
    XrdOucString HostName = FullName;
    XrdOucString Domain   = FullName;

    if ( ( ppos = FullName.find( "." ) ) != STR_NPOS ) {
      HostName.assign( FullName, 0, ppos - 1 );
      Domain.assign( FullName, ppos + 1 );
    } else {
      Domain = "unknown";
    }

    kClientId = "/xmessage/";
    kClientId += HostName;
    kClientId += "/";
    kClientId += Domain;
    free( cfull_name );
  }

  kInternalBufferPosition = 0;
}


//------------------------------------------------------------------------------
// Destructor                                                                 
//------------------------------------------------------------------------------
XrdMqClient::~XrdMqClient()
{
}

