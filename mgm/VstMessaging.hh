// ----------------------------------------------------------------------
// File: VstMessaging.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#ifndef __EOSMGM_VSTMESSAGING__HH__
#define __EOSMGM_VSTMESSAGING__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysDNS.hh"
/*----------------------------------------------------------------------------*/
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class VstMessaging : public XrdMqMessaging, public eos::common::LogId
{
public:
  // we have to clone the base class constructors otherwise we cannot run inside valgrind                                                                                                            
  VstMessaging (const char* url, const char* defaultreceiverqueue, bool advisorystatus = false, bool advisoryquery = false, XrdMqSharedObjectManager* som = 0);

  virtual
  ~VstMessaging () { }

  virtual bool Update (XrdAdvisoryMqMessage* advmsg);
  virtual void Listen ();
  virtual void Process (XrdMqMessage* newmessage);
  // listener thread startup                                                                                                                                                                         
  static void* Start (void*);
  
  bool SetInfluxUdpEndpoint(const char*, bool onlyme);
  int GetInfluxUdpPort() {return InfluxUdpPort;}
  std::string& GetInfluxUdpHost() {return InfluxUdpHost;}
  std::string& GetInfluxUdpEndpoint() {return InfluxUdpEndpoint;}
  
  bool PublishInfluxDbUdp();
  bool KeyIsString(std::string key); //< defines if a published key should be treated as a String
  bool GetPublishOnlySelf() { return PublishOnlySelf; }
private:
  XrdMqClient mMessageClient;   
  std::string mVstMessage;
  std::string& PublishVst();
  
  std::string InfluxUdpEndpoint; //< UDP target host:port
  std::string InfluxUdpHost; //< UDP target hostname
  int InfluxUdpPort; //< UDP target port
  int InfluxUdpSocket; //< UDP socket
  struct sockaddr_in InfluxUdpSocketAddr; //< UDP socket addresss
  bool PublishOnlySelf;
};

EOSMGMNAMESPACE_END

#endif
