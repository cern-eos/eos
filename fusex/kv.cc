//------------------------------------------------------------------------------
//! @file kv.cc
//! @author Andreas-Joachim Peters CERN
//! @brief kv persistency class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "kv.hh"
#include "MacOSXHelper.hh"
#include "common/Logging.hh"
#include "longstring.hh"


kv* kv::sKV = 0;

/* -------------------------------------------------------------------------- */
kv::kv()
/* -------------------------------------------------------------------------- */
{
  sKV = this;
  mContext=0;
}

/* -------------------------------------------------------------------------- */
kv::~kv()
/* -------------------------------------------------------------------------- */
{
  if (mContext)
  {
    redisFree (mContext);
    mContext=0;
  }
  if (mEventBase)
  {
    free(mEventBase);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
kv::connect(std::string connectionstring, int port)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("kv connect");
  mContext = redisConnect(connectionstring.c_str(), port);

  if (mContext->err)
  {
    int retc=mContext->err;
    redisFree(mContext);
    mContext=0;
    return retc;
  }

  mAsyncContext = redisAsyncConnect(connectionstring.c_str(), port);
  
  if (mAsyncContext->err)
  {
    int retc=mAsyncContext->err;
    redisFree(mContext);
    redisAsyncFree(mAsyncContext);
    mContext=0;
    mAsyncContext=0;
    return retc;
  }
 
  mEventBase = event_base_new();
  eos_static_info("attach event loop");
  redisLibeventAttach(mAsyncContext, mEventBase);
  
  eos_static_info("redis@%s:%d connected", connectionstring.c_str(), port);
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
kv::get(std::string &key, std::string &value)
/* -------------------------------------------------------------------------- */
{
  int rc=0;
  
  if (!mContext)
    return rc;
  
  redisReply* reply = (redisReply*)redisCommand( mContext, "GET %s", key.c_str());
  if (reply->type == REDIS_REPLY_ERROR)
  {
    rc = -1;
  }
  
  if (reply->type == REDIS_REPLY_NIL)
  {
    rc = 1;
  }
  
  if (reply->type == REDIS_REPLY_STRING)
  {
    value.assign(reply->str, reply->len);
  }
  
  freeReplyObject(reply);
  
  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
kv::put(std::string &key, std::string &value)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%s context=%d",key.c_str(),mContext);
  if (!mContext)
    return 0;
  
  eos_static_info("key=%s",key.c_str());
  redisAsyncCommand(mAsyncContext, 0, 0, "SET %s %b", 
                    key.c_str(), 
                    value.c_str(), 
                    value.length());
  //event_base_dispatch(mEventBase);
  event_base_loop(mEventBase, EVLOOP_NONBLOCK);
  return 0;
}


/* -------------------------------------------------------------------------- */
int 
/* -------------------------------------------------------------------------- */
kv::get(uint64_t key, std::string &value)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long)key);
  char buffer[128];
  longstring::unsigned_to_decimal (key, buffer);
  std::string sbuf(buffer);
  return get(sbuf, value);
}


int 
/* -------------------------------------------------------------------------- */
kv::put(uint64_t key, std::string &value)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long)key);
  char buffer[128];
  longstring::unsigned_to_decimal (key, buffer);
  std::string sbuf(buffer);
  
  eos_static_info("key=%s", sbuf.c_str());
  return put(sbuf, value);
}
        