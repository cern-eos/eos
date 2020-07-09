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

#include "kv/kv.hh"
#include "eosfuse.hh"
#include "misc/MacOSXHelper.hh"
#include "common/Logging.hh"
#include "misc/longstring.hh"
#include "hiredis/adapters/libevent.h"

/* -------------------------------------------------------------------------- */
RedisKV::RedisKV()
/* -------------------------------------------------------------------------- */
{
  mContext = 0;
  mEventBase = 0;
  mAsyncContext = 0;
}

/* -------------------------------------------------------------------------- */
RedisKV::~RedisKV()
/* -------------------------------------------------------------------------- */
{
  if (mContext) {
    redisFree(mContext);
    mContext = 0;
  }

  if (mEventBase) {
    free(mEventBase);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::connect(const std::string& prefix, const std::string& connectionstring,
                 int port)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("kv connect");
  mContext = redisConnect(connectionstring.c_str(), port);

  if (mContext->err) {
    int retc = mContext->err;
    redisFree(mContext);
    mContext = 0;
    return retc;
  }

  mAsyncContext = redisAsyncConnect(connectionstring.c_str(), port);

  if (mAsyncContext->err) {
    int retc = mAsyncContext->err;
    redisFree(mContext);
    redisAsyncFree(mAsyncContext);
    mContext = 0;
    mAsyncContext = 0;
    return retc;
  }

  mEventBase = event_base_new();
  eos_static_info("attach event loop");
  redisLibeventAttach(mAsyncContext, mEventBase);
  mPrefix = prefix + ":";
  eos_static_info("redis@%s:%d connected - prefix=%s", connectionstring.c_str(),
                  port, mPrefix.c_str());
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::get(const std::string& key, std::string& value)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%s context=%d", key.c_str(), mContext);
  int rc = 0;

  if (!mContext) {
    return rc;
  }

  redisReply* reply = (redisReply*) redisCommand(mContext, "GET %s",
                      prefix(key).c_str());

  if (reply->type == REDIS_REPLY_ERROR) {
    rc = -1;
  }

  if (reply->type == REDIS_REPLY_NIL) {
    rc = 1;
  }

  if (reply->type == REDIS_REPLY_STRING) {
    value.assign(reply->str, reply->len);
  }

  freeReplyObject(reply);
  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::inc(const std::string& key, uint64_t& value)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%s context=%d", key.c_str(), mContext);
  int rc = 0;

  if (!mContext) {
    return rc;
  }

  redisReply* reply = (redisReply*) redisCommand(mContext, "INCR %s",
                      prefix(key).c_str());

  if (reply->type == REDIS_REPLY_ERROR) {
    rc = -1;
  }

  if (reply->type == REDIS_REPLY_NIL) {
    rc = 1;
  }

  if (reply->type == REDIS_REPLY_STRING) {
    std::string svalue;
    svalue.assign(reply->str, reply->len);
    value = strtoull(svalue.c_str(), 0, 10);
  }

  freeReplyObject(reply);
  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::put(const std::string& key, const std::string& value)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%s context=%d", key.c_str(), mContext);

  if (!mContext) {
    return 0;
  }

  XrdSysMutexHelper locker(this);
  redisAsyncCommand(mAsyncContext, 0, 0, "SET %s %b",
                    prefix(key).c_str(),
                    value.c_str(),
                    value.length());
  //event_base_dispatch(mEventBase);
  event_base_loop(mEventBase, EVLOOP_NONBLOCK);
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::erase(const std::string& key)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%s context=%d", key.c_str(), mContext);

  if (!mContext) {
    return 0;
  }

  eos_static_info("key=%s", key.c_str());
  XrdSysMutexHelper locker(this);
  redisAsyncCommand(mAsyncContext, 0, 0, "DEL %s",
                    prefix(key).c_str());
  //event_base_dispatch(mEventBase);
  event_base_loop(mEventBase, EVLOOP_NONBLOCK);
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::get(uint64_t key, std::string& value, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long) key);

  if (!mContext) {
    return ENOENT;
  }

  char buffer[128];
  longstring::unsigned_to_decimal(key, buffer);
  std::string sbuf(buffer);

  if (name_space.length()) {
    sbuf = name_space + ":" + sbuf;
  }

  return get(sbuf, value);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::get(uint64_t key, uint64_t& value, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long) key);

  if (!mContext) {
    return ENOENT;
  }

  char buffer[128];
  longstring::unsigned_to_decimal(key, buffer);
  std::string sbuf(buffer);

  if (name_space.length()) {
    sbuf = name_space + ":" + sbuf;
  }

  return get(sbuf, value);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::get(const std::string& key, uint64_t& value)
/* -------------------------------------------------------------------------- */
{
  if (!mContext) {
    return ENOENT;
  }

  std::string lvalue;
  int rc = get(key, lvalue);

  if (!rc) {
    value = strtoull(lvalue.c_str(), 0, 10);
  }

  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
RedisKV::put(const std::string& key, uint64_t value)
/* -------------------------------------------------------------------------- */
{
  if (!mContext) {
    return 0;
  }

  char buffer[128];
  longstring::unsigned_to_decimal(value, buffer);
  std::string sbuf(buffer);
  return put(key, sbuf);
}

int
/* -------------------------------------------------------------------------- */
RedisKV::put(uint64_t key, const std::string& value,
             const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long) key);

  if (!mContext) {
    return 0;
  }

  char buffer[128];
  longstring::unsigned_to_decimal(key, buffer);
  std::string sbuf(buffer);

  if (name_space.length()) {
    sbuf = name_space + ":" + sbuf;
  }

  eos_static_info("key=%s", sbuf.c_str());
  return put(sbuf, value);
}

int
/* -------------------------------------------------------------------------- */
RedisKV::put(uint64_t key, uint64_t value, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long) key);

  if (!mContext) {
    return 0;
  }

  char buffer[128];
  longstring::unsigned_to_decimal(key, buffer);
  std::string sbuf(buffer);

  if (name_space.length()) {
    sbuf = name_space + ":" + sbuf;
  }

  eos_static_info("key=%s", sbuf.c_str());
  return put(sbuf, value);
}

int
/* -------------------------------------------------------------------------- */
RedisKV::erase(uint64_t key, const std::string& name_space)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("key=%lld", (unsigned long long) key);

  if (!mContext) {
    return 0;
  }

  char buffer[128];
  longstring::unsigned_to_decimal(key, buffer);
  std::string sbuf(buffer);

  if (name_space.length()) {
    sbuf = name_space + ":" + sbuf;
  }

  eos_static_info("key=%s", sbuf.c_str());
  return erase(sbuf);
}
