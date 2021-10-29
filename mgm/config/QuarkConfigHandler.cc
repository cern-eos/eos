// ----------------------------------------------------------------------
// File: QuarkConfigHandler.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "mgm/config/QuarkConfigHandler.hh"
#include "common/Assert.hh"
#include "common/StringUtils.hh"

#include <qclient/QClient.hh>
#include <qclient/ResponseParsing.hh>
#include <qclient/MultiBuilder.hh>
#include "qclient/structures/QScanner.hh"
#include "qclient/structures/QDeque.hh"

#include <folly/Executor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <functional>

using std::placeholders::_1;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkConfigHandler::QuarkConfigHandler(const QdbContactDetails& cd)
  : mContactDetails(cd)
{
  mQcl = std::unique_ptr<qclient::QClient>(
           new qclient::QClient(mContactDetails.members,
                                mContactDetails.constructOptions()));
  mExecutor.reset(new folly::IOThreadPoolExecutor(2));
}

//------------------------------------------------------------------------------
// Ensure connection is established
//------------------------------------------------------------------------------
common::Status QuarkConfigHandler::checkConnection(std::chrono::milliseconds
    timeout)
{
  qclient::Status st = mQcl->checkConnection(timeout);
  return common::Status(st.getErrc(), st.getMsg());
}

//------------------------------------------------------------------------------
// Check if a given configuration exists
//------------------------------------------------------------------------------
common::Status QuarkConfigHandler::checkExistence(const std::string& name,
    bool& existence)
{
  qclient::IntegerParser existsResp(mQcl->exec("HLEN",
                                    SSTR("eos-config:" << name)).get());

  if (!existsResp.ok()) {
    return common::Status(EINVAL,
                          SSTR("Received unexpected response in HLEN existence check: " <<
                               existsResp.err()));
  }

  existence = (existsResp.value() > 0);
  return common::Status();
}

//----------------------------------------------------------------------------
// Obtain list of available configurations, and backups
//----------------------------------------------------------------------------
common::Status QuarkConfigHandler::listConfigurations(std::vector<std::string>&
    configs, std::vector<std::string>& backups)
{
  qclient::QScanner confScanner(*mQcl, "eos-config:*");

  for (; confScanner.valid(); confScanner.next()) {
    configs.emplace_back(confScanner.getValue().substr(11));
  }

  qclient::QScanner confScannerBackups(*mQcl, "eos-config-backup:*");

  for (; confScannerBackups.valid(); confScannerBackups.next()) {
    backups.emplace_back(confScannerBackups.getValue().substr(18));
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Fetch a given configuration
//------------------------------------------------------------------------------
common::Status
QuarkConfigHandler::fetchConfiguration(const std::string& name,
                                       std::map<std::string, std::string>& out)
{
  qclient::redisReplyPtr reply = mQcl->exec("HGETALL", FormHashKey(name)).get();
  qclient::HgetallParser parser(reply);

  if (!parser.ok() || parser.value().empty()) {
    eos_static_warning("msg=\"no such configuration in QDB\" name=\"%s\"",
                       name.c_str());
    // Check also if this is a backup configuration
    reply = mQcl->exec("HGETALL", FormBackupHashKey(name)).get();
    qclient::HgetallParser bkp_parser(reply);

    if (!bkp_parser.ok()) {
      eos_static_warning("msg=\"no such backup configuration in QDB\" "
                         "name=\"%s\"", name.c_str());
      return common::Status(EINVAL, bkp_parser.err());
    }

    out = bkp_parser.value();
  } else {
    out = parser.value();
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Process write configuration reply
//------------------------------------------------------------------------------
static common::Status processWriteConfigurationReply(size_t configSize,
    uint32_t extraReqs, qclient::redisReplyPtr reply)
{
  if (!reply) {
    return common::Status(EINVAL, "null reply, QDB backend not available?");
  }

  if (reply->elements != configSize + extraReqs) {
    return common::Status(EINVAL,
                          SSTR("unexpected number of elements in response: " <<
                               qclient::describeRedisReply(reply)));
  }

  for (size_t i = extraReqs; i < reply->elements; i++) {
    qclient::IntegerParser intParse(reply->element[i]);

    if (!intParse.ok() || intParse.value() != 1) {
      return common::Status(EINVAL,
                            SSTR("unexpected response in position " << i << ": " <<
                                 qclient::describeRedisReply(reply->element[i])));
    }
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Write the given configuration
//------------------------------------------------------------------------------
folly::Future<common::Status> QuarkConfigHandler::writeConfiguration(
  const std::string& name, const std::map<std::string, std::string>& config,
  bool overwrite, const std::string& backup)
{
  std::string configKey = SSTR("eos-config:" << name);
  qclient::IntegerParser hlenResp(mQcl->exec("HLEN", configKey).get());

  if (!hlenResp.ok()) {
    return common::Status(EINVAL,
                          SSTR("received unexpected response in HLEN check: " <<
                               hlenResp.err()));
  }

  if (!overwrite && hlenResp.value() != 0) {
    return common::Status(EINVAL,
                          "There's MGM configuration stored in QDB already -- will not delete.");
  }

  //----------------------------------------------------------------------------
  // Prepare write batch
  //----------------------------------------------------------------------------
  qclient::MultiBuilder multiBuilder;
  uint32_t extraReqs = 1;

  if (!backup.empty()) {
    std::string backupConfigKey = SSTR("eos-config-backup:" << name << "-" <<
                                       backup);
    multiBuilder.emplace_back("DEL", backupConfigKey);
    multiBuilder.emplace_back("HCLONE", configKey, backupConfigKey);
    extraReqs += 2;
  }

  multiBuilder.emplace_back("DEL", configKey);

  for (auto it = config.begin(); it != config.end(); it++) {
    multiBuilder.emplace_back("HSET", configKey, it->first, it->second);
  }

  return mQcl->follyExecute(multiBuilder.getDeque())
         .via(mExecutor.get())
         .thenValue(std::bind(processWriteConfigurationReply, config.size(), extraReqs,
                              _1));
}

//------------------------------------------------------------------------------
// Validate appendChangelog response
//------------------------------------------------------------------------------
common::Status checkAppendChangelogResponse(qclient::redisReplyPtr reply)
{
  if (!reply) {
    return common::Status(EINVAL, "no response from QDB backend");
  }

  if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2u) {
    return common::Status(EINVAL,
                          SSTR("unexpected reply from QDB: " << qclient::describeRedisReply(reply)));
  }

  redisReply* reply0 = reply->element[0];
  redisReply* reply1 = reply->element[1];

  if (reply0->type != REDIS_REPLY_INTEGER || reply0->integer != 1) {
    return common::Status(EINVAL,
                          SSTR("unexpected reply from QDB: " << qclient::describeRedisReply(reply)));
  }

  if (reply1->type != REDIS_REPLY_INTEGER) {
    return common::Status(EINVAL,
                          SSTR("unexpected reply from QDB: " << qclient::describeRedisReply(reply)));
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Append an entry to the changelog
//------------------------------------------------------------------------------
folly::Future<common::Status> QuarkConfigHandler::appendChangelog(
  const eos::mgm::ConfigChangelogEntry& entry)
{
  std::string serialized;

  if (!entry.SerializeToString(&serialized)) {
    return common::Status(EINVAL, "protobuf seriaization to string failed");
  }

  qclient::MultiBuilder multiBuilder;
  multiBuilder.emplace_back("deque-push-back", "eos-config-changelog:default",
                            serialized);
  multiBuilder.emplace_back("deque-trim-front", "eos-config-changelog:default",
                            "500000");
  return mQcl->follyExecute(multiBuilder.getDeque())
         .via(mExecutor.get())
         .thenValue(std::bind(checkAppendChangelogResponse, _1));
}

//------------------------------------------------------------------------------
// Show configuration changelog
//------------------------------------------------------------------------------
common::Status QuarkConfigHandler::tailChangelog(int nlines,
    std::vector<std::string>& entries)
{
  entries.clear();
  qclient::redisReplyPtr reply = mQcl->exec("deque-scan-back",
                                 "eos-config-changelog", "0",
                                 "COUNT", SSTR(nlines)).get();

  if (!reply || reply->type != REDIS_REPLY_ARRAY) {
    return common::Status(EINVAL,
                          SSTR("received unexpected reply type: " << qclient::describeRedisReply(reply)));
  }

  if (reply->elements != 2) {
    return common::Status(EINVAL,
                          SSTR("received unexpected number of elements in reply: " <<
                               qclient::describeRedisReply(reply)));
  }

  redisReply* array = reply->element[1];

  for (size_t i = 0; i < array->elements; i++) {
    if (array->element[i]->type != REDIS_REPLY_STRING) {
      return common::Status(EINVAL,
                            SSTR("received unexpected reply type for element #" << i << ": " <<
                                 qclient::describeRedisReply(array->element[i])));
    }

    entries.emplace_back(array->element[i]->str, array->element[i]->len);
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Trim backups to the nth most recent ones. If no more than N backups exist
// anyway, do nothing.
//
// We will delete a maximum of 200 backups at a time -- you may have to call
// this function multiple times to trim everything.
//------------------------------------------------------------------------------
common::Status QuarkConfigHandler::trimBackups(const std::string& name,
    size_t limit, size_t& deleted)
{
  std::vector<std::string> configs, backups;
  deleted = 0;
  common::Status st = listConfigurations(configs, backups);

  if (!st) {
    return st;
  }

  std::string targetPrefix = SSTR(name << "-");
  std::vector<std::string> targetsToClean;

  for (size_t i = 0; i < backups.size(); i++) {
    if (common::startsWith(backups[i], targetPrefix)) {
      targetsToClean.emplace_back(backups[i]);
    }
  }

  int backupsToDelete = (int) targetsToClean.size() - (int) limit;
  backupsToDelete = std::min(backupsToDelete, 200);

  if (backupsToDelete <= 0) {
    return common::Status();
  }

  std::vector<std::string> requestPayload = {"DEL"};

  for (int i = 0; i < backupsToDelete; i++) {
    requestPayload.emplace_back(SSTR("eos-config-backup:" << targetsToClean[i]));
  }

  qclient::redisReplyPtr reply = mQcl->execute(requestPayload).get();
  qclient::IntegerParser intParse(reply);

  if (!intParse.ok()) {
    return common::Status(EINVAL, intParse.err());
  }

  deleted = intParse.value();
  return common::Status();
}

//------------------------------------------------------------------------------
// Form configuration target key
//------------------------------------------------------------------------------
std::string QuarkConfigHandler::FormHashKey(const std::string& name)
{
  return SSTR("eos-config:" << name);
}

//------------------------------------------------------------------------------
// Form configuration backup target key
//------------------------------------------------------------------------------
std::string QuarkConfigHandler::FormBackupHashKey(const std::string& name)
{
  return SSTR("eos-config-backup:" << name);
}

//------------------------------------------------------------------------------
// Form configuration backup key
//------------------------------------------------------------------------------
std::string
QuarkConfigHandler::FormBackupHashKey(const std::string& name,
                                      time_t timestamp)
{
  char buff[128];
  strftime(buff, 127, "%Y%m%d%H%M%S", localtime(&timestamp));
  return SSTR("eos-config-backup" << ":" << name << "-" << buff);
}

EOSMGMNAMESPACE_END
