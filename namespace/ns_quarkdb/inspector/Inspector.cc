/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/inspector/Inspector.hh"
#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <qclient/QClient.hh>
#include <qclient/structures/QLocalityHash.hh>
#include <google/protobuf/util/json_util.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Inspector::Inspector(qclient::QClient &qcl) : mQcl(qcl) { }

//------------------------------------------------------------------------------
// Is the connection to QDB ok? If not, pointless to run anything else.
//------------------------------------------------------------------------------
bool Inspector::checkConnection(std::string &err) {
  qclient::redisReplyPtr reply = mQcl.exec("PING").get();

  if(!reply) {
    err = "Could not connect to the given QDB cluster";
    return false;
  }

  if(reply->type != REDIS_REPLY_STATUS || std::string(reply->str, reply->len) != "PONG") {
    err = SSTR("Received unexpected response in checkConnection: " << qclient::describeRedisReply(reply));
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Dump contents of the given path. ERRNO-like integer return value, 0
// means no error.
//------------------------------------------------------------------------------
int Inspector::dump(const std::string &dumpPath, std::ostream& out) {
  ExplorationOptions explorerOpts;
  std::unique_ptr<folly::Executor> executor(new folly::IOThreadPoolExecutor(4));

  NamespaceExplorer explorer(dumpPath, explorerOpts, mQcl, executor.get());
  NamespaceItem item;

  while(explorer.fetch(item)) {
    out << "path=" << item.fullPath << std::endl;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Check intra-container conflicts, such as a container having two entries
// with the name name.
//------------------------------------------------------------------------------
int Inspector::checkNamingConflicts(std::ostream &out, std::ostream &err) {
  qclient::QLocalityHash::Iterator iter(&mQcl, "eos-container-md");

  std::map<std::string, uint64_t> containerContents;
  uint64_t currentContainer = -1;
  int64_t processed = 0;

  for(; iter.valid(); iter.next()) {
    processed++;
    std::string item = iter.getValue();

    eos::ns::ContainerMdProto proto;
    eos::MDStatus status = Serialization::deserialize(item.c_str(), item.size(), proto);

    if(!status.ok()) {
      err  << "Error while deserializing: " << item << std::endl;
      continue;
    }

    if(processed % 100000 == 0) {
      err << "Processed so far: " << processed << std::endl;
    }

    if(currentContainer == proto.parent_id()) {
      auto conflict = containerContents.find(proto.name());
      if(conflict != containerContents.end()) {
        out << "Detected conflict for '" << proto.name() << "' in container " << currentContainer << ", between containers " << conflict->second << " and " << proto.id() << std::endl;
      }
    }
    else {
      currentContainer = proto.parent_id();
      containerContents.clear();
    }

    containerContents[proto.name()] = proto.id();
  }

  return 0;
}

//------------------------------------------------------------------------------
// Print out _everything_ known about the given file.
//------------------------------------------------------------------------------
int Inspector::printFileMD(uint64_t fid, std::ostream &out, std::ostream &err) {
  folly::Future<eos::ns::FileMdProto> fut = MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fid));
  eos::ns::FileMdProto val;

  try {
    val = fut.get();
  }
  catch(const MDException &e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what() << std::endl;
    return 1;
  }

  google::protobuf::util::JsonPrintOptions opts;
  opts.add_whitespace = true;

  std::string jsonString;
  google::protobuf::util::MessageToJsonString(val, &jsonString, opts);

  err << jsonString << std::endl;
  return 0;
}

EOSNSNAMESPACE_END
