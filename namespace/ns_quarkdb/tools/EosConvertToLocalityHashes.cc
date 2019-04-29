/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include <getopt.h>
#include <iostream>
#include <qclient/Utils.hh>
#include <qclient/Members.hh>
#include <qclient/QClient.hh>
#include <qclient/structures/QHash.hh>
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/persistency/Serialization.hh"
#include "namespace/utils/LocalityHint.hh"

using qclient::redisReplyPtr;

void processFileBucket(qclient::QClient &qcl, uint64_t bucketId) {
  std::string bucketString = SSTR(bucketId << eos::constants::sFileKeySuffix);
  std::cout << "Processing file bucket " << bucketString << std::endl;

  redisReplyPtr len = qcl.exec("HLEN", bucketString).get();
  if(len->type != REDIS_REPLY_INTEGER) {
    std::cerr << "Received unexpected response: " << qclient::describeRedisReply(len) << std::endl;
    std::abort();
  }

  if(len->integer == 0u) {
    std::cout << "--- Bucket is empty!" << std::endl;
  }
  else {
    std::cout << "--- Found " << len->integer << " items, converting..." << std::endl;
  }

  qclient::QHash bucket(qcl, bucketString);

  size_t processed = 0;
  for(auto it = bucket.getIterator(); it.valid(); it.next()) {
    processed++;
    std::cout << "Key: " << it.getKey() << std::endl;

    std::string value = it.getValue();
    eos::ns::FileMdProto fileProto;
    eos::MDStatus status = eos::Serialization::deserialize(value.c_str(), value.size(), fileProto);

    if(!status.ok()) {
      std::cout << "ERROR WHILE CONVERTING FileID " << it.getKey() << ", COULD NOT PARSE" << std::endl;
      std::abort();
    }

    std::string localityHint = eos::LocalityHint::build(eos::ContainerIdentifier(fileProto.cont_id()), fileProto.name());
    qcl.exec("CONVERT-HASH-FIELD-TO-LHASH", bucketString, it.getKey(), eos::constants::sFileKey, it.getKey(), localityHint);
    if(processed % 1024 == 0) std::cout << "Processed " << processed << std::endl;
  }
}

void processContainerBucket(qclient::QClient &qcl, uint64_t bucketId) {
  std::string bucketString = SSTR(bucketId << eos::constants::sContKeySuffix);
  std::cout << "Processing container bucket " << bucketString << std::endl;

  redisReplyPtr len = qcl.exec("HLEN", bucketString).get();
  if(len->type != REDIS_REPLY_INTEGER) {
    std::cerr << "Received unexpected response: " << qclient::describeRedisReply(len) << std::endl;
    std::abort();
  }

  if(len->integer == 0u) {
    std::cout << "--- Bucket is empty!" << std::endl;
  }
  else {
    std::cout << "--- Found " << len->integer << " items, converting..." << std::endl;
  }

  qclient::QHash bucket(qcl, bucketString);

  size_t processed = 0;
  for(auto it = bucket.getIterator(); it.valid(); it.next()) {
    processed++;
    std::cout << "Key: " << it.getKey() << std::endl;

    std::string value = it.getValue();
    eos::ns::ContainerMdProto containerProto;
    eos::MDStatus status = eos::Serialization::deserialize(value.c_str(), value.size(), containerProto);

    if(!status.ok()) {
      std::cout << "ERROR WHILE CONVERTING ContainerID " << it.getKey() << ", COULD NOT PARSE" << std::endl;
      std::abort();
    }

    std::string localityHint = eos::LocalityHint::build(eos::ContainerIdentifier(containerProto.parent_id()), containerProto.name());
    qcl.exec("CONVERT-HASH-FIELD-TO-LHASH", bucketString, it.getKey(), eos::constants::sContainerKey, it.getKey(), localityHint);
    if(processed % 1024 == 0) std::cout << "Processed " << processed << std::endl;
  }
}


int main(int argc, char* argv[]) {
  // TODO(gbitzes): Remove this tool eventually in a couple of months, when no
  // mixed-layout instances remain

  if(argc != 2) {
    std::cerr << "This tools converts old EOS NS layouts using hash-buckets, to the new one using locality hashes." << std::endl;
    std::cerr << "You most probably never need to run this tool. EOS instances created " << std::endl;
    std::cerr << "after 18 May 2018 should have the new layout automatically." << std::endl;
    std::cerr << "Usage: " << argv[0] << " <quarkdb command-separated endpoints, such as localhost:7777,localhost:7778>" << std::endl;
    exit(1);
  }

  qclient::Members members;
  if(!members.parse(argv[1])) {
    std::cerr << "Cannot parse cluster members." << std::endl;
    exit(1);
  }

  qclient::Options opts;
  opts.transparentRedirects = true;
  opts.retryStrategy = qclient::RetryStrategy::WithTimeout(std::chrono::seconds(20));
  qclient::QClient qcl(members, std::move(opts));

  for(uint64_t i = 0; i < eos::RequestBuilder::sNumFileBuckets; i++) {
    processFileBucket(qcl, i);
    processFileBucket(qcl, i);
  }

  for(uint64_t i = 0; i < eos::RequestBuilder::sNumContBuckets; i++) {
    processContainerBucket(qcl, i);
    processContainerBucket(qcl, i);
  }

  return 0;
}
