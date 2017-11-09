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

#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "common/StringTokenizer.hh"
#include "common/Logging.hh"
#include "qclient/QScanner.hh"
#include <iostream>

EOSNSNAMESPACE_BEGIN

bool parseFsId(const std::string& str, IFileMD::location_t& fsid,
               bool& unlinked)
{
  std::vector<std::string> parts =
    eos::common::StringTokenizer::split<std::vector<std::string>>(str, ':');

  if (parts.size() != 3) {
    return false;
  }

  if (parts[0] + ":" != fsview::sPrefix) {
    return false;
  }

  fsid = std::stoull(parts[1]);

  if (parts[2] == fsview::sFilesSuffix) {
    unlinked = false;
  } else if (parts[2] == fsview::sUnlinkedSuffix) {
    unlinked = true;
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemView::FileSystemView():
  pQcl(BackendClient::getInstance()),
  pNoReplicasSet(*pQcl, fsview::sNoReplicaPrefix)
{
  pFlusher = nullptr;
}

//------------------------------------------------------------------------------
// Configure the container service
//------------------------------------------------------------------------------
void
FileSystemView::configure(const std::map<std::string, std::string>& config)
{
  uint32_t port{0};
  std::string host{""};
  const std::string key_host = "qdb_host";
  const std::string key_port = "qdb_port";

  if (config.find(key_host) != config.end()) {
    host = config.at(key_host);
  }

  if (config.find(key_port) != config.end()) {
    port = std::stoul(config.at(key_port));
  }

  pQcl = BackendClient::getInstance(host, port);
  pNoReplicasSet.setClient(*pQcl);
  pFlusher = MetadataFlusherFactory::getInstance("default", host, port);
}

//------------------------------------------------------------------------------
// Notify the me about the changes in the main view
//------------------------------------------------------------------------------
void
FileSystemView::fileMDChanged(IFileMDChangeListener::Event* e)
{
  std::string key, val;
  FileMD* file = static_cast<FileMD*>(e->file);
  qclient::QSet fs_set;

  switch (e->action) {
  // New file has been created
  case IFileMDChangeListener::Created:
    pFlusher->sadd(fsview::sNoReplicaPrefix, std::to_string(file->getId()));
    break;

  // File has been deleted
  case IFileMDChangeListener::Deleted:
    pFlusher->srem(fsview::sNoReplicaPrefix, std::to_string(file->getId()));
    break;

  // Add location
  case IFileMDChangeListener::LocationAdded:
    key = keyFilesystemFiles(e->location);
    val = std::to_string(file->getId());
    pFlusher->sadd(key, val);
    pFlusher->srem(fsview::sNoReplicaPrefix, val);
    break;

  // Replace location
  case IFileMDChangeListener::LocationReplaced:
    key = keyFilesystemFiles(e->oldLocation);
    val = std::to_string(file->getId());
    pFlusher->srem(key, val);
    key = keyFilesystemFiles(e->location);
    pFlusher->sadd(key, val);
    break;

  // Remove location
  case IFileMDChangeListener::LocationRemoved:
    key = keyFilesystemUnlinked(e->location);
    val = std::to_string(file->getId());
    pFlusher->srem(key, val);

    if (!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation()) {
      pFlusher->sadd(fsview::sNoReplicaPrefix, val);
    }

    break;

  // Unlink location
  case IFileMDChangeListener::LocationUnlinked:
    key = keyFilesystemFiles(e->location);
    val = std::to_string(e->file->getId());
    pFlusher->srem(key, val);
    key = keyFilesystemUnlinked(e->location);
    pFlusher->sadd(key, val);
    break;

  default:
    break;
  }
}

//------------------------------------------------------------------------------
// Recheck the current file object and make any modifications necessary so
// that the information is consistent in the back-end KV store.
//------------------------------------------------------------------------------
bool
FileSystemView::fileMDCheck(IFileMD* file)
{
  std::string key;
  IFileMD::LocationVector replica_locs = file->getLocations();
  IFileMD::LocationVector unlink_locs = file->getUnlinkedLocations();
  bool has_no_replicas = replica_locs.empty() && unlink_locs.empty();
  std::string cursor {"0"};
  std::pair<std::string, std::vector<std::string>> reply;
  std::atomic<bool> has_error{false};
  qclient::AsyncHandler ah;

  // If file has no replicas make sure it's accounted for
  if (has_no_replicas) {
    ah.Register(pNoReplicasSet.sadd_async(file->getId()),
                pNoReplicasSet.getClient());
  } else {
    ah.Register(pNoReplicasSet.srem_async(file->getId()),
                pNoReplicasSet.getClient());
  }

  // Make sure all active locations are accounted for.
  qclient::QSet replica_set(*pQcl, "");

  for (IFileMD::location_t location : replica_locs) {
    replica_set.setKey(keyFilesystemFiles(location));
    ah.Register(replica_set.sadd_async(file->getId()), replica_set.getClient());
  }

  // Make sure all unlinked locations are accounted for.
  qclient::QSet unlink_set(*pQcl, "");

  for (IFileMD::location_t location : unlink_locs) {
    unlink_set.setKey(keyFilesystemUnlinked(location));
    ah.Register(unlink_set.sadd_async(file->getId()), unlink_set.getClient());
  }

  // Make sure there's no other filesystems that erroneously contain this file.
  // This will generate a gazillion writes towards QuarkDB, one for each
  // filesystem not containing the file.. which is almost all of them!
  // Maybe rethink if there's a better way?

  for (auto it = this->getFilesystemIterator(); it->valid(); it->next()) {
    IFileMD::location_t fsid = it->getFilesystemID();

    if (std::find(replica_locs.begin(), replica_locs.end(),
                  fsid) == replica_locs.end()) {
      replica_set.setKey(keyFilesystemFiles(fsid));
      ah.Register(replica_set.srem_async(file->getId()), replica_set.getClient());
    }

    if (std::find(unlink_locs.begin(), unlink_locs.end(),
                  fsid) == unlink_locs.end()) {
      unlink_set.setKey(keyFilesystemUnlinked(fsid));
      ah.Register(unlink_set.srem_async(file->getId()), unlink_set.getClient());
    }
  }

  // Wait for all async responses
  (void) ah.Wait();
  return !has_error;
}

//------------------------------------------------------------------------------
// Get set of files on filesystem
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getFileList(IFileMD::location_t location)
{
  std::string key = keyFilesystemFiles(location);
  IFsView::FileList set_files;
  set_files.set_empty_key(-1);
  std::pair<std::string, std::vector<std::string>> reply;
  std::string cursor {"0"};
  long long count = 10000;
  qclient::QSet fs_set(*pQcl, key);

  do {
    reply = fs_set.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_files.insert(std::stoul(elem));
    }
  } while (cursor != "0");

  return set_files;
}

//------------------------------------------------------------------------------
// Get set of unlinked files
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = keyFilesystemUnlinked(location);
  IFsView::FileList set_unlinked;
  set_unlinked.set_empty_key(-1);
  std::pair<std::string, std::vector<std::string>> reply;
  std::string cursor = {"0"};
  long long count = 10000;
  qclient::QSet fs_set(*pQcl, key);

  do {
    reply = fs_set.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_unlinked.insert(std::stoul(elem));
    }
  } while (cursor != "0");

  return set_unlinked;
}

//------------------------------------------------------------------------------
// Get set of files without replicas
//------------------------------------------------------------------------------
IFsView::FileList
FileSystemView::getNoReplicasFileList()
{
  IFsView::FileList set_noreplicas;
  set_noreplicas.set_empty_key(-1);
  std::pair<std::string, std::vector<std::string>> reply;
  std::string cursor {"0"};
  long long count = 10000;

  do {
    reply = pNoReplicasSet.sscan(cursor, count);
    cursor = reply.first;

    for (const auto& elem : reply.second) {
      set_noreplicas.insert(std::stoul(elem));
    }
  } while (cursor != "0");

  return set_noreplicas;
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  std::string key = keyFilesystemUnlinked(location);
  return (pQcl->del(key) >= 0);
}

//----------------------------------------------------------------------------
// Get iterator object to run through all currently active filesystem IDs
//----------------------------------------------------------------------------
std::shared_ptr<IFsIterator>
FileSystemView::getFilesystemIterator()
{
  qclient::QScanner replicaSets(*pQcl, fsview::sPrefix + "*:*");
  std::set<IFileMD::location_t> uniqueFilesytems;
  std::vector<std::string> results;

  while (replicaSets.next(results)) {
    for (std::string& rep : results) {
      // extract fsid from key
      IFileMD::location_t fsid;
      bool unused;

      if (!parseFsId(rep, fsid, unused)) {
        eos_static_crit("Unable to parse redis key: %s", rep.c_str());
        continue;
      }

      uniqueFilesytems.insert(fsid);
    }
  }

  return std::shared_ptr<IFsIterator>(new FilesystemIterator(std::move(
                                        uniqueFilesytems)));
}

EOSNSNAMESPACE_END
