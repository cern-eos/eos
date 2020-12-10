//------------------------------------------------------------------------------
// File: FuseServer/Clients.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include <string>
#include <cstdlib>
#include <thread>

#include "mgm/FuseServer/Clients.hh"
#include "common/Logging.hh"
#include "common/CommentLog.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"
#include "mgm/ZMQ.hh"

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Retrieve global eosxd client statistics
//------------------------------------------------------------------------------
void
FuseServer::Clients::ClientStats(size_t& nclients, size_t& active_clients,
                                 size_t& locked_clients)
{
  nclients = 0;
  active_clients = 0 ;
  locked_clients = 0;
  struct timespec now_time;
  eos::common::Timing::GetTimeSpec(now_time, true);
  eos::common::RWMutexReadLock lLock(*this);

  for (auto it = this->map().begin(); it != this->map().end(); ++it) {
    nclients++;

    if (it->second.statistics().blockedms() > (5 * 1000 * 60)) {
      locked_clients++;
    }

    int64_t idletime = (it->second.get_opstime_sec()) ? (now_time.tv_sec -
                       it->second.get_opstime_sec()) : -1;

    if (idletime <= 300) {
      active_clients++;
    }
  }
}

//------------------------------------------------------------------------------
// Monitor heart beat
//------------------------------------------------------------------------------
void
FuseServer::Clients::MonitorHeartBeat()
{
  eos_static_info("msg=\"starting fusex heart beat thread\"");

  while (true) {
    client_uuid_t evictmap;
    client_uuid_t evictversionmap;
    struct timespec tsnow;
    {
      eos::common::RWMutexReadLock lLock(*this);
      eos::common::Timing::GetTimeSpec(tsnow);

      for (auto it = map().begin(); it != map().end(); ++it) {
        double last_heartbeat = tsnow.tv_sec - it->second.heartbeat().clock() +
                                (((int64_t) tsnow.tv_nsec - (int64_t) it->second.heartbeat().clock_ns())
                                 * 1.0 / 1000000000.0);

        if (it->second.heartbeat().shutdown()) {
          evictmap[it->second.heartbeat().uuid()] = it->first;
          it->second.set_state(Client::EVICTED);
          eos_static_info("client='%s' shutdown [ %s ] ",
                          it->first.c_str(), Info(it->first).c_str());
          gOFS->MgmStats.Add("Eosxd::prot::umount", 0, 0, 1);
        } else {
          if (last_heartbeat > mHeartBeatWindow) {
            if (last_heartbeat > mHeartBeatOfflineWindow) {
              if (last_heartbeat > mHeartBeatRemoveWindow) {
                evictmap[it->second.heartbeat().uuid()] = it->first;
                it->second.set_state(Client::EVICTED);
                eos_static_info("client='%s' evicted [ %s ] ",
                                it->first.c_str(), Info(it->first).c_str());
                gOFS->MgmStats.Add("Eosxd::prot::evicted", 0, 0, 1);
              } else {
                // drop locks once
                if (it->second.state() != Client::OFFLINE) {
                  gOFS->zMQ->gFuseServer.Locks().dropLocks(it->second.heartbeat().uuid());
                  eos_static_info("client='%s' offline [ %s ] ",
                                  it->first.c_str(), Info(it->first).c_str());
                  gOFS->MgmStats.Add("Eosxd::prot::offline", 0, 0, 1);
                }

                it->second.set_state(Client::OFFLINE);
              }
            } else {
              it->second.set_state(Client::VOLATILE);
            }
          } else {
            it->second.set_state(Client::ONLINE);
          }
        }

        if (it->second.heartbeat().protversion() < it->second.heartbeat().PROTOCOLV2) {
          // protocol version mismatch, evict this client
          evictversionmap[it->second.heartbeat().uuid()] = it->first;
          it->second.set_state(Client::EVICTED);
        }
      }
    }

    // Delete clients to be evicted
    if (!evictmap.empty()) {
      eos::common::RWMutexWriteLock lLock(*this);

      for (auto it = evictmap.begin(); it != evictmap.end(); ++it) {
        mMap.erase(it->second);
        mUUIDView.erase(it->first);
        gOFS->zMQ->gFuseServer.Locks().dropLocks(it->first);
      }
    }

    // Delete client ot be evicted because of a version mismatch
    if (!evictversionmap.empty()) {
      for (auto it = evictversionmap.begin(); it != evictversionmap.end(); ++it) {
        std::string versionerror =
          "Server supports PROTOCOLV4 and requires atleast PROTOCOLV2";
        std::string uuid = it->first;
        Evict(uuid, versionerror);
        eos::common::RWMutexWriteLock lLock(*this);
        mMap.erase(it->second);
        mUUIDView.erase(it->first);
      }
    }

    gOFS->zMQ->gFuseServer.Flushs().expireFlush();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (should_terminate()) {
      break;
    }
  }

  return ;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
FuseServer::Clients::Dispatch(const std::string identity,
                              eos::fusex::heartbeat& hb)
{
  gOFS->MgmStats.Add("Eosxd::int::Heartbeat", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::Heartbeat");
  bool rc = true;
  eos::common::RWMutexWriteLock lLock(*this);
  std::set<Caps::shared_cap> caps_to_revoke;

  if (this->map().count(identity)) {
    rc = false;
  }

  // if heartbeats are older than the offline window, we just ignore them to avoid client 'waving'
  struct timespec tsnow;
  eos::common::Timing::GetTimeSpec(tsnow);
  double heartbeat_delay = tsnow.tv_sec - hb.clock() + (((
                             int64_t) tsnow.tv_nsec - (int64_t) hb.clock_ns()) * 1.0 / 1000000000.0);

  if (heartbeat_delay > mHeartBeatOfflineWindow) {
    eos_static_warning("delayed heartbeat from client=%s - delay=%.02f - dropping heartbeat",
                       identity.c_str(), heartbeat_delay);
    return rc;
  }

  if (hb.log().size()) {
    gOFS->mFusexLogTraces->Add(time(NULL), hb.host().c_str(), hb.uuid().c_str(),
                               hb.version().c_str(), std::string(hb.host() + ":" + hb.mount()).c_str() ,
                               hb.log().c_str(), 0);
    hb.clear_log();
  }

  if (hb.trace().size()) {
    gOFS->mFusexStackTraces->Add(time(NULL), hb.host().c_str(), hb.uuid().c_str(),
                                 hb.version().c_str(), std::string(hb.host() + ":" + hb.mount()).c_str() ,
                                 hb.trace().c_str(), 0);
    hb.clear_trace();
  }

  (this->map())[identity].heartbeat() = hb;

  // tag first ops time
  if (!this->map()[identity].get_opstime_sec()) {
    this->map()[identity].tag_opstime();
  }

  (this->uuidview())[hb.uuid()] = identity;
  lLock.Release();
  {
    // apply auth revocation requested by the client
    auto map = hb.mutable_authrevocation();

    for (auto it = map->begin(); it != map->end(); ++it) {
      Caps::shared_cap cap = gOFS->zMQ->gFuseServer.Cap().GetTS(it->first);

      if (cap) {
        caps_to_revoke.insert(cap);
        eos_static_debug("cap-revocation: authid=%s vtime:= %u",
                         it->first.c_str(),
                         cap->vtime());
      }
    }
  }

  if (rc) {
    eos_static_info("client='%s' mount [ %s ] ", identity.c_str(),
                    Info(identity).c_str());
    gOFS->MgmStats.Add("Eosxd::prot::mount", 0, 0, 1);
    // ask a client to drop all caps when we see him the first time because we might have lost our caps due to a restart/failover
    BroadcastDropAllCaps(identity, hb);
    // communicate our current heart-beat interval
    eos::fusex::config cfg;
    cfg.set_hbrate(mHeartBeatInterval);
    cfg.set_dentrymessaging(true);
    cfg.set_writesizeflush(true);
    cfg.set_appname(true);
    cfg.set_mdquery(true);
    cfg.set_hideversion(true);
    cfg.set_serverversion(std::string(VERSION) + std::string("::") + std::string(
                            RELEASE));
    BroadcastConfig(identity, cfg);
  } else {
    if (caps_to_revoke.size()) {
      gOFS->MgmStats.Add("Eosxd::int::AuthRevocation", 0, 0, caps_to_revoke.size());
      EXEC_TIMING_BEGIN("Eosxd::int::AuthRevocation");

      // revoke LEASES by cap
      for (auto it = caps_to_revoke.begin(); it != caps_to_revoke.end(); ++it) {
        eos::common::RWMutexWriteLock lLock(gOFS->zMQ->gFuseServer.Cap());
        gOFS->zMQ->gFuseServer.Cap().Remove(*it);
      }

      EXEC_TIMING_END("Eosxd::int::AuthRevocation");
    }
  }

  EXEC_TIMING_END("Eosxd::int::Heartbeat");
  return rc;
}



//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
std::string
FuseServer::Clients::Info(const std::string& identity)
{
  std::string out;
  char formatline[4096];
  struct timespec tsnow;
  eos::common::Timing::GetTimeSpec(tsnow);
  auto it = this->map().find(identity);

  if (it != this->map().end()) {
    snprintf(formatline, sizeof(formatline),
             "name=%s host=%s version=%s state=%s start=%s dt=[%.02f:%.02f] uuid=%s pid=%u fds=%u type=%s mount=%s",
             it->second.heartbeat().name().c_str(),
             it->second.heartbeat().host().c_str(),
             it->second.heartbeat().version().c_str(),
             it->second.status[it->second.state()],
             eos::common::Timing::utctime(it->second.heartbeat().starttime()).c_str(),
             (int64_t)tsnow.tv_sec - (int64_t)it->second.heartbeat().clock() +
             (((int64_t) tsnow.tv_nsec -
               (int64_t) it->second.heartbeat().clock_ns()) * 1.0 / 1000000000.0),
             it->second.heartbeat().delta() * 1000,
             it->second.heartbeat().uuid().c_str(),
             it->second.heartbeat().pid(),
             it->second.statistics().open_files(),
             it->second.heartbeat().automounted() ? "autofs" : "static",
             it->second.heartbeat().mount().c_str()
            );
    out += formatline;
  }

  return out;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Clients::Print(std::string& out, std::string options)
{
  struct timespec tsnow;
  eos::common::Timing::GetTimeSpec(tsnow);
  std::map<std::string, size_t> clientcaps;
  {
    eos::common::RWMutexReadLock lLock(gOFS->zMQ->gFuseServer.Cap());

    // count caps per client uuid
    for (auto it = gOFS->zMQ->gFuseServer.Cap().InodeCaps().begin();
         it != gOFS->zMQ->gFuseServer.Cap().InodeCaps().end(); ++it) {
      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
        if (gOFS->zMQ->gFuseServer.Cap().GetCaps().count(*sit)) {
          FuseServer::Caps::shared_cap cap = gOFS->zMQ->gFuseServer.Cap().GetCaps()[*sit];
          clientcaps[cap->clientuuid()]++;
        }
      }
    }
  }
  struct timespec now_time;
  eos::common::Timing::GetTimeSpec(now_time, true);
  eos::common::RWMutexReadLock lLock(*this);

  for (auto it = this->map().begin(); it != this->map().end(); ++it) {
    char formatline[4096];
    bool t5min_idle, t1h_idle, t1d_idle, t1w_idle;
    t5min_idle = t1h_idle = t1d_idle = t1w_idle = false;
    int64_t idletime = (it->second.get_opstime_sec()) ? (now_time.tv_sec -
                       it->second.get_opstime_sec()) : -1;

    if (idletime >= 300) {
      t5min_idle = it->second.validate_opstime(now_time, 300);
      t1h_idle = it->second.validate_opstime(now_time, 3600);
      t1d_idle = it->second.validate_opstime(now_time, 86400);
      t1w_idle = it->second.validate_opstime(now_time, 7 * 86400);
    }

    std::string idle;
    std::string lockup;
    std::string lockfunc;

    // preset the idle string
    if (idletime > 300) {
      if (t1w_idle) {
        idle = ">1w";
      } else {
        if (t1d_idle) {
          idle = ">1d";
        } else {
          if (t1h_idle) {
            idle = ">1h";
          } else {
            if (t5min_idle) {
              idle = ">5m";
            } else {
              idle = "act";
            }
          }
        }
      }
    } else {
      idle = "act";
    }

    //    if ( it->second.statistics().blockedms() > (5*1000*60) ) {
    if (it->second.statistics().blockedms() > (5 * 1000)) {
      // a mutex hanging for longer than 5 minutes marks a client as locked up
      lockup = "locked:";
      lockup += it->second.statistics().blockedfunc();
      lockfunc = it->second.statistics().blockedfunc();
    } else {
      lockup = "vacant";
    }

    if (options.find("m") == std::string::npos) {
      snprintf(formatline, sizeof(formatline),
               "client : %-8s %32s %-8s %-8s %s %.02f %.02f %36s p=%u caps=%lu fds=%u %s [%s] %s mount=%s \n",
               it->second.heartbeat().name().c_str(),
               it->second.heartbeat().host().c_str(),
               it->second.heartbeat().version().c_str(),
               it->second.status[it->second.state()],
               eos::common::Timing::utctime(it->second.heartbeat().starttime()).c_str(),
               (int64_t)tsnow.tv_sec - (int64_t)it->second.heartbeat().clock() +
               (((int64_t) tsnow.tv_nsec -
                 (int64_t) it->second.heartbeat().clock_ns()) * 1.0 / 1000000000.0),
               it->second.heartbeat().delta() * 1000,
               it->second.heartbeat().uuid().c_str(),
               it->second.heartbeat().pid(),
               clientcaps[it->second.heartbeat().uuid()],
               it->second.statistics().open_files(),
               it->second.heartbeat().automounted() ? "autofs" : "static",
               lockup.c_str(),
               idle.c_str(),
               it->second.heartbeat().mount().c_str()
              );
      out += formatline;
    }

    if (options.find("l") != std::string::npos) {
      snprintf(formatline, sizeof(formatline),
               "......   ino          : %ld\n"
               "......   ino-to-del   : %ld\n"
               "......   ino-backlog  : %ld\n"
               "......   ino-ever     : %ld\n"
               "......   ino-ever-del : %ld\n"
               "......   threads      : %d\n"
               "......   total-ram    : %.03f GB\n"
               "......   free-ram     : %.03f GB\n"
               "......   vsize        : %.03f GB\n"
               "......   rsize        : %.03f GB\n"
               "......   wr-buf-mb    : %.00f MB\n"
               "......   ra-buf-mb     :%.00f MB\n"
               "......   load1        : %.02f\n"
               "......   leasetime    : %u s\n"
               "......   open-files   : %u\n"
               "......   logfile-size : %lu\n"
               "......   rbytes       : %lu\n"
               "......   wbytes       : %lu\n"
               "......   n-op         : %lu\n"
               "......   rd60         : %.02f MB/s\n"
               "......   wr60         : %.02f MB/s\n"
               "......   iops60       : %.02f \n"
               "......   xoff         : %lu\n"
               "......   ra-xoff      : %lu\n"
               "......   ra-nobuf     : %lu\n"
               "......   wr-nobuf     : %lu\n"
               "......   idle         : %ld\n"
               "......   blockedms    : %.02f [%s]\n",
               it->second.statistics().inodes(),
               it->second.statistics().inodes_todelete(),
               it->second.statistics().inodes_backlog(),
               it->second.statistics().inodes_ever(),
               it->second.statistics().inodes_ever_deleted(),
               it->second.statistics().threads(),
               it->second.statistics().total_ram_mb() / 1024.0,
               it->second.statistics().free_ram_mb() / 1024.0,
               it->second.statistics().vsize_mb() / 1024.0,
               it->second.statistics().rss_mb() / 1024.0,
               it->second.statistics().wr_buf_mb(),
               it->second.statistics().ra_buf_mb(),
               it->second.statistics().load1(),
               it->second.heartbeat().leasetime() ? it->second.heartbeat().leasetime() : 300,
               it->second.statistics().open_files(),
               it->second.statistics().logfilesize(),
               it->second.statistics().rbytes(),
               it->second.statistics().wbytes(),
               it->second.statistics().nio(),
               it->second.statistics().rd_rate_60_mb(),
               it->second.statistics().wr_rate_60_mb(),
               it->second.statistics().iops_60(),
               it->second.statistics().xoff(),
               it->second.statistics().raxoff(),
               it->second.statistics().ranobuf(),
               it->second.statistics().wrnobuf(),
               idletime,
               it->second.statistics().blockedms(),
               it->second.statistics().blockedfunc().c_str()
              );
      out += formatline;
    }

    if (options.find("m") != std::string::npos) {
      snprintf(formatline, sizeof(formatline),
               "client=%s host=%s version=%s state=%s time=\"%s\" tof=%.02f delta=%.02f uuid=%s pid=%u caps=%lu fds=%u type=%s mount=\"%s\" "
               "ino=%ld "
               "ino-to-del=%ld "
               "ino-backlog=%ld "
               "ino-ever=%ld "
               "ino-ever-del=%ld "
               "threads=%d "
               "total-ram-gb=%.03f "
               "free-ram-gb=%.03f "
               "vsize-gb=%.03f "
               "rsize-gb=%.03f "
               "wr-buf-mb=%.00f "
               "ra-buf-mb=%.00f "
               "load1=%.02f "
               "leasetime=%u "
               "open-files=%u "
               "logfile-size=%lu "
               "rbytes=%lu "
               "wbytes=%lu "
               "n-op=%lu "
               "rd60-rate-mb=%.02f "
               "wr60-rate-mb=%.02f "
               "iops60=%.02f "
               "xoff=%lu "
               "ra-xoff=%lu "
               "ra-nobuf=%lu "
               "wr-nobuf=%lu "
               "idle=%ld "
               "blockedms=%f "
               "blockedfunc=%s\n",
               it->second.heartbeat().name().c_str(),
               it->second.heartbeat().host().c_str(),
               it->second.heartbeat().version().c_str(),
               it->second.status[it->second.state()],
               eos::common::Timing::utctime(it->second.heartbeat().starttime()).c_str(),
               (int64_t)tsnow.tv_sec - (int64_t)it->second.heartbeat().clock() +
               (((int64_t) tsnow.tv_nsec -
                 (int64_t) it->second.heartbeat().clock_ns()) * 1.0 / 1000000000.0),
               it->second.heartbeat().delta() * 1000,
               it->second.heartbeat().uuid().c_str(),
               it->second.heartbeat().pid(),
               clientcaps[it->second.heartbeat().uuid()],
               it->second.statistics().open_files(),
               it->second.heartbeat().automounted() ? "autofs" : "static",
               it->second.heartbeat().mount().c_str(),
               it->second.statistics().inodes(),
               it->second.statistics().inodes_todelete(),
               it->second.statistics().inodes_backlog(),
               it->second.statistics().inodes_ever(),
               it->second.statistics().inodes_ever_deleted(),
               it->second.statistics().threads(),
               it->second.statistics().total_ram_mb() / 1024.0,
               it->second.statistics().free_ram_mb() / 1024.0,
               it->second.statistics().vsize_mb() / 1024.0,
               it->second.statistics().rss_mb() / 1024.0,
               it->second.statistics().wr_buf_mb(),
               it->second.statistics().ra_buf_mb(),
               it->second.statistics().load1(),
               it->second.heartbeat().leasetime() ? it->second.heartbeat().leasetime() : 300,
               it->second.statistics().open_files(),
               it->second.statistics().logfilesize(),
               it->second.statistics().rbytes(),
               it->second.statistics().wbytes(),
               it->second.statistics().nio(),
               it->second.statistics().rd_rate_60_mb(),
               it->second.statistics().wr_rate_60_mb(),
               it->second.statistics().iops_60(),
               it->second.statistics().xoff(),
               it->second.statistics().raxoff(),
               it->second.statistics().ranobuf(),
               it->second.statistics().wrnobuf(),
               idletime,
               it->second.statistics().blockedms(),
               it->second.statistics().blockedfunc().length() ?
               it->second.statistics().blockedfunc().c_str() : "none"
              );
      out += formatline;
    }

    if (options.find("k") != std::string::npos) {
      std::map<uint64_t, std::set < pid_t>> rlocks;
      std::map<uint64_t, std::set < pid_t>> wlocks;
      gOFS->zMQ->gFuseServer.Locks().lsLocks(it->second.heartbeat().uuid(), rlocks,
                                             wlocks);

      for (auto rit = rlocks.begin(); rit != rlocks.end(); ++rit) {
        if (rit->second.size()) {
          snprintf(formatline, sizeof(formatline), "      t:rlock i:%016lx p:",
                   rit->first);
          out += formatline;
          std::string pidlocks;

          for (auto pit = rit->second.begin(); pit != rit->second.end(); ++pit) {
            if (pidlocks.length()) {
              pidlocks += ",";
            }

            char spid[16];
            snprintf(spid, sizeof(spid), "%u", *pit);
            pidlocks += spid;
          }

          out += pidlocks;
          out += "\n";
        }
      }

      for (auto wit = wlocks.begin(); wit != wlocks.end(); ++wit) {
        if (wit->second.size()) {
          snprintf(formatline, sizeof(formatline), "      t:wlock i:%016lx p:",
                   wit->first);
          out += formatline;
          std::string pidlocks;

          for (auto pit = wit->second.begin(); pit != wit->second.end(); ++pit) {
            if (pidlocks.length()) {
              pidlocks += ",";
            }

            char spid[16];
            snprintf(spid, sizeof(spid), "%u", *pit);
            pidlocks += spid;
          }

          out += pidlocks;
          out += "\n";
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
size_t
FuseServer::Clients::leasetime(const std::string& uuid)
{
  // requires a Client read lock
  size_t leasetime = 0;

  if (this->uuidview().count(uuid) &&
      this->map().count(this->uuidview()[uuid])) {
    leasetime = this->map()[this->uuidview()[uuid]].heartbeat().leasetime();
  }

  if (leasetime > (7 * 86400)) {
    // don't allow longer lease times as a week
    leasetime = 7 * 86400;
  }

  return leasetime;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::Evict(std::string& uuid, std::string reason,
                           std::vector<std::string>* evicted_out)
{
  if ((uuid == "static") ||
      (uuid == "autofs")) {
    std::vector<std::pair<std::string, std::string>> evict_uuids;

    // evict static or autofs clients with criteria
    if (reason.substr(0, 4) == "mem:") {
      // evict according to memory footprint
      uint64_t memory_condition = strtoull(reason.substr(4).c_str(), 0, 10);

      if (memory_condition) {
        eos::common::RWMutexReadLock lLock(*this);

        for (auto it = this->map().begin(); it != this->map().end(); ++it) {
          if ((uuid == "static") && (it->second.heartbeat().automounted())) {
            continue;
          }

          if ((uuid == "autofs") && (!it->second.heartbeat().automounted())) {
            continue;
          }

          if (it->second.statistics().rss_mb() > memory_condition) {
            std::string memreason = "consuming ";
            memreason += std::to_string(it->second.statistics().rss_mb());
            memreason += " MB of resident memory";
            evict_uuids.push_back(std::pair<std::string, std::string>
                                  (it->second.heartbeat().uuid(),
                                   memreason));
          }
        }
      }

      int retc = 0;

      for (auto it : evict_uuids) {
        retc |= Evict(it.first, it.second, evicted_out);
      }

      return retc;
    } else if (reason.substr(0, 5) == "idle:") {
      // evict according to idle time
      int64_t idle_condition = strtoull(reason.substr(5).c_str(), 0, 10);

      if (idle_condition) {
        struct timespec now_time;
        eos::common::Timing::GetTimeSpec(now_time, true);
        eos::common::RWMutexReadLock lLock(*this);

        for (auto it = this->map().begin(); it != this->map().end(); ++it) {
          if ((uuid == "static") && (it->second.heartbeat().automounted())) {
            continue;
          }

          if ((uuid == "autofs") && (!it->second.heartbeat().automounted())) {
            continue;
          }

          int64_t idletime = (it->second.get_opstime_sec()) ? (now_time.tv_sec -
                             it->second.get_opstime_sec()) : -1;

          if (idletime > idle_condition) {
            std::string idlereason = "longer than ";
            idlereason += std::to_string(idletime);
            idlereason += " seconds idle";
            evict_uuids.push_back(std::pair<std::string, std::string>
                                  (it->second.heartbeat().uuid(),
                                   idlereason));
          }
        }
      }

      int retc = 0;

      for (auto it : evict_uuids) {
        retc |= Evict(it.first, it.second, evicted_out);
      }

      return retc;
    }
  } else {
    // prepare eviction message for a client by uuid
    eos::fusex::response rsp;
    rsp.set_type(rsp.EVICT);
    rsp.mutable_evict_()->set_reason(reason);
    std::string rspstream;
    rsp.SerializeToString(&rspstream);
    eos::common::RWMutexReadLock lLock(*this);

    if (!mUUIDView.count(uuid)) {
      // even if this uuid does not exist we can use it to remove stale locks
      gOFS->zMQ->gFuseServer.Locks().dropLocks(uuid);
      return ENOENT;
    }

    std::string id = mUUIDView[uuid];
    lLock.Release();
    eos_static_info("msg=\"evicting client\" uuid=%s name=%s",
                    uuid.c_str(), id.c_str());

    if (evicted_out) {
      std::string out = "uuid=";
      out += uuid;
      out += " name=";
      out += id;
      out += " reason='";
      out += reason;
      out += "'";
      evicted_out->push_back(out);
    }

    gOFS->zMQ->mTask->reply(id, rspstream);
    return 0;
  }

  return EINVAL;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::Dropcaps(const std::string& uuid, std::string& out)
{
  {
    eos::common::RWMutexReadLock rlock(*this);

    if (!mUUIDView.count(uuid)) {
      return ENOENT;
    }
  }
  out += " dropping caps of '";
  out += uuid;
  out += "' : ";
  Caps::ino_set_t cleanup_authids;
  eos::common::RWMutexWriteLock lLock(gOFS->zMQ->gFuseServer.Cap());

  for (auto it = gOFS->zMQ->gFuseServer.Cap().InodeCaps().begin();
       it != gOFS->zMQ->gFuseServer.Cap().InodeCaps().end(); ++it) {
    std::set<FuseServer::Caps::shared_cap> cap2delete;

    for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
      if (gOFS->zMQ->gFuseServer.Cap().HasCap(*sit)) {
        FuseServer::Caps::shared_cap cap = gOFS->zMQ->gFuseServer.Cap().GetCaps()[*sit];

        if (cap->clientuuid() == uuid) {
          cap2delete.insert(cap);
          out += "\n ";
          char ahex[20];
          snprintf(ahex, sizeof(ahex), "%016lx", (unsigned long) cap->id());
          std::string match = "";
          match += "# i:";
          match += ahex;
          match += " a:";
          match += cap->authid();
          out += match;
        }
      }
    }

    for (auto scap = cap2delete.begin(); scap != cap2delete.end(); ++scap) {
      gOFS->zMQ->gFuseServer.Client().ReleaseCAP((uint64_t)(*scap)->id(),
          (*scap)->clientuuid(),
          (*scap)->clientid());
      eos_static_info("erasing %llx %s %s", (*scap)->id(),
                      (*scap)->clientid().c_str(), (*scap)->authid().c_str());
      gOFS->zMQ->gFuseServer.Cap().Remove(*scap);
    }
  }

  for (auto it = cleanup_authids.begin(); it != cleanup_authids.end(); ++it) {
    if (!gOFS->zMQ->gFuseServer.Cap().InodeCaps()[*it].size()) {
      gOFS->zMQ->gFuseServer.Cap().InodeCaps().erase(*it);
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::ReleaseCAP(uint64_t md_ino,
                                const std::string& uuid,
                                const std::string& clientid
                               )
{
  gOFS->MgmStats.Add("Eosxd::int::ReleaseCap", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::ReleaseCap");
  // prepare release cap message
  eos::fusex::response rsp;
  rsp.set_type(rsp.LEASE);
  rsp.mutable_lease_()->set_type(eos::fusex::lease::RELEASECAP);
  rsp.mutable_lease_()->set_md_ino(md_ino);
  rsp.mutable_lease_()->set_clientid(clientid);
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos::common::RWMutexReadLock lLock(*this);

  if (!mUUIDView.count(uuid)) {
    return ENOENT;
  }

  std::string id = mUUIDView[uuid];
  lLock.Release();
  eos_static_info("msg=\"asking cap release\" uuid=%s clientid=%s id=%lx",
                  uuid.c_str(), clientid.c_str(), md_ino);
  gOFS->zMQ->mTask->reply(id, rspstream);
  EXEC_TIMING_END("Eosxd::int::ReleaseCap");
  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::DeleteEntry(uint64_t md_ino,
                                 const std::string& uuid,
                                 const std::string& clientid,
                                 const std::string& name
                                )
{
  gOFS->MgmStats.Add("Eosxd::int::DeleteEntry", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::DeleteEntry");
  // prepare release cap message
  eos::fusex::response rsp;
  rsp.set_type(rsp.DENTRY);
  rsp.mutable_dentry_()->set_type(eos::fusex::dentry::REMOVE);
  rsp.mutable_dentry_()->set_name(name);
  rsp.mutable_dentry_()->set_md_ino(md_ino);
  rsp.mutable_dentry_()->set_clientid(clientid);
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos::common::RWMutexReadLock lLock(*this);

  if (!mUUIDView.count(uuid)) {
    return ENOENT;
  }

  std::string id = mUUIDView[uuid];
  lLock.Release();
  eos_static_info("msg=\"asking dentry deletion\" uuid=%s clientid=%s id=%lx name=%s",
                  uuid.c_str(), clientid.c_str(), md_ino, name.c_str());
  gOFS->zMQ->mTask->reply(id, rspstream);
  EXEC_TIMING_END("Eosxd::int::DeleteEntry");
  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::RefreshEntry(uint64_t md_ino,
                                  const std::string& uuid,
                                  const std::string& clientid
                                 )
{
  gOFS->MgmStats.Add("Eosxd::int::RefreshEntry", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::RefreshEntry");
  // prepare release cap message
  eos::fusex::response rsp;
  rsp.set_type(rsp.REFRESH);
  rsp.mutable_refresh_()->set_md_ino(md_ino);
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos::common::RWMutexReadLock lLock(*this);

  if (!mUUIDView.count(uuid)) {
    return ENOENT;
  }

  std::string id = mUUIDView[uuid];
  eos_static_info("client=%s\n", map()[id].heartbeat().version().c_str());

  if (DeferClient(map()[id].heartbeat().version(), "4.4.18")) {
    // dont' send refresh to client version < 4.4.18 (4.4.17 deadlocks, others ignore)
    eos_static_info("suppressing refresh to client '%s' version='%s'",
                    clientid.c_str(), map()[id].heartbeat().version().c_str());
  } else {
    std::string id = mUUIDView[uuid];
    lLock.Release();
    eos_static_info("msg=\"asking dentry refresh\" uuid=%s clientid=%s id=%lx",
                    uuid.c_str(), clientid.c_str(), md_ino);
    gOFS->zMQ->mTask->reply(id, rspstream);
  }

  EXEC_TIMING_END("Eosxd::int::RefreshEntry");
  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::SendMD(const eos::fusex::md& md,
                            const std::string& uuid,
                            const std::string& clientid,
                            uint64_t md_ino,
                            uint64_t md_pino,
                            uint64_t clock,
                            struct timespec& p_mtime
                           )
/*----------------------------------------------------------------------------*/

{
  gOFS->MgmStats.Add("Eosxd::int::SendMD", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::SendMD");
  // prepare update message
  eos::fusex::response rsp;
  rsp.set_type(rsp.MD);
  *(rsp.mutable_md_()) = md;
  rsp.mutable_md_()->set_type(eos::fusex::md::MD);
  // the client needs this to sort out the quota accounting using the cap map
  rsp.mutable_md_()->set_clientid(clientid);
  // when a file is created the inode is not yet written in the const md object
  rsp.mutable_md_()->set_md_ino(md_ino);
  rsp.mutable_md_()->set_md_pino(md_pino);

  if (p_mtime.tv_sec) {
    rsp.mutable_md_()->set_pt_mtime(p_mtime.tv_sec);
    rsp.mutable_md_()->set_pt_mtime_ns(p_mtime.tv_nsec);
  }

  rsp.mutable_md_()->set_clock(clock);
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos::common::RWMutexReadLock lLock(*this);

  if (!mUUIDView.count(uuid)) {
    return ENOENT;
  }

  lLock.Release();
  std::string id = mUUIDView[uuid];
  eos_static_info("msg=\"sending md update\" uuid=%s clientid=%s id=%lx",
                  uuid.c_str(), clientid.c_str(), md_ino);
  gOFS->zMQ->mTask->reply(id, rspstream);
  EXEC_TIMING_END("Eosxd::int::SendMD");
  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Clients::SendCAP(FuseServer::Caps::shared_cap cap)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::SendCAP", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::SendCAP");
  // prepare update message
  eos::fusex::response rsp;
  rsp.set_type(rsp.CAP);
  *(rsp.mutable_cap_()) = *cap;
  const std::string& uuid = cap->clientuuid();
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos::common::RWMutexReadLock lLock(*this);

  if (!mUUIDView.count(uuid)) {
    return ENOENT;
  }

  const std::string& clientid = mUUIDView[uuid];
  lLock.Release();
  eos_static_info("msg=\"sending cap update\" uuid=%s clientid=%s cap-id=%lx",
                  uuid.c_str(), clientid.c_str(), cap->id());
  gOFS->zMQ->mTask->reply(clientid, rspstream);
  EXEC_TIMING_END("Eosxd::int::SendCAP");
  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Clients::HandleStatistics(const std::string identity,
                                      const eos::fusex::statistics& stats)
{
  eos::common::RWMutexWriteLock lLock(*this);
  uint64_t previous_ops = (this->map())[identity].statistics().nio();
  (this->map())[identity].statistics() = stats;

  // update the last ops time whenever the operations counter changes
  // this is very rough and only precise to the interval of statistic updates
  if (!previous_ops ||
      ((this->map())[identity].statistics().nio() != previous_ops)) {
    (this->map())[identity].tag_opstime();
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("");
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
FuseServer::Clients::DeferClient(std::string clientversion,
                                 std::string allowversion)
{
  std::vector<std::string> v1;
  std::vector<std::string> v2;
  eos::common::StringConversion::Tokenize(clientversion, v1, ".");
  eos::common::StringConversion::Tokenize(allowversion,  v2, ".");
  unsigned long client_v = 0;
  unsigned long allowd_v = 0;

  if (v1.size() == v2.size()) {
    for (size_t i = 0; i != v1.size(); i++) {
      if (i != 0) {
        client_v *= 1000;
        allowd_v *= 1000;
      }

      client_v += strtoul(v1[i].c_str(), 0, 10);
      allowd_v += strtoul(v2[i].c_str(), 0, 10);
    }
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("client-v:%lu allowd-v:%lu (%s/%s)", client_v, allowd_v,
                     clientversion.c_str(), allowversion.c_str());
  }

  return (client_v < allowd_v);
}
/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::SetHeartbeatInterval(int interval)
{
  // broadcast to all clients
  eos::common::RWMutexWriteLock lLock(*this);
  mHeartBeatInterval = interval;

  for (auto it = this->map().begin(); it != this->map().end(); ++it) {
    std::string uuid = it->second.heartbeat().uuid();
    std::string id = mUUIDView[uuid];

    if (id.length()) {
      eos::fusex::config cfg;
      cfg.set_hbrate(interval);
      cfg.set_dentrymessaging(true);
      cfg.set_writesizeflush(true);
      cfg.set_appname(true);
      cfg.set_mdquery(true);
      cfg.set_serverversion(std::string(VERSION) + std::string("::") + std::string(
                              RELEASE));
      BroadcastConfig(id, cfg);
    }
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::SetQuotaCheckInterval(int interval)
{
  eos::common::RWMutexWriteLock lLock(*this);
  mQuotaCheckInterval = interval;
  return 0;
}

/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::BroadcastConfig(const std::string& identity,
                                     eos::fusex::config& cfg)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::BcConfig", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcConfig");
  // prepare new heartbeat interval message
  eos::fusex::response rsp;
  rsp.set_type(rsp.CONFIG);
  *(rsp.mutable_config_()) = cfg;
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos_static_info("msg=\"broadcast config to client\" name=%s heartbeat-rate=%d",
                  identity.c_str(), cfg.hbrate());
  gOFS->zMQ->mTask->reply(identity, rspstream);
  EXEC_TIMING_END("Eosxd::int::BcConfig");
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::BroadcastDropAllCaps(const std::string& identity,
    eos::fusex::heartbeat& hb)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::BcDropAll", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcDropAll");
  // prepare drop all caps message
  eos::fusex::response rsp;
  rsp.set_type(rsp.DROPCAPS);
  std::string rspstream;
  rsp.SerializeToString(&rspstream);
  eos_static_info("msg=\"broadcast drop-all-caps to  client\" uuid=%s name=%s",
                  hb.uuid().c_str(), identity.c_str());
  gOFS->zMQ->mTask->reply(identity, rspstream);
  EXEC_TIMING_END("Eosxd::int::BcDropAll");
  return 0;
}


/*----------------------------------------------------------------------------*/
void
FuseServer::Clients::SetBroadCastMaxAudience(int size)
/*----------------------------------------------------------------------------*/
{
  mMaxBroadCastAudience = size;
}

/*----------------------------------------------------------------------------*/
void
FuseServer::Clients::SetBroadCastAudienceSuppressMatch(const std::string& match)
{
  mMaxbroadCastAudienceMatch = match;
}





EOSMGMNAMESPACE_END
