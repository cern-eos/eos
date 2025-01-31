// ----------------------------------------------------------------------
// File: ShouldStall.cc
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

#include "mgm/XrdMgmOfs.hh"
#include <chrono>

// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

bool
XrdMgmOfs::ShouldStall(const char* function,
                       int __AccessMode__,
                       eos::common::VirtualIdentity& vid,
                       int& stalltime, XrdOucString& stallmsg)
{
  // Check for user, group or host banning
  std::string smsg = "";
  stalltime = 0;
  bool stall = true;
  std::string functionname = function;
  bool saturated = false;
  double limit = 0;

  // After booting don't stall FST nodes
  if (gOFS->IsNsBooted() && (vid.prot == "sss") &&
      vid.hasUid(DAEMONUID)) {
    eos_static_debug("info=\"avoid stalling of the FST node\" host=%s",
                     vid.host.c_str());
    stall = false;
  }

  // Avoid stalling HTTP requests as these translate into errors on the client
  // except if the adminstrator has set the environment variable allowing
  // stalling to take place with HTTP
  if (vid.prot == "https" && !getenv("EOS_MGM_ALLOW_HTTP_STALL")) {
    stall = false;
  }

  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  std::string stallid = "Stall";
  size_t uid_threads = 1;

  if (stall) {
    if ((vid.uid > 3) && (functionname != "stat")  && (vid.app != "fuse::restic")) {
      if ((stalltime = gOFS->mTracker.ShouldStall(vid.uid, saturated, uid_threads))) {
        smsg = SSTR("operate - uid=" << vid.uid << " exceeding the "
                    "thread pool limit");
        stallid += "::threads::";
        stallid += std::to_string(vid.uid);;
      } else if (Access::gBannedUsers.count(vid.uid)) {
        smsg = SSTR("operate - uid=" << vid.uid << " banned in this instance "
                    "- contact an administrator");

        // fuse clients don't get stalled by a booted namespace, they get EACCES
        if (vid.app.substr(0, 4) == "fuse") {
          stallmsg = smsg.c_str();
          return true;
        }

        // BANNED USER
        stalltime = 300;
      } else if (Access::gBannedGroups.count(vid.gid)) {
        smsg = SSTR("operate - gid=" << vid.gid << " banned in this instance "
                    "- contact an administrator");

        // fuse clients don't get stalled by a booted namespace, they get EACCES
        if (vid.app.substr(0, 4) == "fuse") {
          stallmsg = smsg.c_str();
          return true;
        }

        // BANNED GROUP
        stalltime = 300;
      } else if (Access::gBannedHosts.count(vid.host)) {
        smsg = SSTR("operate - client host=" << vid.host << " banned in this "
                    "instance - contact an administrator");
        // BANNED HOST
        stalltime = 300;
      } else if (Access::gBannedDomains.count(vid.domain)) {
        smsg = SSTR("operate -  client domain=" << vid.domain << " banned "
                    "in this instance - contact an administrator");
        // BANNED DOMAINS
        stalltime = 300;
      } else if (vid.token && Access::gBannedTokens.count(vid.token->Voucher())) {
        smsg = "operate - your token is banned in this instance - contact an administrator";
        // BANNED TOKEN
        stalltime = 300;
      } else if (Access::gStallRules.size() && (Access::gStallGlobal)) {
        // GLOBAL STALL
        stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
        smsg = Access::gStallComment[std::string("*")];
      } else if ((IS_ACCESSMODE_R && (Access::gStallRead)) ||
                 (IS_ACCESSMODE_R_MASTER && (Access::gStallRead))) {
        // READ STALL
        stalltime = atoi(Access::gStallRules[std::string("r:*")].c_str());
        smsg = Access::gStallComment[std::string("r:*")];
      } else if (IS_ACCESSMODE_W && (Access::gStallWrite)) {
        stalltime = atoi(Access::gStallRules[std::string("w:*")].c_str());
        smsg = Access::gStallComment[std::string("w:*")];
      } else if (Access::gStallUserGroup) {
        std::string usermatch = "rate:user:";
        usermatch += vid.uid_string;
        std::string groupmatch = "rate:group:";
        groupmatch += vid.gid_string;
        std::string userwildcardmatch = "rate:user:*";
        std::string groupwildcardmatch = "rate:group:*";
        std::map<std::string, std::string>::const_iterator it;

        if ((functionname != "stat") &&  // never stall stats
            (vid.app != "fuse::restic")) {
          for (it = Access::gStallRules.begin();
               it != Access::gStallRules.end();
               it++) {
            stallid = "Stall";
            auto eosxd_pos = it->first.rfind("Eosxd");
            auto pos = it->first.rfind(":");
            std::string cmd = (eosxd_pos != std::string::npos) ?
                              it->first.substr(eosxd_pos) : it->first.substr(pos + 1);
            stallid += "::";
            stallid += cmd;
            eos_static_debug("rule=%s function=%s", cmd.c_str(), function);

            // only Eosxd rates can be fine-grained by function
            if (cmd.substr(0, 5) == "Eosxd") {
              if (cmd != function) {
                continue;
              }
            }

            double cutoff = strtod(it->second.c_str(), 0) * 1.33;

            if ((it->first.find(usermatch) == 0)) {
              // check user rule
              XrdSysMutexHelper statLock(gOFS->MgmStats.mMutex);

              if ((cutoff == 0) ||
                  (gOFS->MgmStats.StatAvgUid.count(cmd) &&
                   gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
                   (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff)
                  )) {
                // rate exceeded
                if (!stalltime) {
                  stalltime = 5;
                }

                limit = cutoff;
                smsg = Access::gStallComment[it->first];
                break;
              }
            } else if ((it->first.find(groupmatch) == 0)) {
              // check group rule
              XrdSysMutexHelper statLock(gOFS->MgmStats.mMutex);

              if ((cutoff == 0) ||
                  (gOFS->MgmStats.StatAvgGid.count(cmd) &&
                   gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
                   (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff)
                  )) {
                // rate exceeded
                if (!stalltime) {
                  stalltime = 5;
                }

                limit = cutoff;
                smsg = Access::gStallComment[it->first];
                break;
              }
            }

            if ((it->first.find(userwildcardmatch) == 0)) {
              // catch all rule = global user rate cut
              XrdSysMutexHelper statLock(gOFS->MgmStats.mMutex);

              if ((cutoff == 0) ||
                  (gOFS->MgmStats.StatAvgUid.count(cmd) &&
                   gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
                   (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff)
                  )) {
                if (!stalltime) {
                  stalltime = 5;
                }

                limit = cutoff;
                smsg = Access::gStallComment[it->first];
                break;
              }
            } else if ((it->first.find(groupwildcardmatch) == 0)) {
              // catch all rule = global user rate cut
              XrdSysMutexHelper statLock(gOFS->MgmStats.mMutex);

              if ((cutoff == 0) ||
                  (gOFS->MgmStats.StatAvgGid.count(cmd) &&
                   gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
                   (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff)
                  )) {
                if (!stalltime) {
                  stalltime = 5;
                }

                limit = cutoff;
                smsg = Access::gStallComment[it->first];
                break;
              }
            }
          }
        }
      }

      if (stalltime && (saturated || ! limit)) {
        // add random offset between 0 and 5 to stalltime
        int random_stall = eos::common::getRandom(0, 5);
        stalltime += random_stall;
        stallmsg = "Attention: you are currently hold in this instance and each"
                   " request is stalled for ";
        stallmsg += (int) stalltime;
        stallmsg += " seconds ... ";
        stallmsg += smsg.c_str();
        eos_static_info("info=\"stalling access to\" uid=%u gid=%u host=%s stall=%d",
                        vid.uid, vid.gid, vid.host.c_str(), stalltime);
        gOFS->MgmStats.Add(stallid.c_str(), vid.uid, vid.gid, 1);
        return true;
      } else {
        if (limit) {
          stallid = "Delay";
          stallid += "::threads::";
          stallid += std::to_string(vid.uid);;
          std::string delayid = stallid;
          delayid += "::ms";
          size_t ms_to_delay = 1000.0 / limit;

          if (uid_threads) {
            // renomalize with the curent user thread pool size
            ms_to_delay *= uid_threads;

            if (ms_to_delay > 40000) {
              // we should not hang longer than 40s not to trigger timeouts, which are 60s by default for FUSE clients and 5min for XRootD clients
              ms_to_delay = 40000;
            }
          }

          lock.Release();
          std::this_thread::sleep_for(std::chrono::milliseconds(ms_to_delay));
          gOFS->MgmStats.Add(stallid.c_str(), vid.uid, vid.gid, 1);
          gOFS->MgmStats.Add(delayid.c_str(), vid.uid, vid.gid, ms_to_delay);
          return false;
        }
      }
    } else {
      if (Access::gStallRules.size() &&
          Access::gStallRules.count(std::string("*"))) {
        if ((vid.host != "localhost.localdomain") &&
            (vid.host != "localhost")) {
          // admin/root is only stalled for global stalls not,
          // for write-only or read-only stalls
          stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
          stallmsg = "Attention: you are currently hold in this instance and each"
                     " request is stalled for ";
          stallmsg += (int) stalltime;
          stallmsg += " seconds ...";
          eos_static_info("info=\"stalling access to\" uid=%u gid=%u host=%s",
                          vid.uid, vid.gid, vid.host.c_str());
          gOFS->MgmStats.Add("Stall", vid.uid, vid.gid, 1);
          return true;
        } else {
          // localhost does not get stalled but receives an error during boot when trying to write
          if (IS_ACCESSMODE_W) {
            stalltime = 0 ;
            stallmsg = "do modifications - writing is currently stalled on the instance";
            return true;
          }
        }
      }
    }
  }

  eos_static_debug("info=\"allowing access to\" uid=%u gid=%u host=%s",
                   vid.uid, vid.gid, vid.host.c_str());
  return false;
}
