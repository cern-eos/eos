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

  // After booting don't stall FST nodes
  if (gOFS->IsNsBooted() && (vid.prot == "sss") &&
      vid.hasUid(DAEMONUID)) {
    eos_static_debug("info=\"avoid stalling of the FST node\" host=%s",
                      vid.host.c_str());
    stall = false;
  }

  eos::common::RWMutexReadLock lock(Access::gAccessMutex);

  std::string stallid="Stall";

  if (stall) {
    if ((vid.uid > 3)) {
      if (Access::gBannedUsers.count(vid.uid)) {
        smsg = "operate - you are banned in this instance - contact an administrator";

        // fuse clients don't get stalled by a booted namespace, they get EACCES
        if (vid.app.substr(0,4) == "fuse") {
          return true;
        }

        // BANNED USER
        stalltime = 300;
      } else if (Access::gBannedGroups.count(vid.gid)) {
        smsg = "operate - your group is banned in this instance - contact an administrator";

        // fuse clients don't get stalled by a booted namespace, they get EACCES
        if (vid.app.substr(0,4) == "fuse") {
          return true;
        }

        // BANNED GROUP
        stalltime = 300;
      } else if (Access::gBannedHosts.count(vid.host)) {
        smsg = "operate - your client host is banned in this instance - contact an administrator";

        // fuse clients don't get stalled by a booted namespace, they get EACCES
        if (vid.app.substr(0,4) == "fuse") {
          return true;
        }

        // BANNED HOST
        stalltime = 300;
      } else if (Access::gBannedDomains.count(vid.domain)) {
        smsg = "operate - your client domain is banned in this instance - contact an administrator";

        // fuse clients don't get stalled by a booted namespace, they get EACCES
        if (vid.app.substr(0,4) == "fuse") {
          return true;
        }

        // BANNED DOMAINS
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

	if (vid.app != "fuse::restic") {
	  for (it = Access::gStallRules.begin();
	       it != Access::gStallRules.end();
	       it++) {

	    stallid="Stall";

	    auto eosxd_pos = it->first.rfind("Eosxd");
	    auto pos = it->first.rfind(":");

	    std::string cmd = (eosxd_pos != std::string::npos)?
	      it->first.substr(eosxd_pos) : it->first.substr(pos + 1);

	    stallid += "::";
	    stallid += cmd;
	    if (EOS_LOGS_DEBUG) {
	      eos_static_debug("rule=%s function=%s", cmd.c_str(), function);
	    }
	    // only Eosxd rates can be fine-grained by function
	    if (cmd.substr(0,5) == "Eosxd") {
	      if (cmd != function)
		continue;
	    }
	    double cutoff = strtod(it->second.c_str(), 0) * 1.33;

	    if ((it->first.find(userwildcardmatch) == 0)) {
	      // catch all rule = global user rate cut
	      XrdSysMutexHelper statLock(gOFS->MgmStats.mMutex);

	      if (gOFS->MgmStats.StatAvgUid.count(cmd) &&
		  gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
		  (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff)) {
		if (!stalltime) {
		  stalltime = 5;
		}
		smsg = Access::gStallComment[it->first];
		break;
	      }
	    } else if ((it->first.find(groupwildcardmatch) == 0)) {
	      // catch all rule = global user rate cut
	      XrdSysMutexHelper statLock(gOFS->MgmStats.mMutex);

	      if (gOFS->MgmStats.StatAvgGid.count(cmd) &&
		  gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
		  (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff)) {
		if (!stalltime) {
		  stalltime = 5;
		}
		smsg = Access::gStallComment[it->first];
		break;
	      }
	    } else if ((it->first.find(usermatch) == 0)) {
	      // check user rule
	      if (gOFS->MgmStats.StatAvgUid.count(cmd) &&
		  gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
		  (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff)) {
		// rate exceeded
		if (!stalltime) {
		  stalltime = 5;
		}
		smsg = Access::gStallComment[it->first];
		break;
	      }
	    } else if ((it->first.find(groupmatch) == 0)) {
	      // check group rule
	      if (gOFS->MgmStats.StatAvgGid.count(cmd) &&
		  gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
		  (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff)) {
		// rate exceeded
		if (!stalltime) {
		  stalltime = 5;
		}
		smsg = Access::gStallComment[it->first];
		break;
	      }
	    }
	  }
	}
      }
      if (stalltime) {
	// add random offset between 0 and 5 to stalltime
	int random_stall = rand() % 6;
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
