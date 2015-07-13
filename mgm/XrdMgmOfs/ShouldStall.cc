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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
bool
XrdMgmOfs::ShouldStall (const char* function,
                        int __AccessMode__,
                        eos::common::Mapping::VirtualIdentity &vid,
                        int &stalltime, XrdOucString &stallmsg)
/*----------------------------------------------------------------------------*/
/*
 * @brief Function to test if a client based on the called function and his
 * @brief identity should be stalled
 *
 * @param function name of the function to check
 * @param __AccessMode__ macro generated parameter defining if this is a reading or writing (namespace modifying) function
 * @param stalltime returns the time for a stall
 * @param stallmsg returns the message to be displayed to a user during a stall
 * @return true if client should get a stall otherwise false
 *
 * The stall rules are defined by globals in the Access object (see Access.cc)
 */
/*----------------------------------------------------------------------------*/
{
  // ---------------------------------------------------------------------------
  // check for user, group or host banning
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  std::string smsg = "";
  stalltime = 0;

  if ((vid.uid > 3))
  {
    if (Access::gBannedUsers.count(vid.uid))
    {
      // fuse clients don't get stalled by a booted namespace 
      if ( vid.app == "fuse" )
	return false;
      
      // BANNED USER
      stalltime = 300;
      smsg = "you are banned in this instance - contact an administrator";
    }
    else
      if (Access::gBannedGroups.count(vid.gid))
    {
      // fuse clients don't get stalled by a booted namespace 
      if ( vid.app == "fuse" )
	return false;
      
      // BANNED GROUP
      stalltime = 300;
      smsg = "your group is banned in this instance - contact an administrator";
    }
    else
      if (Access::gBannedHosts.count(vid.host))
    {
      // fuse clients don't get stalled by a booted namespace 
      if ( vid.app == "fuse" )
	return false;
      
      // BANNED HOST
      stalltime = 300;
      smsg = "your client host is banned in this instance - contact an administrator";
    }
    else
      if (Access::gStallRules.size() && (Access::gStallGlobal))
    {
      // GLOBAL STALL
      stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
      smsg = Access::gStallComment[std::string("*")];
    }
    else
      if ((IS_ACCESSMODE_R && (Access::gStallRead)) || (IS_ACCESSMODE_R_MASTER && (Access::gStallRead)))
    {
      // READ STALL
      stalltime = atoi(Access::gStallRules[std::string("r:*")].c_str());
      smsg = Access::gStallComment[std::string("r:*")];
    }
    else
      if (IS_ACCESSMODE_W && (Access::gStallWrite))
    {
      stalltime = atoi(Access::gStallRules[std::string("w:*")].c_str());
      smsg = Access::gStallComment[std::string("w:*")];
    }
    else
      if (Access::gStallUserGroup)
    {
      std::string usermatch = "rate:user:";
      usermatch += vid.uid_string;
      std::string groupmatch = "rate:group:";
      groupmatch += vid.gid_string;
      std::string userwildcardmatch = "rate:user:*";
      std::string groupwildcardmatch = "rate:group:*";

      std::map<std::string, std::string>::const_iterator it;
      for (it = Access::gStallRules.begin();
              it != Access::gStallRules.end();
              it++)
      {
        std::string cmd = it->first.substr(it->first.rfind(":") + 1);
        double cutoff = strtod(it->second.c_str(), 0) * 1.33;
        if ((it->first.find(userwildcardmatch) == 0))
        {
          // catch all rule = global user rate cut
          XrdSysMutexHelper statLock(gOFS->MgmStats.Mutex);
          if (gOFS->MgmStats.StatAvgUid.count(cmd) &&
              gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
              (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff))
          {
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
        else
          if ((it->first.find(groupwildcardmatch) == 0))
        {
          // catch all rule = global user rate cut
          XrdSysMutexHelper statLock(gOFS->MgmStats.Mutex);
          if (gOFS->MgmStats.StatAvgGid.count(cmd) &&
              gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
              (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff))
          {
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
        else
          if ((it->first.find(usermatch) == 0))
        {
          // check user rule
          if (gOFS->MgmStats.StatAvgUid.count(cmd) &&
              gOFS->MgmStats.StatAvgUid[cmd].count(vid.uid) &&
              (gOFS->MgmStats.StatAvgUid[cmd][vid.uid].GetAvg5() > cutoff))
          {
            // rate exceeded
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
        else
          if ((it->first.find(groupmatch) == 0))
        {
          // check group rule
          if (gOFS->MgmStats.StatAvgGid.count(cmd) &&
              gOFS->MgmStats.StatAvgGid[cmd].count(vid.gid) &&
              (gOFS->MgmStats.StatAvgGid[cmd][vid.gid].GetAvg5() > cutoff))
          {
            // rate exceeded
            stalltime = 5;
            smsg = Access::gStallComment[it->first];
          }
        }
      }

    }
    if (stalltime)
    {
      stallmsg = "Attention: you are currently hold in this instance and each request is stalled for ";
      stallmsg += (int) stalltime;
      stallmsg += " seconds ... ";
      stallmsg += smsg.c_str();
      eos_static_info("info=\"stalling access to\" uid=%u gid=%u host=%s",
                      vid.uid, vid.gid, vid.host.c_str());
      gOFS->MgmStats.Add("Stall", vid.uid, vid.gid, 1);
      return true;
    }
  }
  else
  {
    // admin/root is only stalled for global stalls not,
    // for write-only or read-only stalls
    if (Access::gStallRules.size())
    {
      if (Access::gStallRules.count(std::string("*")))
      {
        if ((vid.host != "localhost.localdomain") && (vid.host != "localhost"))
        {
          stalltime = atoi(Access::gStallRules[std::string("*")].c_str());
          stallmsg = "Attention: you are currently hold in this instance and each request is stalled for ";
          stallmsg += (int) stalltime;
          stallmsg += " seconds ...";
          eos_static_info("info=\"stalling access to\" uid=%u gid=%u host=%s",
                          vid.uid, vid.gid, vid.host.c_str());
          gOFS->MgmStats.Add("Stall", vid.uid, vid.gid, 1);
          return true;
        }
      }
    }
  }
  eos_static_debug("info=\"allowing access to\" uid=%u gid=%u host=%s",
                   vid.uid, vid.gid, vid.host.c_str());
  return false;
}

