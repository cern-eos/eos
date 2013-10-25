// ----------------------------------------------------------------------
// File: proc/admin/Space.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Space ()
{
  if (mSubCmd == "ls")
  {
    {
      std::string output = "";
      std::string format = "";
      std::string mListFormat = "";
      format = FsView::GetSpaceFormat(std::string(mOutFormat.c_str()));
      if ((mOutFormat == "l"))
        mListFormat = FsView::GetFileSystemFormat(std::string(mOutFormat.c_str()));

      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      FsView::gFsView.PrintSpaces(output, format, mListFormat, mSelection);
      stdOut += output.c_str();
    }
  }

  if (mSubCmd == "status")
  {
    std::string space = (pOpaque->Get("mgm.space")) ? pOpaque->Get("mgm.space") : "";
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mSpaceView.count(space))
    {
      stdOut += "# ------------------------------------------------------------------------------------\n";
      stdOut += "# Space Variables\n";
      stdOut += "# ....................................................................................\n";
      std::vector<std::string> keylist;
      FsView::gFsView.mSpaceView[space]->GetConfigKeys(keylist);
      std::sort(keylist.begin(), keylist.end());
      for (size_t i = 0; i < keylist.size(); i++)
      {
        char line[1024];
        if ((keylist[i] == "nominalsize") ||
            (keylist[i] == "headroom"))
        {
          XrdOucString sizestring;
          // size printout
          snprintf(line, sizeof (line) - 1, "%-32s := %s\n", keylist[i].c_str(), eos::common::StringConversion::GetReadableSizeString(sizestring, strtoull(FsView::gFsView.mSpaceView[space]->GetConfigMember(keylist[i].c_str()).c_str(), 0, 10), "B"));
        }
        else
        {
          snprintf(line, sizeof (line) - 1, "%-32s := %s\n", keylist[i].c_str(), FsView::gFsView.mSpaceView[space]->GetConfigMember(keylist[i].c_str()).c_str());
        }
        stdOut += line;
      }
    }
    else
    {
      stdErr = "error: cannot find space - no space with name=";
      stdErr += space.c_str();
      retc = ENOENT;
    }
  }

  if (mSubCmd == "set")
  {
    if (pVid->uid == 0)
    {
      std::string spacename = (pOpaque->Get("mgm.space")) ? pOpaque->Get("mgm.space") : "";
      std::string status = (pOpaque->Get("mgm.space.state")) ? pOpaque->Get("mgm.space.state") : "";

      if ((!spacename.length()) || (!status.length()))
      {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
      }
      else
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        if (!FsView::gFsView.mSpaceView.count(spacename))
        {
          stdErr = "error: no such space - define one using 'space define' or add a filesystem under that space!";
          retc = EINVAL;
        }
        else
        {
          std::string key = "status";
          {
            // loop over all groups
            std::map<std::string, FsGroup*>::const_iterator it;
            for (it = FsView::gFsView.mGroupView.begin(); it != FsView::gFsView.mGroupView.end(); it++)
            {
              if (!it->second->SetConfigMember(key, status, true, "/eos/*/mgm"))
              {
                stdErr += "error: cannot set status in group <";
                stdErr += it->first.c_str();
                stdErr += ">\n";
                retc = EIO;
              }
            }
          }
          {
            // loop over all nodes
            std::map<std::string, FsNode*>::const_iterator it;
            for (it = FsView::gFsView.mNodeView.begin(); it != FsView::gFsView.mNodeView.end(); it++)
            {
              if (!it->second->SetConfigMember(key, status, true, "/eos/*/mgm"))
              {
                stdErr += "error: cannot set status for node <";
                stdErr += it->first.c_str();
                stdErr += ">\n";
                retc = EIO;
              }
            }
          }
        }
      }
    }
    else
    {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "define")
  {
    if (pVid->uid == 0)
    {
      std::string spacename = (pOpaque->Get("mgm.space")) ? pOpaque->Get("mgm.space") : "";
      std::string groupsize = (pOpaque->Get("mgm.space.groupsize")) ? pOpaque->Get("mgm.space.groupsize") : "";
      std::string groupmod = (pOpaque->Get("mgm.space.groupmod")) ? pOpaque->Get("mgm.space.groupmod") : "";

      int gsize = atoi(groupsize.c_str());
      int gmod = atoi(groupmod.c_str());
      char line[1024];
      snprintf(line, sizeof (line) - 1, "%d", gsize);
      std::string sgroupsize = line;
      snprintf(line, sizeof (line) - 1, "%d", gmod);
      std::string sgroupmod = line;

      if ((!spacename.length()) || (!groupsize.length())
          || (groupsize != sgroupsize) || (gsize < 0) || (gsize > 1024)
          || (groupmod != sgroupmod) || (gmod < 0) || (gmod > 1024))
      {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
        if ((groupsize != sgroupsize) || (gsize < 0) || (gsize > 1024))
        {
          stdErr = "error: <groupsize> must be a positive integer (<=1024)!";
          retc = EINVAL;
        }
        if ((groupmod != sgroupmod) || (gmod < 0) || (gmod > 256))
        {
          stdErr = "error: <groupmod> must be a positive integer (<=256)!";
          retc = EINVAL;
        }
      }
      else
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        if (!FsView::gFsView.mSpaceView.count(spacename))
        {
          stdOut = "info: creating space '";
          stdOut += spacename.c_str();
          stdOut += "'";

          if (!FsView::gFsView.RegisterSpace(spacename.c_str()))
          {
            stdErr = "error: cannot register space <";
            stdErr += spacename.c_str();
            stdErr += ">";
            retc = EIO;
          }
        }

        if (!retc)
        {
          // set this new space parameters
          if ((!FsView::gFsView.mSpaceView[spacename]->SetConfigMember(std::string("groupsize"), groupsize, true, "/eos/*/mgm")) ||
              (!FsView::gFsView.mSpaceView[spacename]->SetConfigMember(std::string("groupmod"), groupmod, true, "/eos/*/mgm")))
          {
            retc = EIO;
            stdErr = "error: cannot set space config value";
          }
        }
      }
    }
    else
    {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "config")
  {
    if (pVid->uid == 0)
    {
      std::string identifier = (pOpaque->Get("mgm.space.name")) ? pOpaque->Get("mgm.space.name") : "";
      std::string key = (pOpaque->Get("mgm.space.key")) ? pOpaque->Get("mgm.space.key") : "";
      std::string value = (pOpaque->Get("mgm.space.value")) ? pOpaque->Get("mgm.space.value") : "";

      if ((!identifier.length()) || (!key.length()) || (!value.length()))
      {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
      }
      else
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

        FileSystem* fs = 0;
        // by host:port name
        std::string path = identifier;

        if (FsView::gFsView.mSpaceView.count(identifier))
        {
          // set a space related parameter
          if (!key.compare(0, 6, "space."))
          {
            key.erase(0, 6);
            if ((key == "nominalsize") ||
                (key == "headroom") ||
                (key == "scaninterval") ||
                (key == "graceperiod") ||
                (key == "drainperiod") ||
                (key == "balancer") ||
                (key == "balancer.node.rate") ||
                (key == "balancer.node.ntx") ||
                (key == "drainer.node.rate") ||
                (key == "drainer.node.ntx") ||
                (key == "converter") ||
                (key == "lru") ||
                (key == "lru.interval") ||
                (key == "converter.ntx") ||
                (key == "autorepair") ||
                (key == "balancer.threshold"))
            {
              if ((key == "balancer") || (key == "converter") || (key == "lru") || (key == "autorepair"))
              {
                if ((value != "on") && (value != "off"))
                {
                  retc = EINVAL;
                  stdErr = "error: value has to either on or off";
                }
                else
                {
                  if (!FsView::gFsView.mSpaceView[identifier]->SetConfigMember(key, value, true, "/eos/*/mgm"))
                  {
                    retc = EIO;
                    stdErr = "error: cannot set space config value";
                  }
                  else
                  {
                    if (key == "balancer")
                    {
                      if (value == "on")
                        stdOut += "success: balancer is enabled!";
                      else
                        stdOut += "success: balancer is disabled!";
                    }
                    if (key == "converter")
                    {
                      if (value == "on")
                        stdOut += "success: converter is enabled!";
                      else
                        stdOut += "success: converter is disabled!";
                    }
                    if (key == "autorepair")
                    {
                      if (key == "on")
                        stdOut += "success: auto-repair is enabled!";
                      else
                        stdOut += "success: auto-repair is dieabled!";
                    }
                  }
                }
              }
              else
              {
                errno = 0;
                unsigned long long size = eos::common::StringConversion::GetSizeFromString(value.c_str());
                if (!errno)
                {
                  if (key != "balancer.threshold")
                  {
                    // the threshold is allowed to be decimal!
                    char ssize[1024];
                    snprintf(ssize, sizeof (ssize) - 1, "%llu", size);
                    value = ssize;
                  }
                  if (!FsView::gFsView.mSpaceView[identifier]->SetConfigMember(key, value, true, "/eos/*/mgm"))
                  {
                    retc = EIO;
                    stdErr = "error: cannot set space config value";
                  }
                  else
                  {
                    stdOut = "success: setting ";
                    stdOut += key.c_str();
                    stdOut += "=";
                    stdOut += value.c_str();
                  }
                }
                else
                {
                  retc = EINVAL;
                  stdErr = "error: value has to be a positiv number";
                }
              }
            }
          }

          // set a filesystem related parameter
          if (!key.compare(0, 3, "fs."))
          {
            key.erase(0, 3);

            // we disable the autosave, do all the updates and then switch back to autosave and evt. save all changes
            bool autosave = gOFS->ConfEngine->GetAutoSave();
            gOFS->ConfEngine->SetAutoSave(false);

            std::set<eos::common::FileSystem::fsid_t>::iterator it;

            // store these as a global parameter of the space
            if (((key == "headroom") || (key == "scaninterval") || (key == "graceperiod") || (key == "drainperiod")))
            {
              if ((!FsView::gFsView.mSpaceView[identifier]->SetConfigMember(key, value, true, "/eos/*/mgm")))
              {
                stdErr += "error: failed to set space parameter <";
                stdErr += key.c_str();
                stdErr += ">\n";
                retc = EINVAL;
              }
            }
            else
            {
              stdErr += "error: not an allowed parameter <";
              stdErr += key.c_str();
              stdErr += ">\n";
              retc = EINVAL;
            }

            for (it = FsView::gFsView.mSpaceView[identifier]->begin(); it != FsView::gFsView.mSpaceView[identifier]->end(); it++)
            {
              if (FsView::gFsView.mIdView.count(*it))
              {
                fs = FsView::gFsView.mIdView[*it];
                if (fs)
                {
                  // check the allowed strings
                  if (((key == "configstatus") && (eos::common::FileSystem::GetConfigStatusFromString(value.c_str()) != eos::common::FileSystem::kUnknown)))
                  {

                    fs->SetString(key.c_str(), value.c_str());
                    if (value == "off")
                    {
                      // we have to remove the errc here, otherwise we cannot terminate drainjobs on file systems with errc set
                      fs->SetString("errc", "0");
                    }
                    FsView::gFsView.StoreFsConfig(fs);
                  }
                  else
                  {
                    errno = 0;
                    eos::common::StringConversion::GetSizeFromString(value.c_str());
                    if (((key == "headroom") || (key == "scaninterval") || (key == "graceperiod") || (key == "drainperiod")) && (!errno))
                    {
                      fs->SetLongLong(key.c_str(), eos::common::StringConversion::GetSizeFromString(value.c_str()));
                      FsView::gFsView.StoreFsConfig(fs);
                    }
                    else
                    {
                      stdErr += "error: not an allowed parameter <";
                      stdErr += key.c_str();
                      stdErr += ">\n";
                      retc = EINVAL;
                    }
                  }
                }
                else
                {
                  stdErr += "error: cannot identify the filesystem by <";
                  stdErr += identifier.c_str();
                  stdErr += ">\n";
                  retc = EINVAL;
                }
              }
            }
            gOFS->ConfEngine->SetAutoSave(autosave);
            gOFS->ConfEngine->AutoSave();
          }
        }
        else
        {
          retc = EINVAL;
          stdErr = "error: cannot find space <";
          stdErr += identifier.c_str();
          stdErr += ">";
        }
      }
    }
    else
    {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "quota")
  {
    std::string spacename = (pOpaque->Get("mgm.space")) ? pOpaque->Get("mgm.space") : "";
    std::string onoff = (pOpaque->Get("mgm.space.quota")) ? pOpaque->Get("mgm.space.quota") : "";
    std::string key = "quota";

    if (pVid->uid == 0)
    {
      if ((!spacename.length()) || (!onoff.length()) || ((onoff != "on") && (onoff != "off")))
      {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
      }
      else
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        if (FsView::gFsView.mSpaceView.count(spacename))
        {
          if (!FsView::gFsView.mSpaceView[spacename]->SetConfigMember(key, onoff, true, "/eos/*/mgm"))
          {
            retc = EIO;
            stdErr = "error: cannot set space config value";
          }
        }
        else
        {
          retc = EINVAL;
          stdErr = "error: no such space defined";
        }
      }
    }
    else
    {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }

  if (mSubCmd == "rm")
  {
    if (pVid->uid == 0)
    {
      std::string spacename = (pOpaque->Get("mgm.space")) ? pOpaque->Get("mgm.space") : "";
      if ((!spacename.length()))
      {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
      }
      else
      {
        eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
        if (!FsView::gFsView.mSpaceView.count(spacename))
        {
          stdErr = "error: no such space '";
          stdErr += spacename.c_str();
          stdErr += "'";
          retc = ENOENT;
        }
        else
        {
          std::string spaceconfigname = eos::common::GlobalConfig::gConfig.QueuePrefixName(FsSpace::sGetConfigQueuePrefix(), spacename.c_str());
          if (!eos::common::GlobalConfig::gConfig.SOM()->DeleteSharedHash(spaceconfigname.c_str()))
          {
            stdErr = "error: unable to remove config of space '";
            stdErr += spacename.c_str();
            stdErr += "'";
            retc = EIO;
          }
          else
          {
            if (FsView::gFsView.UnRegisterSpace(spacename.c_str()))
            {
              stdOut = "success: removed space '";
              stdOut += spacename.c_str();
              stdOut += "'";
            }
            else
            {
              stdErr = "error: unable to unregister space '";
              stdErr += spacename.c_str();
              stdErr += "'";
            }
          }
        }
      }
    }
    else
    {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
