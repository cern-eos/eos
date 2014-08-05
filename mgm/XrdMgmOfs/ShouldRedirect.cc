// ----------------------------------------------------------------------
// File: ShouldRedirect.cc
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
XrdMgmOfs::ShouldRedirect (const char* function,
                           int __AccessMode__,
                           eos::common::Mapping::VirtualIdentity &vid,
                           XrdOucString &host,
                           int &port)
/*----------------------------------------------------------------------------*/
/*
 * @brief Function to test if a client based on the called function and his identity should be redirected
 *
 * @param function name of the function to check
 * @param __AccessMode__ macro generated parameter defining if this is a reading or writing (namespace modifying) function
 * @param host returns the target host of a redirection
 * @param port returns the target port of a redirection
 * @return true if client should get a redirected otherwise false
 *
 * The redirection rules are defined by globals in the Access object (see Access.cc)
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(Access::gAccessMutex);
  if ((vid.host == "localhost") || (vid.host == "localhost.localdomain") || (vid.uid == 0))
  {
    if (MgmMaster.IsMaster() || (IS_ACCESSMODE_R))
    {
      // the slave is redirected to the master for everything which sort of 'writes'
      return false;
    }
  }

  if (Access::gRedirectionRules.size())
  {
    bool c1 = Access::gRedirectionRules.count(std::string("*"));
    bool c3 = (IS_ACCESSMODE_R && Access::gRedirectionRules.count(std::string("r:*")));
    bool c2 = (IS_ACCESSMODE_W && Access::gRedirectionRules.count(std::string("w:*")));
    bool c4 = (IS_ACCESSMODE_R_MASTER && Access::gRedirectionRules.count(std::string("w:*")));
    if (c1 || c2 || c3 || c4)
    {
      // redirect
      std::string delimiter = ":";
      std::vector<std::string> tokens;
      if (c1)
      {
        eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("*")], tokens, delimiter);
        gOFS->MgmStats.Add("Redirect", vid.uid, vid.gid, 1);
      }
      else
      {
        if (c2)
        {
          eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("w:*")], tokens, delimiter);
          gOFS->MgmStats.Add("RedirectW", vid.uid, vid.gid, 1);
        }
        else
        {
          if (c3)
          {
            eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("r:*")], tokens, delimiter);
            gOFS->MgmStats.Add("RedirectR", vid.uid, vid.gid, 1);
          }
          else
          {
            if (c4)
            {
              eos::common::StringConversion::Tokenize(Access::gRedirectionRules[std::string("w:*")], tokens, delimiter);
              gOFS->MgmStats.Add("RedirectR-Master", vid.uid, vid.gid, 1);
            }
          }
        }
      }

      if (tokens.size() == 1)
      {
        host = tokens[0].c_str();
        port = 1094;
      }
      else
      {
        host = tokens[0].c_str();
        port = atoi(tokens[1].c_str());
        if (port == 0)
          port = 1094;
      }
      return true;
    }
  }
  return false;
}