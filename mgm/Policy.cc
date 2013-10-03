// ----------------------------------------------------------------------
// File: Policy.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "mgm/Policy.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Policy::GetLayoutAndSpace (const char* path, eos::ContainerMD::XAttrMap &attrmap, const eos::common::Mapping::VirtualIdentity &vid, unsigned long &layoutId, XrdOucString &space, XrdOucEnv &env, unsigned long &forcedfsid)

{
  // this is for the moment only defaulting or manual selection
  unsigned long layout = eos::common::LayoutId::GetLayoutFromEnv(env);
  unsigned long xsum = eos::common::LayoutId::GetChecksumFromEnv(env);
  unsigned long bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(env);
  unsigned long stripes = eos::common::LayoutId::GetStripeNumberFromEnv(env);
  unsigned long blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(env);


  const char* val = 0;
  if ((val = env.Get("eos.space")))
  {
    space = val;
  }
  else
  {
    space = "default";
  }

  if ((vid.uid == 0) && (val = env.Get("eos.layout.noforce")))
  {
    // root can request not to apply any forced settings
  }
  else
  {
    if (attrmap.count("sys.forced.space"))
    {
      // we force to use a certain space in this directory even if the user wants something else
      space = attrmap["sys.forced.space"].c_str();
      eos_static_debug("sys.forced.space in %s", path);
    }

    if (attrmap.count("sys.forced.layout"))
    {
      XrdOucString layoutstring = "eos.layout.type=";
      layoutstring += attrmap["sys.forced.layout"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified layout in this directory even if the user wants something else
      layout = eos::common::LayoutId::GetLayoutFromEnv(layoutenv);
      eos_static_debug("sys.forced.layout in %s", path);
    }

    if (attrmap.count("sys.forced.checksum"))
    {
      XrdOucString layoutstring = "eos.layout.checksum=";
      layoutstring += attrmap["sys.forced.checksum"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified checksumming in this directory even if the user wants something else
      xsum = eos::common::LayoutId::GetChecksumFromEnv(layoutenv);
      eos_static_debug("sys.forced.checksum in %s", path);
    }

    if (attrmap.count("sys.forced.blockchecksum"))
    {
      XrdOucString layoutstring = "eos.layout.blockchecksum=";
      layoutstring += attrmap["sys.forced.blockchecksum"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified checksumming in this directory even if the user wants something else
      bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(layoutenv);
      eos_static_debug("sys.forced.blockchecksum in %s %x", path, bxsum);
    }

    if (attrmap.count("sys.forced.nstripes"))
    {
      XrdOucString layoutstring = "eos.layout.nstripes=";
      layoutstring += attrmap["sys.forced.nstripes"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe number in this directory even if the user wants something else
      stripes = eos::common::LayoutId::GetStripeNumberFromEnv(layoutenv);
      eos_static_debug("sys.forced.nstripes in %s", path);
    }

    if (attrmap.count("sys.forced.blocksize"))
    {
      XrdOucString layoutstring = "eos.layout.blocksize=";
      layoutstring += attrmap["sys.forced.blocksize"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe width in this directory even if the user wants something else
      blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(layoutenv);
      eos_static_debug("sys.forced.blocksize in %s : %llu", path, blocksize);
    }

    if (((!attrmap.count("sys.forced.nouserlayout")) || (attrmap["sys.forced.nouserlayout"] != "1")) &&
        ((!attrmap.count("user.forced.nouserlayout")) || (attrmap["user.forced.nouserlayout"] != "1")))
    {

      if (attrmap.count("user.forced.space"))
      {
        // we force to use a certain space in this directory even if the user wants something else
        space = attrmap["user.forced.space"].c_str();
        eos_static_debug("user.forced.space in %s", path);
      }

      if (attrmap.count("user.forced.layout"))
      {
        XrdOucString layoutstring = "eos.layout.type=";
        layoutstring += attrmap["user.forced.layout"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified layout in this directory even if the user wants something else
        layout = eos::common::LayoutId::GetLayoutFromEnv(layoutenv);
        eos_static_debug("user.forced.layout in %s", path);
      }

      if (attrmap.count("user.forced.checksum"))
      {
        XrdOucString layoutstring = "eos.layout.checksum=";
        layoutstring += attrmap["user.forced.checksum"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified checksumming in this directory even if the user wants something else
        xsum = eos::common::LayoutId::GetChecksumFromEnv(layoutenv);
        eos_static_debug("user.forced.checksum in %s", path);
      }

      if (attrmap.count("user.forced.blockchecksum"))
      {
        XrdOucString layoutstring = "eos.layout.blockchecksum=";
        layoutstring += attrmap["user.forced.blockchecksum"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified checksumming in this directory even if the user wants something else
        bxsum = eos::common::LayoutId::GetBlockChecksumFromEnv(layoutenv);
        eos_static_debug("user.forced.blockchecksum in %s", path);
      }

      if (attrmap.count("user.forced.nstripes"))
      {
        XrdOucString layoutstring = "eos.layout.nstripes=";
        layoutstring += attrmap["user.forced.nstripes"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified stripe number in this directory even if the user wants something else
        stripes = eos::common::LayoutId::GetStripeNumberFromEnv(layoutenv);
        eos_static_debug("user.forced.nstripes in %s", path);
      }

      if (attrmap.count("user.forced.blocksize"))
      {
        XrdOucString layoutstring = "eos.layout.blocksize=";
        layoutstring += attrmap["user.forced.blocksize"].c_str();
        XrdOucEnv layoutenv(layoutstring.c_str());
        // we force to use a specified stripe width in this directory even if the user wants something else
        blocksize = eos::common::LayoutId::GetBlocksizeFromEnv(layoutenv);
        eos_static_debug("user.forced.blocksize in %s", path);
      }
    }

    if ((attrmap.count("sys.forced.nofsselection") && (attrmap["sys.forced.nofsselection"] == "1")) ||
        (attrmap.count("user.forced.nofsselection") && (attrmap["user.forced.nofsselection"] == "1")))
    {
      eos_static_debug("<sys|user>.forced.nofsselection in %s", path);
      forcedfsid = 0;
    }
    else
    {
      if ((val = env.Get("eos.force.fsid")))
      {
        forcedfsid = strtol(val, 0, 10);
      }
      else
      {
        forcedfsid = 0;
      }
    }
  }

  layoutId = eos::common::LayoutId::GetId(layout, xsum, stripes, blocksize, bxsum);
  return;
}

/*----------------------------------------------------------------------------*/
bool
Policy::Set (const char* value)
{
  XrdOucEnv env(value);
  XrdOucString policy = env.Get("mgm.policy");

  XrdOucString skey = env.Get("mgm.policy.key");

  XrdOucString policycmd = env.Get("mgm.policy.cmd");

  if (!skey.length())
    return false;

  bool set = false;

  if (!value)
    return false;

  //  gOFS->ConfigEngine->SetConfigValue("policy",skey.c_str(), svalue.c_str());

  return set;
}

/*----------------------------------------------------------------------------*/
bool
Policy::Set (XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  int envlen;
  // no '&' are allowed into stdOut !
  XrdOucString inenv = env.Env(envlen);
  while (inenv.replace("&", " "))
  {
  };
  bool rc = Set(env.Env(envlen));
  if (rc == true)
  {
    stdOut += "success: set policy [ ";
    stdOut += inenv;
    stdOut += "]\n";
    errno = 0;
    retc = 0;
    return true;
  }
  else
  {
    stdErr += "error: failed to set policy [ ";
    stdErr += inenv;
    stdErr += "]\n";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void
Policy::Ls (XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr) { }

/*----------------------------------------------------------------------------*/
bool
Policy::Rm (XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  return true;
}

/*----------------------------------------------------------------------------*/
const char*
Policy::Get (const char* key)
{
  return 0;
}

EOSMGMNAMESPACE_END
