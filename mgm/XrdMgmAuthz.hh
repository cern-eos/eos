//------------------------------------------------------------------------------
//! @file XrdMgmAuthz.hh
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#pragma once
#include "XrdAcc/XrdAccAuthorize.hh"
#include "common/Logging.hh"

//------------------------------------------------------------------------------
//! Class XrdMgmAuthz
//! @brief EOS MGM authorization plugin
//------------------------------------------------------------------------------
class XrdMgmAuthz: public XrdAccAuthorize, public eos::common::LogId
{
public:
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  XrdMgmAuthz() = default;

  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  virtual ~XrdMgmAuthz() = default;

  //------------------------------------------------------------------------------
  //! Check whether or not the client is permitted specified access to a path.
  //!
  //! @param     Entity    -> Authentication information
  //! @param     path      -> The logical path which is the target of oper
  //! @param     oper      -> The operation being attempted (see the enum above).
  //!                         If the oper is AOP_Any, then the actual privileges
  //!                         are returned and the caller may make subsequent
  //!                         tests using Test().
  //! @param     Env       -> Environmental information at the time of the
  //!                         operation as supplied by the path CGI string.
  //!                         This is optional and the pointer may be zero.
  //!
  //! @return    Permit: a non-zero value (access is permitted)
  //!            Deny:   zero             (access is denied)
  //------------------------------------------------------------------------------
  virtual XrdAccPrivs Access(const XrdSecEntity*    Entity,
                             const char*            path,
                             const Access_Operation oper,
                             XrdOucEnv*       Env = 0) override;

  //------------------------------------------------------------------------------
  //! Route an audit message to the appropriate audit exit routine. See
  //! XrdAccAudit.h for more information on how the default implementation works.
  //! Currently, this method is not called by the ofs but should be used by the
  //! implementation to record denials or grants, as warranted.
  //!
  //! @param     accok     -> True is access was grated; false otherwise.
  //! @param     Entity    -> Authentication information
  //! @param     path      -> The logical path which is the target of oper
  //! @param     oper      -> The operation being attempted (see above)
  //! @param     Env       -> Environmental information at the time of the
  //!                         operation as supplied by the path CGI string.
  //!                         This is optional and the pointer may be zero.
  //!
  //! @return    Success: !0 information recorded.
  //!            Failure:  0 information could not be recorded.
  //------------------------------------------------------------------------------
  virtual int Audit(const int              accok,
                    const XrdSecEntity*    Entity,
                    const char*            path,
                    const Access_Operation oper,
                    XrdOucEnv*             Env = 0) override
  {
    return 1;
  }

  //------------------------------------------------------------------------------
  //! Check whether the specified operation is permitted.
  //!
  //! @param     priv      -> the privileges as returned by Access().
  //! @param     oper      -> The operation being attempted (see above)
  //!
  //! @return    Permit: a non-zero value (access is permitted)
  //!            Deny:   zero             (access is denied)
  //------------------------------------------------------------------------------
  virtual int Test(const XrdAccPrivs priv,
                   const Access_Operation oper) override
  {
    return 0;
  }

private:

};

extern XrdMgmAuthz* gMgmAuthz; ///< Global handle to XrdMgmAuthz object
