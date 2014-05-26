// ----------------------------------------------------------------------
// File: VstView.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_VSTVIEW__HH__
#define __EOSMGM_VSTVIEW__HH__

/*----------------------------------------------------------------------------*/

#include "mgm/Namespace.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include <map>
#include <set>

/*----------------------------------------------------------------------------*/
/**
 * @file VstView.hh
 * 
 * @brief Class representing the global VST view
 * 
 */

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class describing an EOS VST view
 */

/*----------------------------------------------------------------------------*/
class VstView : public eos::common::LogId
{
  friend class VstMessaging;
  
protected:

  std::map<std::string, std::map<std::string,std::string> > mView;
  
public:
  /// Mutex protecting all ...View variables
  XrdSysMutex ViewMutex;
  // ---------------------------------------------------------------------------
  // Print view
  void Print (std::string &out, 
              std::string option, 
              const char* selection = 0);
 

  void Reset ()
  {
    XrdSysMutexHelper rLock(ViewMutex);
    for (auto it=mView.begin(); it!= mView.end(); ++it)
    {
      it->second.clear();
    }
    mView.clear();
  }

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  VstView ()
  {
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  virtual
  ~VstView ()
  {
  };

  static VstView gVstView;
};

EOSMGMNAMESPACE_END

#endif
