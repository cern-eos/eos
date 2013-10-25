// ----------------------------------------------------------------------
// File: FmdClient.hh
// Author: Geoffray Adde - CERN
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

/**
 * @file   FmdClient.hh
 * 
 * @brief  Classes for FST File Meta Data Request from a client
 * 
 * 
 */

#ifndef __EOSFST_FMDCLIENT_HH__
#define __EOSFST_FMDCLIENT_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "fst/Fmd.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <google/sparsehash/densehashtable.h>
#include <sys/time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <zlib.h>
#include <openssl/sha.h>

#ifdef __APPLE__
#define ECOMM 70
#endif

EOSFSTNAMESPACE_BEGIN

// ---------------------------------------------------------------------------
//! Class handling Request to Fmd
// ---------------------------------------------------------------------------

class FmdClient : public eos::common::LogId
{
public:


  /*----------------------------------------------------------------------------*/
  /**
   * Convert an FST env representation to an Fmd struct
   *
   * @param env env representation
   * @param fmd reference to Fmd struct
   *
   * @return true if successful otherwise false
   */

  /*----------------------------------------------------------------------------*/
  bool
  EnvFstToFmdSqlite (XrdOucEnv &env, struct Fmd &fmd);

  /*----------------------------------------------------------------------------*/
  /**
   * Convert an FST env representation to an Fmd struct
   *
   * @param env env representation
   * @param fmd reference to Fmd struct
   *
   * @return true if successful otherwise false
   */

  /*----------------------------------------------------------------------------*/
  bool
  EnvMgmToFmdSqlite (XrdOucEnv &env, struct Fmd &fmd);

  /**
   * Return Fmd from an mgm
   *
   * @param manager host:port of the mgm to contact
   * @param fid file id
   * @param fmd reference to the Fmd struct to store Fmd
   *
   * @return
   */
  int
  GetMgmFmd (const char* manager,
      eos::common::FileId::fileid_t fid,
      struct Fmd& fmd);

  /*----------------------------------------------------------------------------*/
  /**
   * Return a remote file attribute
   *
   * @param manager host:port of the server to contact
   * @param key extended attribute key to get
   * @param path file path to read attributes from
   * @param attribute reference where to store the attribute value
   *
   * @return
   */

  /*----------------------------------------------------------------------------*/
  int GetRemoteAttribute (const char* manager,
      const char* key,
      const char* path,
      XrdOucString& attribute);

  /**
   * Return Fmd from a remote filesystem
   *
   * @param manager host:port of the server to contact
   * @param shexfid hex string of the file id
   * @param sfsid string of filesystem id
   * @param fmd reference to the Fmd struct to store Fmd
   *
   * @return
   */
  int GetRemoteFmdSqlite (const char* manager,
      const char* shexfid,
      const char* sfsid,
      struct Fmd& fmd);

  
  /**
   * @brief call the 'auto repair' function e.g. 'file convert --rewrite'
   * 
   * @param manager host:port of the server to contact
   * @param fid file id to auto-repair
   * 
   * @return 
   */
  int CallAutoRepair ( const char* manager,
                       eos::common::FileId::fileid_t fid);
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  FmdClient ()
  {
    SetLogId("CommonFmdClient");
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  ~FmdClient ()
  {
  }
};


// ---------------------------------------------------------------------------
extern FmdClient gFmdClient;

EOSFSTNAMESPACE_END

#endif
