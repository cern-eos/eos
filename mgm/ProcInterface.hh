// ----------------------------------------------------------------------
// File: ProcInterface.hh
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

#ifndef __EOSMGM_PROCINTERFACE__HH__
#define __EOSMGM_PROCINTERFACE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "proc/proc_fs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class ProcCommand : public eos::common::LogId {
private:
    XrdOucString path;
    eos::common::Mapping::VirtualIdentity* pVid;
    XrdOucString cmd;
    XrdOucString subcmd;
    XrdOucString args;

    XrdOucString stdOut;
    XrdOucString stdErr;
    XrdOucString stdJson;
    int retc;
    XrdOucString resultStream;
    XrdOucEnv*   opaque;
    const char*  ininfo;
    bool dosort;
    const char*  selection;
    XrdOucString outformat;
    
    // the 'find' command writes results into temporary files
    FILE* fstdout;
    FILE* fstderr;
    FILE* fresultStream;
    XrdOucString fstdoutfilename;
    XrdOucString fstderrfilename;
    XrdOucString fresultStreamfilename;
    XrdOucErrInfo* error;

    XrdOucString comment;
    time_t exectime;

    size_t len;
    off_t offset;
    void MakeResult();

    bool adminCmd;
    bool userCmd;

    bool fuseformat;
    bool jsonformat;
    bool closed;
public:

    int open(const char* path, const char* info, eos::common::Mapping::VirtualIdentity &vid, XrdOucErrInfo *error);
    int read(XrdSfsFileOffset offset, char *buff, XrdSfsXferSize blen);
    int stat(struct stat* buf);
    int close();

    void AddOutput(XrdOucString &lStdOut, XrdOucString &lStdErr) {
        lStdOut += stdOut;
        lStdErr += stdErr;
    }

    bool OpenTemporaryOutputFiles();

    int GetRetc() {
        return retc;
    }

    //! -------------------------------------------------------------
    //! User Proc Commands
    //! -------------------------------------------------------------
    int Attr();
    int Cd();
    int Chmod();
    int Find();
    int File();
    int Fileinfo();
    int Fuse();
    int Ls();
    int Map();
    int Mkdir();
    int Motd();
    int Quota();
    int Rm();
    int Rmdir();
    int Version();
    int Who();
    int Whoami();

    //! -------------------------------------------------------------
    //! Admin Proc Commands
    //! -------------------------------------------------------------
    int Access();
    int Chown();
    int Config();
    int Debug();
    int Fs();
    int Fsck();
    int Group();
    int Io();
    int Node();
    int Ns();
    int AdminQuota();
    int Rtlog();
    int Space();
    int Transfer();
    int Vid();

    ProcCommand();
    ~ProcCommand();
};

class ProcInterface {
private:

public:

    static bool IsProcAccess(const char* path);
    static bool IsWriteAccess(const char* path, const char* info);
    static bool Authorize(const char* path, const char* info, eos::common::Mapping::VirtualIdentity &vid, const XrdSecEntity* entity);

    ProcInterface();
    ~ProcInterface();
};

EOSMGMNAMESPACE_END
        
#endif
