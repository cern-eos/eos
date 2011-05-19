#ifndef __EOSMGM_PROC_FS__HH__
#define __EOSMGM_PROC_FS__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "mgm/FileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int proc_fs_dumpmd(std::string &fsidst, XrdOucString &dp, XrdOucString &df, XrdOucString &ds, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_config(std::string &identifier, std::string &key, std::string &value, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_add(std::string &sfsid, std::string &uuid, std::string &nodename, std::string &mountpoint, std::string &space, std::string &configstatus, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

FileSystem* proc_fs_source(std::string source_group, std::string target_group);

std::string proc_fs_target(std::string target_group);

int proc_fs_mv(std::string &sfsid, std::string &space, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_rm(std::string &nodename, std::string &mountpoint, std::string &id, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

EOSMGMNAMESPACE_END

#endif
