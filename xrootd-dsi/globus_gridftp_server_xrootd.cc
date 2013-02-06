// ----------------------------------------------------------------------
// File: globus_gridftp_server_xrootd.cc
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#if defined(linux)
#define _LARGE_FILES
#endif

#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <iostream>

/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include "./XrdUtils.hh"
#include "dsi_xrootd.hh"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

struct globus_l_gfs_xrootd_config {
  bool EosCks;
  bool EosChmod;
  bool EosAppTag;
  bool EosBook;
  std::string XrootdVmp;

  globus_l_gfs_xrootd_config() { 
    const char *cptr=0;
    EosCks=EosChmod=EosAppTag=false;
    cptr=getenv("XROOTD_VMP");
    if(cptr!=0) XrootdVmp=cptr;
    EosCks = (getenv("XROOTD_DSI_EOS_CKS")!=0);
    EosChmod = (getenv("XROOTD_DSI_EOS_CHMOD")!=0);
    EosAppTag = (getenv("XROOTD_DSI_EOS_APPTAG")!=0);
    EosBook = (getenv("XROOTD_DSI_EOS_BOOK")!=0);
    EosBook = EosCks = EosChmod = EosAppTag = (getenv("XROOTD_DSI_EOS")!=0);
  }
};


XrootPath XP;
globus_l_gfs_xrootd_config config;

extern "C" {

static globus_version_t local_version =
{
    0, /* major version number */
    1, /* minor version number */
    1157544130,
    0 /* branch ID */
};

/// utility function to make errors
static globus_result_t
globus_l_gfs_make_error(const char *msg, int errCode) {
  char *err_str;
  globus_result_t result;
  GlobusGFSName(globus_l_gfs_make_error);
  err_str = globus_common_create_string
      ("%s error: %s", msg,  strerror(errCode));
  result = GlobusGFSErrorGeneric(err_str);
  globus_free(err_str);
  return result;
}

/* fill the statbuf into globus_gfs_stat_t */
void fill_stat_array(globus_gfs_stat_t * filestat,
    struct stat statbuf,
    char *name) {
  filestat->mode = statbuf.st_mode;;
  filestat->nlink = statbuf.st_nlink;
  filestat->uid = statbuf.st_uid;
  filestat->gid = statbuf.st_gid;
  filestat->size = statbuf.st_size;

  filestat->mtime = statbuf.st_mtime;
  filestat->atime = statbuf.st_atime;
  filestat->ctime = statbuf.st_ctime;

  filestat->dev = statbuf.st_dev;
  filestat->ino = statbuf.st_ino;
  filestat->name = strdup(name);
}
/* free memory in stat_array from globus_gfs_stat_t->name */
void free_stat_array(globus_gfs_stat_t * filestat, int count) {
  int i;
  for(i=0;i<count;i++) free(filestat[i].name);
}

/*************************************************************************
 *  start
 *  -----
 *  This function is called when a new session is initialized, ie a user
 *  connects to the server.  This hook gives the dsi an opportunity to
 *  set internal state that will be threaded through to all other
 *  function calls associated with this session.  And an opportunity to
 *  reject the user.
 *
 *  finished_info.info.session.session_arg should be set to an DSI
 *  defined data structure.  This pointer will be passed as the void *
 *  user_arg parameter to all other interface functions.
 *
 *  NOTE: at nice wrapper function should exist that hides the details
 *        of the finished_info structure, but it currently does not.
 *        The DSI developer should jsut follow this template for now
 ************************************************************************/
static
void
globus_l_gfs_xrootd_start(globus_gfs_operation_t op,
    globus_gfs_session_info_t *session_info) {
  globus_l_gfs_xrootd_handle_t *xrootd_handle;
  globus_gfs_finished_info_t finished_info;
  const char *func="globus_l_gfs_xrootd_start";

  GlobusGFSName(globus_l_gfs_xrootd_start);

  xrootd_handle = (globus_l_gfs_xrootd_handle_t *)
      globus_malloc(sizeof(globus_l_gfs_xrootd_handle_t));

  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: started, uid: %u, gid: %u\n",
      func, getuid(),getgid());
  globus_mutex_init(&xrootd_handle->mutex,NULL);

  memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
  finished_info.type = GLOBUS_GFS_OP_SESSION_START;
  finished_info.result = GLOBUS_SUCCESS;
  finished_info.info.session.session_arg = xrootd_handle;
  finished_info.info.session.username = session_info->username;
  // if null we will go to HOME directory
  finished_info.info.session.home_dir = NULL;

  globus_gridftp_server_operation_finished(op, GLOBUS_SUCCESS, &finished_info);
  return;
}

/*************************************************************************
 *  destroy
 *  -------
 *  This is called when a session ends, ie client quits or disconnects.
 *  The dsi should clean up all memory they associated wit the session
 *  here.
 ************************************************************************/
static void
globus_l_gfs_xrootd_destroy(void *user_arg) {
  globus_l_gfs_xrootd_handle_t *xrootd_handle;
  xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;
  globus_mutex_destroy(&xrootd_handle->mutex);
  globus_free(xrootd_handle);
}


void
globus_l_gfs_file_copy_stat(
    globus_gfs_stat_t *                 stat_object,
    XrdCl::StatInfo *                       stat_buf,
    const char *                        filename,
    const char *                        symlink_target)
{
  GlobusGFSName(globus_l_gfs_file_copy_stat);

  XrootStatUtils::initStat(stat_object);

  stat_object->mode     = XrootStatUtils::mapFlagsXrd2Pos(stat_buf->GetFlags());
  stat_object->size     = stat_buf->GetSize(); // stat
  stat_object->mtime    = stat_buf->GetModTime();
  stat_object->atime    = stat_object->mtime;
  stat_object->ctime    = stat_object->mtime;

  if(filename && *filename)
  {
    stat_object->name = strdup(filename);
  }
  else
  {
    stat_object->name = NULL;
  }
  if(symlink_target && *symlink_target)
  {
    stat_object->symlink_target = strdup(symlink_target);
  }
  else
  {
    stat_object->symlink_target = NULL;
  }
}

static
void
globus_l_gfs_file_destroy_stat(
    globus_gfs_stat_t *                 stat_array,
    int                                 stat_count)
{
  int                                 i;
  GlobusGFSName(globus_l_gfs_file_destroy_stat);

  for(i = 0; i < stat_count; i++)
  {
    if(stat_array[i].name != NULL)
    {
      globus_free(stat_array[i].name);
    }
    if(stat_array[i].symlink_target != NULL)
    {
      globus_free(stat_array[i].symlink_target);
    }
  }
  globus_free(stat_array);
}

/* basepath and filename must be MAXPATHLEN long
 * the pathname may be absolute or relative, basepath will be the same */
static
void
globus_l_gfs_file_partition_path(
    const char *                        pathname,
    char *                              basepath,
    char *                              filename)
{
  char                                buf[MAXPATHLEN];
  char *                              filepart;
  GlobusGFSName(globus_l_gfs_file_partition_path);

  strncpy(buf, pathname, MAXPATHLEN);
  buf[MAXPATHLEN - 1] = '\0';

  filepart = strrchr(buf, '/');
  while(filepart && !*(filepart + 1) && filepart != buf)
  {
    *filepart = '\0';
    filepart = strrchr(buf, '/');
  }

  if(!filepart)
  {
    strcpy(filename, buf);
    basepath[0] = '\0';
  }
  else
  {
    if(filepart == buf)
    {
      if(!*(filepart + 1))
      {
        basepath[0] = '\0';
        filename[0] = '/';
        filename[1] = '\0';
      }
      else
      {
        *filepart++ = '\0';
        basepath[0] = '/';
        basepath[1] = '\0';
        strcpy(filename, filepart);
      }
    }
    else
    {
      *filepart++ = '\0';
      strcpy(basepath, buf);
      strcpy(filename, filepart);
    }
  }
}




/*************************************************************************
 *  stat
 *  ----
 *  This interface function is called whenever the server needs
 *  information about a given file or resource.  It is called then an
 *  LIST is sent by the client, when the server needs to verify that
 *  a file exists and has the proper permissions, etc.
 ************************************************************************/


static
void
globus_l_gfs_xrootd_stat(
    globus_gfs_operation_t              op,
    globus_gfs_stat_info_t *            stat_info,
    void *                              user_arg)
{
  globus_result_t                     result;
  globus_gfs_stat_t *                 stat_array;
  int                                 stat_count = 0;
  char                                basepath[MAXPATHLEN];
  char                                filename[MAXPATHLEN];
  char                                symlink_target[MAXPATHLEN];
  char                                *PathName;
  char                                 myServerPart[MAXPATHLEN],myPathPart[MAXPATHLEN];
  GlobusGFSName(globus_l_gfs_xrootd_stat);
  PathName=stat_info->pathname;

  globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: globus_l_gfs_xrootd_stat!\n","globus_l_gfs_xrootd_stat");

  std::string request(MAXPATHLEN*2,'\0');
  XrdCl::Buffer arg;
  XrdCl::StatInfo* xrdstatinfo = 0;
  XrdCl::XRootDStatus status;
  XrdCl::URL server;
  /*
       If we do stat_info->pathname++, it will cause third-party transfer
       hanging if there is a leading // in path. Don't know why. To work
       around, we replaced it with PathName.
   */
  while ( (strlen(PathName)>1) && (PathName[0] == '/' && PathName[1] == '/'))
  {
    PathName++;
  }

  char *myPath, buff[2048];
  if (!(myPath = XP.BuildURL(PathName, buff, sizeof(buff))))
    myPath=PathName;

  if(XrootPath::SplitURL(myPath,myServerPart,myPathPart,MAXPATHLEN)) {
    result = GlobusGFSErrorSystemError("stat", ECANCELED );
    globus_gridftp_server_finished_stat(op, result, NULL, 0);
    return;
  }

  arg.FromString( myPathPart );
  server.FromString(myServerPart);
  XrdCl::FileSystem  fs( server );
  status = fs.Stat( myPathPart,xrdstatinfo);
  if ( status.IsError() ) {
    if(xrdstatinfo) delete xrdstatinfo;
    result = GlobusGFSErrorSystemError("stat", XrootStatUtils::mapError(status.errNo) );
    goto error_stat1;
  }

  globus_l_gfs_file_partition_path(myPathPart, basepath, filename);

  if(!(xrdstatinfo->GetFlags()&XrdCl::StatInfo::IsDir) || stat_info->file_only)
  {
    stat_array = (globus_gfs_stat_t *) globus_malloc(sizeof(globus_gfs_stat_t));
    if(!stat_array)
    {
      result = GlobusGFSErrorMemory("stat_array");
      goto error_alloc1;
    }

    globus_l_gfs_file_copy_stat(
        stat_array, xrdstatinfo, filename, symlink_target);
    stat_count = 1;
  }
  else
  {
    XrdCl::DirectoryList *dirlist=0;
    status = fs.DirList(myPathPart,XrdCl::DirListFlags::Stat,dirlist,(uint16_t)0);
    if(!status.IsOK())
    {
      if(dirlist) delete dirlist;
      result = GlobusGFSErrorSystemError("opendir", XrootStatUtils::mapError(status.errNo));
      goto error_open;
    }

    stat_count = dirlist->GetSize();

    stat_array = (globus_gfs_stat_t *) globus_malloc(sizeof(globus_gfs_stat_t) * (stat_count+1));
    if(!stat_array)
    {
      if(dirlist) delete dirlist;
      result = GlobusGFSErrorMemory("stat_array");
      goto error_alloc2;
    }

    int i=0;
    for(XrdCl::DirectoryList::Iterator it=dirlist->Begin(); it!=dirlist->End(); it++) {
      std::string path=(*it)->GetName();
      globus_l_gfs_file_partition_path(path.c_str(), basepath, filename);
      globus_l_gfs_file_copy_stat(
          &stat_array[i++], (*it)->GetStatInfo(), filename, NULL );
    }
    if(dirlist) delete dirlist;
  }

  globus_gridftp_server_finished_stat(
      op, GLOBUS_SUCCESS, stat_array, stat_count);

  globus_l_gfs_file_destroy_stat(stat_array, stat_count);

  return;

  error_alloc2:
  error_open:
  error_alloc1:
  error_stat1:
  globus_gridftp_server_finished_stat(op, result, NULL, 0);

  /*    GlobusGFSFileDebugExitWithError();  */
}

/*************************************************************************
 *  command
 *  -------
 *  This interface function is called when the client sends a 'command'.
 *  commands are such things as mkdir, remdir, delete.  The complete
 *  enumeration is below.
 *
 *  To determine which command is being requested look at:
 *      cmd_info->command
 *
 *      GLOBUS_GFS_CMD_MKD = 1,
 *      GLOBUS_GFS_CMD_RMD,
 *      GLOBUS_GFS_CMD_DELE,
 *      GLOBUS_GFS_CMD_RNTO,
 *      GLOBUS_GFS_CMD_RNFR,
 *      GLOBUS_GFS_CMD_CKSM,
 *      GLOBUS_GFS_CMD_SITE_CHMOD,
 *      GLOBUS_GFS_CMD_SITE_DSI
 *
 *      the complete list is :
 *      GLOBUS_GFS_CMD_MKD = 1,
 *      GLOBUS_GFS_CMD_RMD,
 *      GLOBUS_GFS_CMD_DELE,
 *      GLOBUS_GFS_CMD_SITE_AUTHZ_ASSERT,
 *      GLOBUS_GFS_CMD_SITE_RDEL,
 *      GLOBUS_GFS_CMD_RNTO,
 *      GLOBUS_GFS_CMD_RNFR,
 *      GLOBUS_GFS_CMD_CKSM,
 *      GLOBUS_GFS_CMD_SITE_CHMOD,
 *      GLOBUS_GFS_CMD_SITE_DSI,
 *      GLOBUS_GFS_CMD_SITE_SETNETSTACK,
 *      GLOBUS_GFS_CMD_SITE_SETDISKSTACK,
 *      GLOBUS_GFS_CMD_SITE_CLIENTINFO,
 *      GLOBUS_GFS_CMD_DCSC,
 *      GLOBUS_GFS_CMD_SITE_CHGRP,
 *      GLOBUS_GFS_CMD_SITE_UTIME,
 *      GLOBUS_GFS_CMD_SITE_SYMLINKFROM,
 *      GLOBUS_GFS_CMD_SITE_SYMLINK,
 *      GLOBUS_GFS_MIN_CUSTOM_CMD = 4096
 ************************************************************************/
static void
globus_l_gfs_xrootd_command(globus_gfs_operation_t op,
    globus_gfs_command_info_t* cmd_info,
    void *user_arg) {

  GlobusGFSName(globus_l_gfs_xrootd_command);

  char                                cmd_data[MAXPATHLEN];
  char *                              PathName;
  globus_result_t                     rc = GLOBUS_SUCCESS;
  std::string                         cks;

  // create the full path and split it
  char *myPath, buff[2048];
  char myServerPart[MAXPATHLEN],myPathPart[MAXPATHLEN];
  PathName=cmd_info->pathname;
  while (PathName[0] == '/' && PathName[1] == '/') PathName++;
  if (!(myPath = XP.BuildURL(PathName, buff, sizeof(buff))))
    myPath=PathName;
  if(XrootPath::SplitURL(myPath,myServerPart,myPathPart,MAXPATHLEN)) {
    rc = GlobusGFSErrorGeneric("command fail : error parsing the filename");
    globus_gridftp_server_finished_command(op, rc, NULL);
    return ;
  }

  // open the filesystem
  XrdCl::URL server;
  XrdCl::Buffer arg,*resp;
  XrdCl::Status status;
  arg.FromString( myPathPart );
  server.FromString(myServerPart);
  XrdCl::FileSystem  fs( server );

  switch(cmd_info->command)
  {
  case GLOBUS_GFS_CMD_MKD:
    (status=fs.MkDir(myPathPart,XrdCl::MkDirFlags::None,(XrdCl::Access::Mode)XrootStatUtils::mapModePos2Xrd(0777))).IsError() &&
    (rc = GlobusGFSErrorGeneric( (std::string("mkdir() fail : ")+=status.ToString()).c_str() ));
    break;
  case GLOBUS_GFS_CMD_RMD:
    (status=fs.RmDir(myPathPart)).IsError() &&
    (rc = GlobusGFSErrorGeneric( (std::string("rmdir() fail")+=status.ToString()).c_str() ));
    break;
  case GLOBUS_GFS_CMD_DELE:
    (fs.Rm(myPathPart)).IsError() &&
    (rc = GlobusGFSErrorGeneric( (std::string("rm() fail")+=status.ToString()).c_str() ));
    break;
  case GLOBUS_GFS_CMD_SITE_RDEL:
    /*
          result = globus_l_gfs_file_delete(
          op, PathName, GLOBUS_TRUE);
     */
    rc = GLOBUS_FAILURE;
    break;
  case GLOBUS_GFS_CMD_RNTO:
    char myServerPart2[MAXPATHLEN],myPathPart2[MAXPATHLEN];
    if (!(myPath = XP.BuildURL(cmd_info->from_pathname, buff, sizeof(buff))))
      myPath=cmd_info->from_pathname;
    if(XrootPath::SplitURL(myPath,myServerPart2,myPathPart2,MAXPATHLEN)) {
      rc = GlobusGFSErrorGeneric("rename() fail : error parsing the target filename");
      globus_gridftp_server_finished_command(op, rc, NULL);
      return ;
    }
    (status=fs.Mv(myPathPart2,myPathPart)).IsError() &&
        (rc = GlobusGFSErrorGeneric( (std::string("rename() fail")+=status.ToString()).c_str() ));
    break;
  case GLOBUS_GFS_CMD_SITE_CHMOD:
    if(config.EosChmod) { // Using EOS Chmod
      char request[16384];
      sprintf(request,"%s?mgm.pcmd=chmod&mode=%d",myPathPart, cmd_info->chmod_mode); // specific to eos
      arg.FromString(request);
      status = fs.Query(XrdCl::QueryCode::OpaqueFile,arg,resp);
      rc = GlobusGFSErrorGeneric("chmod() fail");
      if (status.IsOK()) {
        char tag[4096];
        int retc=0;
        int items = sscanf(resp->GetBuffer(),"%s retc=%d",tag, &retc);
        fflush(stderr);
        if (retc || (items != 2) || (strcmp(tag,"chmod:"))) {
          // error
        } else {
          rc = GLOBUS_SUCCESS;
        }
      }
      delete resp;
    } else {    // Using XRoot Chmod
      (status=fs.ChMod(myPathPart,(XrdCl::Access::Mode)XrootStatUtils::mapModePos2Xrd(cmd_info->chmod_mode))).IsError() &&
          (rc = GlobusGFSErrorGeneric( (std::string("chmod() fail")+=status.ToString()).c_str() ));
    }
    break;
  case GLOBUS_GFS_CMD_CKSM:
    fflush(stderr);
    if(config.EosCks) { // Using EOS checksum
      if (!strcmp(cmd_info->cksm_alg, "adler32") ||
          !strcmp(cmd_info->cksm_alg, "ADLER32")) {
        char request[16384];
        sprintf(request,"%s?mgm.pcmd=checksum",myPathPart); // specific to eos
        arg.FromString(request);
        status = fs.Query(XrdCl::QueryCode::OpaqueFile,arg,resp);
        fflush(stderr);
        if (status.IsOK()) {
          if ( (strstr(resp->GetBuffer(),"retc=0") && (strlen(resp->GetBuffer())> 10))) {
            // the server returned a checksum via 'checksum: <checksum> retc='
            const char* cbegin = resp->GetBuffer() + 10;
            const char* cend   = strstr(resp->GetBuffer(),"retc=");
            if (cend > (cbegin+8)) {
              cend = cbegin + 8;
            }
            if (cbegin && cend) {
              strncpy(cmd_data,cbegin, cend-cbegin);
              // 0-terminate
              cmd_data[cend-cbegin]=0;
              rc = GLOBUS_SUCCESS;
              globus_gridftp_server_finished_command(op, rc, cmd_data);
              return;
            } else {
              rc = GlobusGFSErrorGeneric("checksum() fail : error parsing response");
            }
          } else {
            rc = GlobusGFSErrorGeneric("checksum() fail : error parsing response");
          }
        }
      }
      rc = GLOBUS_FAILURE;
    } else {    // Using XRootD checksum
      if((status=XrdUtils::GetRemoteCheckSum(cks,cmd_info->cksm_alg,myServerPart,myPathPart)).IsError() || (cks.size()>=MAXPATHLEN) ) { //UPPER CASE CHECKSUM ?
        rc = GlobusGFSErrorGeneric( (std::string("checksum() fail")+=status.ToString()).c_str() );
        break;
      }
      strcpy(cmd_data,cks.c_str());
      globus_gridftp_server_finished_command(op, GLOBUS_SUCCESS, cmd_data);
      return;
    }
    break;
  default:
    rc = GlobusGFSErrorGeneric("not implemented");
    break;
  }
  globus_gridftp_server_finished_command(op, rc, NULL);
}

/*************************************************************************
 *  recv
 *  ----
 *  This interface function is called when the client requests that a
 *  file be transfered to the server.
 *
 *  To receive a file the following functions will be used in roughly
 *  the presented order.  They are doced in more detail with the
 *  gridftp server documentation.
 *
 *      globus_gridftp_server_begin_transfer();
 *      globus_gridftp_server_register_read();
 *      globus_gridftp_server_finished_transfer();
 *
 ************************************************************************/

int xrootd_open_file
(char *path, int flags, int mode,
    globus_l_gfs_xrootd_handle_t *xrootd_handle) {
  XrdCl::XRootDStatus st;
  const char *func = "xrootd_open_file";
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: open file \"%s\"\n",
      func,
      path);
  try {
    char *myPath, buff[2048];
    if (!(myPath = XP.BuildURL(path, buff, sizeof(buff)))) {
      strcpy(buff,path);
      myPath=buff;
    }

    if(config.EosAppTag) {  // add the 'eos.gridftp' application tag
      if (strlen(myPath)) {
        if (strchr(myPath,'?')) {
          strcat(myPath, "&eos.app=eos/gridftp");     // specific to EOS
        } else {
          strcat(myPath, "?eos.app=eos/gridftp");     // specific to EOS
        }
      }
    }
    xrootd_handle->file = new XrdCl::File;
    st = xrootd_handle->file->Open(myPath, (XrdCl::OpenFlags::Flags)XrootStatUtils::mapFlagsPos2Xrd(flags), (XrdCl::Access::Mode)XrootStatUtils::mapModePos2Xrd(mode));

    if ( st.IsError()) {
      globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,
          "%s: XrdCl::File::Open error code %d\n",
          func, st.errNo);
    }
  } catch (...) {
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
        "%s: Exception caught when calling XrdCl::File::Open\n",
        func);
    return -1;
  }
  return (st.IsError());
}

/* receive from client */
static void globus_l_gfs_file_net_read_cb(globus_gfs_operation_t op,
    globus_result_t result,
    globus_byte_t *buffer,
    globus_size_t nbytes,
    globus_off_t offset,
    globus_bool_t eof,
    void *user_arg) {
  globus_l_gfs_xrootd_handle_t *xrootd_handle;
  XrdCl::XRootDStatus status;

  xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;
  globus_mutex_lock(&xrootd_handle->mutex);
  {
    if(eof) xrootd_handle->done = GLOBUS_TRUE;
    xrootd_handle->outstanding--;
    if(result != GLOBUS_SUCCESS) {
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
    }
    else if(nbytes > 0) {
      status = xrootd_handle->file->Write(offset, nbytes, buffer);
      if(status.IsError()) {
        errno = status.errNo;
        xrootd_handle->cached_res =
            globus_l_gfs_make_error("write",errno);
        xrootd_handle->done = GLOBUS_TRUE;
      } else globus_gridftp_server_update_bytes_written(op,offset,nbytes);
    }

    globus_free(buffer);
    /* if not done just register the next one */
    if(!xrootd_handle->done) {
      globus_l_gfs_xrootd_read_from_net(xrootd_handle);  // !!!!!!!!!!!!!!!!!!!!!!!! RECURSIVE STACK SIZE????????
    }
    /* if done and there are no outstanding callbacks finish */
    else if(xrootd_handle->outstanding == 0){
      delete xrootd_handle->file;
      globus_gridftp_server_finished_transfer
      (op, xrootd_handle->cached_res);
    }
  }
  globus_mutex_unlock(&xrootd_handle->mutex);
}

static void globus_l_gfs_xrootd_read_from_net
(globus_l_gfs_xrootd_handle_t *xrootd_handle) {
  globus_byte_t *buffer;
  globus_result_t result;
  const char *func = "globus_l_gfs_xrootd_read_from_net";

  GlobusGFSName(globus_l_gfs_xrootd_read_from_net);
  /* in the read case this number will vary */
  globus_gridftp_server_get_optimal_concurrency
  (xrootd_handle->op, &xrootd_handle->optimal_count);

  while(xrootd_handle->outstanding <
      xrootd_handle->optimal_count) {
    buffer=(globus_byte_t*)globus_malloc(xrootd_handle->block_size);
    if (buffer == NULL) {
      result = GlobusGFSErrorGeneric("error: globus malloc failed");
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
      if(xrootd_handle->outstanding == 0) {
        delete xrootd_handle->file;
        globus_gridftp_server_finished_transfer
        (xrootd_handle->op, xrootd_handle->cached_res);
      }
      return;
    }
    result= globus_gridftp_server_register_read(xrootd_handle->op,
        buffer,
        xrootd_handle->block_size,
        globus_l_gfs_file_net_read_cb,
        xrootd_handle);

    if(result != GLOBUS_SUCCESS)  {
      globus_gfs_log_message
      (GLOBUS_GFS_LOG_ERR,
          "%s: register read has finished with a bad result \n",
          func);
      globus_free(buffer);
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
      if(xrootd_handle->outstanding == 0) {
        delete xrootd_handle->file;
        globus_gridftp_server_finished_transfer
        (xrootd_handle->op, xrootd_handle->cached_res);
      }
      return;
    }
    xrootd_handle->outstanding++;
  }
}

static void
globus_l_gfs_xrootd_recv(globus_gfs_operation_t op,
    globus_gfs_transfer_info_t *transfer_info,
    void *user_arg) {
  globus_l_gfs_xrootd_handle_t *xrootd_handle;
  globus_result_t result;
  const char *func = "globus_l_gfs_xrootd_recv";
  char pathname[16384];
  int flags;
  int rc;

  GlobusGFSName(globus_l_gfs_xrootd_recv);
  xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;

  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

  if(config.EosBook && transfer_info->alloc_size) {
    snprintf(pathname,sizeof(pathname)-1, "%s?eos.bookingsize=%lu&eos.targetsize=%lu",transfer_info->pathname, transfer_info->alloc_size, transfer_info->alloc_size); // specific to eos
  } else {
    snprintf(pathname,sizeof(pathname),"%s", transfer_info->pathname);
  }

  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: pathname: %s \n",
      func,pathname);

  // try to open
  flags = O_WRONLY | O_CREAT;
  if(transfer_info->truncate) flags |= O_TRUNC;

  rc = xrootd_open_file(pathname, flags, 0644, xrootd_handle);

  if (rc) {
    result=globus_l_gfs_make_error("open/create", errno);
    globus_gridftp_server_finished_transfer(op, result);
    return;
  }

  // reset all the needed variables in the handle
  xrootd_handle->cached_res = GLOBUS_SUCCESS;
  xrootd_handle->outstanding = 0;
  xrootd_handle->done = GLOBUS_FALSE;
  xrootd_handle->blk_length = 0;
  xrootd_handle->blk_offset = 0;
  xrootd_handle->op = op;

  globus_gridftp_server_get_block_size(op, &xrootd_handle->block_size);
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: block size: %ld\n",
      func,xrootd_handle->block_size);

  globus_gridftp_server_begin_transfer(op, 0, xrootd_handle);

  globus_mutex_lock(&xrootd_handle->mutex);
  {
    globus_l_gfs_xrootd_read_from_net(xrootd_handle);
  }
  globus_mutex_unlock(&xrootd_handle->mutex);
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
  return;
}

/*************************************************************************
 *  send
 *  ----
 *  This interface function is called when the client requests to receive
 *  a file from the server.
 *
 *  To send a file to the client the following functions will be used in roughly
 *  the presented order.  They are doced in more detail with the
 *  gridftp server documentation.
 *
 *      globus_gridftp_server_begin_transfer();
 *      globus_gridftp_server_register_write();
 *      globus_gridftp_server_finished_transfer();
 *
 ************************************************************************/
static void
globus_l_gfs_xrootd_send(globus_gfs_operation_t op,
    globus_gfs_transfer_info_t *transfer_info,
    void *user_arg) {
  globus_l_gfs_xrootd_handle_t *xrootd_handle;
  const char *func = "globus_l_gfs_xrootd_send";
  char *pathname;
  int i,rc;
  globus_bool_t done;
  globus_result_t result;

  GlobusGFSName(globus_l_gfs_xrootd_send);
  xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

  pathname=strdup(transfer_info->pathname);

  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: pathname: %s\n",
      func,pathname);
  rc =xrootd_open_file(pathname, O_RDONLY, 0, xrootd_handle);   /* mode is ignored */

  if(rc) {
    result = globus_l_gfs_make_error("open", errno);
    globus_gridftp_server_finished_transfer(op, result);
    free(pathname);
    return;
  }
  free(pathname);

  /* reset all the needed variables in the handle */
  xrootd_handle->cached_res = GLOBUS_SUCCESS;
  xrootd_handle->outstanding = 0;
  xrootd_handle->done = GLOBUS_FALSE;
  xrootd_handle->blk_length = 0;
  xrootd_handle->blk_offset = 0;
  xrootd_handle->op = op;

  globus_gridftp_server_get_optimal_concurrency
  (op, &xrootd_handle->optimal_count);
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: optimal_concurrency: %u\n",
      func,xrootd_handle->optimal_count);

  globus_gridftp_server_get_block_size(op, &xrootd_handle->block_size);
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
      "%s: block_size: %ld\n",
      func,xrootd_handle->block_size);

  globus_gridftp_server_begin_transfer(op, 0, xrootd_handle);
  done = GLOBUS_FALSE;
  globus_mutex_lock(&xrootd_handle->mutex);
  {
    for(i = 0; i < xrootd_handle->optimal_count && !done; i++) {
      done = globus_l_gfs_xrootd_send_next_to_client(xrootd_handle);
    }
  }
  globus_mutex_unlock(&xrootd_handle->mutex);
  globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
}

static globus_bool_t
globus_l_gfs_xrootd_send_next_to_client
(globus_l_gfs_xrootd_handle_t *xrootd_handle) {
  globus_result_t result;
  globus_result_t res;
  globus_off_t read_length;
  uint32_t nbread;
  globus_byte_t *buffer;
  const char *func = "globus_l_gfs_xrootd_send_next_to_client";
  XrdCl::XRootDStatus status;

  globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: Hello!\n",func);

  GlobusGFSName(globus_l_gfs_xrootd_send_next_to_client);

  if (xrootd_handle->blk_length == 0) {
    // check the next range to read
    globus_gridftp_server_get_read_range(xrootd_handle->op,
        &xrootd_handle->blk_offset,
        &xrootd_handle->blk_length);
    if (xrootd_handle->blk_length == 0) {
      result = GLOBUS_SUCCESS;
      delete xrootd_handle->file;
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
      if (xrootd_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer
        (xrootd_handle->op, xrootd_handle->cached_res);
      }
      return xrootd_handle->done;
    }
  }

  if (xrootd_handle->blk_length == -1 ||
      xrootd_handle->blk_length > (globus_off_t)xrootd_handle->block_size) {
    read_length = xrootd_handle->block_size;
  } else {
    read_length = xrootd_handle->blk_length;
  }

  buffer = (globus_byte_t*)globus_malloc(read_length);
  if(buffer == NULL) {
    result = GlobusGFSErrorGeneric("error: malloc failed");
    delete xrootd_handle->file;
    xrootd_handle->cached_res = result;
    xrootd_handle->done = GLOBUS_TRUE;
    if (xrootd_handle->outstanding == 0) {
      globus_gridftp_server_finished_transfer(xrootd_handle->op,
          xrootd_handle->cached_res);
    }
    return xrootd_handle->done;
  }

  status = xrootd_handle->file->Read(xrootd_handle->blk_offset,read_length, buffer, nbread);
  if (status.IsOK() && nbread==0) { // eof
    result = GLOBUS_SUCCESS;
    globus_free(buffer);
    delete xrootd_handle->file;
    xrootd_handle->cached_res = result;
    xrootd_handle->done = GLOBUS_TRUE;
    if (xrootd_handle->outstanding == 0) {
      globus_gridftp_server_finished_transfer(xrootd_handle->op,
          xrootd_handle->cached_res);
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished (eof)\n",func);
    return xrootd_handle->done;
  }
  if (status.IsError()) { // error
    result = globus_l_gfs_make_error("read", errno);
    globus_free(buffer);
    delete xrootd_handle->file;
    xrootd_handle->cached_res = result;
    xrootd_handle->done = GLOBUS_TRUE;
    if (xrootd_handle->outstanding == 0) {
      globus_gridftp_server_finished_transfer(xrootd_handle->op,
          xrootd_handle->cached_res);
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: finished (error)\n",func);
    return xrootd_handle->done;
  }

  if (read_length>=nbread) {
    // if we have a file with size less than block_size we do not
    // have use parrallel connections (one will be enough)
    xrootd_handle->optimal_count--;
  }
  read_length = nbread;

  if (xrootd_handle->blk_length != -1) {
    xrootd_handle->blk_length -= read_length;
  }

  res = globus_gridftp_server_register_write(xrootd_handle->op,
      buffer,
      read_length,
      xrootd_handle->blk_offset,
      -1,
      globus_l_gfs_net_write_cb,
      xrootd_handle);
  xrootd_handle->blk_offset += read_length;
  if (res != GLOBUS_SUCCESS) {
    globus_free(buffer);
    delete xrootd_handle->file;
    xrootd_handle->cached_res = res;
    xrootd_handle->done = GLOBUS_TRUE;
    if (xrootd_handle->outstanding == 0) {
      globus_gridftp_server_finished_transfer(xrootd_handle->op,
          xrootd_handle->cached_res);
    }
    return xrootd_handle->done;
  }

  xrootd_handle->outstanding++;
  return GLOBUS_FALSE;
}


static void
globus_l_gfs_net_write_cb(globus_gfs_operation_t op,
    globus_result_t result,
    globus_byte_t *buffer,
    globus_size_t,
    void * user_arg) {
  globus_l_gfs_xrootd_handle_t *xrootd_handle;
  const char *func = "globus_l_gfs_net_write_cb";

  xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;

  globus_free(buffer);
  globus_mutex_lock(&xrootd_handle->mutex);
  {
    xrootd_handle->outstanding--;
    if (result != GLOBUS_SUCCESS) {
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
    }
    if (!xrootd_handle->done) {
      globus_l_gfs_xrootd_send_next_to_client(xrootd_handle);
    } else if (xrootd_handle->outstanding == 0) {
      delete xrootd_handle->file;
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
          "%s: finished transfer\n",
          func);
      globus_gridftp_server_finished_transfer
      (op, xrootd_handle->cached_res);
    }
  }
  globus_mutex_unlock(&xrootd_handle->mutex);
}


static int globus_l_gfs_xrootd_activate(void);

static int globus_l_gfs_xrootd_deactivate(void);

/// no need to change this
static globus_gfs_storage_iface_t globus_l_gfs_xrootd_dsi_iface =
{
    GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
    globus_l_gfs_xrootd_start,
    globus_l_gfs_xrootd_destroy,
    NULL, /* list */
    globus_l_gfs_xrootd_send,
    globus_l_gfs_xrootd_recv,
    NULL, /* trev */
    NULL, /* active */
    NULL, /* passive */
    NULL, /* data destroy */
    globus_l_gfs_xrootd_command,
    globus_l_gfs_xrootd_stat,
    NULL,
    NULL
};


/// no need to change this
GlobusExtensionDefineModule(globus_gridftp_server_xrootd) =
{
    (char*)"globus_gridftp_server_xrootd",
    globus_l_gfs_xrootd_activate,
    globus_l_gfs_xrootd_deactivate,
    NULL,
    NULL,
    &local_version,
    NULL
};

/// no need to change this
static int globus_l_gfs_xrootd_activate(void) {
  globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: Activating XRootD DSI plugin\n","globus_l_gfs_xrootd_activate");
  globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: XRootD Virtual Mount Point is set to: %s\n","globus_l_gfs_xrootd_activate",config.XrootdVmp.c_str());
  std::stringstream ss;
  if(config.EosAppTag) ss<<" EosAppTag";
  if(config.EosChmod)  ss<<" EosChmod";
  if(config.EosCks)    ss<<" EosCks";
  if(config.EosBook)   ss<<" EosBook";
  std::string eosspec(ss.str());
  if(eosspec.size()) {
    ss.str("");
    ss<<"globus_l_gfs_xrootd_activate: XRootD DSI plugin runs the following EOS specifics:";
    ss<<eosspec<<std::endl;
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,ss.str().c_str());
  }

  globus_extension_registry_add
  (GLOBUS_GFS_DSI_REGISTRY,
      (void*)"xrootd",
      GlobusExtensionMyModule(globus_gridftp_server_xrootd),
      &globus_l_gfs_xrootd_dsi_iface);
  return 0;
}

/// no need to change this
static int globus_l_gfs_xrootd_deactivate(void) {
  globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, (void*)"xrootd");

  return 0;
}

} // end extern "C"
