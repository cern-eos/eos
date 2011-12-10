// ----------------------------------------------------------------------
// File: globus_gridftp_server_eos.cc
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

#if defined(linux)
#define _LARGE_FILES
#endif

#include <sys/types.h>
#include <dirent.h>
#include <string.h>

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdPosix/XrdPosixExtern.hh>
#include "dsi_eos.hh"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

XrdPosixXrootd posixsingleton;

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
  globus_l_gfs_eos_start(globus_gfs_operation_t op,
                         globus_gfs_session_info_t *session_info) {
    globus_l_gfs_eos_handle_t *eos_handle;
    globus_gfs_finished_info_t finished_info;
    const char *func="globus_l_gfs_eos_start";

    GlobusGFSName(globus_l_gfs_eos_start);

    eos_handle = (globus_l_gfs_eos_handle_t *)
      globus_malloc(sizeof(globus_l_gfs_eos_handle_t));

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: started, uid: %u, gid: %u\n",
                           func, getuid(),getgid());
    globus_mutex_init(&eos_handle->mutex,NULL);

    memset(&finished_info, '\0', sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = GLOBUS_SUCCESS;
    finished_info.info.session.session_arg = eos_handle;
    finished_info.info.session.username = session_info->username;
    // if null we will go to HOME directory
    finished_info.info.session.home_dir = NULL;

    globus_gridftp_server_operation_finished(op, GLOBUS_SUCCESS, &finished_info);

    // uncomment that, if you want to add debug printouts
    //    freopen("/tmp/xrdlog.gsiftp","a+", stderr);
  }

  /*************************************************************************
   *  destroy
   *  -------
   *  This is called when a session ends, ie client quits or disconnects.
   *  The dsi should clean up all memory they associated wit the session
   *  here.
   ************************************************************************/
  static void
  globus_l_gfs_eos_destroy(void *user_arg) {
    globus_l_gfs_eos_handle_t *eos_handle;
    eos_handle = (globus_l_gfs_eos_handle_t *) user_arg;
    globus_mutex_destroy(&eos_handle->mutex);
    globus_free(eos_handle);
  }


  void
  globus_l_gfs_file_copy_stat(
                              globus_gfs_stat_t *                 stat_object,
                              struct stat *                       stat_buf,
                              const char *                        filename,
                              const char *                        symlink_target)
  {
    GlobusGFSName(globus_l_gfs_file_copy_stat);
    
    stat_object->mode     = stat_buf->st_mode;
    stat_object->nlink    = stat_buf->st_nlink;
    stat_object->uid      = stat_buf->st_uid;
    stat_object->gid      = stat_buf->st_gid;
    stat_object->size     = stat_buf->st_size;
    stat_object->mtime    = stat_buf->st_mtime;
    stat_object->atime    = stat_buf->st_atime;
    stat_object->ctime    = stat_buf->st_ctime;
    stat_object->dev      = stat_buf->st_dev;
    stat_object->ino      = stat_buf->st_ino;

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
  globus_l_gfs_eos_stat(
                        globus_gfs_operation_t              op,
                        globus_gfs_stat_info_t *            stat_info,
                        void *                              user_arg)
  {
    globus_result_t                     result;
    struct stat                         stat_buf;
    globus_gfs_stat_t *                 stat_array;
    int                                 stat_count = 0;
    DIR *                               dir;
    char                                basepath[MAXPATHLEN];
    char                                filename[MAXPATHLEN];
    char                                symlink_target[MAXPATHLEN];
    char *                              PathName;
    GlobusGFSName(globus_l_gfs_eos_stat);
    PathName=stat_info->pathname;

    //    freopen("/tmp/xrdlog.gsiftp","a+", stderr);
    /* 
       If we do stat_info->pathname++, it will cause third-party transfer
       hanging if there is a leading // in path. Don't know why. To work
       around, we replaced it with PathName.
    */
    while ( (strlen(PathName)>1) && (PathName[0] == '/' && PathName[1] == '/'))
      {
        PathName++;
      }
    
    /* lstat is the same as stat when not operating on a link */
    if(XrdPosix_Stat(PathName, &stat_buf) != 0)
      {
        result = GlobusGFSErrorSystemError("stat", errno);
        goto error_stat1;
      }

    globus_l_gfs_file_partition_path(PathName, basepath, filename);
    
    if(!S_ISDIR(stat_buf.st_mode) || stat_info->file_only)
      {
        stat_array = (globus_gfs_stat_t *)
          globus_malloc(sizeof(globus_gfs_stat_t));
        if(!stat_array)
          {
            result = GlobusGFSErrorMemory("stat_array");
            goto error_alloc1;
          }
        
        globus_l_gfs_file_copy_stat(
                                    stat_array, &stat_buf, filename, symlink_target);
        stat_count = 1;
      }
    else
      {
        struct dirent *                 dir_entry = 0 ;
        int                             i;
        char                            dir_path[MAXPATHLEN];
    
        dir = XrdPosix_Opendir(PathName);
        if(!dir)
          {
            result = GlobusGFSErrorSystemError("opendir", errno);
            goto error_open;
          }
        
        stat_count = 0;
        int rc = 0;

        while( dir_entry =  XrdPosix_Readdir(dir) )
          {
            stat_count++;
          }
        XrdPosix_Rewinddir(dir);

        
        stat_array = (globus_gfs_stat_t *)
          globus_malloc(sizeof(globus_gfs_stat_t) * (stat_count+1));
        if(!stat_array)
          {
            result = GlobusGFSErrorMemory("stat_array");
            goto error_alloc2;
          }
        
        snprintf(dir_path, sizeof(dir_path), "%s/%s", basepath, filename);
        dir_path[MAXPATHLEN - 1] = '\0';
        
        for(i = 0;
            dir_entry = XrdPosix_Readdir(dir);
            i++)
          {
            char                        tmp_path[MAXPATHLEN];
            char                        *path;
                
            snprintf(tmp_path, sizeof(tmp_path), "%s/%s", dir_path, dir_entry->d_name);
            tmp_path[MAXPATHLEN - 1] = '\0';
            path=tmp_path;
        
            /* function globus_l_gfs_file_partition_path() seems to add two 
               extra '/'s to the beginning of tmp_path. XROOTD is sensitive 
               to the extra '/'s not defined in XROOTD_VMP so we remove them */
            if (path[0] == '/' && path[1] == '/') { path++; }
            while (path[0] == '/' && path[1] == '/') { path++; }
            /* lstat is the same as stat when not operating on a link */
            if(XrdPosix_Stat(path, &stat_buf) != 0)
              {
                result = GlobusGFSErrorSystemError("lstat", errno);
                /* just skip invalid entries */
                stat_count--;
                i--;
                continue;
              }
            globus_l_gfs_file_copy_stat(
                                        &stat_array[i], &stat_buf, dir_entry->d_name, symlink_target);
          }
        
        if(i != stat_count)
          {
            result = GlobusGFSErrorSystemError("readdir", errno);
            goto error_read;
          }
        
        XrdPosix_Closedir(dir);
      }
    
    globus_gridftp_server_finished_stat(
                                        op, GLOBUS_SUCCESS, stat_array, stat_count);
    
    
    globus_l_gfs_file_destroy_stat(stat_array, stat_count);
    
    return;

  error_read:
    globus_l_gfs_file_destroy_stat(stat_array, stat_count);
    
  error_alloc2:
    closedir(dir);
    
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
   ************************************************************************/
  static void
  globus_l_gfs_eos_command(globus_gfs_operation_t op,
                           globus_gfs_command_info_t* cmd_info,
                           void *user_arg) {
    globus_l_gfs_eos_handle_t *eos_handle;
    globus_result_t result;

    char                                cmd_data[128];

    GlobusGFSName(globus_l_gfs_eos_command);
    eos_handle = (globus_l_gfs_eos_handle_t *) user_arg;

    char *                              PathName;
    globus_result_t                     rc;

    
    PathName=cmd_info->pathname;
    while (PathName[0] == '/' && PathName[1] == '/')
      {
        PathName++;
      }

    fflush(stderr);
    rc = GLOBUS_SUCCESS;
    switch(cmd_info->command)
      {
      case GLOBUS_GFS_CMD_MKD:
        (XrdPosix_Mkdir(PathName, 0777) == 0) || 
          (rc = GlobusGFSErrorGeneric("mkdir() fail"));
        break;
      case GLOBUS_GFS_CMD_RMD:
        (XrdPosix_Rmdir(PathName) == 0) || 
          (rc = GlobusGFSErrorGeneric("rmdir() fail"));
        break;
      case GLOBUS_GFS_CMD_DELE:
        (XrdPosix_Unlink(PathName) == 0) ||
          (rc = GlobusGFSErrorGeneric("unlink() fail"));
        break;
      case GLOBUS_GFS_CMD_SITE_RDEL:
        /*
          result = globus_l_gfs_file_delete(
          op, PathName, GLOBUS_TRUE);
        */
        rc = GLOBUS_FAILURE;
        break;
      case GLOBUS_GFS_CMD_RNTO:
        (XrdPosix_Rename(cmd_info->rnfr_pathname, PathName) == 0) || 
          (rc = GlobusGFSErrorGeneric("rename() fail"));
        break;
      case GLOBUS_GFS_CMD_SITE_CHMOD:
        char request[16384];
        char response[4096];
        sprintf(response,"");
        sprintf(request,"root://%s%s?mgm.pcmd=chmod&mode=%d",getenv("XROOTD_VMP")?getenv("XROOTD_VMP"):"",PathName, cmd_info->chmod_mode);
        
        long long result;
        result = XrdPosixXrootd::QueryOpaque(request,response,4096);
        rc = GlobusGFSErrorGeneric("chmod() fail");
        
        if (result>0) {
          char tag[4096];
          int retc=0;
          int items = sscanf(response,"%s retc=%d",tag, &retc);
          fflush(stderr);
          if (retc || (items != 2) || (strcmp(tag,"chmod:"))) {
            // error
          } else {
            rc = GLOBUS_SUCCESS;
          }
        }
        break;
      case GLOBUS_GFS_CMD_CKSM:
        fflush(stderr);
        if (!strcmp(cmd_info->cksm_alg, "adler32") || 
            !strcmp(cmd_info->cksm_alg, "ADLER32")) {
          char request[16384];
          char response[4096];
          sprintf(response,"");
          sprintf(request,"root://%s%s?mgm.pcmd=checksum",getenv("XROOTD_VMP")?getenv("XROOTD_VMP"):"",PathName);
          // put 0 terminated hex string in cmd_data
          long long result;
          result = XrdPosixXrootd::QueryOpaque(request,response,4096);
          fflush(stderr);
          if (result>0) {
            if ( (strstr(response,"retc=0") && (strlen(response)> 10))) {
              // the server returned a checksum via 'checksum: <checksum> retc=' 
              const char* cbegin = response + 10;
              const char* cend   = strstr(response,"retc=");
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
                rc = GlobusGFSErrorGeneric("checksum() fail");
              }
            } else {
              // the server returned an error
              char* e = strstr(response,"retc=");
              rc = GlobusGFSErrorGeneric("checksum() fail");
            }
          }
        }
        rc = GLOBUS_FAILURE;
        break;
        
      default:
        rc = GLOBUS_FAILURE;
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

  int eos_handle_open
  (char *path, int flags, int mode,
   globus_l_gfs_eos_handle_t *eos_handle) {
    int rc;
    const char *func = "eos_handle_open";
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: open file \"%s\"\n",
                           func,
                           path);
    try {
      system("printenv | grep XROOT");
      rc = XrdPosix_Open(path, flags, mode);
      if (rc < 0) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,
                               "%s: XrdPosixXrootd::Open returned error code %d\n",
                               func, errno);
      }
    } catch (...) {
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                             "%s: Exception caught when calling XrdPosixXrootd::Open\n",
                             func);
      return -1;
    }
    return (rc);
  }

  /* receive from client */
  static void globus_l_gfs_file_net_read_cb(globus_gfs_operation_t op,
                                            globus_result_t result,
                                            globus_byte_t *buffer,
                                            globus_size_t nbytes,
                                            globus_off_t offset,
                                            globus_bool_t eof,
                                            void *user_arg) {
    globus_off_t start_offset;
    globus_l_gfs_eos_handle_t *eos_handle;
    globus_size_t bytes_written;

    eos_handle = (globus_l_gfs_eos_handle_t *) user_arg;
    globus_mutex_lock(&eos_handle->mutex);
    {
      if(eof) eos_handle->done = GLOBUS_TRUE;
      eos_handle->outstanding--;
      if(result != GLOBUS_SUCCESS) {
        eos_handle->cached_res = result;
        eos_handle->done = GLOBUS_TRUE;
      }
      else if(nbytes > 0) {
        start_offset = XrdPosix_Lseek(eos_handle->fd, offset, SEEK_SET);
        if(start_offset != offset) {
          eos_handle->cached_res = globus_l_gfs_make_error("seek",errno);
          eos_handle->done = GLOBUS_TRUE;
        }
        else {
          bytes_written = XrdPosix_Write(eos_handle->fd, buffer, nbytes);
          if(bytes_written < nbytes) {
            errno = ENOSPC;
            eos_handle->cached_res =
              globus_l_gfs_make_error("write",errno);
            eos_handle->done = GLOBUS_TRUE;
          } else globus_gridftp_server_update_bytes_written(op,offset,nbytes);
        }
      }

      globus_free(buffer);
      /* if not done just register the next one */
      if(!eos_handle->done) {
        globus_l_gfs_eos_read_from_net(eos_handle);
      }
      /* if done and there are no outstanding callbacks finish */
      else if(eos_handle->outstanding == 0){
        XrdPosix_Close(eos_handle->fd);
        globus_gridftp_server_finished_transfer
          (op, eos_handle->cached_res);
      }
    }
    globus_mutex_unlock(&eos_handle->mutex);
  }

  static void globus_l_gfs_eos_read_from_net
  (globus_l_gfs_eos_handle_t *eos_handle) {
    globus_byte_t *buffer;
    globus_result_t result;
    const char *func = "globus_l_gfs_eos_read_from_net";

    GlobusGFSName(globus_l_gfs_eos_read_from_net);
    /* in the read case this number will vary */
    globus_gridftp_server_get_optimal_concurrency
      (eos_handle->op, &eos_handle->optimal_count);

    while(eos_handle->outstanding <
          eos_handle->optimal_count) {
      buffer=(globus_byte_t*)globus_malloc(eos_handle->block_size);
      if (buffer == NULL) {
        result = GlobusGFSErrorGeneric("error: globus malloc failed");
        eos_handle->cached_res = result;
        eos_handle->done = GLOBUS_TRUE;
        if(eos_handle->outstanding == 0) {
          XrdPosix_Close(eos_handle->fd);
          globus_gridftp_server_finished_transfer
            (eos_handle->op, eos_handle->cached_res);
        }
        return;
      }
      result= globus_gridftp_server_register_read(eos_handle->op,
                                                  buffer,
                                                  eos_handle->block_size,
                                                  globus_l_gfs_file_net_read_cb,
                                                  eos_handle);

      if(result != GLOBUS_SUCCESS)  {
        globus_gfs_log_message
          (GLOBUS_GFS_LOG_ERR,
           "%s: register read has finished with a bad result \n",
           func);
        globus_free(buffer);
        eos_handle->cached_res = result;
        eos_handle->done = GLOBUS_TRUE;
        if(eos_handle->outstanding == 0) {
          XrdPosix_Close(eos_handle->fd);
          globus_gridftp_server_finished_transfer
            (eos_handle->op, eos_handle->cached_res);
        }
        return;
      }
      eos_handle->outstanding++;
    }
  }

  static void
  globus_l_gfs_eos_recv(globus_gfs_operation_t op,
                        globus_gfs_transfer_info_t *transfer_info,
                        void *user_arg) {
    globus_l_gfs_eos_handle_t *eos_handle;
    globus_result_t result;
    const char *func = "globus_l_gfs_eos_recv";
    char *pathname;
    int flags;

    GlobusGFSName(globus_l_gfs_eos_recv);
    eos_handle = (globus_l_gfs_eos_handle_t *) user_arg;

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

    pathname = strdup(transfer_info->pathname);

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: pathname: %s \n",
                           func,pathname);

    // try to open
    flags = O_WRONLY | O_CREAT;
    if(transfer_info->truncate) flags |= O_TRUNC;

    eos_handle->fd =
      eos_handle_open(pathname, flags, 0644, eos_handle);

    if (eos_handle->fd < 0) {
      result=globus_l_gfs_make_error("open/create", errno);
      globus_gridftp_server_finished_transfer(op, result);
      free(pathname);
      return;
    }

    // reset all the needed variables in the handle
    eos_handle->cached_res = GLOBUS_SUCCESS;
    eos_handle->outstanding = 0;
    eos_handle->done = GLOBUS_FALSE;
    eos_handle->blk_length = 0;
    eos_handle->blk_offset = 0;
    eos_handle->op = op;

    globus_gridftp_server_get_block_size(op, &eos_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: block size: %ld\n",
                           func,eos_handle->block_size);

    globus_gridftp_server_begin_transfer(op, 0, eos_handle);

    globus_mutex_lock(&eos_handle->mutex);
    {
      globus_l_gfs_eos_read_from_net(eos_handle);
    }
    globus_mutex_unlock(&eos_handle->mutex);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
    free(pathname);
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
  globus_l_gfs_eos_send(globus_gfs_operation_t op,
                        globus_gfs_transfer_info_t *transfer_info,
                        void *user_arg) {
    globus_l_gfs_eos_handle_t *eos_handle;
    const char *func = "globus_l_gfs_eos_send";
    char *pathname;
    int i;
    globus_bool_t done;
    globus_result_t result;

    GlobusGFSName(globus_l_gfs_eos_send);
    eos_handle = (globus_l_gfs_eos_handle_t *) user_arg;
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: started\n",func);

    pathname=strdup(transfer_info->pathname);

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: pathname: %s\n",
                           func,pathname);
    eos_handle->fd =
      eos_handle_open(pathname, O_RDONLY, 0, eos_handle);   /* mode is ignored */

    if(eos_handle->fd < 0) {
      result = globus_l_gfs_make_error("open", errno);
      globus_gridftp_server_finished_transfer(op, result);
      free(pathname);
      return;
    }

    free(pathname);

    /* reset all the needed variables in the handle */
    eos_handle->cached_res = GLOBUS_SUCCESS;
    eos_handle->outstanding = 0;
    eos_handle->done = GLOBUS_FALSE;
    eos_handle->blk_length = 0;
    eos_handle->blk_offset = 0;
    eos_handle->op = op;

    globus_gridftp_server_get_optimal_concurrency
      (op, &eos_handle->optimal_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: optimal_concurrency: %u\n",
                           func,eos_handle->optimal_count);

    globus_gridftp_server_get_block_size(op, &eos_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,
                           "%s: block_size: %ld\n",
                           func,eos_handle->block_size);

    globus_gridftp_server_begin_transfer(op, 0, eos_handle);
    done = GLOBUS_FALSE;
    globus_mutex_lock(&eos_handle->mutex);
    {
      for(i = 0; i < eos_handle->optimal_count && !done; i++) {
        done = globus_l_gfs_eos_send_next_to_client(eos_handle);
      }
    }
    globus_mutex_unlock(&eos_handle->mutex);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP,"%s: finished\n",func);
  }

  static globus_bool_t
  globus_l_gfs_eos_send_next_to_client
  (globus_l_gfs_eos_handle_t *eos_handle) {
    globus_result_t result;
    globus_result_t res;
    globus_off_t read_length;
    globus_off_t nbread;
    globus_off_t start_offset;
    globus_byte_t *buffer;
    const char *func = "globus_l_gfs_eos_send_next_to_client";

    GlobusGFSName(globus_l_gfs_eos_send_next_to_client);

    if (eos_handle->blk_length == 0) {
      // check the next range to read
      globus_gridftp_server_get_read_range(eos_handle->op,
                                           &eos_handle->blk_offset,
                                           &eos_handle->blk_length);
      if (eos_handle->blk_length == 0) {
        result = GLOBUS_SUCCESS;
        XrdPosix_Close(eos_handle->fd);
        eos_handle->cached_res = result;
        eos_handle->done = GLOBUS_TRUE;
        if (eos_handle->outstanding == 0) {
          globus_gridftp_server_finished_transfer
            (eos_handle->op, eos_handle->cached_res);
        }
        return eos_handle->done;
      }
    }

    if (eos_handle->blk_length == -1 ||
        eos_handle->blk_length > (globus_off_t)eos_handle->block_size) {
      read_length = eos_handle->block_size;
    } else {
      read_length = eos_handle->blk_length;
    }

    start_offset = XrdPosix_Lseek(eos_handle->fd,
                                  eos_handle->blk_offset,
                                  SEEK_SET);
    // verify that it worked
    if (start_offset != eos_handle->blk_offset) {
      result = globus_l_gfs_make_error("seek", errno);
      XrdPosix_Close(eos_handle->fd);
      eos_handle->cached_res = result;
      eos_handle->done = GLOBUS_TRUE;
      if (eos_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(eos_handle->op,
                                                eos_handle->cached_res);
      }
      return eos_handle->done;
    }

    buffer = (globus_byte_t*)globus_malloc(read_length);
    if(buffer == NULL) {
      result = GlobusGFSErrorGeneric("error: malloc failed");
      XrdPosix_Close(eos_handle->fd);
      eos_handle->cached_res = result;
      eos_handle->done = GLOBUS_TRUE;
      if (eos_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(eos_handle->op,
                                                eos_handle->cached_res);
      }
      return eos_handle->done;
    }

    nbread = XrdPosix_Read(eos_handle->fd, buffer, read_length);
    if (nbread == 0) { // eof
      result = GLOBUS_SUCCESS;
      globus_free(buffer);
      XrdPosix_Close(eos_handle->fd);
      eos_handle->cached_res = result;
      eos_handle->done = GLOBUS_TRUE;
      if (eos_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(eos_handle->op,
                                                eos_handle->cached_res);
      }
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,"%s: finished (eof)\n",func);
      return eos_handle->done;
    }
    if (nbread < 0) { // error
      result = globus_l_gfs_make_error("read", errno);
      globus_free(buffer);
      XrdPosix_Close(eos_handle->fd);
      eos_handle->cached_res = result;
      eos_handle->done = GLOBUS_TRUE;
      if (eos_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(eos_handle->op,
                                                eos_handle->cached_res);
      }
      globus_gfs_log_message(GLOBUS_GFS_LOG_ERR,"%s: finished (error)\n",func);
      return eos_handle->done;

    }

    if (read_length>=nbread) {
      // if we have a file with size less than block_size we do not
      // have use parrallel connections (one will be enough)
      eos_handle->optimal_count--;
    }
    read_length = nbread;

    if (eos_handle->blk_length != -1) {
      eos_handle->blk_length -= read_length;
    }

    // start offset?
    res = globus_gridftp_server_register_write(eos_handle->op,
                                               buffer,
                                               read_length,
                                               eos_handle->blk_offset,
                                               -1,
                                               globus_l_gfs_net_write_cb,
                                               eos_handle);
    eos_handle->blk_offset += read_length;
    if (res != GLOBUS_SUCCESS) {
      globus_free(buffer);
      XrdPosix_Close(eos_handle->fd);
      eos_handle->cached_res = res;
      eos_handle->done = GLOBUS_TRUE;
      if (eos_handle->outstanding == 0) {
        globus_gridftp_server_finished_transfer(eos_handle->op,
                                                eos_handle->cached_res);
      }
      return eos_handle->done;
    }

    eos_handle->outstanding++;
    return GLOBUS_FALSE;
  }


  static void
  globus_l_gfs_net_write_cb(globus_gfs_operation_t op,
                            globus_result_t result,
                            globus_byte_t *buffer,
                            globus_size_t,
                            void * user_arg) {
    globus_l_gfs_eos_handle_t *eos_handle;
    const char *func = "globus_l_gfs_net_write_cb";

    eos_handle = (globus_l_gfs_eos_handle_t *) user_arg;

    globus_free(buffer);
    globus_mutex_lock(&eos_handle->mutex);
    {
      eos_handle->outstanding--;
      if (result != GLOBUS_SUCCESS) {
        eos_handle->cached_res = result;
        eos_handle->done = GLOBUS_TRUE;
      }
      if (!eos_handle->done) {
        globus_l_gfs_eos_send_next_to_client(eos_handle);
      } else if (eos_handle->outstanding == 0) {
        XrdPosix_Close(eos_handle->fd);
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
                               "%s: finished transfer\n",
                               func);
        globus_gridftp_server_finished_transfer
          (op, eos_handle->cached_res);
      }
    }
    globus_mutex_unlock(&eos_handle->mutex);
  }


  static int globus_l_gfs_eos_activate(void);

  static int globus_l_gfs_eos_deactivate(void);

  /// no need to change this
  static globus_gfs_storage_iface_t globus_l_gfs_eos_dsi_iface =
    {
      GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER,
      globus_l_gfs_eos_start,
      globus_l_gfs_eos_destroy,
      NULL, /* list */
      globus_l_gfs_eos_send,
      globus_l_gfs_eos_recv,
      NULL, /* trev */
      NULL, /* active */
      NULL, /* passive */
      NULL, /* data destroy */
      globus_l_gfs_eos_command,
      globus_l_gfs_eos_stat,
      NULL,
      NULL
    };


  /// no need to change this
  GlobusExtensionDefineModule(globus_gridftp_server_eos) =
  {
    (char*)"globus_gridftp_server_eos",
    globus_l_gfs_eos_activate,
    globus_l_gfs_eos_deactivate,
    NULL,
    NULL,
    &local_version,
    NULL
  };

  /// no need to change this
  static int globus_l_gfs_eos_activate(void) {
    globus_extension_registry_add
      (GLOBUS_GFS_DSI_REGISTRY,
       (void*)"eos",
       GlobusExtensionMyModule(globus_gridftp_server_eos),
       &globus_l_gfs_eos_dsi_iface);
    return 0;
  }

  /// no need to change this
  static int globus_l_gfs_eos_deactivate(void) {
    globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, (void*)"eos");

    return 0;
  }

} // end extern "C"
