// ----------------------------------------------------------------------
// File: XrdVstOfs.hh
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

#ifndef __XRDVSTOFS_FSTOFS_HH__
#define __XRDVSTOFS_FSTOFS_HH__

/*----------------------------------------------------------------------------*/
#include "vst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
// TODO: XRootd 4.0 #include "XrdOuc/XrdOucIOVec.hh"
/*----------------------------------------------------------------------------*/


EOSVSTNAMESPACE_BEGIN

class XrdVstOfs : public XrdSfsFileSystem {
public:

  //-----------------------------------------------------------------------------
  //! Obtain a new director object to be used for future directory requests.
  //!
  //! @param  user   - Text identifying the client responsible for this call.
  //!                  The pointer may be null if identification is missing.
  //! @param  MonID  - The monitoring identifier assigned to this and all
  //!                  future requests using the returned object.
  //!
  //! @return pointer- Pointer to an XrdSfsDirectory object.
  //! @return nil    - Insufficient memory to allocate an object.
  //-----------------------------------------------------------------------------

  virtual XrdSfsDirectory *newDir (char *user = 0, int MonID = 0);

  //-----------------------------------------------------------------------------
  //! Obtain a new file object to be used for a future file requests.
  //!
  //! @param  user   - Text identifying the client responsible for this call.
  //!                  The pointer may be null if identification is missing.
  //! @param  MonID  - The monitoring identifier assigned to this and all
  //!                  future requests using the returned object.
  //!
  //! @return pointer- Pointer to an XrdSfsFile object.
  //! @return nil    - Insufficient memory to allocate an object.
  //-----------------------------------------------------------------------------

  virtual XrdSfsFile *
  newFile (char *user = 0, int MonID = 0);

  //-----------------------------------------------------------------------------
  //! Obtain checksum information for a file.
  //!
  //! @param  Func   - The checksum operation to be performed:
  //!                  csCalc  - (re)calculate and return the checksum value
  //!                  csGet   - return the existing checksum value, if any
  //!                  csSize  - return the size of the checksum value that
  //!                            corresponds to csName (path may be null).
  //! @param  csName - The name of the checksum value wanted.
  //! @param  path   - Pointer to the path of the file in question.
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, or SFS_REDIRECT. When SFS_OK is returned,
  //!         eInfo should contain results, as follows:
  //!         csCalc/csGet eInfo.message - null terminated string with the
  //!                                      checksum value in ASCII hex.
  //!         csSize       eInfo.code    - size of binary checksum value.
  //-----------------------------------------------------------------------------

  enum csFunc {
    csCalc = 0, csGet, csSize
  };

  virtual int
  chksum (csFunc Func,
          const char *csName,
          const char *path,
          XrdOucErrInfo &eInfo,
          const XrdSecEntity *client = 0,
          const char *opaque = 0) {
    eInfo.setErrInfo(ENOTSUP, "Not supported.");
    return SFS_ERROR;
  }

  //-----------------------------------------------------------------------------
  //! Change file mode settings.
  //!
  //! @param  path   - Pointer to the path of the file in question.
  //! @param  mode   - The new file mode setting.
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int chmod (const char *path,
                     XrdSfsMode mode,
                     XrdOucErrInfo &eInfo,
                     const XrdSecEntity *client = 0,
                     const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Perform a filesystem control operation (version 1)
  //!
  //! @param  cmd    - The operation to be performed:
  //!                  SFS_FSCTL_LOCATE  Locate a file or file servers
  //!                  SFS_FSCTL_STATFS  Return physical filesystem information
  //!                  SFS_FSCTL_STATLS  Return logical  filesystem information
  //!                  SFS_FSCTL_STATXA  Return extended attributes
  //! @param  args   - Arguments specific to cmd.
  //!                  SFS_FSCTL_LOCATE  args points to the path to be located
  //!                                    ""   path is the first exported path
  //!                                    "*"  return all current servers
  //!                                    "*/" return servers exporting path
  //!                                    o/w  return servers having the path
  //!                  SFS_FSCTL_STATFS  Path in the filesystem in question.
  //!                  SFS_FSCTL_STATLS  Path in the filesystem in question.
  //!                  SFS_FSCTL_STATXA  Path of the file whose xattr is wanted.
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //!
  //! @return SFS_OK   a null response is sent.
  //! @return SFS_DATA error.code    length of the data to be sent.
  //!                  error.message contains the data to be sent.
  //! @return SFS_STARTED Operation started result will be returned via callback.
  //!                  Valid only for for SFS_FSCTL_LOCATE, SFS_FSCTL_STATFS, and
  //!                  SFS_FSCTL_STATXA
  //!         o/w      one of SFS_ERROR, SFS_REDIRECT, or SFS_STALL.
  //-----------------------------------------------------------------------------

  virtual int
  FSctl (const int cmd,
         XrdSfsFSctl &args,
         XrdOucErrInfo &eInfo,
         const XrdSecEntity *client = 0) {
    return SFS_OK;
  }

  //-----------------------------------------------------------------------------
  //! Perform a filesystem control operation (version 2)
  //!
  //! @param  cmd    - The operation to be performed:
  //!                  SFS_FSCTL_PLUGIN  Return Implementation Dependent Data v1
  //!                  SFS_FSCTL_PLUGIO  Return Implementation Dependent Data v2
  //! @param  args   - Arguments specific to cmd.
  //!                  SFS_FSCTL_PLUGIN  path and opaque information.
  //!                  SFS_FSCTL_PLUGIO  Unscreened argument string.
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //!
  //! @return SFS_OK   a null response is sent.
  //!         SFS_DATA error.code    length of the data to be sent.
  //!                  error.message contains the data to be sent.
  //!         o/w      one of SFS_ERROR, SFS_REDIRECT, or SFS_STALL.
  //-----------------------------------------------------------------------------

  virtual int fsctl (const int cmd,
                     const char *args,
                     XrdOucErrInfo &eInfo,
                     const XrdSecEntity *client = 0);

  //-----------------------------------------------------------------------------
  //! Return statistical information.
  //!
  //! @param  buff   - Pointer to the buffer where results are to be returned.
  //!                  Statistics should be in standard XML format. If buff is
  //!                  nil then only maximum size information is wanted.
  //! @param  blen   - The length available in buff.
  //!
  //! @return Number of bytes placed in buff. When buff is nil, the maximum
  //!         number of bytes that could have been placed in buff.
  //-----------------------------------------------------------------------------

  virtual int getStats (char *buff, int blen);

  //-----------------------------------------------------------------------------
  //! Get version string.
  //!
  //! @return The version string. Normally this is the XrdVERSION value.
  //-----------------------------------------------------------------------------

  virtual const char *getVersion ();

  //-----------------------------------------------------------------------------
  //! Return directory/file existence information (short stat).
  //!
  //! @param  path   - Pointer to the path of the file/directory in question.
  //! @param  eFlag  - Where the results are to be returned.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //!         When SFS_OK is returned, eFlag must be properly set, as follows:
  //!         XrdSfsFileExistNo            - path does not exist
  //!         XrdSfsFileExistIsFile        - path refers to an  online file
  //!         XrdSfsFileExistIsDirectory   - path refers to an  online directory
  //!         XrdSfsFileExistIsOffline     - path refers to an offline file
  //!         XrdSfsFileExistIsOther       - path is neither a file nor directory
  //-----------------------------------------------------------------------------

  virtual int exists (const char *path,
                      XrdSfsFileExistence &eFlag,
                      XrdOucErrInfo &eInfo,
                      const XrdSecEntity *client = 0,
                      const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Create a directory.
  //!
  //! @param  path   - Pointer to the path of the directory to be created.
  //! @param  mode   - The directory mode setting.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int mkdir (const char *path,
                     XrdSfsMode mode,
                     XrdOucErrInfo &eInfo,
                     const XrdSecEntity *client = 0,
                     const char *opaque = 0); 

  //-----------------------------------------------------------------------------
  //! Preapre a file for future processing.
  //!
  //! @param  pargs  - The preapre arguments.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int prepare (XrdSfsPrep &pargs,
                       XrdOucErrInfo &eInfo,
                       const XrdSecEntity *client = 0);

  //-----------------------------------------------------------------------------
  //! Remove a file.
  //!
  //! @param  path   - Pointer to the path of the file to be removed.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int rem (const char *path,
                   XrdOucErrInfo &eInfo,
                   const XrdSecEntity *client = 0,
                   const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Remove a directory.
  //!
  //! @param  path   - Pointer to the path of the directory to be removed.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int remdir (const char *path,
                      XrdOucErrInfo &eInfo,
                      const XrdSecEntity *client = 0,
                      const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Rename a file or directory.
  //!
  //! @param  oPath   - Pointer to the path to be renamed.
  //! @param  nPath   - Pointer to the path oPath is to have.
  //! @param  eInfo   - The object where error info is to be returned.
  //! @param  client  - Client's identify (see common description).
  //! @param  opaqueO - oPath's CGI information (see common description).
  //! @param  opaqueN - nPath's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int rename (const char *oPath,
                      const char *nPath,
                      XrdOucErrInfo &eInfo,
                      const XrdSecEntity *client = 0,
                      const char *opaqueO = 0,
                      const char *opaqueN = 0);

  //-----------------------------------------------------------------------------
  //! Return state information on a file or directory.
  //!
  //! @param  path   - Pointer to the path in question.
  //! @param  buf    - Pointer to the structure where info it to be returned.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //!         When SFS_OK is returned, buf must contain stat information.
  //-----------------------------------------------------------------------------

  virtual int stat (const char *Name,
                    struct stat *buf,
                    XrdOucErrInfo &eInfo,
                    const XrdSecEntity *client = 0,
                    const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Return mode information on a file or directory.
  //!
  //! @param  path   - Pointer to the path in question.
  //! @param  mode   - Where full mode information is to be returned.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //!         When SFS_OK is returned, mode must contain mode information. If
  //!         teh mode is -1 then it is taken as an offline file.
  //-----------------------------------------------------------------------------

  virtual int stat (const char *path,
                    mode_t &mode,
                    XrdOucErrInfo &eInfo,
                    const XrdSecEntity *client = 0,
                    const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Truncate a file.
  //!
  //! @param  path   - Pointer to the path of the file to be truncated.
  //! @param  fsize  - The size that the file is to have.
  //! @param  eInfo  - The object where error info is to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int truncate (const char *path,
                        XrdSfsFileOffset fsize,
                        XrdOucErrInfo &eInfo,
                        const XrdSecEntity *client = 0,
                        const char *opaque = 0);
  //-----------------------------------------------------------------------------
  //! Constructor and Destructor
  //-----------------------------------------------------------------------------

  XrdVstOfs () { }

  virtual
  ~XrdVstOfs () { }
};

/******************************************************************************/
/*              F i l e   S y s t e m   I n s t a n t i a t o r               */
/******************************************************************************/

//-----------------------------------------------------------------------------
/*! When building a shared library plugin, the following "C" entry point must
    exist in the library:

    @param  nativeFS - the filesystem that would have been used. You may return
                       this pointer if you wish.
    @param  Logger   - The message logging object to be used for messages.
    @param  configFN - pointer to the path of the configuration file. If nil
                       there is no configuration file.

    @return Pointer to the file system object to be used or nil if an error
            occurred.

   extern "C"
         {XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *nativeFS,
                                                XrdSysLogger     *Logger,
                                                const char       *configFn);
         }
 */
//-----------------------------------------------------------------------------

//------------------------------------------------------------------------------
/*! Specify the compilation version.

    Additionally, you *should* declare the xrootd version you used to compile
    your plug-in. The plugin manager automatically checks for compatability.
    Declare it as follows:

    #include "XrdVersion.hh"
    XrdVERSIONINFO(XrdSfsGetFileSystem,<name>);

    where <name> is a 1- to 15-character unquoted name identifying your plugin.
 */
//------------------------------------------------------------------------------

/******************************************************************************/
/*                            X r d S f s F i l e                             */
/******************************************************************************/

//------------------------------------------------------------------------------
//! The XrdSfsFile object is returned by XrdSfsFileSystem::newFile() when
//! the caller wants to be able to perform file oriented operations.
//------------------------------------------------------------------------------

class XrdSfsAio;
class XrdSfsDio;
class XrdSfsXio;

class XrdVstOfsFile : public XrdSfsFile {
public:

  //-----------------------------------------------------------------------------
  //! The error object is used to return details whenever something other than
  //! SFS_OK is returned from the methods in this class, when noted.
  //-----------------------------------------------------------------------------

  XrdOucErrInfo error;

  //-----------------------------------------------------------------------------
  //! Open a file.
  //!
  //! @param  path   - Pointer to the path of the file to be opened.
  //! @param  oMode  - Flags indicating how the open is to be handled.
  //!                  SFS_O_CREAT   create the file
  //!                  SFS_O_MKPTH   Make directory path if missing
  //!                  SFS_O_NOWAIT  do not impose operational delays
  //!                  SFS_O_POSC    persist only on successful close
  //!                  SFS_O_RAWIO   allow client-side decompression
  //!                  SFS_O_RDONLY  open read/only
  //!                  SFS_O_RDWR    open read/write
  //!                  SFS_O_REPLICA Open for replication
  //!                  SFS_O_RESET   Reset any cached information
  //!                  SFS_O_TRUNC   truncate existing file to zero length
  //!                  SFS_O_WRONLY  open write/only
  //! @param  cMode  - The file's mode if it will be created.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //-----------------------------------------------------------------------------

  virtual int open (const char *fileName,
                    XrdSfsFileOpenMode openMode,
                    mode_t createMode,
                    const XrdSecEntity *client = 0,
                    const char *opaque = 0) ;

  //-----------------------------------------------------------------------------
  //! Close the file.
  //!
  //! @return One of SFS_OK or SFS_ERROR.
  //-----------------------------------------------------------------------------

  virtual int close ();

  //-----------------------------------------------------------------------------
  //! Execute a special operation on the file (version 1)
  //!
  //! @param  cmd   - The operation to be performed (see below).
  //!                 SFS_FCTL_GETFD    Return file descriptor if possible
  //!                 SFS_FCTL_STATV    Reserved for future use.
  //! @param  args  - specific arguments to cmd
  //!                 SFS_FCTL_GETFD    Set to zero.
  //!
  //! @return If an error occurs or the operation is not support, SFS_ERROR
  //!         should be returned with error.code set to errno. Otherwise,
  //!         SFS_FCTL_GETFD  error.code holds the real file descriptor number
  //!                         If the value is negative, sendfile() is not used.
  //!                         If the value is SFS_SFIO_FDVAL then the SendData()
  //!                         method is used for future read requests.
  //-----------------------------------------------------------------------------

  virtual int fctl (const int cmd,
                    const char *args,
                    XrdOucErrInfo &eInfo);

  //-----------------------------------------------------------------------------
  //! Execute a special operation on the file (version 2)
  //!
  //! @param  cmd    - The operation to be performed:
  //!                  SFS_FCTL_SPEC1    Perform implementation defined action
  //! @param  alen   - Length of data pointed to by args.
  //! @param  args   - Data sent with request, zero if alen is zero.
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //!
  //! @return SFS_OK   a null response is sent.
  //! @return SFS_DATA error.code    length of the data to be sent.
  //!                  error.message contains the data to be sent.
  //!         o/w      one of SFS_ERROR, SFS_REDIRECT, or SFS_STALL.
  //-----------------------------------------------------------------------------

  virtual int
  fctl (const int cmd,
        int alen,
        const char *args,
        XrdOucErrInfo &eInfo,
        const XrdSecEntity *client = 0) {
    return SFS_OK;
  }

  //-----------------------------------------------------------------------------
  //! Get the file path.
  //!
  //! @return Null terminated string of the path used in open().
  //-----------------------------------------------------------------------------

  virtual const char *FName ();


  //-----------------------------------------------------------------------------
  //! Get file's memory mapping if one exists (memory mapped files only).
  //!
  //! @param  addr   - Place where the starting memory address is returned.
  //! @param  size   - Place where the file's size is returned.
  //!
  //! @return SFS_OK when the file is memory mapped or any other code otherwise.
  //-----------------------------------------------------------------------------

  virtual int getMmap (void **Addr, off_t &Size);

  //-----------------------------------------------------------------------------
  //! Preread file blocks into the file system cache.
  //!
  //! @param  offset  - The offset where the read is to start.
  //! @param  size    - The number of bytes to pre-read.
  //!
  //! @return >= 0      The number of bytes that will be pre-read.
  //! @return SFS_ERROR File could not be preread, error holds the reason.
  //-----------------------------------------------------------------------------

  virtual XrdSfsXferSize read (XrdSfsFileOffset offset,
                               XrdSfsXferSize size);

  //-----------------------------------------------------------------------------
  //! Read file bytes into a buffer.
  //!
  //! @param  offset  - The offset where the read is to start.
  //! @param  buffer  - pointer to buffer where the bytes are to be placed.
  //! @param  size    - The number of bytes to read.
  //!
  //! @return >= 0      The number of bytes that placed in buffer.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //-----------------------------------------------------------------------------

  virtual XrdSfsXferSize read (XrdSfsFileOffset offset,
                               char *buffer,
                               XrdSfsXferSize size);

  //-----------------------------------------------------------------------------
  //! Read file bytes using asynchronous I/O.
  //!
  //! @param  aioparm - Pointer to async I/O object controlling the I/O.
  //!
  //! @return SFS_OK    Request accepted and will be scheduled.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //-----------------------------------------------------------------------------

  virtual XrdSfsXferSize read (XrdSfsAio *aioparm) {return 0;}

  //-----------------------------------------------------------------------------
  //! Given an array of read requests (size rdvCnt), read them from the file
  //! and place the contents consecutively in the provided buffer. A dumb default
  //! implementation is supplied but should be replaced to increase performance.
  //!
  //! @param  readV     pointer to the array of read requests.
  //! @param  rdvcnt    the number of elements in readV.
  //!
  //! @return >=0       The numbe of bytes placed into the buffer.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //-----------------------------------------------------------------------------

  // TODO: XRootD 4.0
  /*virtual XrdSfsXferSize
  readv (XrdOucIOVec *readV,
         int rdvCnt) {
    XrdSfsXferSize rdsz, totbytes = 0;
    for (int i = 0; i < rdvCnt; i++) {
      rdsz = read(readV[i].offset,
              readV[i].data, readV[i].size);
      if (rdsz != readV[i].size) {
        if (rdsz < 0) return rdsz;
        error.setErrInfo(ESPIPE, "read past eof");
        return SFS_ERROR;
      }
      totbytes += rdsz;
    }
    return totbytes;
  }
  */
  //-----------------------------------------------------------------------------
  //! Send file bytes via a XrdSfsDio sendfile object to a client (optional).
  //!
  //! @param  sfDio   - Pointer to the sendfile object for data transfer.
  //! @param  offset  - The offset where the read is to start.
  //! @param  size    - The number of bytes to read and send.
  //!
  //! @return SFS_ERROR File not read, error object has reason.
  //! @return SFS_OK    Either data has been successfully sent via sfDio or no
  //!                   data has been sent and a normal read() should be issued.
  //-----------------------------------------------------------------------------

  virtual int
  SendData (XrdSfsDio *sfDio,
            XrdSfsFileOffset offset,
            XrdSfsXferSize size) {
    return SFS_OK;
  }

  //-----------------------------------------------------------------------------
  //! Write file bytes from a buffer.
  //!
  //! @param  offset  - The offset where the write is to start.
  //! @param  buffer  - pointer to buffer where the bytes reside.
  //! @param  size    - The number of bytes to write.
  //!
  //! @return >= 0      The number of bytes that were written.
  //! @return SFS_ERROR File could not be written, error holds the reason.
  //-----------------------------------------------------------------------------

  virtual XrdSfsXferSize write (XrdSfsFileOffset offset,
                                const char *buffer,
                                XrdSfsXferSize size);

  //-----------------------------------------------------------------------------
  //! Write file bytes using asynchrnous I/O.
  //!
  //! @param  aioparm - Pointer to async I/O object controlling the I/O.
  //!
  //! @return  0       Request accepted and will be scheduled.
  //! @return !0       Request not accepted, returned value is errno.
  //-----------------------------------------------------------------------------

  virtual int write (XrdSfsAio *aioparm) {return 0;}

  //-----------------------------------------------------------------------------
  //! Given an array of write requests (size wdvcnt), write them to the file
  //! from the provided associated buffer. A dumb default implementation is
  //! supplied but should be replaced to increase performance.
  //!
  //! @param  writeV    pointer to the array of write requests.
  //! @param  wdvcnt    the number of elements in writeV.
  //!
  //! @return >=0       The total number of bytes written to the file.
  //! @return SFS_ERROR File could not be written, error holds the reason.
  //-----------------------------------------------------------------------------

  // TODO: XRootD 4.0
  /*
  virtual XrdSfsXferSize
  writev (XrdOucIOVec *writeV,
          int wdvCnt) {
    XrdSfsXferSize wrsz, totbytes = 0;
    for (int i = 0; i < wdvCnt; i++) {
      wrsz = write(writeV[i].offset,
               writeV[i].data, writeV[i].size);
      if (wrsz != writeV[i].size) {
        if (wrsz < 0) return wrsz;
        error.setErrInfo(ESPIPE, "write past eof");
        return SFS_ERROR;
      }
      totbytes += wrsz;
    }
    return totbytes;
  }
  */
  
  //-----------------------------------------------------------------------------
  //! Return state information on the file.
  //!
  //! @param  buf    - Pointer to the structure where info it to be returned.
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL. When SFS_OK
  //!         is returned, buf must hold stat information.
  //-----------------------------------------------------------------------------

  virtual int stat (struct stat *buf) ;

  //-----------------------------------------------------------------------------
  //! Make sure all outstanding data is actually written to the file (sync).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //-----------------------------------------------------------------------------

  virtual int sync ();

  //-----------------------------------------------------------------------------
  //! Make sure all outstanding data is actually written to the file (async).
  //!
  //! @return SFS_OK    Request accepted and will be scheduled.
  //! @return SFS_ERROR Request could not be accepted, return error has reason.
  //-----------------------------------------------------------------------------

  virtual int sync (XrdSfsAio *aiop) { return 0;}

  //-----------------------------------------------------------------------------
  //! Truncate the file.
  //!
  //! @param  fsize  - The size that the file is to have.
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int truncate (XrdSfsFileOffset fsize);

  //-----------------------------------------------------------------------------
  //! Get compression information for the file.
  //!
  //! @param  cxtype - Place where the compression algorithm name is to be placed
  //! @param  cxrsz  - Place where the compression page size is to be returned
  //!
  //! @return One of the valid SFS return codes described above. If the file
  //!         is not compressed or an error is returned, cxrsz must be set to 0.
  //-----------------------------------------------------------------------------

  virtual int getCXinfo (char cxtype[4], int &cxrsz);

  //-----------------------------------------------------------------------------
  //! Enable exchange buffer I/O for write calls.
  //!
  //! @param  - Pointer to the XrdSfsXio object to be used for buffer exchanges.
  //-----------------------------------------------------------------------------

  virtual void
  setXio (XrdSfsXio *xioP) { }

  //-----------------------------------------------------------------------------
  //! Constructor (user and MonID are the ones passed to newFile()!)
  //!
  //! @param  user   - Text identifying the client responsible for this call.
  //!                  The pointer may be null if identification is missing.
  //! @param  MonID  - The monitoring identifier assigned to this and all
  //!                  future requests using the returned object.
  //-----------------------------------------------------------------------------

  XrdVstOfsFile (const char *user = 0, int MonID = 0)
  : error (user, MonID) { }

  //-----------------------------------------------------------------------------
  //! Destructor
  //-----------------------------------------------------------------------------

  virtual
  ~XrdVstOfsFile () { }

}; // class XrdVstOfsFile

/******************************************************************************/
/*                       X r d S f s D i r e c t o r y                        */
/******************************************************************************/

//------------------------------------------------------------------------------
//! The XrdSfsDirectory object is returned by XrdSfsFileSystem::newFile() when
//! the caller wants to be able to perform directory oriented operations.
//------------------------------------------------------------------------------

class XrdVstOfsDirectory : public XrdSfsDirectory {
public:

  //-----------------------------------------------------------------------------
  //! The error object is used to return details whenever something other than
  //! SFS_OK is returned from the methods in this class, when noted.
  //-----------------------------------------------------------------------------

  XrdOucErrInfo error;

  //-----------------------------------------------------------------------------
  //! Open a directory.
  //!
  //! @param  path   - Pointer to the path of the directory to be opened.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, ir SFS_STALL
  //-----------------------------------------------------------------------------

  virtual int open (const char *path,
                    const XrdSecEntity *client = 0,
                    const char *opaque = 0);

  //-----------------------------------------------------------------------------
  //! Get the next directory entry.
  //!
  //! @return A null terminated string with the directory name. Normally, "."
  //!         ".." are not returned. If a null pointer is returned then if this
  //!         is due to an error, error.code should contain errno. Otherwise,
  //!         error.code should contain zero to indicate that no more entries
  //!         exist (i.e. end of list).
  //-----------------------------------------------------------------------------

  virtual const char *nextEntry ();

  //-----------------------------------------------------------------------------
  //! Close the file.
  //!
  //! @return One of SFS_OK or SFS_ERROR
  //-----------------------------------------------------------------------------

  virtual int close ();

  //-----------------------------------------------------------------------------
  //! Get the directory path.
  //!
  //! @return Null terminated string of the path used in open().
  //-----------------------------------------------------------------------------

  virtual const char *FName ();

  //-----------------------------------------------------------------------------
  //! Set the stat() buffer where stat information is to be placed corresponding
  //! to the directory entry returned by nextEntry().
  //!
  //! @return If supported, SFS_OK should be returned. If not supported, then
  //!         SFS_ERROR should be returned with error.code set to ENOTSUP.
  //-----------------------------------------------------------------------------

  virtual int
  autoStat (struct stat *buf) {
    error.setErrInfo(ENOTSUP, "Not supported.");
    return SFS_ERROR;
  }

  //-----------------------------------------------------------------------------
  //! Constructor (user and MonID are the ones passed to newDir()!)
  //!
  //! @param  user   - Text identifying the client responsible for this call.
  //!                  The pointer may be null if identification is missing.
  //! @param  MonID  - The monitoring identifier assigned to this and all
  //!                  future requests using the returned object.
  //-----------------------------------------------------------------------------

  XrdVstOfsDirectory (const char *user = 0, int MonID = 0)
  : error (user, MonID) { }

  //-----------------------------------------------------------------------------
  //! Destructor
  //-----------------------------------------------------------------------------

  virtual
  ~XrdVstOfsDirectory () { }

}; // class XrdVstOfsDirectory
EOSVSTNAMESPACE_END

#endif
