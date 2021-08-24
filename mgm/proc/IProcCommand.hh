//------------------------------------------------------------------------------
//! @file IProcCommand.hh
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "mgm/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "proto/ConsoleReply.pb.h"
#include "proto/ConsoleRequest.pb.h"
#include "XrdSfs/XrdSfsInterface.hh"
#include <future>
#include <sstream>

//! Forward declarations
class XrdOucErrInfo;

namespace Json
{
class Value;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class IProcCommand - interface that needs to be implemented by all types
//! of commands executed by the MGM.
//------------------------------------------------------------------------------
class IProcCommand: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IProcCommand():
    mHasSlot(false), mExecRequest(false), mReqProto(), mDoAsync(false),
    mForceKill(false), mVid(), mComment(), mRoutingInfo(),
    stdOut(""), stdErr(""), stdJson(""),
    retc(0), mTmpResp()
  {
    mTimestamp = time(NULL);
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client protobuf request
  //! @param vid client virtual identity
  //! @param async if true then use thread pool to execute the command
  //----------------------------------------------------------------------------
  IProcCommand(eos::console::RequestProto&& req,
               eos::common::VirtualIdentity& vid, bool async):
    IProcCommand()
  {
    mReqProto = req;
    mDoAsync = async;
    mVid = vid;
    mComment = req.comment().c_str();
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IProcCommand()
  {
    mForceKill.store(true);

    if (ofstdoutStream.is_open()) {
      ofstdoutStream.close();
    }

    unlink(ofstdoutStreamFilename.c_str());

    if (ofstderrStream.is_open()) {
      ofstderrStream.close();
    }

    unlink(ofstderrStreamFilename.c_str());

    if (mHasSlot) {
      std::unique_lock<std::mutex> lock(mMapCmdsMutex);
      --mCmdsExecuting[mReqProto.command_case()];
    }
  }

  //----------------------------------------------------------------------------
  //! Open a proc command e.g. call the appropriate user or admin command and
  //! store the output in a resultstream of in case of find in temporary output
  //! files.
  //!
  //! @param inpath path indicating user or admin command
  //! @param info CGI describing the proc command
  //! @param vid_in virtual identity of the user requesting a command
  //! @param error object to store errors
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  virtual int open(const char* path, const char* info,
                   eos::common::VirtualIdentity& vid,
                   XrdOucErrInfo* error);

  //----------------------------------------------------------------------------
  //! Read a part of the result stream created during open
  //!
  //! @param boff offset where to start
  //! @param buff buffer to store stream
  //! @param blen len to return
  //!
  //! @return number of bytes read
  //----------------------------------------------------------------------------
  virtual size_t read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen);

  //----------------------------------------------------------------------------
  //! Get the size of the result stream
  //!
  //! @param buf stat structure to fill
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  virtual int stat(struct stat* buf)
  {
    off_t size = 0;

    if (readStdOutStream) {
      ifstdoutStream.seekg(0, ifstdoutStream.end);
      size += ifstdoutStream.tellg();
      ifstdoutStream.seekg(0, ifstdoutStream.beg);
      ifstderrStream.seekg(0, ifstderrStream.end);
      size += ifstderrStream.tellg();
      ifstderrStream.seekg(0, ifstderrStream.beg);
      iretcStream.seekg(0, iretcStream.end);
      size += iretcStream.tellg();
      iretcStream.seekg(0, iretcStream.beg);
    } else {
      size = mTmpResp.length();
    }

    memset(buf, 0, sizeof(struct stat));
    buf->st_size = size;
    return SFS_OK;
  }

  //----------------------------------------------------------------------------
  //! Close the proc stream
  //!
  //! @return 0 if comment has been successfully stored otherwise != 0
  //----------------------------------------------------------------------------
  virtual int close()
  {
    if (ifstdoutStream.is_open()) {
      ifstdoutStream.close();
    }

    if (ifstderrStream.is_open()) {
      ifstderrStream.close();
    }

    return SFS_OK;
  }

  virtual std::string GetCmd(const char* cgi = 0)
  {
    return "proto";
  }

  //----------------------------------------------------------------------------
  //! Method implementing the specific behavior of the command executed
  //----------------------------------------------------------------------------
  virtual eos::console::ReplyProto ProcessRequest() noexcept = 0;

  //----------------------------------------------------------------------------
  //! Launch command asynchronously, creating the corresponding promise and
  //! future
  //----------------------------------------------------------------------------
  virtual void LaunchJob() final;

  //----------------------------------------------------------------------------
  //! Check if we can safely delete the current object as there is no async
  //! thread executing the ProcessResponse method
  //!
  //! @return true if deletion if safe, otherwise false
  //----------------------------------------------------------------------------
  virtual bool KillJob() final;

  //----------------------------------------------------------------------------
  //! Return the result
  //----------------------------------------------------------------------------
  virtual const char* GetResult(size_t& size) const
  {
    return "bla";
  }

  virtual void SetError(XrdOucErrInfo* error) {};


  // static public functions of id to path converter
  static int GetPathFromFid(std::string& path, unsigned long long fid,
			    std::string& err_msg, bool takeLock = true);

  static int GetPathFromCid(std::string& path, unsigned long long cid,
			    std::string& err_msg, bool takeLock = true);


protected:
  virtual bool OpenTemporaryOutputFiles();
  virtual bool CloseTemporaryOutputFiles();

  //----------------------------------------------------------------------------
  //! Retrieve the file's full path given its numeric id.
  //! This method executes under the NamespaceView lock.
  //!
  //! @param path full path of the file
  //! @param fid file numeric id
  //! @param err_msg_prefix error message to be displayed in case of exception
  //! @return retc return code
  //----------------------------------------------------------------------------
  void GetPathFromFid(XrdOucString& path, unsigned long long fid,
                      const std::string&
                      err_msg_prefix); // drop when we drop non-proto commands using it

  //----------------------------------------------------------------------------
  //! Retrieve the container's full path given its numeric id.
  //! This method executes under the NamespaceView lock.
  //!
  //! @param path full path of the container
  //! @param cid container numeric id
  //! @param err_msg_prefix error message to be displayed in case of exception
  //----------------------------------------------------------------------------
  void GetPathFromCid(XrdOucString& path, unsigned long long cid,
                      const std::string&
                      err_msg_prefix); // drop when we drop non-proto commands using it

  //----------------------------------------------------------------------------
  //! Format console output string as json.
  //!
  //! @note
  //! This will work only if the given output follows <key>=<value> format.
  //! Also, provided values must follow a proper JSON hierarchy !
  //!
  //! Although the function tries to correct some values,
  //! the correction is not exhaustive.
  //!
  //! Valid example:
  //! stat.drain.status=<value> / stat.drain.otherkey=<value>
  //!
  //! Invalid example:
  //! stat.drain=<value> / stat.drain.status=<value> (will throw an exception)
  //!
  //! @param stdOut console output string
  //!
  //! @return jsonOut json formatted output
  //----------------------------------------------------------------------------
  static Json::Value ConvertOutputToJsonFormat(const std::string& stdOut);

  //----------------------------------------------------------------------------
  //! Create a JSON string from the command output, error and return code.
  //!
  //! @param out console output string
  //! @param err console error string
  //! @param retc console return code
  //!
  //! @return jsonOut json response string containing output, error
  //!                 and return code
  //----------------------------------------------------------------------------
  std::string ResponseToJsonString(const std::string& out,
                                   const std::string& err = "",
                                   int rc = 0);

  //----------------------------------------------------------------------------
  //! Indicate whether output should be in JSON format
  //----------------------------------------------------------------------------
  inline bool WantsJsonOutput()
  {
    return mReqProto.format() == eos::console::RequestProto::JSON;
  }

  //----------------------------------------------------------------------------
  //! Check if operation forbidden
  //!
  //! @param path path of the request
  //! @param vid client virtual identity
  //! @param err_check output error message
  //! @param errno_check output errno in case of errors
  //!
  //! @return true if operation forbidden, false otherwise
  //----------------------------------------------------------------------------
  bool IsOperationForbidden(const std::string& path,
                            const eos::common::VirtualIdentity& vid,
                            std::string& err_check, int& errno_check) const;

  //----------------------------------------------------------------------------
  //! Check if a routing redirect should happen.
  //!
  //! @note
  //! In case routing is needed, fills the routing info object
  //! and sets the reply return code to SFS_REDIRECT.
  //!
  //! @param path path to route
  //! @param reply the reply proto object
  //!
  //! @return true if should route, false otherwise
  //----------------------------------------------------------------------------
  bool ShouldRoute(const std::string& path,
                   eos::console::ReplyProto& reply);

  //----------------------------------------------------------------------------
  //! Check if there is still an available slot for the current type of command
  //! in the queue served by the thread pool
  //!
  //! @return true if command can be queued, otherwise false
  //----------------------------------------------------------------------------
  bool HasSlot();

  //----------------------------------------------------------------------------
  //! Store routing information
  //----------------------------------------------------------------------------
  struct RoutingInfo {
    std::string path;
    std::string host;
    int port;
    int stall_timeout;
  };

  static std::atomic_uint_least64_t uuid;
  static std::mutex mMapCmdsMutex; ///< Mutex protecting the cmds map
  //! Map of command types to number of commands actually queued
  static std::map<eos::console::RequestProto::CommandCase, uint64_t>
  mCmdsExecuting;

  //! Indicate if current command has taken a slot in the queue
  std::atomic<bool> mHasSlot;
  bool mExecRequest; ///< Indicate if request is launched asynchronously
  eos::console::RequestProto mReqProto; ///< Client request protobuf object
  std::future<eos::console::ReplyProto> mFuture; ///< Response future
  bool mDoAsync; ///< If true use thread pool to do the work
  std::atomic<bool> mForceKill; ///< Flag to notify worker thread
  eos::common::VirtualIdentity mVid; ///< Copy of original vid
  time_t mTimestamp; ///< Timestamp of the proc command
  XrdOucString mComment; ///< Comment issued by the user for the proc command
  RoutingInfo mRoutingInfo; ///< Routing information of the proc command
  XrdOucString stdOut; ///< stdOut returned by proc command
  XrdOucString stdErr; ///< stdErr returned by proc command
  XrdOucString stdJson; ///< JSON output returned by proc command
  int retc; ///< Return code from the proc command
  std::string mTmpResp; ///< String used for streaming the response
  std::ofstream ofstdoutStream;
  std::ofstream ofstderrStream;
  std::string ofstdoutStreamFilename;
  std::string ofstderrStreamFilename;
  std::ifstream ifstdoutStream;
  std::ifstream ifstderrStream;
  std::istringstream iretcStream;
  bool readStdOutStream {false};
  bool readStdErrStream {false};
  bool readRetcStream {false};
};

EOSMGMNAMESPACE_END
