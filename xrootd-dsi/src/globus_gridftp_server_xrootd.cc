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

//------------------------------------------------------------------------------
//! @file globus_gridftp_server_xrootd.cc
//! @author Geoffray Adde - CERN
//! @brief Implementation of a GridFTP DSI plugin for XRootD with optional EOS features
//------------------------------------------------------------------------------
#if defined(linux)
#define _LARGE_FILES
#endif

#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <iostream>
#include <set>

/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdSys/XrdSysPthread.hh"

#include "XrdUtils.hh"
#include "ChunkHandler.hh"
#include "AsyncMetaHandler.hh"
#include "XrdFileIo.hh"
#include "dsi_xrootd.hh"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <sys/time.h>
#include <stdarg.h>
#include <algorithm>

class MyTimer
{
public:
  MyTimer()
  {
    file = fopen("/tmp/MyTimer.txt", "w+");
    pthread_mutex_init(&mutex, NULL);
  }

  ~MyTimer()
  {
    fclose(file);
    pthread_mutex_destroy(&mutex);
  }

  void
  PrintAndFlush(const char *format, ...)
  {
    va_list argptr;
    va_start(argptr, format);
    Print(format, argptr);
    va_end(argptr);
    fflush(file);
  }

  void
  Print(const char *format, ...)
  {
    timeval t1;
    gettimeofday(&t1, NULL);
    pthread_mutex_lock(&mutex);
    fprintf(file, "%10.10d.%6.6d\t", (int) t1.tv_sec, (int) t1.tv_usec);

    va_list argptr;
    va_start(argptr, format);
    vfprintf(file, format, argptr);
    va_end(argptr);
    fflush(file);
    pthread_mutex_unlock(&mutex);
  }
protected:
  FILE* file;
  pthread_mutex_t mutex;
};

//MyTimer TimeLog;

/**
 * Class for handling async responses when DSI receives data.
 * In that case, globus reads from network and XRootD writes the data to a file.
 */
class DsiRcvResponseHandler : public AsyncMetaHandler
{
public:
  /**
   * \brief Constructor
   *
   * @param XRootD handle
   */
  DsiRcvResponseHandler(globus_l_gfs_xrood_handle_s *handle) :
      AsyncMetaHandler(), mNumRegRead(0), mNumCbRead(0), mNumRegWrite(0), mNumCbWrite(0), mHandle(handle), mAllBufferMet(false), mOver(false), mNumExpectedBuffers(
          -1), clean_tid(0)
  {
    globus_mutex_init(&mOverMutex, NULL);
    globus_cond_init(&mOverCond, NULL);
  }

  /**
   * \brief Destructor
   */
  virtual
  ~DsiRcvResponseHandler()
  {
    globus_mutex_destroy(&mOverMutex);
    globus_cond_destroy(&mOverCond);
  }

  /**
   * \brief Register the buffer associated to a given file chunk
   *
   * @param offset Offset of the file chunk
   * @param length Length of the file chunk
   * @param buffer Buffer
   */
  void
  RegisterBuffer(uint64_t offset, uint64_t length, globus_byte_t* buffer)
  {
    mBufferMap[std::pair<uint64_t, uint32_t>(offset, (uint32_t) length)] = buffer;
  }

  /**
   * \brief Disable the buffer
   *
   * The function disables the buffer.
   * When all the buffer are disable, that shows that the activity is over for the current copy.
   *
   * @param buffer Buffer to disable
   */
  void
  DisableBuffer(globus_byte_t* buffer)
  {
    mActiveBufferSet.erase(buffer);
    if (!mAllBufferMet)
    {
      // check the expected number of buffers
      mMetBufferSet.insert(buffer);
      // to cope with the fact that a buffer might be unregistered without having being used in any callback (typically small files)
      if ((int) mMetBufferSet.size() == mNumExpectedBuffers)
      {
        mAllBufferMet = true;
      }
    }
  }

  /**
   * \brief Set the number of buffers used for the copy.
   *
   * @param nBuffers
   */
  void
  SetExpectedBuffers(int nBuffers)
  {
    pthread_mutex_lock(&mHandle->mutex);
    mNumExpectedBuffers = nBuffers;
    pthread_mutex_unlock(&mHandle->mutex);
  }

  /**
   * \brief Get the count of active buffers
   *
   * An active buffer is merely a buffer that has been used once and not disabled.
   *
   * @return The count of active buffers
   */
  size_t
  GetActiveCount() const
  {
    return mActiveBufferSet.size();
  }

  /**
   * \brief Get the count of buffers
   *
   * @return The number of different buffers ever called by RegisterBuffer
   */
  size_t
  GetBufferCount() const
  {
    return mMetBufferSet.size();
  }

  /**
   * \brief Check if the copy is over.
   *
   * @return
   */
  bool
  IsOver() const
  {
    return (GetActiveCount() == 0) && (GetBufferCount() != 0) && ((int) GetBufferCount() == mNumExpectedBuffers) && (mNumCbRead == mNumRegRead)
        && (mNumExpectedResp == mNumReceivedResp);
  }

  /**
   * \brief XRootD response handler function.
   *
   * This function is called after a write to XRootD is executed.
   * If everything ran fine and if there is some data left, another Globus read is registered.
   *
   * @param pStatus The status of the XRootD write operation.
   * @param chunk The chunk handler associated to the writes for the money transfer to be registered.
   */
  virtual void
  HandleResponse(XrdCl::XRootDStatus* pStatus, ChunkHandler* chunk)
  {
    mNumCbWrite++;
    const char *func = "DsiRcvResponseHandler::HandleResponse";
    pthread_mutex_lock(&mHandle->mutex);

    if (!mAllBufferMet)
    {
      // check the expected number of buffers
      mMetBufferSet.insert(mBufferMap[std::pair<uint64_t, uint32_t>(chunk->GetOffset(), chunk->GetLength())]);
      mActiveBufferSet.insert(mBufferMap[std::pair<uint64_t, uint32_t>(chunk->GetOffset(), chunk->GetLength())]);
      if ((int) mMetBufferSet.size() == mNumExpectedBuffers)
      {
        mAllBufferMet = true;
      }
    }

    globus_byte_t* buffer = mBufferMap[std::pair<uint64_t, uint32_t>(chunk->GetOffset(), chunk->GetLength())];
    if (pStatus->IsError())
    { // if there is a xrootd write error
      if (mHandle->cached_res == GLOBUS_SUCCESS)
      { //if it's the first error
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: XRootd write issued an error response : %s \n", func, pStatus->ToStr().c_str());
        mHandle->cached_res = globus_l_gfs_make_error(pStatus->ToStr().c_str(), pStatus->errNo);
        mHandle->done = GLOBUS_TRUE;
      }
      DisableBuffer(buffer);
    }
    else
    { // if there is no error, continue
      globus_gridftp_server_update_bytes_written(mHandle->op, chunk->GetOffset(), chunk->GetLength());

      bool spawn = (mHandle->optimal_count >= (int) GetActiveCount());
      // if required and valid, spawn again
      if (spawn && (mHandle->done == GLOBUS_FALSE))
      {
        mBufferMap.erase(std::pair<uint64_t, uint32_t>(chunk->GetOffset(), chunk->GetLength()));
        globus_result_t result = globus_gridftp_server_register_read(mHandle->op, buffer, mHandle->block_size, globus_l_gfs_file_net_read_cb, mHandle);

        if (result != GLOBUS_SUCCESS)
        {
          globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: register Globus read has finished with a bad result \n", func);
          mHandle->cached_res = globus_l_gfs_make_error("Error registering globus read", result);
          mHandle->done = GLOBUS_TRUE;
          DisableBuffer(buffer);
        }
        else
          mNumRegRead++;
      }
      else
      { // if not spawning, delete the buffer
        DisableBuffer(buffer);
      }
    }
    AsyncMetaHandler::HandleResponse(pStatus, chunk); // MUST be called AFTER the actual processing of the inheriting class
    SignalIfOver();
    pthread_mutex_unlock(&mHandle->mutex);
  }

  /**
   * \brief Signal that cleanup is to be done if the copy is over.
   *
   * Note that the cleaning-up should be explicitly triggered after WaitOK
   */
  void
  SignalIfOver()
  {
    if (IsOver())
    {
      globus_mutex_lock(&mOverMutex);
      mOver = true;
      globus_cond_signal(&mOverCond);
      globus_mutex_unlock(&mOverMutex);
    }
  }

  /**
   * \brief Wait for the copy to be over.
   *
   * @return
   */
  virtual bool
  WaitOK()
  {
    // wait for the end of the copy to be signaled
    globus_mutex_lock(&mOverMutex);
    while (!mOver)
      globus_cond_wait(&mOverCond, &mOverMutex);
    globus_mutex_unlock(&mOverMutex);

    return AsyncMetaHandler::WaitOK();
  }

  /**
   * not necessary in normal operations, for debug purpose only.
   * Refer also to mNumExpectedResp mNumReceivedResp though it's not the same information.
   */
  int mNumRegRead, mNumCbRead;

  int mNumRegWrite, mNumCbWrite;

  /**
   * \brief Clean-up the handler making it ready for another copy
   *
   */
  void*
  CleanUp()
  {
    // close the XRootD destination file
    delete mHandle->fileIo;
    mHandle->fileIo = NULL;
    // Reset the Response Handler
    Reset();
    pthread_mutex_unlock(&mHandle->mutex);

    return NULL;
  }

protected:
  /**
   * \brief Reset the state of the handler, it's part of the clean-up procedure.
   */
  void
  Reset()
  {
    for (std::set<globus_byte_t*>::iterator it = mMetBufferSet.begin(); it != mMetBufferSet.end(); it++)
      globus_free(*it);
    mMetBufferSet.clear();
    mActiveBufferSet.clear();
    mBufferMap.clear();
    mNumExpectedBuffers = -1;
    mAllBufferMet = false;
    mNumRegRead = mNumCbRead = mNumRegWrite = mNumCbWrite = 0;
    mOver = false; // to finalize the Reset of the handler
    static_cast<AsyncMetaHandler*>(this)->Reset();
  }

  globus_l_gfs_xrood_handle_s *mHandle;
  std::map<std::pair<uint64_t, uint32_t>, globus_byte_t*> mBufferMap;
  std::set<globus_byte_t*> mMetBufferSet;
  std::set<globus_byte_t*> mActiveBufferSet;
  bool mAllBufferMet, mOver;
  int mNumExpectedBuffers;
  mutable globus_mutex_t mOverMutex;
  mutable globus_cond_t mOverCond; ///< condition variable to signal that the cleanup is done
  mutable pthread_t clean_tid;
};

/**
 * Class for handling async responses when DSI sends data.
 * In that case, XRootD reads from a file and Globus writes to the network.
 * To overcome a globus limitation (bug?), a mechanism forces writes to Globus to be issued in order.
 * This mechanism can be disabled.
 */
class DsiSendResponseHandler : public AsyncMetaHandler
{
public:
  /**
   * \brief Constructor
   *
   * @param handle XRootD handle
   * @param writeinorder indicate that the response handler should request Globus writes with offsets strictly ordered
   */
  DsiSendResponseHandler(globus_l_gfs_xrood_handle_s *handle, bool writeinorder = true) :
      AsyncMetaHandler(), mNumRegRead(0), mNumCbRead(0), mNumRegWrite(0), mNumCbWrite(0), mWriteInOrder(writeinorder), mHandle(handle), mAllBufferMet(false), mOver(
          false), mNumExpectedBuffers(-1), clean_tid(0)
  {
    globus_mutex_init(&mOverMutex, NULL);
    globus_cond_init(&mOverCond, NULL);
    if (mWriteInOrder)
      pthread_cond_init(&mOrderCond, NULL);
  }

  /**
   * \brief Destructor
   */
  virtual
  ~DsiSendResponseHandler()
  {
    globus_mutex_destroy(&mOverMutex);
    globus_cond_destroy(&mOverCond);
    if (mWriteInOrder)
      pthread_cond_destroy(&mOrderCond);
  }

  /**
   * \brief Register the buffer associated to a given file chunk
   *
   * @param offset Offset of the file chunck
   * @param length Length of the file chunk
   * @param buffer Buffer
   */
  void
  RegisterBuffer(uint64_t offset, uint64_t length, globus_byte_t* buffer)
  {
    mBufferMap[std::pair<uint64_t, uint32_t>(offset, length)] = buffer;
    mRevBufferMap[buffer] = std::pair<uint64_t, uint32_t>(offset, length);
  }

  /**
   * \brief Disable the buffer
   *
   * The function disables the buffer.
   * When all the buffer are disable, that shows that the activity is over for the current copy.
   *
   * @param buffer Buffer to disable
   */
  void
  DisableBuffer(globus_byte_t* buffer)
  {
    mActiveBufferSet.erase(buffer);
    mBufferMap.erase(mRevBufferMap[buffer]);
    mRevBufferMap.erase(buffer);
    if (!mAllBufferMet)
    { // check the expected number of buffers
      mMetBufferSet.insert(buffer);
      // to cope with the fact that a buffer might be unregistered without having being used in any callback (typically small files)
      if ((int) mMetBufferSet.size() == mNumExpectedBuffers)
      {
        mAllBufferMet = true;
      }
    }
  }
  ;

  /**
   * \brief Set the number of buffers used for the copy.
   *
   * @param nBuffers
   */
  void
  SetExpectedBuffers(int nBuffers)
  {
    pthread_mutex_lock(&mHandle->mutex);
    mNumExpectedBuffers = nBuffers;
    pthread_mutex_unlock(&mHandle->mutex);
  }

  /**
   * \brief Get the count of active buffers
   *
   * An active buffer is merely a buffer that has been used once and not disabled.
   *
   * @return The count of active buffers
   */
  size_t
  GetActiveCount() const
  {
    return mActiveBufferSet.size();
  }

  /**
   * \brief Get the count of buffers
   *
   * @return The number of different buffers ever called by RegisterBuffer
   */
  size_t
  GetBufferCount() const
  {
    return mMetBufferSet.size();
  }

  /**
   * \brief Check if the copy is over.
   *
   * @return
   */
  bool
  IsOver() const
  {
    return (GetActiveCount() == 0) && (GetBufferCount() != 0) && ((int) GetBufferCount() == mNumExpectedBuffers) && (mNumCbWrite == mNumRegWrite)
        && (mNumExpectedResp == mNumReceivedResp);
  }

  /**
   * \brief XRootD response handler function.
   *
   * This function is called after a read from XRootD is executed.
   * If the read ran fine, the corresponding Globus write is registered.
   *
   * @param pStatus The status of the XRootD write operation.
   * @param chunk The chunk handler associated to the read
   */
  virtual void
  HandleResponse(XrdCl::XRootDStatus* pStatus, ChunkHandler* chunk)
  {
    HandleResponse(pStatus->IsError(), pStatus->errNo, chunk->GetOffset(), chunk->GetLength(), chunk->GetRespLength(), pStatus, chunk);
  }

  /**
   * \brief XRootD response handler function.
   * @param isErr Error status of the response to handle
   * @param errNo Error number of the response to handle
   * @param offset Offset of the read attached to the response
   * @param len Length of the read attached to the response
   * @param rlen Length of the response itself (can be different from the length of the read)
   * @param pStatus Status object of the read operation (can be NULL)
   * @param chunk Read handler of the read operation (can be NULL)
   */
  void
  HandleResponse(bool isErr, uint32_t errNo, uint64_t offset, uint32_t len, uint32_t rlen, XrdCl::XRootDStatus* pStatus = 0, ChunkHandler* chunk = 0)
  {
    mNumCbRead++;
    const char *func = "DsiSendResponseHandler::HandleResponse";
    pthread_mutex_lock(&mHandle->mutex);

    globus_byte_t* buffer = mBufferMap[std::pair<uint64_t, uint32_t>(offset, len)];

    if (!mAllBufferMet)
    { // check the expected number of buffers
      mMetBufferSet.insert(mBufferMap[std::pair<uint64_t, uint32_t>(offset, len)]);
      mActiveBufferSet.insert(buffer);
      if ((int) mMetBufferSet.size() == mNumExpectedBuffers)
      {
        mAllBufferMet = true;
      }
    }

    size_t nbread = rlen;
    if (isErr && errNo != EFAULT)
    { // if there is a xrootd read error which is not bad (offset,len)
      if (mHandle->cached_res == GLOBUS_SUCCESS)
      { //if it's the first one
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: XRootd read issued an error response : %s \n", func, pStatus->ToStr().c_str());
        mHandle->cached_res = globus_l_gfs_make_error(pStatus->ToStr().c_str(), pStatus->errNo);
        mHandle->done = GLOBUS_TRUE;
      }
      DisableBuffer(buffer);
    }
    else if (isErr && errNo == EFAULT && nbread == 0)
    { // if there is a bad (offset,len) error with an empty response
      DisableBuffer(buffer);
      mHandle->done = GLOBUS_TRUE;
    }
    else
    { // if there is no error or if bad (offset,len) but non empty response

      // !!!!! WARNING the value of the offset argument of globus_gridftp_server_register_write is IGNORED
      // !!!!! a mechanism is then implemented to overcome this limitation, it can be enabled or disbaled
      if (mWriteInOrder)
      {
        // wait that the current offset is the next in the order or that the set of offsets to process is empty
        while ((globus_off_t) offset != (*mRegisterReadOffsets.begin()) || mRegisterReadOffsets.size() == 0)
          pthread_cond_wait(&mOrderCond, &mHandle->mutex);
        mRegisterReadOffsets.erase((globus_off_t) offset);
        pthread_cond_broadcast(&mOrderCond);
      }

      globus_result_t result = globus_gridftp_server_register_write(mHandle->op, buffer, nbread, offset, // !! this value doesn't matter
          -1, globus_l_gfs_net_write_cb, mHandle);

      if (result != GLOBUS_SUCCESS)
      {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: register Globus write has finished with a bad result \n", func);
        mHandle->cached_res = globus_l_gfs_make_error("Error registering globus write", result);
        mHandle->done = GLOBUS_TRUE;
        DisableBuffer(buffer);
      } // spawn
      else
        mNumRegWrite++;
    }
    if (pStatus != 0 && chunk != 0)
      AsyncMetaHandler::HandleResponse(pStatus, chunk); // MUST be called AFTER the actual processing of the inheriting class
    else
      mNumReceivedResp++;
    SignalIfOver();
    pthread_mutex_unlock(&mHandle->mutex);
  }

  struct HandleRespStruct
  {
    DsiSendResponseHandler *_this;
    bool isErr;
    uint32_t errNo;
    uint64_t offset;
    uint32_t len;
    uint32_t rlen;
  };

  static void*
  RunHandleResp(void* handleRespStruct)
  {
    HandleRespStruct *hrs = static_cast<HandleRespStruct *>(handleRespStruct);
    hrs->_this->HandleResponse(hrs->isErr, hrs->errNo, hrs->offset, hrs->len, hrs->rlen);
    delete hrs;
    return NULL;
  }

  void
  HandleResponseAsync(bool isErr, uint32_t errNo, uint64_t offset, uint32_t len, uint32_t rlen)
  {
    HandleRespStruct *hrs = new HandleRespStruct;
    //*hrs={this,isErr,errNo,offset,len,rlen};
    hrs->_this = this;
    hrs->isErr = isErr;
    hrs->errNo = errNo;
    hrs->offset = offset;
    hrs->len = len;
    hrs->rlen = rlen;
    pthread_t thread;
    mNumExpectedResp++;
    XrdSysThread::Run(&thread, RunHandleResp, (void*) hrs);
  }

  /**
   * \brief Signal that cleanup is to be done if the copy is over.
   *
   * Note that the cleaning-up should be explicitly triggered after WaitOK
   */
  void
  SignalIfOver()
  {
    if (IsOver())
    {
      globus_mutex_lock(&mOverMutex);
      mOver = true;
      globus_cond_signal(&mOverCond);
      globus_mutex_unlock(&mOverMutex);
    }
  }

  /**
   * \brief Wait for the copy to be over.
   *
   * @return
   */
  virtual bool
  WaitOK()
  {
    // wait for the end of the copy to be signaled
    globus_mutex_lock(&mOverMutex);
    while (!mOver)
      globus_cond_wait(&mOverCond, &mOverMutex);
    globus_mutex_unlock(&mOverMutex);

    return AsyncMetaHandler::WaitOK();
  }

  int mNumRegRead, mNumCbRead;
  /**
   * not necessary in normal operations, for debug purpose only.
   * Refer also to mNumExpectedResp mNumReceivedResp though it's not the same information.
   */
  int mNumRegWrite, mNumCbWrite;
  /**
   * This map maps buffer -> (offset,length)
   */
  std::map<std::pair<uint64_t, uint32_t>, globus_byte_t*> mBufferMap;
  /**
   * This map maps (offset,length) -> buffer
   */
  std::map<globus_byte_t*, std::pair<uint64_t, uint32_t> > mRevBufferMap;

  /**
   * This boolean shows if the writes to globus must be issued in order
   */
  const bool mWriteInOrder;
  mutable pthread_cond_t mOrderCond; ///< condition variable to signal that the next write is the offset order is to be issued
  mutable std::set<globus_off_t> mRegisterReadOffsets; ///< Set of issued write to process the lowest offset when issuing a register_write

  /**
   * \brief Clean-up the handler making it ready for another copy
   *
   */
  void*
  CleanUp()
  {
    pthread_mutex_lock(&mHandle->mutex);
    // close the XRootD source file
    delete mHandle->fileIo;
    mHandle->fileIo = NULL;
    // Reset the Response Handler
    Reset();
    pthread_mutex_unlock(&mHandle->mutex);

    return NULL;
  }

protected:
  /**
   * \brief Reset the state of the handler, it's part of the clean-up procedure.
   */
  void
  Reset()
  {
    for (std::set<globus_byte_t*>::iterator it = mMetBufferSet.begin(); it != mMetBufferSet.end(); it++)
      globus_free(*it);
    mMetBufferSet.clear();
    mActiveBufferSet.clear();
    mBufferMap.clear();
    mRevBufferMap.clear();
    mNumExpectedBuffers = -1;
    mAllBufferMet = false;
    mNumRegRead = mNumCbRead = mNumRegWrite = mNumCbWrite = 0;
    mOver = false; // to finalize the Reset of the handler
    static_cast<AsyncMetaHandler*>(this)->Reset();
  }

  globus_l_gfs_xrood_handle_s *mHandle;
  std::set<globus_byte_t*> mMetBufferSet;
  std::set<globus_byte_t*> mActiveBufferSet;
  bool mAllBufferMet, mOver;
  int mNumExpectedBuffers;
  mutable globus_mutex_t mOverMutex;
  mutable globus_cond_t mOverCond; ///< condition variable to signal that the cleanup is done
  mutable pthread_t clean_tid;
};

/**
 * \brief Compute the length of the next chunk to be read and update the xroot_handle struct.
 */
int
next_read_chunk(globus_l_gfs_xrood_handle_s *xrootd_handle, int64_t &nextreadl)
{
  //const char *func="next_read_chunk";

  if (xrootd_handle->blk_length == 0)
  { // for initialization and next block
    // check the next range to read
    globus_gridftp_server_get_read_range(xrootd_handle->op, &xrootd_handle->blk_offset, &xrootd_handle->blk_length);
    if (xrootd_handle->blk_length == 0)
    {
      return 1; // means no more chunk to read
    }
  }
  else
  {
    if (xrootd_handle->blk_length != -1)
    {
      //xrootd_handle->blk_length -= lastreadl;
      // here we suppose that when a read succeed it always read block size or block length
      xrootd_handle->blk_offset += (
          (xrootd_handle->blk_length >= (globus_off_t) xrootd_handle->block_size) ?
              (globus_off_t) xrootd_handle->block_size : (globus_off_t) xrootd_handle->blk_length);
      xrootd_handle->blk_length -=
          xrootd_handle->blk_length >= (globus_off_t) xrootd_handle->block_size ?
              (globus_off_t) xrootd_handle->block_size : (globus_off_t) xrootd_handle->blk_length;
    }
    else
    {
      xrootd_handle->blk_offset += xrootd_handle->block_size;
    }
  }
  if (xrootd_handle->blk_length == -1 || xrootd_handle->blk_length > (globus_off_t) xrootd_handle->block_size)
  {
    nextreadl = xrootd_handle->block_size;
  }
  else
  {
    nextreadl = xrootd_handle->blk_length;
  }

  return 0; // means chunk updated
}

/**
 * \brief Struct to handle the configuration of the DSI plugin.
 *
 * \details This structure should have only one instance.
 * The configuration is read from the environment as the struct is constructed.
 *
 */
struct globus_l_gfs_xrootd_config
{
  bool EosCks;
  bool EosChmod;
  bool EosAppTag;
  bool EosBook;
  int XrdReadAheadBlockSize, XrdReadAheadNBlocks;
  std::string XrootdVmp;

  globus_l_gfs_xrootd_config()
  {
    const char *cptr = 0;
    EosCks = EosChmod = EosAppTag = false;
    XrdReadAheadBlockSize = (int) ReadaheadBlock::sDefaultBlocksize;
    XrdReadAheadNBlocks = (int) XrdFileIo::sNumRdAheadBlocks;

    cptr = getenv("XROOTD_VMP");
    if (cptr != 0)
      XrootdVmp = cptr;

    if (getenv("XROOTD_DSI_EOS"))
    {
      EosBook = EosCks = EosChmod = EosAppTag = true;
    }
    else
    {
      EosCks = (getenv("XROOTD_DSI_EOS_CKS") != 0);
      EosChmod = (getenv("XROOTD_DSI_EOS_CHMOD") != 0);
      EosAppTag = (getenv("XROOTD_DSI_EOS_APPTAG") != 0);
      EosBook = (getenv("XROOTD_DSI_EOS_BOOK") != 0);
    }

    cptr = getenv("XROOTD_DSI_READAHEADBLOCKSIZE");
    if (cptr != 0)
      XrdReadAheadBlockSize = atoi(cptr);

    cptr = getenv("XROOTD_DSI_READAHEADNBLOCKS");
    if (cptr != 0)
      XrdReadAheadNBlocks = atoi(cptr);
  }
};

XrootPath XP;
globus_l_gfs_xrootd_config config;
DsiRcvResponseHandler* RcvRespHandler;
DsiSendResponseHandler* SendRespHandler;

extern "C"
{

  static globus_version_t local_version =
  { 0, /* major version number */
  1, /* minor version number */
  1157544130, 0 /* branch ID */
  };

/// utility function to make errors
  static globus_result_t
  globus_l_gfs_make_error(const char *msg, int errCode)
  {
    char *err_str;
    globus_result_t result;
    GlobusGFSName(__FUNCTION__);
    err_str = globus_common_create_string("%s error: %s", msg, strerror(errCode));
    result = GlobusGFSErrorGeneric(err_str);
    globus_free(err_str);
    return result;
  }

  /* fill the statbuf into globus_gfs_stat_t */
  void
  fill_stat_array(globus_gfs_stat_t * filestat, struct stat statbuf, char *name)
  {
    filestat->mode = statbuf.st_mode;
    ;
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
  void
  free_stat_array(globus_gfs_stat_t * filestat, int count)
  {
    int i;
    for (i = 0; i < count; i++)
      free(filestat[i].name);
  }

  /**
   *  \brief This hook is called when a session is initialized
   *
   *  \details This function is called when a new session is initialized, ie a user
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
   *        The DSI developer should just follow this template for now
   *
   */
  static
  void
  globus_l_gfs_xrootd_start(globus_gfs_operation_t op, globus_gfs_session_info_t *session_info)
  {
    globus_l_gfs_xrootd_handle_t *xrootd_handle;
    globus_gfs_finished_info_t finished_info;
    const char *func = "globus_l_gfs_xrootd_start";

    GlobusGFSName(__FUNCTION__);

    xrootd_handle = (globus_l_gfs_xrootd_handle_t *) globus_malloc(sizeof(globus_l_gfs_xrootd_handle_t));
    if (!xrootd_handle)
      GlobusGFSErrorMemory("xroot_handle");
    try
    {
      RcvRespHandler = new DsiRcvResponseHandler(xrootd_handle);
      SendRespHandler = new DsiSendResponseHandler(xrootd_handle);
    }
    catch (...)
    {
      GlobusGFSErrorMemory("xrootResponseHandler");
    }

    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: started, uid: %u, gid: %u\n", func, getuid(), getgid());
    pthread_mutex_init(&xrootd_handle->mutex, NULL);
    xrootd_handle->isInit = true;

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

  /**
   *  \brief This hook is called when a session ends
   *
   *  \details This is called when a session ends, ie client quits or disconnects.
   *  The dsi should clean up all memory they associated wit the session
   *  here.
   *
   */
  static void
  globus_l_gfs_xrootd_destroy(void *user_arg)
  {
    if (user_arg)
    {
      globus_l_gfs_xrootd_handle_t *xrootd_handle;
      xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;
      if (xrootd_handle->isInit)
      {
        delete RcvRespHandler;
        delete SendRespHandler;
        pthread_mutex_destroy(&xrootd_handle->mutex);
        globus_free(xrootd_handle);
      }
    }
    else
    {
      //globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s: xrootd_handle not allocated : no clean-up to make \n", "globus_l_gfs_xrootd_destroy");
    }
  }

  void
  globus_l_gfs_file_copy_stat(globus_gfs_stat_t * stat_object, XrdCl::StatInfo * stat_buf, const char * filename, const char * symlink_target)
  {
    GlobusGFSName(__FUNCTION__);

    XrootStatUtils::initStat(stat_object);

    stat_object->mode = XrootStatUtils::mapFlagsXrd2Pos(stat_buf->GetFlags());
    stat_object->size = stat_buf->GetSize(); // stat
    stat_object->mtime = stat_buf->GetModTime();
    stat_object->atime = stat_object->mtime;
    stat_object->ctime = stat_object->mtime;

    if (filename && *filename)
    {
      stat_object->name = strdup(filename);
    }
    else
    {
      stat_object->name = NULL;
    }
    if (symlink_target && *symlink_target)
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
  globus_l_gfs_file_destroy_stat(globus_gfs_stat_t * stat_array, int stat_count)
  {
    int i;
    GlobusGFSName(__FUNCTION__);

    for (i = 0; i < stat_count; i++)
    {
      if (stat_array[i].name != NULL)
      {
        globus_free(stat_array[i].name);
      }
      if (stat_array[i].symlink_target != NULL)
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
  globus_l_gfs_file_partition_path(const char * pathname, char * basepath, char * filename)
  {
    char buf[MAXPATHLEN];
    char * filepart;
    GlobusGFSName(__FUNCTION__);

    strncpy(buf, pathname, MAXPATHLEN);
    buf[MAXPATHLEN - 1] = '\0';

    filepart = strrchr(buf, '/');
    while (filepart && !*(filepart + 1) && filepart != buf)
    {
      *filepart = '\0';
      filepart = strrchr(buf, '/');
    }

    if (!filepart)
    {
      strcpy(filename, buf);
      basepath[0] = '\0';
    }
    else
    {
      if (filepart == buf)
      {
        if (!*(filepart + 1))
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

  /**
   *  \brief Stat a file.
   *
   *  \details This interface function is called whenever the server needs
   *  information about a given file or resource.  It is called then an
   *  LIST is sent by the client, when the server needs to verify that
   *  a file exists and has the proper permissions, etc.
   *
   */
  static
  void
  globus_l_gfs_xrootd_stat(globus_gfs_operation_t op, globus_gfs_stat_info_t * stat_info, void * user_arg)
  {
    globus_result_t result;
    globus_gfs_stat_t * stat_array;
    int stat_count = 0;
    char basepath[MAXPATHLEN];
    char filename[MAXPATHLEN];
    char symlink_target[MAXPATHLEN];
    char *PathName;
    char myServerPart[MAXPATHLEN], myPathPart[MAXPATHLEN];
    GlobusGFSName(__FUNCTION__);
    PathName = stat_info->pathname;

    std::string request(MAXPATHLEN * 2, '\0');
    XrdCl::Buffer arg;
    XrdCl::StatInfo* xrdstatinfo = 0;
    XrdCl::XRootDStatus status;
    XrdCl::URL server;
    /*
     If we do stat_info->pathname++, it will cause third-party transfer
     hanging if there is a leading // in path. Don't know why. To work
     around, we replaced it with PathName.
     */
    while ((strlen(PathName) > 1) && (PathName[0] == '/' && PathName[1] == '/'))
    {
      PathName++;
    }

    char *myPath, buff[2048];
    if (!(myPath = XP.BuildURL(PathName, buff, sizeof(buff))))
      myPath = PathName;

    if (XrootPath::SplitURL(myPath, myServerPart, myPathPart, MAXPATHLEN))
    {
      result = GlobusGFSErrorSystemError("stat", ECANCELED);
      globus_gridftp_server_finished_stat(op, result, NULL, 0);
      return;
    }

    arg.FromString(myPathPart);
    server.FromString(myServerPart);
    XrdCl::FileSystem fs(server);
    status = fs.Stat(myPathPart, xrdstatinfo);
    if (status.IsError())
    {
      if (xrdstatinfo)
        delete xrdstatinfo;
      result = GlobusGFSErrorSystemError("stat", XrootStatUtils::mapError(status.errNo));
      goto error_stat1;
    }

    globus_l_gfs_file_partition_path(myPathPart, basepath, filename);

    if (!(xrdstatinfo->GetFlags() & XrdCl::StatInfo::IsDir) || stat_info->file_only)
    {
      stat_array = (globus_gfs_stat_t *) globus_malloc(sizeof(globus_gfs_stat_t));
      if (!stat_array)
      {
        result = GlobusGFSErrorMemory("stat_array");
        goto error_alloc1;
      }

      globus_l_gfs_file_copy_stat(stat_array, xrdstatinfo, filename, symlink_target);
      stat_count = 1;
    }
    else
    {
      XrdCl::DirectoryList *dirlist = 0;
      status = fs.DirList(myPathPart, XrdCl::DirListFlags::Stat, dirlist, (uint16_t) 0);
      if (!status.IsOK())
      {
        if (dirlist)
          delete dirlist;
        result = GlobusGFSErrorSystemError("opendir", XrootStatUtils::mapError(status.errNo));
        goto error_open;
      }

      stat_count = dirlist->GetSize();

      stat_array = (globus_gfs_stat_t *) globus_malloc(sizeof(globus_gfs_stat_t) * (stat_count + 1));
      if (!stat_array)
      {
        if (dirlist)
          delete dirlist;
        result = GlobusGFSErrorMemory("stat_array");
        goto error_alloc2;
      }

      int i = 0;
      for (XrdCl::DirectoryList::Iterator it = dirlist->Begin(); it != dirlist->End(); it++)
      {
        std::string path = (*it)->GetName();
        globus_l_gfs_file_partition_path(path.c_str(), basepath, filename);
        globus_l_gfs_file_copy_stat(&stat_array[i++], (*it)->GetStatInfo(), filename, NULL);
      }
      if (dirlist)
        delete dirlist;
    }

    globus_gridftp_server_finished_stat(op, GLOBUS_SUCCESS, stat_array, stat_count);

    globus_l_gfs_file_destroy_stat(stat_array, stat_count);

    return;

    error_alloc2: error_open: error_alloc1: error_stat1: globus_gridftp_server_finished_stat(op, result, NULL, 0);

    /*    GlobusGFSFileDebugExitWithError();  */
  }

  /**
   *  \brief Executes a gridftp command on the DSI back-end
   *
   *  \details This interface function is called when the client sends a 'command'.
   *  commands are such things as mkdir, remdir, delete.  The complete
   *  enumeration is below.
   *
   *  To determine which command is being requested look at:
   *      cmd_info->command
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
   *
   */
  static void
  globus_l_gfs_xrootd_command(globus_gfs_operation_t op, globus_gfs_command_info_t* cmd_info, void *user_arg)
  {

    GlobusGFSName(__FUNCTION__);

    char cmd_data[MAXPATHLEN];
    char * PathName;
    globus_result_t rc = GLOBUS_SUCCESS;
    std::string cks;

    // create the full path and split it
    char *myPath, buff[2048];
    char myServerPart[MAXPATHLEN], myPathPart[MAXPATHLEN];
    PathName = cmd_info->pathname;
    while (PathName[0] == '/' && PathName[1] == '/')
      PathName++;
    if (!(myPath = XP.BuildURL(PathName, buff, sizeof(buff))))
      myPath = PathName;
    if (XrootPath::SplitURL(myPath, myServerPart, myPathPart, MAXPATHLEN))
    {
      rc = GlobusGFSErrorGeneric("command fail : error parsing the filename");
      globus_gridftp_server_finished_command(op, rc, NULL);
      return;
    }

    // open the filesystem
    XrdCl::URL server;
    XrdCl::Buffer arg, *resp;
    XrdCl::Status status;
    arg.FromString(myPathPart);
    server.FromString(myServerPart);
    XrdCl::FileSystem fs(server);

    switch (cmd_info->command)
    {
    case GLOBUS_GFS_CMD_MKD:
      (status = fs.MkDir(myPathPart, XrdCl::MkDirFlags::None, (XrdCl::Access::Mode) XrootStatUtils::mapModePos2Xrd(0777))).IsError() && (rc =
          GlobusGFSErrorGeneric((std::string("mkdir() fail : ") += status.ToString()).c_str()));
      break;
    case GLOBUS_GFS_CMD_RMD:
      (status = fs.RmDir(myPathPart)).IsError() && (rc = GlobusGFSErrorGeneric((std::string("rmdir() fail") += status.ToString()).c_str()));
      break;
    case GLOBUS_GFS_CMD_DELE:
      (fs.Rm(myPathPart)).IsError() && (rc = GlobusGFSErrorGeneric((std::string("rm() fail") += status.ToString()).c_str()));
      break;
    case GLOBUS_GFS_CMD_SITE_RDEL:
      /*
       result = globus_l_gfs_file_delete(
       op, PathName, GLOBUS_TRUE);
       */
      rc = GLOBUS_FAILURE;
      break;
    case GLOBUS_GFS_CMD_RNTO:
      char myServerPart2[MAXPATHLEN], myPathPart2[MAXPATHLEN];
      if (!(myPath = XP.BuildURL(cmd_info->from_pathname, buff, sizeof(buff))))
        myPath = cmd_info->from_pathname;
      if (XrootPath::SplitURL(myPath, myServerPart2, myPathPart2, MAXPATHLEN))
      {
        rc = GlobusGFSErrorGeneric("rename() fail : error parsing the target filename");
        globus_gridftp_server_finished_command(op, rc, NULL);
        return;
      }
      (status = fs.Mv(myPathPart2, myPathPart)).IsError() && (rc = GlobusGFSErrorGeneric((std::string("rename() fail") += status.ToString()).c_str()));
      break;
    case GLOBUS_GFS_CMD_SITE_CHMOD:
      if (config.EosChmod)
      { // Using EOS Chmod
        char request[16384];
        sprintf(request, "%s?mgm.pcmd=chmod&mode=%d", myPathPart, cmd_info->chmod_mode); // specific to eos
        arg.FromString(request);
        status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, resp);
        rc = GlobusGFSErrorGeneric("chmod() fail");
        if (status.IsOK())
        {
          char tag[4096];
          int retc = 0;
          int items = sscanf(resp->GetBuffer(), "%s retc=%d", tag, &retc);
          fflush(stderr);
          if (retc || (items != 2) || (strcmp(tag, "chmod:")))
          {
            // error
          }
          else
          {
            rc = GLOBUS_SUCCESS;
          }
        }
        delete resp;
      }
      else
      { // Using XRoot Chmod
        (status = fs.ChMod(myPathPart, (XrdCl::Access::Mode) XrootStatUtils::mapModePos2Xrd(cmd_info->chmod_mode))).IsError()
            && (rc = GlobusGFSErrorGeneric((std::string("chmod() fail") += status.ToString()).c_str()));
      }
      break;
    case GLOBUS_GFS_CMD_CKSM:
      fflush(stderr);
      if (config.EosCks)
      { // Using EOS checksum
        if (!strcmp(cmd_info->cksm_alg, "adler32") || !strcmp(cmd_info->cksm_alg, "ADLER32"))
        {
          char request[16384];
          sprintf(request, "%s?mgm.pcmd=checksum", myPathPart); // specific to eos
          arg.FromString(request);
          status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, resp);
          fflush(stderr);
          if (status.IsOK())
          {
            if ((strstr(resp->GetBuffer(), "retc=0") && (strlen(resp->GetBuffer()) > 10)))
            {
              // the server returned a checksum via 'checksum: <checksum> retc='
              const char* cbegin = resp->GetBuffer() + 10;
              const char* cend = strstr(resp->GetBuffer(), "retc=");
              if (cend > (cbegin + 8))
              {
                cend = cbegin + 8;
              }
              if (cbegin && cend)
              {
                strncpy(cmd_data, cbegin, cend - cbegin);
                // 0-terminate
                cmd_data[cend - cbegin] = 0;
                rc = GLOBUS_SUCCESS;
                globus_gridftp_server_finished_command(op, rc, cmd_data);
                return;
              }
              else
              {
                rc = GlobusGFSErrorGeneric("checksum() fail : error parsing response");
              }
            }
            else
            {
              rc = GlobusGFSErrorGeneric("checksum() fail : error parsing response");
            }
          }
        }
        rc = GLOBUS_FAILURE;
      }
      else
      { // Using XRootD checksum
        if ((status = XrdUtils::GetRemoteCheckSum(cks, cmd_info->cksm_alg, myServerPart, myPathPart)).IsError() || (cks.size() >= MAXPATHLEN))
        { //UPPER CASE CHECKSUM ?
          rc = GlobusGFSErrorGeneric((std::string("checksum() fail") += status.ToString()).c_str());
          break;
        }
        strcpy(cmd_data, cks.c_str());
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

  /**
   *  \brief Receive a file from globus and store it into the DSI back-end.
   *
   *  \details This interface function is called when the client requests that a
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
   */

  int
  xrootd_open_file(char *path, int flags, int mode, globus_l_gfs_xrootd_handle_t *xrootd_handle, std::string *error = NULL)
  {
    XrdCl::XRootDStatus st;
    const char *func = "xrootd_open_file";
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: open file \"%s\"\n", func, path);
    bool caught = false;
    try
    {
      char *myPath, buff[2048];
      if (!(myPath = XP.BuildURL(path, buff, sizeof(buff))))
      {
        strcpy(buff, path);
        myPath = buff;
      }

      if (config.EosAppTag)
      { // add the 'eos.gridftp' application tag
        if (strlen(myPath))
        {
          if (strchr(myPath, '?'))
          {
            strcat(myPath, "&eos.app=eos/gridftp"); // specific to EOS
          }
          else
          {
            strcat(myPath, "?eos.app=eos/gridftp"); // specific to EOS
          }
        }
      }

      xrootd_handle->fileIo = new XrdFileIo;
      st = xrootd_handle->fileIo->Open(myPath, (XrdCl::OpenFlags::Flags) XrootStatUtils::mapFlagsPos2Xrd(flags),
          (XrdCl::Access::Mode) XrootStatUtils::mapModePos2Xrd(mode));

      if (!st.IsOK())
      {
        *error = st.ToStr();
        replace(error->begin(), error->end(), '\n', ' ');
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: XrdCl::File::Open error : %s\n", func, error->c_str());
      }
    }
    catch (const std::exception& ex)
    {
      *error = ex.what();
      caught = true;
    }
    catch (const std::string& ex)
    {
      *error = ex;
      caught = true;
    }
    catch (...)
    {
      *error = "unknown";
      caught = true;
    }
    if (caught)
    {
      replace(error->begin(), error->end(), '\n', ' ');
      globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: Exception caught when calling XrdCl::File::Open : %s \n", func, error->c_str());
      *error = "exception : " + *error;
      return GLOBUS_FAILURE;
    }

    return ((!st.IsOK()) ? GLOBUS_FAILURE : GLOBUS_SUCCESS);
  }

  /* receive from client */
  static void
  globus_l_gfs_file_net_read_cb(globus_gfs_operation_t op, globus_result_t result, globus_byte_t *buffer, globus_size_t nbytes, globus_off_t offset,
      globus_bool_t eof, void *user_arg)
  {
    const char *func = "globus_l_gfs_file_net_read_cb";
    RcvRespHandler->mNumCbRead++;
    globus_l_gfs_xrootd_handle_t *xrootd_handle;

    xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;
    pthread_mutex_lock(&xrootd_handle->mutex);

    if (eof == GLOBUS_TRUE)
    {
      // if eof is reached, we are done, but we still need the buffer to write
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
    }
    if ((result != GLOBUS_SUCCESS) || (nbytes == 0))
    {
      // if the read failed or succeeded with 0 byte, we are done.
      // The buffer is not needed anymore. Regardless if it's an error or not
      xrootd_handle->cached_res = result;
      xrootd_handle->done = GLOBUS_TRUE;
      RcvRespHandler->DisableBuffer(buffer);
    }
    else
    {
      RcvRespHandler->RegisterBuffer(offset, nbytes, buffer);
      int64_t ret = xrootd_handle->fileIo->Write(offset, (const char*) buffer, nbytes, RcvRespHandler);
      if (ret < 0)
      {
        xrootd_handle->cached_res = ret;
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: register XRootD write has finished with a bad result \n", func);
        GlobusGFSName(__FUNCTION__);
        xrootd_handle->cached_res = GlobusGFSErrorGeneric("Error registering XRootD write");
        xrootd_handle->done = GLOBUS_TRUE;
        RcvRespHandler->DisableBuffer(buffer);
      }
      else
      {
        RcvRespHandler->mNumRegWrite++;
      }
    }

    RcvRespHandler->SignalIfOver();
    pthread_mutex_unlock(&xrootd_handle->mutex);
  }

  static void
  globus_l_gfs_xrootd_read_from_net(globus_l_gfs_xrootd_handle_t *xrootd_handle)
  {
    globus_byte_t **buffers;
    globus_result_t result;
    const char *func = "globus_l_gfs_xrootd_read_from_net";

    GlobusGFSName(globus_l_gfs_xrootd_read_from_net);
    /* in the read case this number will vary */
    globus_gridftp_server_get_optimal_concurrency(xrootd_handle->op, &xrootd_handle->optimal_count);

    pthread_mutex_lock(&xrootd_handle->mutex);
    // allocations of the buffers
    buffers = (globus_byte_t**) globus_malloc(xrootd_handle->optimal_count * sizeof(globus_byte_t**));
    if (!buffers)
      goto error_alloc;
    if (buffers != 0)
    {
      for (int c = 0; c < xrootd_handle->optimal_count; c++)
      {
        buffers[c] = (globus_byte_t*) globus_malloc(xrootd_handle->block_size);
        if (!buffers[c])
          goto error_alloc;
      }
    }
    pthread_mutex_unlock(&xrootd_handle->mutex);

    // this optimal count is kept because for every finishing request is replaced by a new one by the response handler if required
    int c;
    for (c = 0; c < xrootd_handle->optimal_count; c++)
    {
      result = globus_gridftp_server_register_read(xrootd_handle->op, buffers[c], xrootd_handle->block_size, globus_l_gfs_file_net_read_cb, xrootd_handle);
      if (result != GLOBUS_SUCCESS)
      {
        pthread_mutex_lock(&xrootd_handle->mutex);
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: register Globus read has finished with a bad result \n", func);
        xrootd_handle->cached_res = GlobusGFSErrorGeneric("Error registering globus read");
        xrootd_handle->done = GLOBUS_TRUE;
        pthread_mutex_unlock(&xrootd_handle->mutex);
        break; // if an error happens just let the ResponseHandlers all terminate
      }
      else
        RcvRespHandler->mNumRegRead++;
    }

    RcvRespHandler->SetExpectedBuffers(c);
    RcvRespHandler->WaitOK();
    RcvRespHandler->CleanUp();

    globus_gridftp_server_finished_transfer(xrootd_handle->op, xrootd_handle->cached_res);

    globus_free(buffers);

    return;

    error_alloc: result = GlobusGFSErrorMemory("buffers");
    xrootd_handle->cached_res = result;
    xrootd_handle->done = GLOBUS_TRUE;
    delete xrootd_handle->fileIo;
    globus_gridftp_server_finished_transfer(xrootd_handle->op, xrootd_handle->cached_res);
    // free the allocated memory
    if (buffers)
    {
      for (int c = 0; c < xrootd_handle->optimal_count; c++)
        if (buffers[c])
          globus_free(buffers[c]);
      globus_free(buffers);
    }
    pthread_mutex_unlock(&xrootd_handle->mutex);
    return;
  }

  static void
  globus_l_gfs_xrootd_recv(globus_gfs_operation_t op, globus_gfs_transfer_info_t *transfer_info, void *user_arg)
  {
    globus_l_gfs_xrootd_handle_t *xrootd_handle;
    globus_result_t result;
    //const char *func = "globus_l_gfs_xrootd_recv";
    char pathname[16384];
    int flags;
    int rc;

    GlobusGFSName(globus_l_gfs_xrootd_recv);
    xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;

    if (config.EosBook && transfer_info->alloc_size)
    {
      snprintf(pathname, sizeof(pathname) - 1, "%s?eos.bookingsize=%lu&eos.targetsize=%lu", transfer_info->pathname, transfer_info->alloc_size,
          transfer_info->alloc_size); // specific to eos
    }
    else
    {
      snprintf(pathname, sizeof(pathname), "%s", transfer_info->pathname);
    }

    // try to open
    flags = O_WRONLY | O_CREAT;
    if (transfer_info->truncate)
      flags |= O_TRUNC;

    std::string error;
    rc = xrootd_open_file(pathname, flags, 0644, xrootd_handle, &error);

    if (rc)
    {
      //result = globus_l_gfs_make_error("open/create", errno);
      result = GlobusGFSErrorGeneric((std::string("open/create : ") + error).c_str());
      delete xrootd_handle->fileIo;
      globus_gridftp_server_finished_transfer(op, result);
      return;
    }

    // reset all the needed variables in the handle
    xrootd_handle->cached_res = GLOBUS_SUCCESS;
    xrootd_handle->done = GLOBUS_FALSE;
    xrootd_handle->blk_length = 0;
    xrootd_handle->blk_offset = 0;
    xrootd_handle->op = op;

    globus_gridftp_server_get_block_size(op, &xrootd_handle->block_size);

    globus_gridftp_server_begin_transfer(op, 0, xrootd_handle);

    globus_l_gfs_xrootd_read_from_net(xrootd_handle);

    return;
  }

  /*************************************************************************
   *  \brief Read a file from the DSI back-end and send it to the network.
   *
   *  \details This interface function is called when the client requests to receive
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
  globus_l_gfs_xrootd_send(globus_gfs_operation_t op, globus_gfs_transfer_info_t *transfer_info, void *user_arg)
  {
    globus_l_gfs_xrootd_handle_t *xrootd_handle;
    const char *func = "globus_l_gfs_xrootd_send";
    char *pathname;
    int rc;
    globus_bool_t done;
    globus_result_t result;

    GlobusGFSName(globus_l_gfs_xrootd_send);
    xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;

    pathname = strdup(transfer_info->pathname);

    std::string error;
    rc = xrootd_open_file(pathname, O_RDONLY, 0, xrootd_handle, &error); /* mode is ignored */

    if (rc)
    {
      delete xrootd_handle->fileIo;
      //result = globus_l_gfs_make_error("open", errno);
      result = GlobusGFSErrorGeneric((std::string("open : ") + error).c_str());
      globus_gridftp_server_finished_transfer(op, result);
      free(pathname);
      return;
    }
    free(pathname);

    /* reset all the needed variables in the handle */
    xrootd_handle->cached_res = GLOBUS_SUCCESS;
    xrootd_handle->done = GLOBUS_FALSE;
    xrootd_handle->blk_length = 0;
    xrootd_handle->blk_offset = 0;
    xrootd_handle->op = op;

    globus_gridftp_server_get_optimal_concurrency(op, &xrootd_handle->optimal_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: optimal_concurrency: %u\n", func, xrootd_handle->optimal_count);

    globus_gridftp_server_get_block_size(op, &xrootd_handle->block_size);
    globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: block_size: %ld\n", func, xrootd_handle->block_size);

    globus_gridftp_server_begin_transfer(op, 0, xrootd_handle);
    done = GLOBUS_FALSE;
    done = globus_l_gfs_xrootd_send_next_to_client(xrootd_handle);
    pthread_mutex_unlock(&xrootd_handle->mutex);
  }

  /* receive from client */
  void
  globus_l_gfs_net_write_cb_lock(globus_gfs_operation_t op, globus_result_t result, globus_byte_t *buffer, globus_size_t nbwrite, void * user_arg, bool lock =
      true)
  {
    const char *func = "globus_l_gfs_net_write_cb";
    globus_off_t read_length;
    int64_t nbread;
    bool usedReadCallBack = false;
    SendRespHandler->mNumCbWrite++;
    globus_l_gfs_xrootd_handle_t *xrootd_handle;

    xrootd_handle = (globus_l_gfs_xrootd_handle_t *) user_arg;

    if (lock)
      pthread_mutex_lock(&xrootd_handle->mutex);

    GlobusGFSName(globus_l_gfs_xrootd_send_next_to_client);

    if ((result != GLOBUS_SUCCESS))
    { // if the write failed, we are done the buffer is not needed anymore
      if (xrootd_handle->cached_res != GLOBUS_SUCCESS)
      { // don't overwrite the first error
        xrootd_handle->cached_res = result;
        xrootd_handle->done = GLOBUS_TRUE;
      }
      SendRespHandler->DisableBuffer(buffer);
      SendRespHandler->SignalIfOver();
      if (lock)
        pthread_mutex_unlock(&xrootd_handle->mutex);
      return;
    }

    if (nbwrite == 0) // don't update on the first call
      globus_gridftp_server_update_bytes_written(xrootd_handle->op, SendRespHandler->mRevBufferMap[buffer].first,
          SendRespHandler->mRevBufferMap[buffer].second);

    if (xrootd_handle->done == GLOBUS_FALSE)
    { // if we are not done, look for something else to copy
      if (next_read_chunk(xrootd_handle, read_length))
      { // if return is non zero, no more source to copy from
        xrootd_handle->cached_res = GLOBUS_SUCCESS;
        xrootd_handle->done = GLOBUS_TRUE;
        SendRespHandler->DisableBuffer(buffer);
        SendRespHandler->SignalIfOver();
        if (lock)
          pthread_mutex_unlock(&xrootd_handle->mutex);
        return;
      }

      if (nbwrite != 0)
      {
        SendRespHandler->mBufferMap.erase(SendRespHandler->mRevBufferMap[buffer]);
        SendRespHandler->mRevBufferMap.erase(buffer);
      }
      SendRespHandler->RegisterBuffer(xrootd_handle->blk_offset, read_length, buffer);

      globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: register XRootD read from globus_l_gfs_net_write_cb \n", func);
      if (SendRespHandler->mWriteInOrder)
        SendRespHandler->mRegisterReadOffsets.insert(xrootd_handle->blk_offset);
      nbread = xrootd_handle->fileIo->Read(xrootd_handle->blk_offset, (char*) buffer, read_length, SendRespHandler, true, &usedReadCallBack);

      if (nbread < 0)
      {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: register XRootD read has finished with a bad result %d\n", func, nbread);
        xrootd_handle->cached_res = globus_l_gfs_make_error("Error registering XRootD read", nbread);
        xrootd_handle->done = GLOBUS_TRUE;
        SendRespHandler->DisableBuffer(buffer);
        SendRespHandler->SignalIfOver();
      }
      else if (nbread == 0)
      { // empty read, EOF is reached
        xrootd_handle->done = GLOBUS_TRUE;
        SendRespHandler->DisableBuffer(buffer);
        SendRespHandler->SignalIfOver();
      }
      else
      { // succeed
        if (usedReadCallBack)
          SendRespHandler->mNumRegRead++;
        if (usedReadCallBack)
          globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: register XRootD read from globus_l_gfs_net_write_cb ==> usedReadCallBack\n", func);
      }
    }
    else
    { // we are done, just drop the buffer
      SendRespHandler->DisableBuffer(buffer);
      SendRespHandler->SignalIfOver();
      if (lock)
        pthread_mutex_unlock(&xrootd_handle->mutex);
      return;
    }
    int64_t loffset = xrootd_handle->blk_offset;
    //pthread_mutex_unlock(&xrootd_handle->mutex);
    if ((!usedReadCallBack) && (nbread > 0))
    {
      // take care if requested a read beyond the end of the file, an actual read will be issued even if the file is cache.
      // the current situation happens only if the read didn't issue any error.
      SendRespHandler->HandleResponseAsync(false, 0, loffset, read_length, nbread);
      globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "%s: %p register XRootD read from globus_l_gfs_net_write_cb ==> Explicit Callback %d %d\n", func, buffer,
          (int) read_length, (int) nbread);
    }
    if (lock)
      pthread_mutex_unlock(&xrootd_handle->mutex);
  }

  void
  globus_l_gfs_net_write_cb(globus_gfs_operation_t op, globus_result_t result, globus_byte_t *buffer, globus_size_t nbwrite, void * user_arg)
  {
    globus_l_gfs_net_write_cb_lock(op, result, buffer, nbwrite, user_arg, true);
  }

  static globus_bool_t
  globus_l_gfs_xrootd_send_next_to_client(globus_l_gfs_xrootd_handle_t *xrootd_handle)
  {
    globus_byte_t **buffers;
    globus_result_t result;

    GlobusGFSName(globus_l_gfs_xrootd_send_next_to_client);
    /* in the read case this number will vary */
    globus_gridftp_server_get_optimal_concurrency(xrootd_handle->op, &xrootd_handle->optimal_count);

    pthread_mutex_lock(&xrootd_handle->mutex);
    // allocations of the buffers
    buffers = (globus_byte_t**) globus_malloc(xrootd_handle->optimal_count * sizeof(globus_byte_t**));
    if (!buffers)
      goto error_alloc;
    if (buffers != 0)
    {
      for (int c = 0; c < xrootd_handle->optimal_count; c++)
      {
        buffers[c] = (globus_byte_t*) globus_malloc(xrootd_handle->block_size);
        if (!buffers[c])
          goto error_alloc;
      }
    }
    pthread_mutex_unlock(&xrootd_handle->mutex);

    // this optimal count is kept because for every finishing request is replaced by a new one by the response handler if required
    int c;
    pthread_mutex_lock(&xrootd_handle->mutex);
    xrootd_handle->blk_length = 0;
    for (c = 0; c < xrootd_handle->optimal_count; c++)
    {
      SendRespHandler->mNumRegWrite++;
      globus_l_gfs_net_write_cb_lock(xrootd_handle->op, GLOBUS_SUCCESS, buffers[c], xrootd_handle->blk_length, xrootd_handle, false);
    }
    pthread_mutex_unlock(&xrootd_handle->mutex);

    // wait for the write to be done
    SendRespHandler->SetExpectedBuffers(c);
    SendRespHandler->WaitOK();
    SendRespHandler->CleanUp();
    globus_gridftp_server_finished_transfer(xrootd_handle->op, xrootd_handle->cached_res);
    globus_free(buffers);

    return GLOBUS_TRUE;

    error_alloc: result = GlobusGFSErrorMemory("buffers");
    xrootd_handle->cached_res = result;
    xrootd_handle->done = GLOBUS_TRUE;
    delete xrootd_handle->fileIo;
    globus_gridftp_server_finished_transfer(xrootd_handle->op, xrootd_handle->cached_res);
    // free the allocated memory
    if (buffers)
    {
      for (int c = 0; c < xrootd_handle->optimal_count; c++)
        if (buffers[c])
          globus_free(buffers[c]);
      globus_free(buffers);
    }
    pthread_mutex_unlock(&xrootd_handle->mutex);
    return GLOBUS_FALSE;

  }

  static int
  globus_l_gfs_xrootd_activate(void);

  static int
  globus_l_gfs_xrootd_deactivate(void);

/// no need to change this
  static globus_gfs_storage_iface_t globus_l_gfs_xrootd_dsi_iface =
  { GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER, globus_l_gfs_xrootd_start, globus_l_gfs_xrootd_destroy, NULL, /* list */
  globus_l_gfs_xrootd_send, globus_l_gfs_xrootd_recv, NULL, /* trev */
  NULL, /* active */
  NULL, /* passive */
  NULL, /* data destroy */
  globus_l_gfs_xrootd_command, globus_l_gfs_xrootd_stat, NULL, NULL };
/// no need to change this
  GlobusExtensionDefineModule (globus_gridftp_server_xrootd) =
  { (char*) "globus_gridftp_server_xrootd", globus_l_gfs_xrootd_activate, globus_l_gfs_xrootd_deactivate, NULL, NULL, &local_version, NULL };

  static int
  globus_l_gfs_xrootd_activate(void)
  {
    ReadaheadBlock::sDefaultBlocksize = config.XrdReadAheadBlockSize;
    XrdFileIo::sNumRdAheadBlocks = config.XrdReadAheadNBlocks;
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s: My Environment is as follow : \n", "globus_l_gfs_xrootd_activate");
    for (char **env = environ; *env; ++env)
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s:     %s\n", *env);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s: Activating XRootD DSI plugin\n", "globus_l_gfs_xrootd_activate");
    if(config.XrootdVmp.empty())
    {
      globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: XRootD Virtual Mount Point is NOT set. DSI plugin cannot start. \n", "globus_l_gfs_xrootd_activate");
      return 1;
    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s: XRootD Virtual Mount Point is set to: %s\n", "globus_l_gfs_xrootd_activate", config.XrootdVmp.c_str());
    {
      const char *PathName;
      char myServerPart[MAXPATHLEN], myPathPart[MAXPATHLEN];
      GlobusGFSName(__FUNCTION__);
      PathName = "/";//(config.XrootdVmp+"/").c_str();

      std::string request(MAXPATHLEN * 2, '\0');
      XrdCl::Buffer arg;
      XrdCl::StatInfo* xrdstatinfo = 0;
      XrdCl::XRootDStatus status;
      XrdCl::URL server;
      /*
       If we do stat_info->pathname++, it will cause third-party transfer
       hanging if there is a leading // in path. Don't know why. To work
       around, we replaced it with PathName.
       */
      while ((strlen(PathName) > 1) && (PathName[0] == '/' && PathName[1] == '/'))
      {
        PathName++;
      }

      const char *myPath;
      char buff[2048];
      if (!(myPath = XP.BuildURL(PathName, buff, sizeof(buff))))
        myPath = PathName;

      if (XrootPath::SplitURL(myPath, myServerPart, myPathPart, MAXPATHLEN))
      {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: Error : cannot parse Xrootd Virtual Mount Point %s. DSI plugin cannot start. \n", "globus_l_gfs_xrootd_activate",
            myPath);
        return 1;
      }

      arg.FromString(myPathPart);
      server.FromString(myServerPart);
      XrdCl::FileSystem fs(server);
      status = fs.Stat(myPathPart, xrdstatinfo);
      if (status.IsError())
      {
        if (xrdstatinfo)
          delete xrdstatinfo;
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "%s: Error : cannot stat Xrootd Virtual Mount Point %s. DSI plugin cannot start. \n", "globus_l_gfs_xrootd_activate",
            config.XrootdVmp.c_str());
        return 1;
      }

    }
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s: XRootD Read Ahead Block Size is set to: %d\n", "globus_l_gfs_xrootd_activate",
        config.XrdReadAheadBlockSize);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "%s: XRootD number of Read Ahead Blocks is set to: %d\n", "globus_l_gfs_xrootd_activate",
        config.XrdReadAheadNBlocks);
    std::stringstream ss;
    if (config.EosAppTag)
      ss << " EosAppTag";
    if (config.EosChmod)
      ss << " EosChmod";
    if (config.EosCks)
      ss << " EosCks";
    if (config.EosBook)
      ss << " EosBook";
    std::string eosspec(ss.str());
    if (eosspec.size())
    {
      ss.str("");
      ss << "globus_l_gfs_xrootd_activate: XRootD DSI plugin runs the following EOS specifics:";
      ss << eosspec << std::endl;
      globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, ss.str().c_str());
    }

    globus_extension_registry_add(GLOBUS_GFS_DSI_REGISTRY, (void*) "xrootd", GlobusExtensionMyModule(globus_gridftp_server_xrootd),
        &globus_l_gfs_xrootd_dsi_iface);
    return 0;
  }

  static int
  globus_l_gfs_xrootd_deactivate(void)
  {
    globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, (void*) "xrootd");

    return 0;
  }

} // end extern "C"
