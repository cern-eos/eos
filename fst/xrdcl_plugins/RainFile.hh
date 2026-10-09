//------------------------------------------------------------------------------
//! @file RainFilePlugin.hh
//! @author Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

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

#ifndef __EOSFST_XRDCLPLUGINS_RAINFILEPLUGIN_HH__
#define __EOSFST_XRDCLPLUGINS_RAINFILEPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include <XrdCl/XrdClPlugInInterface.hh>
#include <memory>
#include <string>
/*----------------------------------------------------------------------------*/

using namespace XrdCl;

// Forward declaration
namespace eos
{
namespace fst
{
class RainMetaLayout;
class CheckSum;
}
}

EOSFSTNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! RAIN file plugin
//----------------------------------------------------------------------------
class RainFile: public XrdCl::FilePlugIn, eos::common::LogId
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RainFile();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~RainFile();


  //----------------------------------------------------------------------------
  //! Open
  //----------------------------------------------------------------------------
  virtual XRootDStatus Open(const std::string& url,
                            OpenFlags::Flags flags,
                            Access::Mode mode,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Close
  //----------------------------------------------------------------------------
  virtual XRootDStatus Close(ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Stat
  //----------------------------------------------------------------------------
  virtual XRootDStatus Stat(bool force,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Read
  //----------------------------------------------------------------------------
  virtual XRootDStatus Read(uint64_t offset,
                            uint32_t size,
                            void* buffer,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Write
  //----------------------------------------------------------------------------
  virtual XRootDStatus Write(uint64_t offset,
                             uint32_t size,
                             const void* buffer,
                             ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Sync
  //----------------------------------------------------------------------------
  virtual XRootDStatus Sync(ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Truncate
  //----------------------------------------------------------------------------
  virtual XRootDStatus Truncate(uint64_t size,
                                ResponseHandler* handler,
                                uint16_t timeout);


  //----------------------------------------------------------------------------
  //! VectorRead
  //----------------------------------------------------------------------------
  virtual XRootDStatus VectorRead(const ChunkList& chunks,
                                  void* buffer,
                                  ResponseHandler* handler,
                                  uint16_t timeout);


  //------------------------------------------------------------------------
  //! Fcntl
  //------------------------------------------------------------------------
  virtual XRootDStatus Fcntl(const Buffer& arg,
                             ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Visa
  //----------------------------------------------------------------------------
  virtual XRootDStatus Visa(ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! IsOpen
  //----------------------------------------------------------------------------
  virtual bool IsOpen() const;


  //----------------------------------------------------------------------------
  //! @see XrdCl::File::SetProperty
  //----------------------------------------------------------------------------
  virtual bool SetProperty(const std::string& name,
                           const std::string& value);


  //----------------------------------------------------------------------------
  //! @see XrdCl::File::GetProperty
  //----------------------------------------------------------------------------
  virtual bool GetProperty(const std::string& name,
                           std::string& value) const;


  //----------------------------------------------------------------------------
  //! @see XrdCl::File::GetDataServer
  //----------------------------------------------------------------------------
  virtual std::string GetDataServer() const;


  //----------------------------------------------------------------------------
  //! @see XrdCl::File::GetLastURL
  //----------------------------------------------------------------------------
  virtual URL GetLastURL() const;

private:
  //----------------------------------------------------------------------------
  //! Open the file in PIO mode i.e. ask the MGM for the stripe locations and
  //! contact all the stripes directly. For writes the client computes the
  //! parity and commits the file to the MGM at close, authorized by the PIO
  //! write capability handed out at open.
  //!
  //! @param url file URL
  //! @param is_write if true open for writing, otherwise for reading
  //! @param flags XrdCl open flags
  //! @param mode XrdCl access mode
  //!
  //! @return status of the operation
  //----------------------------------------------------------------------------
  XRootDStatus OpenPio(const std::string& url, bool is_write, OpenFlags::Flags flags,
                       Access::Mode mode);

  //----------------------------------------------------------------------------
  //! Close file written in PIO mode and commit it to the MGM
  //!
  //! @return status of the operation
  //----------------------------------------------------------------------------
  XRootDStatus ClosePioWrite();

  //----------------------------------------------------------------------------
  //! Compute the final checksum of the file written in PIO mode, rescanning
  //! the file if the writes were not sequential
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool FinalizeChecksum();

  //----------------------------------------------------------------------------
  //! Commit the file written in PIO mode to the MGM i.e. size, checksum and
  //! all the stripe locations in a single request
  //!
  //! @return status of the operation
  //----------------------------------------------------------------------------
  XRootDStatus CommitToMgm();

  //----------------------------------------------------------------------------
  //! Drop the file written in PIO mode from the MGM together with all its
  //! stripes - best effort
  //----------------------------------------------------------------------------
  void DropFromMgm();

  //----------------------------------------------------------------------------
  //! Send opaque query to the MGM
  //!
  //! @param query opaque query
  //!
  //! @return status of the operation
  //----------------------------------------------------------------------------
  XRootDStatus QueryMgm(const std::string& query);

  bool mIsOpen;
  std::string mUrl; ///< URL used at open, reported as LastURL in PIO mode
  XrdCl::File* pFile;
  eos::fst::RainMetaLayout* pRainFile;
  //! PIO write state i.e. the client acts as entry server
  bool mIsPioWrite{false};
  bool mHasWrite{false};    ///< File was modified (write or truncate)
  bool mHasWriteErr{false}; ///< There was a write error
  uint64_t mFileSize{0ull}; ///< Logical file size for PIO writes
  std::string mMgmEndpoint; ///< MGM endpoint i.e. root://host:port/
  std::string mMgmLogId;    ///< Log identifier handed out by the MGM
  //! Signed PIO write capability (cap.* fields) handed out by the MGM at open
  //! and sent back with the commit/drop requests to authorize them
  std::string mPioCapability;
  std::unique_ptr<eos::fst::CheckSum> mChecksum; ///< File checksum
};

EOSFSTNAMESPACE_END

#endif // __EOSFST_XRDCLPLUGINS_RAINFILEPLUGIN_HH__
