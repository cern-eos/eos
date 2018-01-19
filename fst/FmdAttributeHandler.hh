//------------------------------------------------------------------------------
//! @file FmdAttributeHandler.hh
//! @author Jozsef Makai<jmakai@cern.ch>
//! @brief Class to handle file meta data attribute operations
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

#ifndef EOS_FMDATTRIBUTEHANDLER_HH
#define EOS_FMDATTRIBUTEHANDLER_HH

#include "fst/Namespace.hh"
#include "fst/io/FileIo.hh"
#include "common/compression/ZStandard.hh"
#include "fst/XrdFstOfs.hh"

EOSFSTNAMESPACE_BEGIN

class FmdAttributeHandler : public eos::common::LogId {
protected:
  static constexpr auto mFmdAttrName = "user.eos.fmd"; //! file meta data attribute name constant
  FmdClient* const mFmdClient = nullptr; //! client for meta data operations
  eos::common::Compression* const mCompressor = nullptr; //! Compressor object for meta data compression

  std::map<eos::common::FileSystem::fsid_t, bool> mIsSyncing;

  //! @brief Creates file if not present and sets meta data
  //! @param fileIo io object of the file
  //! @param fmd the meta data object to set
  void CreateFileAndSetFmd(FileIo* fileIo, Fmd& fmd) const;

  //! @brief Check if there is a difference in the meta data status.
  //! @param oldFmd old meta data object
  //! @param newFmd new meta data object
  //! @return updated or not
  inline bool IsFmdUpdated(const Fmd& oldFmd, const Fmd& newFmd) const;

  //! @brief Calculates the full path of the physical file from file id and file system id.
  //! @param fid id of the file
  //! @param fsid id of the file system
  //! @param env environment object
  //! @return the full physical path of the file
  XrdOucString FullPathOfFile(eos::common::FileId::fileid_t fid,
                              eos::common::FileSystem::fsid_t fsid,
                              XrdOucEnv* env = nullptr) const;

public:
  //! @brief Retrieves the file meta data for the file stored as attributes.
  //! Throws @see MDException if operation was not successful.
  //! @param fileIo the file object
  //! @return the file meta data object if it's present otherwise throws an exception
  Fmd FmdAttrGet(FileIo* fileIo) const;

  //! @brief Retrieves the file meta data for the file stored as attributes.
  //! Throws @see MDException if operation was not successful.
  //! @param filePath FST path of the file
  //! @return the file meta data object if it's present otherwise throws an exception
  Fmd FmdAttrGet(const std::string& filePath) const;

  //! @brief Retrieves the file meta data for the file stored as attributes.
  //! Throws @see MDException if operation was not successful.
  //! @param fid id of the file
  //! @param fsid id of the file system
  //! @param env environment object, can be null
  //! @return the file meta data object if it's present otherwise throws an exception
  Fmd FmdAttrGet(eos::common::FileId::fileid_t fid,
                 eos::common::FileSystem::fsid_t fsid,
                 XrdOucEnv* env = nullptr) const;

  //! @brief Stores the file meta data for a file as an attribute.
  //! Throws @see MDException if operation was not successful.
  //! @param fileIo file Io object
  //! @param fmd meta data object to save
  void FmdAttrSet(FileIo* fileIo, const Fmd& fmd) const;

  //! @brief Sets file meta data attribute for file with fid and fsid
  //! @param fmd meta data object to save
  //! @param fid id of the file
  //! @param fsid id of the file system
  //! @param env environment object, can be null
  void FmdAttrSet(const Fmd& fmd,
                  eos::common::FileId::fileid_t fid,
                  eos::common::FileSystem::fsid_t fsid,
                  XrdOucEnv* env = nullptr) const;

  //! @brief Removes the meta data attribute for the file.
  //! Throws @see MDException if operation was not successful.
  //! @param fileIo the file Io object
  void FmdAttrDelete(FileIo* fileIo) const;

  //! @brief Retrieves file meta data for a particular file from the mgm if possible.
  //! @param fileIo pointer to the IO object representing the file
  //! @param fsid file system id
  //! @param fid file id
  //! @param manager address of the manager
  //! @return boolean value whether it was successful or not
  bool ResyncMgm(FileIo* fileIo, eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager) const;

  //! @brief Retrieves file meta data for a particular file from the mgm if possible.
  //! @param filePath path of the file on FST
  //! @param fsid id of the file system
  //! @param fid id of the file
  //! @param manager address of the manager
  //! @return boolean value whether it was successful or not
  bool ResyncMgm(const std::string& filePath, eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager) const;

  //! @brief Retrieves file meta data for a particular file from the mgm if possible.
  //! @param fsid id of the file system
  //! @param fid id of the file
  //! @param manager address of the manager
  //! @return boolean value whether it was successful or not
  bool ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager) const;

  //! @brief Resync all files on a file system from the MGM.
  //! @param fsid id of the file system
  //! @param manager address of the manager
  //! @return boolean value whether it was successful or not
  bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid, const char* manager);

  //! @brief Resync meta data from the disk
  //! @param path path of the file on the FST
  //! @param fsid id of the file system
  //! @param flaglayouterror there is a layout error or not
  //! @return successful or not
  bool ResyncDisk(const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror) const;

  //! @brief Resync meta data from the disk for files under a certain path
  //! @param path the path under which meta data will be resynced
  //! @param fsid id of the file system
  //! @param flaglayouterror there is a layout error or not
  //! @return successful or not
  bool ResyncAllDisk(const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror);

  //! @brief Reports inconsistencies for the file
  //! @param filePath path of the file on the FST
  //! @param fid id of the file
  //! @param fsid id of the file system
  void ReportFmdInconsistency(const std::string& filePath, eos::common::FileId::fileid_t fid,
                              eos::common::FileSystem::fsid_t fsid) const;

  //! @brief Reports inconsistencies for the file according to its meta data
  //! @param fmd the meta data object of the file
  void ReportFmdInconsistency(const Fmd& fmd) const;

  //! Constructor to create the handler object
  //! @param fmdClient pointer to the @see FmdClient object, default value is the globally available client object
  explicit FmdAttributeHandler(eos::common::Compression* compressor = &(gOFS.fmdCompressor), FmdClient* fmdClient = &gFmdClient)
    : mFmdClient(fmdClient), mCompressor(compressor) {}

  ~FmdAttributeHandler() override = default;

  FmdAttributeHandler(FmdAttributeHandler&) = delete;

  FmdAttributeHandler& operator=(FmdAttributeHandler&) = delete;

  FmdAttributeHandler(FmdAttributeHandler&&) = delete;

  FmdAttributeHandler& operator=(FmdAttributeHandler&&) = delete;
};

extern FmdAttributeHandler gFmdAttributeHandler;

EOSFSTNAMESPACE_END

#endif //EOS_FMDATTRIBUTEHANDLER_HH
