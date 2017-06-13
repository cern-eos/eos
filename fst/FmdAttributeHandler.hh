//------------------------------------------------------------------------------
//! \file FmdAttributeHandler.hh
//! \author Jozsef Makai<jmakai@cern.ch>
//! \brief Class to handle file meta data attribute operations
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

EOSFSTNAMESPACE_BEGIN

class FmdAttributeHandler : public eos::common::LogId {
protected:
  static constexpr auto fmdAttrName = "user.eos.fmd"; //! file meta data attribute name constant
  FmdClient* const fmdClient = nullptr; //! client for meta data operations

  std::map<eos::common::FileSystem::fsid_t, bool> isDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> stayDirty;

  std::map<eos::common::FileSystem::fsid_t, bool> isSyncing;

  void CreateFileAndSetFmd(FileIo* fileIo, Fmd& fmd, eos::common::FileSystem::fsid_t fsid) const;

  XrdOucString fullPathOfFile(eos::common::FileId::fileid_t fid,
                              eos::common::FileSystem::fsid_t fsid,
                              XrdOucEnv* env = nullptr) const;

public:
  //! Retrieves the file meta data for the file stored as attributes.
  //! Throws \see fmd_attribute_error if operation was not successful.
  //! \param fileIo the file object
  //! \return the file meta data object if it's present otherwise throws an exception
  Fmd FmdAttrGet(FileIo* fileIo) const;

  //! Retrieves the file meta data for the file stored as attributes.
  //! Throws \see fmd_attribute_error if operation was not successful.
  //! \param filePath FST path of the file
  //! \return the file meta data object if it's present otherwise throws an exception
  Fmd FmdAttrGet(const std::string& filePath) const;

  //! Retrieves the file meta data for the file stored as attributes.
  //! Throws \see fmd_attribute_error if operation was not successful.
  //! \param fid id of the file
  //! \param fsid id of the file system
  //! \param env environment object, can be null
  //! \return the file meta data object if it's present otherwise throws an exception
  Fmd FmdAttrGet(eos::common::FileId::fileid_t fid,
                 eos::common::FileSystem::fsid_t fsid,
                 XrdOucEnv* env = nullptr) const;

  //! Stores the file meta data for a file as an attribute.
  //! Throws \see fmd_attribute_error if operation was not successful.
  //! \param fileIo file Io object
  //! \param fmd meta data object to save
  void FmdAttrSet(FileIo* fileIo, const Fmd& fmd) const;

  //!
  //! \param fmd meta data object to save
  //! \param fid id of the file
  //! \param fsid id of the file system
  //! \param env environment object, can be null
  void FmdAttrSet(const Fmd& fmd,
                  eos::common::FileId::fileid_t fid,
                  eos::common::FileSystem::fsid_t fsid,
                  XrdOucEnv* env = nullptr) const;

  //! Removes the meta data attribute for the file.
  //! Throws \see fmd_attribute_error if operation was not successful.
  //! \param fileIo the file Io object
  void FmdAttrDelete(FileIo* fileIo) const;

  //! Retrieves file meta data for a particular file from the mgm if possible.
  //! \param fileIo pointer to the IO object representing the file
  //! \param fsid file system id
  //! \param fid file id
  //! \param manager address of the manager
  //! \return boolean value whether it was successful or not
  bool ResyncMgm(FileIo* fileIo, eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager) const;

  //! Retrieves file meta data for a particular file from the mgm if possible.
  //! \param filePath path of the file on FST
  //! \param fsid id of the file system
  //! \param fid id of the file
  //! \param manager address of the manager
  //! \return boolean value whether it was successful or not
  bool ResyncMgm(const std::string& filePath, eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager) const;

  //! Retrieves file meta data for a particular file from the mgm if possible.
  //! \param fsid id of the file system
  //! \param fid id of the file
  //! \param manager address of the manager
  //! \return boolean value whether it was successful or not
  bool ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager) const;

  //! Resync all fiels on a file system from the MGM.
  //! \param fsid id of the file system
  //! \param manager address of the manager
  //! \return boolean value whether it was successful or not
  bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid, const char* manager);

  bool ResyncDisk(const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror) const;

  bool ResyncAllDisk(const char* path, eos::common::FileSystem::fsid_t fsid,  bool flaglayouterror);

  //! Constructor to create the handler object
  //! \param fmdClient pointer to the \see FmdClient object, default value is the globally available client object
  explicit FmdAttributeHandler(FmdClient* fmdClient = &gFmdClient): fmdClient(fmdClient) {}

  virtual ~FmdAttributeHandler() {};

  FmdAttributeHandler (FmdAttributeHandler&) = delete;

  FmdAttributeHandler& operator= (FmdAttributeHandler&) = delete;

  FmdAttributeHandler (FmdAttributeHandler&&) = delete;

  FmdAttributeHandler& operator= (FmdAttributeHandler&&) = delete;
};

struct fmd_attribute_error : public std::runtime_error {
  explicit fmd_attribute_error(const std::string& message) : std::runtime_error {message} {}
};

extern FmdAttributeHandler gFmdAttributeHandler;

EOSFSTNAMESPACE_END

#endif //EOS_FMDATTRIBUTEHANDLER_HH
