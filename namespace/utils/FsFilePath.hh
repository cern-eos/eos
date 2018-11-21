// ----------------------------------------------------------------------
// File: FsFilePath.hh
// Author: Mihai Patrascoiu - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef EOS_NS_FSFILEPATH_HH
#define EOS_NS_FSFILEPATH_HH

/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
#include "namespace/interface/IFileMD.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class which handles file paths on a given filesystem.
//! Provides conversion functions to a file's physical path on a filesystem
//! based on file id or other metadata attributes.
//------------------------------------------------------------------------------

class FsFilePath
{
public:

  //----------------------------------------------------------------------------
  //! Checks whether a file has a logical path mapping for a given filesystem
  //! from the file's extended attributes metadata.
  //----------------------------------------------------------------------------
  static bool HasLogicalPath(unsigned long fsid,
                             const std::shared_ptr<eos::IFileMD>& fmd)
  {
    XrdOucString attributeString;
    bool hasLogicalPath = false;

    if (fmd->hasAttribute("sys.eos.lpath")) {
      attributeString = fmd->getAttribute("sys.eos.lpath").c_str();

      // Check if filesystem id appears in attribute string
      std::string fsIdentifier = std::to_string(fsid);
      fsIdentifier += "|";
      int pos = attributeString.find(fsIdentifier.c_str());

      if ((pos != STR_NPOS) &&
          ((pos == 0) || (attributeString[pos - 1] == '&'))) {
        hasLogicalPath = true;
      }
    }

    return hasLogicalPath;
  }

  //----------------------------------------------------------------------------
  //! Constructs file physical path for a given filesystem from file metadata.
  //! It will search through the extended attributes in search of a
  //! logical path mapping. If none are found, it returns the path obtained
  //! from the file id.
  //----------------------------------------------------------------------------
  static int GetPhysicalPath(unsigned long fsid,
                             const std::shared_ptr<eos::IFileMD>& fmd,
                             XrdOucString& physicalPath)
  {
    if (!fmd) {
      physicalPath = "";
      return -1;
    }

    bool hasLogicalPath = HasLogicalPath(fsid, fmd);

    if (hasLogicalPath) {
      std::map<unsigned long, std::string> map;
      std::string attributeString = fmd->getAttribute("sys.eos.lpath");

      map = attributeStringToFsPathMap(attributeString.c_str());
      physicalPath = map[fsid].c_str();
    } else {
      using eos::common::FileId;
      FileId::FidPrefix2FullPath(FileId::Fid2Hex(fmd->getId()).c_str(),
                                 "path", physicalPath);
      physicalPath.erase(0, 5);
    }

    return 0;
  }

  //----------------------------------------------------------------------------
  //! Constructs complete file physical path for a given filesystem
  //! from file metadata together with a given local prefix.
  //----------------------------------------------------------------------------
  static int GetFullPhysicalPath(unsigned long fsid,
                                 const std::shared_ptr<eos::IFileMD>& fmd,
                                 const char* localprefix,
                                 XrdOucString& fullPhysicalPath)
  {
    if (!localprefix) {
      fullPhysicalPath = "";
      return -1;
    }

    XrdOucString physicalPath;
    int rc = GetPhysicalPath(fsid, fmd, physicalPath);
    if (rc) {
      fullPhysicalPath = "";
      return -1;
    }

    using eos::common::StringConversion;
    fullPhysicalPath = StringConversion::BuildPhysicalPath(localprefix,
                                                           physicalPath.c_str());
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Store a file's physical path for a given filesystem as extended attributes
  //! within the file's metadata.
  //!
  //! This function changes the file's metadata and should be called
  //! in a thread-safe context.
  //----------------------------------------------------------------------------
  static void StorePhysicalPath(unsigned long fsid,
                                std::shared_ptr<eos::IFileMD>& fmd,
                                const std::string physicalPath)
  {
    std::string attributeString = "";

    if (fmd->hasAttribute("sys.eos.lpath")) {
      attributeString = fmd->getAttribute("sys.eos.lpath");
    }

    appendPair(fsid, physicalPath, attributeString);
    fmd->setAttribute("sys.eos.lpath", attributeString);
  }

  //----------------------------------------------------------------------------
  //! Remove a file's physical path for a given filesystem from the file's
  //! extended attributes metadata.
  //! Upon removal, if the extended attribute values becomes empty,
  //! it will be removed entirely from the metadata.
  //!
  //! This function changes the file's metadata and should be called
  //! in a thread-safe context.
  //----------------------------------------------------------------------------
  static void RemovePhysicalPath(unsigned long fsid,
                                 std::shared_ptr<eos::IFileMD>& fmd)
  {
    if (fmd->hasAttribute("sys.eos.lpath")) {
      std::string attributeString = fmd->getAttribute("sys.eos.lpath");
      std::map<unsigned long, std::string> map =
          attributeStringToFsPathMap(attributeString.c_str());

      if (map.count(fsid)) {
        map.erase(fsid);

        if (map.size()) {
          attributeString = fsPathMapToAttributeString(map);
          fmd->setAttribute("sys.eos.lpath", attributeString);
        } else {
          fmd->removeAttribute("sys.eos.lpath");
        }
      }
    }
  }

private:

  //----------------------------------------------------------------------------
  //! Convert attribute string into a FilesystemId <-> PhysicalPath mapping
  //----------------------------------------------------------------------------
  static std::map<unsigned long, std::string>
  attributeStringToFsPathMap(XrdOucString attributeString)
  {
    std::map<unsigned long, std::string> map;
    std::string physicalPath;
    unsigned long fsid;

    while (attributeString.replace("&", " "));
    XrdOucTokenizer subtokenizer((char*) attributeString.c_str());
    subtokenizer.GetLine();
    const char* token;

    // Parse token for filesystem id and path -- format fsid|path
    while ((token = subtokenizer.GetToken())) {
      XrdOucString stoken = token;
      int pos = stoken.find("|");

      if (pos != STR_NPOS) {
        // Extract filesystem id
        stoken.keep(0, pos);
        fsid = (unsigned long) atol(stoken.c_str());

        // Extract physical path
        physicalPath = token + pos + 1;

        // Add to map
        map[fsid] = physicalPath;
      } else {
        eos_static_err("msg=\"parsing token failed\" token=%s "
                       "attribute_string=%s", token, attributeString.c_str());
      }
    }

    return map;
  }

  //----------------------------------------------------------------------------
  //! Convert a FilesystemId <-> PhysicalPath mapping into an attribute string
  //----------------------------------------------------------------------------
  static std::string
  fsPathMapToAttributeString(const std::map<unsigned long, std::string> map)
  {
    std::string attributeString = "";

    for (auto& pair: map) {
      attributeString += std::to_string(pair.first);
      attributeString += "|";
      attributeString += pair.second;
      attributeString += "&";
    }

    return attributeString.erase(attributeString.length() - 1);
  }

  //----------------------------------------------------------------------------
  //! Append FilesystemId <-> PhysicalPath pair to the attribute string
  //----------------------------------------------------------------------------
  static void appendPair(unsigned long fsid,
                         const std::string physicalPath,
                         std::string& attributeString)
  {
    std::string pair = to_string(fsid);
    pair += "|";

    // Avoid map construction if this is an addition operation
    // instead of a replacement operation
    if (attributeString.find(pair) == std::string::npos) {
      pair += physicalPath;

      if (attributeString.empty()) {
        attributeString = pair;
      } else {
        attributeString += '&' + pair;
      }
    } else {
      // Construct map
      std::map<unsigned long, std::string> map =
          attributeStringToFsPathMap(attributeString.c_str());

      // Replace entry
      map[fsid] = physicalPath;

      // Construct string from map
      attributeString = fsPathMapToAttributeString(map);
    }
  }

};

EOSNSNAMESPACE_END

#endif // EOS_NS_FSFILEPATH_HH
