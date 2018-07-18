// ----------------------------------------------------------------------
// File: FileFsPath.hh
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

#ifndef __EOSCOMMON_FILEFSPATH_HH__
#define __EOSCOMMON_FILEFSPATH_HH__

/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "namespace/interface/IFileMD.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>

EOSCOMMONNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//! Class to handle a file's filesystem path.
//! Provides conversion functions to a file's physical path on a filesystem
//! based on file id or other metadata attributes.
//------------------------------------------------------------------------------

class FileFsPath
{
public:

  //----------------------------------------------------------------------------
  //! Builds the complete physical path for a file given a local filesystem
  //! prefix and the file path suffix.
  //----------------------------------------------------------------------------
  static void BuildPhysicalPath(const char* localPrefix,
                                const char* pathSuffix,
                                XrdOucString& physicalPath)
  {
    XrdOucString slocalPrefix = localPrefix;
    if (!slocalPrefix.endswith("/")) {
      slocalPrefix += "/";
    }

    physicalPath = slocalPrefix;
    physicalPath += pathSuffix;
    physicalPath.replace("//", "/", strlen(localPrefix) - 1);
  }

  //----------------------------------------------------------------------------
  //! Constructs file physical path for a given filesystem from file metadata.
  //! It will search through the extended attributes in search of a
  //! logical path mapping. If none are found, it returns the path obtained
  //! from the file id.
  //----------------------------------------------------------------------------
  static int GetPhysicalPath(unsigned long fsid,
                             std::shared_ptr<eos::IFileMD> &fmd,
                             XrdOucString& physicalPath)
  {
    if ((fsid < 0) || (!fmd)) {
      physicalPath = "";
      return -1;
    }

    std::map<unsigned long, std::string> map;
    std::string attributeString;
    bool useLogicalPath = false;

    if (fmd->hasAttribute("logicalpath")) {
      attributeString = fmd->getAttribute("logicalpath").c_str();

      // Check if filesystem id appears in attribute string
      std::string fsIdentifier = std::to_string(fsid);
      fsIdentifier += "|";
      int pos = attributeString.find(fsIdentifier.c_str());

      if ((pos != STR_NPOS) &&
          ((pos == 0) || (attributeString[pos - 1] == '&'))) {
        useLogicalPath = true;
      }
    }

    if (useLogicalPath) {
      map = attributeString2FsPathMap(attributeString.c_str());
      physicalPath = map[fsid].c_str();
    } else {
      FileId::FidPrefix2FullPath(FileId::Fid2Hex(fmd->getId()).c_str(), "path",
                                 physicalPath);
      physicalPath.erase(0, 5);
    }

    return 0;
  }

  //----------------------------------------------------------------------------
  //! Constructs complete file physical path for a given filesystem
  //! from file metadata together with a given local prefix.
  //----------------------------------------------------------------------------
  static int GetFullPhysicalPath(unsigned long fsid,
                                 std::shared_ptr<eos::IFileMD> &fmd,
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

    BuildPhysicalPath(localprefix, physicalPath.c_str(), fullPhysicalPath);
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Store a file's physical path for a given filesystem as extended attributes
  //! within the file's metadata.
  //----------------------------------------------------------------------------
  static void StorePhysicalPath(unsigned long fsid,
                                const std::shared_ptr<eos::IFileMD> &fmd,
                                std::string physicalPath)
  {
    std::string attributeString = "";

    if (fmd->hasAttribute("logicalpath")) {
      attributeString = fmd->getAttribute("logicalpath");
    }

    appendPair(fsid, physicalPath, attributeString);
    fmd->setAttribute("logicalpath", attributeString);
  }

private:

  //----------------------------------------------------------------------------
  //! Convert attribute string into a FilesystemId <-> PhysicalPath mapping
  //----------------------------------------------------------------------------
  static std::map<unsigned long, std::string>
  attributeString2FsPathMap(XrdOucString attributeString)
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
        fsid = atol(stoken.c_str());

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
  static std::string FsPathMap2AttributeString(
                             std::map<unsigned long, std::string> map)
  {
    std::string attributeString = "";

    for (auto it = map.begin(); it != map.end(); ++it) {
      attributeString += std::to_string(it->first);
      attributeString += "|";
      attributeString += it->second;
      attributeString += "&";
    }

    return attributeString.erase(attributeString.length() - 1);
  }

  //----------------------------------------------------------------------------
  //! Append FilesystemId <-> PhysicalPath pair to the attribute string
  //----------------------------------------------------------------------------
  static void appendPair(unsigned long fsid, std::string& physicalPath,
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
          attributeString2FsPathMap(attributeString.c_str());

      // Replace entry
      map[fsid] = physicalPath;

      // Construct string from map
      attributeString = FsPathMap2AttributeString(map);
    }
  }

};

EOSCOMMONNAMESPACE_END

#endif
