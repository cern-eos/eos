//------------------------------------------------------------------------------
// File: DirEos.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

/*-----------------------------------------------------------------------------*/
#include "DirEos.hh"
#include "Configuration.hh"
/*-----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*-----------------------------------------------------------------------------*/


EOSBMKNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
DirEos::DirEos(const std::string& dirPath, const std::string& eosInstance):
    eos::common::LogId(),
    mDirPath(dirPath),
    mFs(NULL)
{
  XrdCl::URL url(eosInstance);

  if (!url.IsValid())
  {
    eos_err("URL is not valid");
    exit(-1);
  }

  mFs = new XrdCl::FileSystem(url);

  if (!mFs)
  {
    eos_err("Error while trying to get XrdCl::FileSystem object");
    exit(-1);
  }    
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DirEos::~DirEos()
{
  delete mFs;
}


//------------------------------------------------------------------------------
// Check if directory exists
//------------------------------------------------------------------------------
bool
DirEos::Exist()
{
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = mDirPath;
  request += "?";
  request += "mgm.pcmd=stat";
  arg.FromString(request);
  XrdCl::XRootDStatus status = mFs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];
    int items = sscanf(response->GetBuffer(),
                       "%s %llu %llu %llu %llu %llu %llu %llu %llu "
                       "%llu %llu %llu %llu %llu %llu %llu %llu",
                       tag, (unsigned long long*) &sval[0],
                       (unsigned long long*) &sval[1],
                       (unsigned long long*) &sval[2],
                       (unsigned long long*) &sval[3],
                       (unsigned long long*) &sval[4],
                       (unsigned long long*) &sval[5],
                       (unsigned long long*) &sval[6],
                       (unsigned long long*) &sval[7],
                       (unsigned long long*) &sval[8],
                       (unsigned long long*) &sval[9],
                       (unsigned long long*) &ival[0],
                       (unsigned long long*) &ival[1],
                       (unsigned long long*) &ival[2],
                       (unsigned long long*) &ival[3],
                       (unsigned long long*) &ival[4],
                       (unsigned long long*) &ival[5]);

    if ((items != 17) || (strcmp(tag, "stat:")))
    {
      delete response;
      return false;
    }
    else {
      return true;
    }
  }
  else {
    delete response;
    return false;
  }
}

//------------------------------------------------------------------------------
// Set extended attribute
//------------------------------------------------------------------------------
bool
DirEos::SetXattr(const std::string& attrName, const std::string& attrValue)
{
  bool retc = true;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = mDirPath;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=set&";
  request += "mgm.xattrname=";
  request += attrName;
  request += "&";
  request += "mgm.xattrvalue=";
  request += attrValue;
  arg.FromString(request);
  XrdCl::XRootDStatus status = mFs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK())
  {
    int ret;
    int items = 0;
    char tag[1024];

    // Parse output
    items = sscanf(response->GetBuffer(), "%s retc=%i", tag, &ret);

    if ((items != 2) || (strcmp(tag, "setxattr:")))
    {
      retc = false;
    }
  }
  else
  {
    retc = false;
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
// Create directory
//------------------------------------------------------------------------------
bool
DirEos::Create()
{
  mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP;
  XrdCl::Access::Mode mode_xrdcl = eos::common::LayoutId::MapModeSfs2XrdCl(mode);
  XrdCl::XRootDStatus status = mFs->MkDir(mDirPath,
                                          XrdCl::MkDirFlags::MakePath,
                                          mode_xrdcl);
  
  return status.IsOK();
}


//------------------------------------------------------------------------------
// Check that the extended attribute matched the reference value
//------------------------------------------------------------------------------
bool
DirEos::CheckXattr(const std::string& attrName, const std::string& refValue)
{
  bool retc = true;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = mDirPath;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=get&";
  request += "mgm.xattrname=";
  request += attrName;
  arg.FromString(request);
  XrdCl::XRootDStatus status = mFs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK())
  {
    int ret;
    int items = 0;
    char tag[1024];
    char rval[4096];

    // Parse output
    items = sscanf(response->GetBuffer(), "%s retc=%i value=%s", tag, &ret, rval);

    if ((items != 3) || (strcmp(tag, "getxattr:")))
    {
      fprintf(stderr, "[%s] Directory does not have the required xattr.\n", __FUNCTION__);
      retc = false;
    }
    else
    {
      std::string attr_value = rval;
      
      if (attr_value.compare(refValue))
      {
        // Attr value is different from refValue
        retc = false;
      }
    }
  }
  else
  {
    retc = false;
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
// Get files form benchmark directory having the requried file size 
//------------------------------------------------------------------------------
std::vector<std::string>
DirEos::GetMatchingFiles(const uint64_t fileSize)
{
  std::vector<std::string> vect_filenames;
  XrdCl::DirectoryList* response = 0;
  XrdCl::DirListFlags::Flags flags = XrdCl::DirListFlags::Stat;
  XrdCl::XRootDStatus status = mFs->DirList(mDirPath, flags, response);
  std::string full_path = "";

  if (status.IsOK())
  {
    for (XrdCl::DirectoryList::ConstIterator iter = response->Begin();
         iter != response->End();
         ++iter)
    {
      XrdCl::DirectoryList::ListEntry* list_entry =
          static_cast<XrdCl::DirectoryList::ListEntry*> (*iter);
      if (list_entry->GetStatInfo()->GetSize() == fileSize)
      {
        full_path = mDirPath;
        full_path += list_entry->GetName();
        vect_filenames.push_back(full_path);
      }
    }
  }

  return vect_filenames;
}


//------------------------------------------------------------------------------
// Check if directory matches with the supplied low level configuration
//------------------------------------------------------------------------------
bool
DirEos::MatchConfig(const ConfigProto& llconfig)
{
  if (!CheckXattr("user.admin.forced.layout",
                  Configuration::GetFileLayout(llconfig.filelayout())))
  {
    eos_warning("Directory attributes do not match with configuration");
    return false;
  }

  // If this is a replica file type we check the number of preplicas
  if (llconfig.filelayout() == ConfigProto_FileLayoutType_REPLICA)
  {
    if (!CheckXattr("user.admin.forced.nstripes",
                    std::to_string((long long int)llconfig.noreplicas())))
    {
      eos_warning("Number of replicas does not match with configuration");
      return false;
    }
  }
  
  return true;
}


//------------------------------------------------------------------------------
// Set the extended attributes of the directory so that they match the config
//------------------------------------------------------------------------------
bool
DirEos::SetConfig(const ConfigProto& llconfig)
{
  // OBS: These predefined configuration are the ones that we expect to be
  //      used in production and therefore we set them like this
  bool ret = true;
  
  if (llconfig.filelayout() == ConfigProto_FileLayoutType_PLAIN)
  {
    ret  = SetXattr("user.admin.forced.layout", "plain");
    ret |= SetXattr("user.admin.forced.checksum", "adler");
    ret |= SetXattr("user.admin.forced.blockchecksum", "crc32c");
    ret |= SetXattr("user.admin.forced.blocksize", "4K");
 
  }
  else if (llconfig.filelayout() == ConfigProto_FileLayoutType_REPLICA)
  {
    ret  = SetXattr("user.admin.forced.layout", "replica");
    ret |= SetXattr("user.admin.forced.nstripes", std::to_string((long long int)llconfig.noreplicas()));
    ret |= SetXattr("user.admin.forced.checksum", "adler");
    ret |= SetXattr("user.admin.forced.blockchecksum", "crc32c");
    ret |= SetXattr("user.admin.forced.blocksize", "1M");
  }
  else if (llconfig.filelayout() == ConfigProto_FileLayoutType_ARCHIVE)
  {
    ret  = SetXattr("user.admin.forced.layout", "archive");
    ret |= SetXattr("user.admin.forced.blockchecksum", "crc32c");
    ret |= SetXattr("user.admin.forced.blocksize", "1M");
  }
  else if (llconfig.filelayout() == ConfigProto_FileLayoutType_RAIDDP)
  {
    ret  = SetXattr("user.admin.forced.layout", "raiddp");
    ret |= SetXattr("user.admin.forced.nstripes", "6");
    ret |= SetXattr("user.admin.forced.blockchecksum", "crc32c");
    ret |= SetXattr("user.admin.forced.blocksize", "1M");
  }
  else if (llconfig.filelayout() == ConfigProto_FileLayoutType_RAID6)
  {
    ret  = SetXattr("user.admin.forced.layout", "raid6");
    ret |= SetXattr("user.admin.forced.nstripes", "6");
    ret |= SetXattr("user.admin.forced.blockchecksum", "crc32c");
    ret |= SetXattr("user.admin.forced.blocksize", "1M");
  }

  if (!ret) {
    cerr << "Error while trying to set extended attributes." << endl;
    return ret;
  }
  
  return ret;
}


//------------------------------------------------------------------------------
//! Remove directory
//------------------------------------------------------------------------------
bool
DirEos::Remove()
{
  XrdCl::XRootDStatus status = mFs->RmDir(mDirPath);
  return status.IsOK();
}

EOSBMKNAMESPACE_END

