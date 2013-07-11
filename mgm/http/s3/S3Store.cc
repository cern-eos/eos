// ----------------------------------------------------------------------
// File: S3Store.cc
// Author: Andreas-Joachim Peters & Justin Lewis Salmon - CERN
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

/*----------------------------------------------------------------------------*/
#include "mgm/http/s3/S3Store.hh"
#include "mgm/http/s3/S3Handler.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define XML_V1_UTF8 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"

/*----------------------------------------------------------------------------*/
S3Store::S3Store (const char *s3defpath)
{
  mS3DefContainer = s3defpath;
  mStoreModificationTime = 1;
  mStoreReloadTime = 1;
}

/*----------------------------------------------------------------------------*/
void
S3Store::Refresh ()
{
  // Refresh the S3 id, keys, container definitions
  time_t now = time(NULL);
  time_t srtime;
  {
    eos::common::RWMutexReadLock sLock(mStoreMutex);
    srtime = mStoreReloadTime;
  }

  // max. enter once per minute this code branch
  if ((now - srtime) > 60)
  {
    eos::common::RWMutexWriteLock sLock(mStoreMutex);

    mStoreReloadTime = now;
    XrdOucErrInfo error;
    eos::common::Mapping::VirtualIdentity vid;
    eos::common::Mapping::Root(vid);
    eos::ContainerMD::XAttrMap map;
    struct stat buf;
    if (gOFS->_stat(mS3DefContainer.c_str(), &buf, error, vid, (const char*) 0) == SFS_OK)
    {
      // check last modification time
      if (buf.st_mtime != mStoreModificationTime)
      {
        // clear all
        mS3ContainerSet.clear();
        mS3Keys.clear();
        mS3ContainerPath.clear();
        if (gOFS->_attr_ls(mS3DefContainer.c_str(), error, vid, 0, map) != SFS_OK)
        {
          eos_static_err("unable to list attributes of % s", mS3DefContainer.c_str());
        }
        else
        {
          // parse the attributes into the store
          for (auto it = map.begin(); it != map.end(); it++)
          {
            eos_static_info("parsing %s=>%s", it->first.c_str(), it->second.c_str());
            if (it->first.substr(0, 6) == "sys.s3")
            {
              // the s3 attributes are built as
              // sys.s3.id.<id> => secret key
              // sys.s3.bucket.<id> => bucket list
              // sys.s3.path.<bucket> => path
              if (it->first.substr(0, 10) == "sys.s3.id.")
              {
                std::string id = it->first.substr(10);
                mS3Keys[id] = it->second;
                eos_static_info("id=%s key=%s", id.c_str(), it->second.c_str());
              }
              if (it->first.substr(0, 14) == "sys.s3.bucket.")
              {
                std::string id = it->first.substr(14);
                std::vector<std::string> svec;
                eos::common::StringConversion::Tokenize(it->second, svec, "|");
                for (size_t i = 0; i < svec.size(); i++)
                {
                  if (svec[i][0] == '\"')
                  {
                    svec[i].erase(0, 1);
                  }

                  if (svec[i][svec[i].length() - 1] == '\"')
                  {
                    svec[i].erase(svec[i].length() - 1);
                  }
                  mS3ContainerSet[id].insert(svec[i]);
                  eos_static_debug("id=%s bucket=%s", id.c_str(), svec[i].c_str());
                }
              }
              if (it->first.substr(0, 12) == "sys.s3.path.")
              {
                std::string bucket = it->first.substr(12);
                mS3ContainerPath[bucket] = it->second;
                eos_static_info("bucket=%s path=%s", bucket.c_str(), it->second.c_str());
              }
            }
          }
          // store the modification time of the loaded s3 definitions
          mStoreModificationTime = buf.st_mtime;
        }
      }
    }
    else
    {
      eos_static_err("unable to stat %s", mS3DefContainer.c_str());
    }
  }
  else
  {
    eos_static_info("skipping reload");
  }
}

/*----------------------------------------------------------------------------*/
bool
S3Store::VerifySignature (S3Handler& s3)
{
  if (!mS3Keys.count(s3.getId()))
  {
    eos_static_err("msg=\"no such account\" id=%s", s3.getId().c_str());
    return false;
  }

  return s3.VerifySignature(mS3Keys[s3.getId()]);
}

/*----------------------------------------------------------------------------*/
std::string
S3Store::ListBuckets (int                                &response_code,
                      S3Handler                          &s3,
                      std::map<std::string, std::string> &response_header)
{
  eos::common::RWMutexReadLock sLock(mStoreMutex);

  response_header["Content-Type"] = "application/xml";
  response_header["x-amz-id-2"] = "unknown";
  response_header["x-amz-request-id"] = "unknown";

  std::string result = XML_V1_UTF8;
  result += "<ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">";
  result += "<Owner><ID>";
  result += s3.getId();
  result += "</ID>";
  result += "<Display>";
  result += s3.getId();
  result += "</Display>";
  result += "</Owner>";
  result += "<Buckets>";
  for (auto it = mS3ContainerSet[s3.getId()].begin(); it != mS3ContainerSet[s3.getId()].end(); it++)
  {
    if (mS3ContainerPath.count(*it))
    {
      // check if we know how to map a bucket name into our regular namespace
      std::string bucketpath = mS3ContainerPath[*it];

      XrdOucErrInfo error;
      eos::common::Mapping::VirtualIdentity vid;
      eos::common::Mapping::Root(vid);
      eos::ContainerMD::XAttrMap map;
      struct stat buf;
      if (gOFS->_stat(bucketpath.c_str(), &buf, error, vid, (const char*) 0) == SFS_OK)
      {
        result += "<Bucket>";
        result += "<Name>";
        result += *it;
        result += "</Name>";
        result += "<CreationDate>";
        result += eos::common::Timing::UnixTimstamp_to_ISO8601(buf.st_ctime);
        result += "</CreationDate>";
        result += "</Bucket>";
      }
      else
      {
        std::string errmsg = "cannot find bucket path ";
        errmsg += bucketpath;
        errmsg += " for bucket ";
        errmsg += *it;
        return s3.RestErrorResponse(response_code, 404, "NoSuchBucket", errmsg, *it, "");
      }
    }
  }
  result += "</Buckets>";
  result += "</ListAllMyBucketsResult>";
  return result;
}

/*----------------------------------------------------------------------------*/
std::string
S3Store::ListBucket (int                                &response_code,
                     S3Handler                          &s3,
                     std::map<std::string, std::string> &response_header)
{
  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);

  eos::common::RWMutexReadLock sLock(mStoreMutex);

  if (!mS3ContainerPath.count(s3.getBucket()))
  {
    // check if this bucket is configured
    return s3.RestErrorResponse(response_code, 404, "NoSuchBucket",
        "Bucket does not exist!", s3.getBucket().c_str(), "");
  }
  else
  {
    // check if this bucket is mapped
    struct stat buf;
    if (gOFS->_stat(mS3ContainerPath[s3.getBucket()].c_str(), &buf, error, vid, (const char*) 0) != SFS_OK)
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchBucket", "Bucket is not mapped into the namespace!", s3.getBucket().c_str(), "");
    }
  }

  std::string result = XML_V1_UTF8;
  result += "<ListBucketResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">";
  result += "<Name>";
  result += s3.getBucket();
  result += "</Name>";

  XrdOucEnv parameter(s3.getQuery().c_str());

  XrdOucString stdErr;

  XrdMgmOfsDirectory bucketdir;

  /*/if (gOFS->_find(mS3ContainerPath[s3.getBucket()].c_str(), error, stdErr, vid, find, false) != SFS_OK)
  {
    return s3.RestErrorResponse(response_code, 404, "NoSuchBucket", "Bucket couldn't be queried in the namespace!", s3.getBucket().c_str(), "");
  }
   */


  uint64_t cnt = 0;
  uint64_t max_keys = 1000;
  std::string marker = "";
  std::string prefix = "";

  bool marker_found = true; // indicates that the given marker has been found in the list and output starts

  const char* val = 0;
  if ((val = parameter.Get("max-keys")))
  {
    max_keys = strtoull(val, 0, 10);
  }
  if ((val = parameter.Get("marker")))
  {
    marker = val;
    if (marker == "(null)")
    {
      marker = "";
    }
  }
  if ((val = parameter.Get("prefix")))
  {
    prefix = val;
  }
  if (marker.length())
  {
    marker_found = false;
  }

  std::string lPrefix = prefix;
  if (prefix == "")
  {
    lPrefix = "/";
  }

  eos_static_info("msg=\"listing\" bucket=%s prefix=%s", s3.getBucket().c_str(), lPrefix.c_str());

  if (!prefix.length())
  {
    result += "<Prefix/>";
  }
  else
  {
    result += "<Prefix>";
    result += prefix;
    result += "</Prefix>";
  }

  if ((!marker.length() || (!marker.c_str())))
  {
    result += "<Marker/>";
  }
  else
  {
    result += "<Marker>";
    result += marker;
    result += "</Marker>";
  }

  result += "<MaxKeys>";
  char smaxkeys[16];
  snprintf(smaxkeys, sizeof (smaxkeys) - 1, "%llu", (unsigned long long) max_keys);
  result += smaxkeys;
  result += "</MaxKeys>";

  bool truncated = false;

  size_t truncate_pos = result.length() + 13;
  result += "<IsTruncated>false</IsTruncated>";

  XrdOucString sPrefix = lPrefix.c_str();
  if (!sPrefix.endswith("/"))
  {
    lPrefix += "/";
  }

  int listrc = bucketdir.open((mS3ContainerPath[s3.getBucket()] + lPrefix).c_str(), vid, (const char*) 0);

  if (!listrc)
  {
    const char* dname1 = 0;

    while ((dname1 = bucketdir.nextEntry()))
    {
      // loop over the directory contents   

      std::string sdname = dname1;
      if ((sdname == ".") || (sdname == ".."))
      {
        continue;
      }

      if (cnt > max_keys)
      {
        truncated = true;
        // don't return more than max-keys
        break;
      }
      std::string objectname = lPrefix;

      std::string fullname;
      objectname += sdname;
      fullname = mS3ContainerPath[s3.getBucket()];
      fullname += objectname;

      if (!marker_found)
      {
        if (marker == objectname)
        {
          marker_found = true;
        }
        continue;
      }

      // get the file md object
      gOFS->eosViewRWMutex.LockRead();
      eos::FileMD* fmd = 0;
      try
      {

        fmd = gOFS->eosView->getFile(fullname.c_str());
        eos::FileMD fmdCopy(*fmd);
        fmd = &fmdCopy;
        gOFS->eosViewRWMutex.UnLockRead();
        //-------------------------------------------

        result += "<Contents>";
        result += "<Key>";
        result += objectname.c_str();
        result += "</Key>";
        result += "<LastModified>";

        eos::FileMD::ctime_t mtime;
        fmd->getMTime(mtime);

        result += eos::common::Timing::UnixTimstamp_to_ISO8601(mtime.tv_sec);
        result += "</LastModified>";
        result += "<ETag>";
        for (unsigned int i = 0; i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++)
        {
          char hb[3];
          sprintf(hb, "%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
          result += hb;
        }
        result += "</ETag>";
        result += "<Size>";
        std::string sconv;
        result += eos::common::StringConversion::GetSizeString(sconv, (unsigned long long) fmd->getSize());
        result += "</Size>";
        result += "<StorageClass>STANDARD</StorageClass>";
        result += "<Owner>";
        result += "<ID>";
        int errc = 0;
        result += eos::common::Mapping::UidToUserName(fmd->getCUid(), errc);
        result += "</ID>";
        result += "<DisplayName>";
        result += eos::common::Mapping::UidToUserName(fmd->getCUid(), errc);
        result += ":";
        result += eos::common::Mapping::GidToGroupName(fmd->getCGid(), errc);
        result += "</DisplayName>";
        result += "</Owner>";
        result += "</Contents>";
      }
      catch (eos::MDException &e)
      {
        gOFS->eosViewRWMutex.UnLockRead();
        //-------------------------------------------
      }

      if (!fmd)
      {
        // TODO: add the real container info here ... 
        // this must be a container
        result += "<Contents>";
        result += "<Key>";
        result += objectname.c_str();
        result += "/";
        result += "</Key>";
        result += "<LastModified>";
        result += eos::common::Timing::UnixTimstamp_to_ISO8601(time(NULL));
        result += "</LastModified>";
        result += "<ETag>";
        result += "</ETag>";
        result += "<Size>0</Size>";
        result += "<StorageClass>STANDARD</StorageClass>";
        result += "<Owner>";
        result += "<ID>";
        result += "0";
        result += "</ID>";
        result += "<DisplayName>";
        result += "fake";
        result += ":";
        result += "fake";
        result += "</DisplayName>";
        result += "</Owner>";
        result += "</Contents>";

      }
      cnt++;
      if (truncated)
      {
        break;
      }
    }
  }

  bucketdir.close();

  if (truncated)
  {
    result.replace(truncate_pos, 19, "true</IsTruncated> ");
  }

  result += "</ListBucketResult>";
  
  response_header["Content-Type"] = "application/xml";
  response_header["Connection"] = "close";
  return result;
}

/*----------------------------------------------------------------------------*/
std::string
S3Store::HeadObject (int                                &response_code,
                     S3Handler                          &s3,
                     std::map<std::string, std::string> &response_header)
{
  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Nobody(vid);
  int errc = 0;
  std::string username = s3.getId();
  uid_t uid = eos::common::Mapping::UserNameToUid(username, errc);
  if (errc)
  {
    // error mapping the s3 id to unix id
    return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to map bucket id to virtual id!", s3.getId().c_str(), "");
  }

  // set the bucket id as vid 
  vid.uid = uid;
  vid.uid_list.push_back(uid);

  struct stat buf;

  // build the full path for the request
  std::string objectpath = mS3ContainerPath[s3.getBucket()];
  if (objectpath[objectpath.length() - 1] == '/')
  {
    objectpath.erase(objectpath.length() - 1);
  }
  objectpath += s3.getPath();

  // stat this object 
  if (gOFS->_stat(objectpath.c_str(), &buf, error, vid, (const char*) 0) != SFS_OK)
  {
    if (error.getErrInfo() == ENOENT)
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchKey", "Unable stat requested object", s3.getId().c_str(), "");
    }
    else
    {
      return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to stat requested object!", s3.getId().c_str(), "");
    }
  }
  else
  {
    if (S_ISDIR(buf.st_mode))
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchKey", "Unable stat requested object - is a bucket subdirectory", s3.getId().c_str(), "");
    }
    // shift back the inode number to the original file id
    buf.st_ino >>= 28;
    std::string sinode;
    response_header["x-amz-id-2"]       = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["x-amz-request-id"] = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["x-amz-version-id"] = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["Date"]             = s3.getDate();
    response_header["Last-Modified"]    = eos::common::Timing::UnixTimstamp_to_ISO8601(buf.st_mtime);
    response_header["ETag"]             = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["Content-Length"]   = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_size);
    response_header["Content-Type"]     = eos::common::HttpResponse::ContentType(s3.getPath());
    response_header["Connection"]       = "close";
    response_header["Server"]           = gOFS->HostName;
    response_code = 200;
    return "OK";
  }
}

/*----------------------------------------------------------------------------*/
std::string
S3Store::HeadBucket (int                                &response_code,
                     S3Handler                          &s3,
                     std::map<std::string, std::string> &response_header)
{
  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Nobody(vid);
  int errc = 0;
  std::string username = s3.getId();
  uid_t uid = eos::common::Mapping::UserNameToUid(username, errc);
  if (errc)
  {
    // error mapping the s3 id to unix id
    return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to map bucket id to virtual id!", s3.getId().c_str(), "");
  }

  // set the bucket id as vid 
  vid.uid = uid;
  vid.uid_list.push_back(uid);

  struct stat buf;

  // build the bucket path
  std::string bucketpath = mS3ContainerPath[s3.getBucket()];

  // stat this object 
  if (gOFS->_stat(bucketpath.c_str(), &buf, error, vid, (const char*) 0) != SFS_OK)
  {
    if (error.getErrInfo() == ENOENT)
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchBucket", "Unable stat requested bucket", s3.getId().c_str(), "");
    }
    else
    {
      return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to stat requested bucket!", s3.getId().c_str(), "");
    }
  }
  else
  {
    if (!S_ISDIR(buf.st_mode))
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchBucket", "Unable stat requested object - is an object", s3.getId().c_str(), "");
    }
    // shift back the inode number to the original file id
    buf.st_ino >>= 28;
    std::string sinode;
    response_header["x-amz-id-2"] = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["x-amz-request-id"] = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["Date"] = s3.getDate();
    response_header["Last-Modified"] = eos::common::Timing::UnixTimstamp_to_ISO8601(buf.st_mtime);
    response_header["ETag"] = eos::common::StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino);
    response_header["Connection"] = "Keep-Alive";
    response_header["Server"] = gOFS->HostName;
    response_code = 200;
    return "OK";
  }
}

/*----------------------------------------------------------------------------*/
std::string
S3Store::GetObject (int                                &response_code,
                    S3Handler                          &s3,
                    std::map<std::string, std::string> &response_header)
{
  std::string result;
  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Nobody(vid);
  int errc = 0;
  std::string username = s3.getId();
  uid_t uid = eos::common::Mapping::UserNameToUid(username, errc);
  if (errc)
  {
    // error mapping the s3 id to unix id
    return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to map bucket id to virtual id!", s3.getId().c_str(), "");
  }

  // set the bucket id as vid 
  vid.uid = uid;
  vid.uid_list.push_back(uid);

  struct stat buf;

  // build the full path for the request
  std::string objectpath = mS3ContainerPath[s3.getBucket()];
  if (objectpath[objectpath.length() - 1] == '/')
  {
    objectpath.erase(objectpath.length() - 1);
  }

  objectpath += s3.getPath();

  // evalutate If-XX requests

  time_t modified_since = 0;
  time_t unmodified_since = 0;

  unsigned long long inode_match = 0;
  unsigned long long inode_none_match = 0;

  if (response_header.count("If-Modified-Since"))
  {
    modified_since = eos::common::Timing::ISO8601_to_UnixTimestamp(response_header["If-Modified-Since"]);
  }

  if (response_header.count("If-Unmodified-Since"))
  {
    modified_since = eos::common::Timing::ISO8601_to_UnixTimestamp(response_header["If-Unmodified-Since"]);
  }

  if (response_header.count("If-Match"))
  {
    inode_match = strtoull(response_header["If-Match"].c_str(), 0, 10);
  }

  if (response_header.count("If-None-Match"))
  {
    inode_none_match = strtoull(response_header["If-None-Match"].c_str(), 0, 10);
  }

  // stat this object 
  if (gOFS->_stat(objectpath.c_str(), &buf, error, vid, (const char*) 0) != SFS_OK)
  {
    if (error.getErrInfo() == ENOENT)
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchKey", "Unable stat requested object", s3.getId().c_str(), "");
    }
    else
    {
      return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to stat requested object!", s3.getId().c_str(), "");
    }
  }
  else
  {
    // check if modified since was asked
    if (modified_since && (buf.st_mtime <= modified_since))
    {
      return s3.RestErrorResponse(response_code, 412, "PreconditionFailed", "Object was not modified since specified time!", s3.getPath().c_str(), "");
    }

    // check if unmodified since was asekd
    if (unmodified_since && (buf.st_mtime != unmodified_since))
    {
      return s3.RestErrorResponse(response_code, 304, "NotModified", "Object was modified since specified time!", s3.getPath().c_str(), "");

    }

    // check if the matching inode was given
    if (inode_match && (buf.st_ino != inode_match))
    {
      return s3.RestErrorResponse(response_code, 412, "PreconditionFailed", "Object was modified!", s3.getPath().c_str(), "");
    }

    // check if a non matching inode was given
    if (inode_none_match && (buf.st_ino == inode_none_match))
    {
      return s3.RestErrorResponse(response_code, 304, "NotModified", "Object was not modified!", s3.getPath().c_str(), "");
    }

    if (S_ISDIR(buf.st_mode))
    {
      return s3.RestErrorResponse(response_code, 404, "NoSuchKey", "Unable stat requested object - is a bucket subdirectory", s3.getId().c_str(), "");
    }

    // FILE requests
    XrdSfsFile* file = gOFS->newFile((char*) s3.getId().c_str());

    if (file)
    {
      XrdSecEntity client("unix");

      client.name = strdup(s3.getId().c_str());
      client.host = strdup(s3.getHost().c_str());
      client.tident = strdup("http");
      int rc = file->open(objectpath.c_str(), 0, 0, &client, response_header["Query"].c_str());
      if (rc == SFS_REDIRECT)
      {

        // the embedded server on FSTs is hardcoded to run on port 8001
        eos::common::HttpResponse *response;
        response = eos::common::HttpServer::HttpRedirect(objectpath,
                                                         file->error.getErrText(),
                                                         8001, false);

        response_header = response->GetHeaders();
        response_header["x-amz-website-redirect-location"] = response_header["Location"];
        response_code = response->GetResponseCode();
        delete response;
      }
      else
        if (rc == SFS_ERROR)
      {
        if (file->error.getErrInfo() == ENOENT)
        {
          result = s3.RestErrorResponse(response_code, 404, "NoSuchKey", "The specified key does not exist", s3.getPath(), "");
        }
        else
          if (file->error.getErrInfo() == EPERM)
        {
          result = s3.RestErrorResponse(response_code, 403, "AccessDenied", "Access Denied", s3.getPath(), "");
        }
        else
        {
          result = s3.RestErrorResponse(response_code, 500, "Internal Error", "File currently unavailable", s3.getPath(), "");
        }
      }
      else
      {
        result = s3.RestErrorResponse(response_code, 500, "Internal Error", "File not accessible in this way", s3.getPath(), "");
      }
      // clean up the object
      delete file;
    }
  }
  return result;
}

/*----------------------------------------------------------------------------*/
std::string
S3Store::PutObject (int                                &response_code,
                    S3Handler                          &s3,
                    std::map<std::string, std::string> &response_header)
{
  std::string result;
  XrdOucErrInfo error;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Nobody(vid);
  int errc = 0;
  std::string username = s3.getId();
  uid_t uid = eos::common::Mapping::UserNameToUid(username, errc);
  if (errc)
  {
    // error mapping the s3 id to unix id
    return s3.RestErrorResponse(response_code, 400, "InvalidArgument", "Unable to map bucket id to virtual id!", s3.getId().c_str(), "");
  }

  // set the bucket id as vid 
  vid.uid = uid;
  vid.uid_list.push_back(uid);

  // build the full path for the request
  std::string objectpath = mS3ContainerPath[s3.getBucket()];
  if (objectpath[objectpath.length() - 1] == '/')
  {
    objectpath.erase(objectpath.length() - 1);
  }

  objectpath += s3.getPath();

  // FILE requests
  XrdSfsFile* file = gOFS->newFile((char*) s3.getId().c_str());

  if (file)
  {
    XrdSecEntity client("unix");

    client.name = strdup(s3.getId().c_str());
    client.host = strdup(s3.getHost().c_str());
    client.tident = strdup("http");
    int rc = file->open(objectpath.c_str(), SFS_O_TRUNC, SFS_O_MKPTH, &client, response_header["Query"].c_str());
    if (rc == SFS_REDIRECT)
    {

      // the embedded server on FSTs is hardcoded to run on port 8001
      eos::common::HttpResponse *response;
      response = eos::common::HttpServer::HttpRedirect(objectpath,
                                                       file->error.getErrText(),
                                                       8001, false);

      response_header = response->GetHeaders();
      response_header["x-amz-website-redirect-location"] = response_header["Location"];
      response_code = response->GetResponseCode();
      delete response;
    }
    else
      if (rc == SFS_ERROR)
    {
      if (file->error.getErrInfo() == EPERM)
      {
        result = s3.RestErrorResponse(response_code, 403, "AccessDenied", "Access Denied", s3.getPath(), "");
      }
      else
      {
        result = s3.RestErrorResponse(response_code, 500, "Internal Error", "File creation currently unavailable", s3.getPath(), "");
      }
    }
    else
    {
      result = s3.RestErrorResponse(response_code, 500, "Internal Error", "File not accessible in this way", s3.getPath(), "");
    }
    // clean up the object
    delete file;
  }
  return result;
}


EOSMGMNAMESPACE_END

