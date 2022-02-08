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

#include "mgm/http/HttpServer.hh"
#include "mgm/http/s3/S3Store.hh"
#include "mgm/http/s3/S3Handler.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Checksum.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/FileId.hh"
#include "common/Timing.hh"

EOSMGMNAMESPACE_BEGIN

#define XML_V1_UTF8 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"

/*----------------------------------------------------------------------------*/
S3Store::S3Store(const char* s3defpath)
{
  mS3DefContainer = s3defpath;
  mStoreModificationTime = 1;
  mStoreReloadTime = 1;
}

/*----------------------------------------------------------------------------*/
void
S3Store::Refresh()
{
  // Refresh the S3 id, keys, container definitions
  time_t now = time(NULL);
  time_t srtime;
  {
    eos::common::RWMutexReadLock sLock(mStoreMutex);
    srtime = mStoreReloadTime;
  }

  // Attempt refresh only once per minute
  if ((now - srtime) > 60) {
    eos::common::RWMutexWriteLock sLock(mStoreMutex);
    mStoreReloadTime = now;
    XrdOucErrInfo error;
    eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
    eos::IContainerMD::XAttrMap map;
    struct stat buf;

    if (gOFS->_stat(mS3DefContainer.c_str(), &buf, error, vid, (const char*) 0)
        == SFS_OK) {
      // check last modification time
      if (buf.st_ctime != mStoreModificationTime) {
        // clear all
        mS3ContainerSet.clear();
        mS3Keys.clear();
        mS3ContainerPath.clear();

        if (gOFS->_attr_ls(mS3DefContainer.c_str(), error, vid, 0, map)
            != SFS_OK) {
          eos_static_err("unable to list attributes of % s",
                         mS3DefContainer.c_str());
        } else {
          // parse the attributes into the store
          for (auto it = map.begin(); it != map.end(); it++) {
            eos_static_info("parsing %s=>%s", it->first.c_str(),
                            it->second.c_str());

            if (it->first.substr(0, 6) == "sys.s3") {
              // the s3 attributes are built as
              // sys.s3.id.<id> => secret key
              // sys.s3.bucket.<id> => bucket list
              // sys.s3.path.<bucket> => path
              if (it->first.substr(0, 10) == "sys.s3.id.") {
                std::string id = it->first.substr(10);
                mS3Keys[id] = it->second;
                eos_static_info("id=%s key=<hidden>", id.c_str());
              }

              if (it->first.substr(0, 14) == "sys.s3.bucket.") {
                std::string id = it->first.substr(14);
                std::vector<std::string> svec;
                eos::common::StringConversion::Tokenize(it->second, svec, "|");

                for (size_t i = 0; i < svec.size(); i++) {
                  if (svec[i][0] == '\"') {
                    svec[i].erase(0, 1);
                  }

                  if (svec[i][svec[i].length() - 1] == '\"') {
                    svec[i].erase(svec[i].length() - 1);
                  }

                  mS3ContainerSet[id].insert(svec[i]);
                  eos_static_debug("id=%s bucket=%s", id.c_str(),
                                   svec[i].c_str());
                }
              }

              if (it->first.substr(0, 12) == "sys.s3.path.") {
                std::string bucket = it->first.substr(12);
                mS3ContainerPath[bucket] = it->second;
                eos_static_info("bucket=%s path=%s", bucket.c_str(),
                                it->second.c_str());
              }
            }
          }

          // store the modification time of the loaded s3 definitions
          mStoreModificationTime = buf.st_ctime;
        }
      } else {
        eos_static_info("skipping S3 configuration reload. "
                        "Reason: no change detected since last refresh");
      }
    } else {
      eos_static_err("unable to stat S3 configuration container %s",
                     mS3DefContainer.c_str());
    }
  } else {
    eos_static_info("skipping S3 configuration reload. "
                    "Reason: refresh performed recently");
  }
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::ListBuckets(const std::string& id)
{
  eos::common::RWMutexReadLock sLock(mStoreMutex);
  eos::common::HttpResponse* response = 0;
  std::string result = XML_V1_UTF8;
  result += "<ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">";
  result += "<Owner><ID>";
  result += id;
  result += "</ID>";
  result += "<Display>";
  result += id;
  result += "</Display>";
  result += "</Owner>";
  result += "<Buckets>";

  for (auto it = mS3ContainerSet[id].begin(); it != mS3ContainerSet[id].end();
       it++) {
    if (mS3ContainerPath.count(*it)) {
      // check if we know how to map a bucket name into our regular namespace
      std::string bucketpath = mS3ContainerPath[*it];
      XrdOucErrInfo error;
      eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
      eos::IContainerMD::XAttrMap map;
      struct stat buf;

      if (gOFS->_stat(bucketpath.c_str(), &buf, error, vid, (const char*) 0)
          == SFS_OK) {
        result += "<Bucket>";
        result += "<Name>";
        result += *it;
        result += "</Name>";
        result += "<CreationDate>";
        result += eos::common::Timing::UnixTimestamp_to_ISO8601(buf.st_ctime);
        result += "</CreationDate>";
        result += "</Bucket>";
      } else {
        std::string errmsg = "cannot find bucket path ";
        errmsg += bucketpath;
        errmsg += " for bucket ";
        errmsg += *it;
        return eos::common::S3Handler::RestErrorResponse(
                 eos::common::HttpResponse::NOT_FOUND,
                 "NoSuchBucket",
                 errmsg, *it, "");
      }
    }
  }

  result += "</Buckets>";
  result += "</ListAllMyBucketsResult>";
  response = new eos::common::PlainHttpResponse();
  response->AddHeader("Content-Type", "application/xml");
  response->AddHeader("x-amz-id-2", "unknown");
  response->AddHeader("x-amz-request-id", "unknown");
  response->SetBody(result);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::ListBucket(const std::string& bucket, const std::string& query)
{
  using namespace eos::common;
  XrdOucErrInfo error;
  VirtualIdentity vid = VirtualIdentity::Root();
  RWMutexReadLock sLock(mStoreMutex);
  HttpResponse* response = 0;

  if (!mS3ContainerPath.count(bucket)) {
    // check if this bucket is configured
    return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                        "NoSuchBucket",
                                        "Bucket does not exist!",
                                        bucket.c_str(), "");
  } else {
    // check if this bucket is mapped
    struct stat buf;

    if (gOFS->_stat(mS3ContainerPath[bucket].c_str(), &buf, error, vid,
                    (const char*) 0) != SFS_OK) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchBucket",
                                          "Bucket is not mapped into the "
                                          "namespace!", bucket.c_str(), "");
    }
  }

  XrdOucEnv parameter(query.c_str());
  XrdOucString lPrefix, lBucket;
  XrdMgmOfsDirectory bucketdir;
  uint64_t cnt = 0;
  uint64_t max_keys = 1000;
  std::string marker = "";
  std::string prefix = "";
  bool marker_reached = true; //!< indicates start of output
  const char* val = 0;

  if ((val = parameter.Get("max-keys"))) {
    max_keys = strtoull(val, 0, 10);
  }

  if ((val = parameter.Get("marker"))) {
    marker = val;

    if (marker == "(null)") {
      marker = "";
    }
  }

  if ((val = parameter.Get("prefix"))) {
    prefix = val;
  }

  if (marker.length()) {
    marker_reached = false;
  }

  // handle ending slash in bucket and prefix paths
  lBucket = mS3ContainerPath[bucket].c_str();

  if (!lBucket.endswith("/")) {
    lBucket += "/";
  }

  lPrefix = prefix.c_str();

  if (lPrefix.length() && !lPrefix.endswith("/")) {
    lPrefix += "/";
  }

  eos_static_info("msg=\"listing\" bucket=%s prefix=%s", bucket.c_str(),
                  lPrefix.c_str());
  // Construct listing response
  std::string result = XML_V1_UTF8;
  result += "<ListBucketResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">";
  result += "<Name>";
  result += bucket;
  result += "</Name>";

  if (!prefix.length()) {
    result += "<Prefix/>";
  } else {
    result += "<Prefix>";
    result += prefix;
    result += "</Prefix>";
  }

  if ((!marker.length() || (!marker.c_str()))) {
    result += "<Marker/>";
  } else {
    result += "<Marker>";
    result += marker;
    result += "</Marker>";
  }

  result += "<Delimiter>/</Delimiter>";
  result += "<MaxKeys>";
  char smaxkeys[16];
  snprintf(smaxkeys, sizeof(smaxkeys) - 1, "%llu",
           (unsigned long long) max_keys);
  result += smaxkeys;
  result += "</MaxKeys>";
  bool truncated = false;
  size_t truncate_pos = result.length() + 13;
  result += "<IsTruncated>false</IsTruncated>";
  // list directory
  std::string directory = lBucket.c_str();
  directory += lPrefix.c_str();
  int listrc = bucketdir.open(directory.c_str(), vid, (const char*) 0);

  if (!listrc) {
    const char* name = 0;

    // loop over the directory contents
    while ((name = bucketdir.nextEntry())) {
      std::string entry = "";
      std::string sname = name;

      if ((sname == ".") || (sname == "..")) {
        continue;
      }

      // don't return more than max-keys
      if (cnt++ > max_keys) {
        truncated = true;
        break;
      }

      // construct object name
      std::string objectname = lPrefix.c_str();
      objectname += sname;
      std::string fullname = lBucket.c_str();
      fullname += objectname;

      // check if output should begin
      if (!marker_reached) {
        if (marker == objectname) {
          marker_reached = true;
        }

        continue;
      }

      {
        // attempt file metadata retrieval
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        std::shared_ptr<eos::IContainerMD> cmd;
        std::shared_ptr<eos::IFileMD> fmd;
        int errc = 0;

        try {
          fmd = gOFS->eosView->getFile(fullname);
          entry = "<Contents>";
          entry += "<Key>";
          entry += objectname.c_str();
          entry += "</Key>";
          entry += "<LastModified>";
          eos::IFileMD::ctime_t mtime;
          fmd->getMTime(mtime);
          entry += Timing::UnixTimestamp_to_ISO8601(mtime.tv_sec);
          entry += "</LastModified>";
          entry += "<ETag>\"";
          eos::appendChecksumOnStringAsHex(fmd.get(), entry);
          entry += "\"</ETag>";
          entry += "<Size>";
          std::string sconv;
          entry += StringConversion::GetSizeString(sconv, (unsigned long long)
                   fmd->getSize());
          entry += "</Size>";
          entry += "<StorageClass>STANDARD</StorageClass>";
          entry += "<Owner>";
          entry += "<ID>";
          entry += Mapping::UidToUserName(fmd->getCUid(), errc);
          entry += "</ID>";
          entry += "<DisplayName>";
          entry += Mapping::UidToUserName(fmd->getCUid(), errc);
          entry += ":";
          entry += Mapping::GidToGroupName(fmd->getCGid(), errc);
          entry += "</DisplayName>";
          entry += "</Owner>";
          entry += "</Contents>";
        } catch (eos::MDException& e) {
          fmd.reset();

          if (e.getErrno() != ENOENT) {
            errno = e.getErrno();
            eos_static_err("msg=\"could not open file\" ec=%d emsg=\"%s\" filepath=%s\n",
                           e.getErrno(), e.getMessage().str().c_str(),
                           fullname.c_str());
            return S3Handler::RestErrorResponse(
                     eos::common::HttpResponse::INTERNAL_SERVER_ERROR,
                     "Internal Error", "Unable to open path",
                     fullname.c_str(), "");
          }
        }

        // should be a container
        if (!fmd) {
          // attempt container metadata retrieval
          try {
            cmd = gOFS->eosView->getContainer(fullname);
            entry = "<Contents>";
            entry += "<Key>";
            entry += objectname.c_str();
            entry += "/";
            entry += "</Key>";
            entry += "<LastModified>";
            eos::IContainerMD::ctime_t mtime;
            cmd->getMTime(mtime);
            entry += Timing::UnixTimestamp_to_ISO8601(mtime.tv_sec);
            entry += "</LastModified>";
            entry += "<ETag>";
            entry += "</ETag>";
            entry += "<Size>0</Size>";
            entry += "<StorageClass>STANDARD</StorageClass>";
            entry += "<Owner>";
            entry += "<ID>";
            entry += Mapping::UidToUserName(cmd->getCUid(), errc);
            entry += "</ID>";
            entry += "<DisplayName>";
            entry += Mapping::UidToUserName(cmd->getCUid(), errc);
            entry += ":";
            entry += Mapping::GidToGroupName(cmd->getCGid(), errc);
            entry += "</DisplayName>";
            entry += "</Owner>";
            entry += "</Contents>";
          } catch (eos::MDException& e) {
            cmd.reset();
            errno = e.getErrno();
            eos_static_err("msg=\"could not open directory\" ec=%d emsg=\"%s\" dirpath=%s\n",
                           e.getErrno(), e.getMessage().str().c_str(),
                           fullname.c_str());
            return S3Handler::RestErrorResponse(
                     eos::common::HttpResponse::INTERNAL_SERVER_ERROR,
                     "Internal Error", "Unable to open path",
                     fullname.c_str(), "");
          }
        }
      }

      // append this entry to the final result
      result += entry;
    }
  }

  bucketdir.close();

  if (truncated) {
    result.replace(truncate_pos, 18, "true</IsTruncated>");
  }

  result += "</ListBucketResult>";
  response = new PlainHttpResponse();
  response->AddHeader("Content-Type", "application/xml");
  response->AddHeader("Connection", "close");
  response->SetBody(result);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::HeadBucket(const std::string& id,
                    const std::string& bucket,
                    const std::string& date)
{
  using namespace eos::common;
  HttpResponse* response = 0;
  XrdOucErrInfo error;
  VirtualIdentity vid = VirtualIdentity::Nobody();
  int errc = 0;
  std::string username = id;
  uid_t uid = Mapping::UserNameToUid(username, errc);

  if (errc) {
    // error mapping the s3 id to unix id
    return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                        "InvalidArgument",
                                        "Unable to map bucket id to virtual id",
                                        id.c_str(), "");
  }

  // set the bucket id as vid
  vid.uid = uid;
  vid.allowed_uids.insert(uid);
  struct stat buf;
  // build the bucket path
  std::string bucketpath = mS3ContainerPath[bucket];

  // stat this object
  if (gOFS->_stat(bucketpath.c_str(), &buf, error, vid,
                  (const char*) 0) != SFS_OK) {
    if (error.getErrInfo() == ENOENT) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchBucket",
                                          "Unable stat requested bucket",
                                          id.c_str(), "");
    } else {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                          "InvalidArgument",
                                          "Unable to stat requested bucket!",
                                          id.c_str(), "");
    }
  } else {
    if (!S_ISDIR(buf.st_mode)) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchBucket",
                                          "Unable stat requested object - is "
                                          "an object", id.c_str(), "");
    }

    response = new PlainHttpResponse();
    // shift back the inode number to the original file id
    buf.st_ino = eos::common::FileId::InodeToFid(buf.st_ino);
    std::string sinode;
    response->AddHeader("x-amz-id-2",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("x-amz-request-id",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("ETag",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("Last-Modified",
                        Timing::UnixTimestamp_to_ISO8601(buf.st_mtime));
    response->AddHeader("Date", date);
    response->AddHeader("Connection", "Keep-Alive");
    response->AddHeader("Server", gOFS->HostName);
    response->SetResponseCode(response->OK);
    return response;
  }
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::HeadObject(const std::string& id,
                    const std::string& bucket,
                    const std::string& path,
                    const std::string& date)
{
  using namespace eos::common;
  XrdOucErrInfo error;
  VirtualIdentity vid = VirtualIdentity::Nobody();
  HttpResponse* response = 0;
  int errc = 0;
  std::string username = id;
  uid_t uid = Mapping::UserNameToUid(username, errc);

  if (errc) {
    // error mapping the s3 id to unix id
    return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                        "InvalidArgument",
                                        "Unable to map bucket id to virtual id",
                                        id.c_str(), "");
  }

  // set the bucket id as vid
  vid.uid = uid;
  vid.allowed_uids.insert(uid);
  struct stat buf;
  // build the full path for the request
  std::string objectpath = mS3ContainerPath[bucket];

  if (objectpath[objectpath.length() - 1] == '/') {
    objectpath.erase(objectpath.length() - 1);
  }

  objectpath += path;

  // stat this object
  if (gOFS->_stat(objectpath.c_str(), &buf, error, vid,
                  (const char*) 0) != SFS_OK) {
    if (error.getErrInfo() == ENOENT) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchKey",
                                          "Unable stat requested object",
                                          id.c_str(), "");
    } else {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                          "InvalidArgument",
                                          "Unable to stat requested object!",
                                          id.c_str(), "");
    }
  } else {
    if (S_ISDIR(buf.st_mode)) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchKey",
                                          "Unable stat requested object - "
                                          "is a bucket subdirectory",
                                          id.c_str(), "");
    }

    // shift back the inode number to the original file id
    buf.st_ino = eos::common::FileId::InodeToFid(buf.st_ino);
    std::string sinode;
    response = new PlainHttpResponse();
    response->AddHeader("x-amz-id-2",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("x-amz-request-id",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("x-amz-version-id",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("ETag",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_ino));
    response->AddHeader("Content-Length",
                        StringConversion::GetSizeString(sinode, (unsigned long long) buf.st_size));
    response->AddHeader("Last-Modified",
                        Timing::UnixTimestamp_to_ISO8601(buf.st_mtime));
    response->AddHeader("Date", date);
    response->AddHeader("Content-Type", HttpResponse::ContentType(path));
    response->AddHeader("Connection", "close");
    response->AddHeader("Server", gOFS->HostName);
    response->SetResponseCode(response->OK);
    return response;
  }
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::GetObject(eos::common::HttpRequest* request,
                   const std::string& id,
                   const std::string& bucket,
                   const std::string& path,
                   const std::string& query)
{
  using namespace eos::common;
  std::string result;
  XrdOucErrInfo error;
  VirtualIdentity vid = VirtualIdentity::Nobody();
  HttpResponse* response = 0;
  int errc = 0;
  std::string username = id;
  uid_t uid = Mapping::UserNameToUid(username, errc);

  if (errc) {
    // error mapping the s3 id to unix id
    return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                        "InvalidArgument",
                                        "Unable to map bucket id to virtual id",
                                        id.c_str(), "");
  }

  // set the bucket id as vid
  vid.uid = uid;
  vid.allowed_uids.insert(uid);
  struct stat buf;
  // build the full path for the request
  std::string objectpath = mS3ContainerPath[bucket];

  if (objectpath[objectpath.length() - 1] == '/') {
    objectpath.erase(objectpath.length() - 1);
  }

  objectpath += path;
  // evalutate If-XX requests
  time_t modified_since = 0;
  time_t unmodified_since = 0;
  unsigned long long inode_match = 0;
  unsigned long long inode_none_match = 0;

  if (request->GetHeaders().count("if-modified-since")) {
    modified_since = Timing::ISO8601_to_UnixTimestamp(request->GetHeaders()
                     ["if-modified-since"]);
  }

  if (request->GetHeaders().count("if-unmodified-since")) {
    unmodified_since = Timing::ISO8601_to_UnixTimestamp(request->GetHeaders()
                       ["if-unmodified-since"]);
  }

  if (request->GetHeaders().count("if-match")) {
    inode_match = strtoull(request->GetHeaders()["if-match"].c_str(), 0, 10);
  }

  if (request->GetHeaders().count("if-none-match")) {
    inode_none_match = strtoull(request->GetHeaders()["if-none-match"].c_str(),
                                0, 10);
  }

  // stat this object
  if (gOFS->_stat(objectpath.c_str(), &buf, error, vid,
                  (const char*) 0) != SFS_OK) {
    if (error.getErrInfo() == ENOENT) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchKey",
                                          "Unable stat requested object",
                                          id.c_str(), "");
    } else {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                          "InvalidArgument",
                                          "Unable to stat requested object!",
                                          id.c_str(), "");
    }
  } else {
    // check if modified since was asked
    if (modified_since && (buf.st_mtime <= modified_since)) {
      return S3Handler::RestErrorResponse(
               eos::common::HttpResponse::PRECONDITION_FAILED,
               "PreconditionFailed",
               "Object was not modified since "
               "specified time!", path.c_str(), "");
    }

    // check if unmodified since was asekd
    if (unmodified_since && (buf.st_mtime != unmodified_since)) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_MODIFIED,
                                          "NotModified",
                                          "Object was modified since specified "
                                          "time!", path.c_str(), "");
    }

    // check if the matching inode was given
    if (inode_match && (buf.st_ino != inode_match)) {
      return S3Handler::RestErrorResponse(
               eos::common::HttpResponse::PRECONDITION_FAILED,
               "PreconditionFailed",
               "Object was modified!",
               path.c_str(), "");
    }

    // check if a non matching inode was given
    if (inode_none_match && (buf.st_ino == inode_none_match)) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_MODIFIED,
                                          "NotModified",
                                          "Object was not modified!",
                                          path.c_str(), "");
    }

    if (S_ISDIR(buf.st_mode)) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchKey",
                                          "Unable stat requested object - is a "
                                          "bucket subdirectory", id.c_str(), "");
    }

    // FILE requests
    XrdSfsFile* file = gOFS->newFile((char*) id.c_str());

    if (file) {
      XrdSecEntity client("unix");
      client.name = strdup(id.c_str());
      client.host = strdup(request->GetHeaders()["host"].c_str());
      client.tident = strdup("http");
      snprintf(client.prot, sizeof(client.prot) - 1, "https");
      int rc = file->open(objectpath.c_str(), 0, 0, &client, query.c_str());

      if (rc == SFS_REDIRECT) {
        // the embedded server on FSTs is hardcoded to run on port 8001
        response = HttpServer::HttpRedirect(objectpath,
                                            file->error.getErrText(),
                                            8001, false);
        response->AddHeader("x-amz-website-redirect-location",
                            response->GetHeaders()["Location"]);
        std::string body = XML_V1_UTF8;
        body += "<Error>"
                "<Code>TemporaryRedirect</Code>"
                "<Message>Please re-send this request to the specified temporary "
                "endpoint. Continue to use the original request endpoint for "
                "future requests.</Message>"
                "<Endpoint>";
        body += response->GetHeaders()["Location"];
        body += "</Endpoint>"
                "</Error>";
        response->SetBody(body);
        eos_static_info("\n\n%s\n\n", response->GetBody().c_str());
      } else if (rc == SFS_ERROR) {
        if (file->error.getErrInfo() == ENOENT) {
          response = S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                                  "NoSuchKey",
                                                  "The specified key does not exist",
                                                  path, "");
        } else if (file->error.getErrInfo() == EPERM) {
          response = S3Handler::RestErrorResponse(eos::common::HttpResponse::FORBIDDEN,
                                                  "AccessDenied",
                                                  "Access Denied",
                                                  path, "");
        } else {
          response = S3Handler::RestErrorResponse(
                       eos::common::HttpResponse::INTERNAL_SERVER_ERROR,
                       "Internal Error",
                       "File currently unavailable",
                       path, "");
        }
      } else {
        response = S3Handler::RestErrorResponse(
                     eos::common::HttpResponse::INTERNAL_SERVER_ERROR,
                     "Internal Error",
                     "File not accessible in this way",
                     path, "");
      }

      // clean up the object
      delete file;
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::PutObject(eos::common::HttpRequest* request,
                   const std::string& id,
                   const std::string& bucket,
                   const std::string& path,
                   const std::string& query)
{
  using namespace eos::common;
  std::string result;
  XrdOucErrInfo error;
  VirtualIdentity vid = VirtualIdentity::Nobody();
  HttpResponse* response = 0;
  int errc = 0;
  std::string username = id;
  uid_t uid = Mapping::UserNameToUid(username, errc);

  if (errc) {
    // error mapping the s3 id to unix id
    return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                        "InvalidArgument",
                                        "Unable to map bucket id to virtual id",
                                        id.c_str(), "");
  }

  // set the bucket id as vid
  vid.uid = uid;
  vid.allowed_uids.insert(uid);
  // build the full path for the request
  std::string objectpath = mS3ContainerPath[bucket];

  if (objectpath[objectpath.length() - 1] == '/') {
    objectpath.erase(objectpath.length() - 1);
  }

  objectpath += path;
  // FILE requests
  XrdSfsFile* file = gOFS->newFile((char*) id.c_str());

  if (file) {
    XrdSecEntity client("unix");
    client.name = strdup(id.c_str());
    client.host = strdup(request->GetHeaders()["host"].c_str());
    client.tident = strdup("http");
    snprintf(client.prot, sizeof(client.prot) - 1, "https");
    // force MD5 checksums for S3 file creation
    std::string newquery = query;
    newquery.insert(0, "&eos.checksum.noforce=1&eos.layout.checksum=md5");
    int rc = file->open(objectpath.c_str(), SFS_O_TRUNC, SFS_O_MKPTH, &client,
                        newquery.c_str());

    if (rc == SFS_REDIRECT) {
      // the embedded server on FSTs is hardcoded to run on port 8001
      response = HttpServer::HttpRedirect(objectpath,
                                          file->error.getErrText(),
                                          8001, false);
      response->AddHeader("x-amz-website-redirect-location",
                          response->GetHeaders()["Location"]);
      std::string body = XML_V1_UTF8;
      body += "<Error>"
              "<Code>TemporaryRedirect</Code>"
              "<Message>Please re-send this request to the specified temporary "
              "endpoint. Continue to use the original request endpoint for "
              "future requests.</Message>"
              "<Endpoint>";
      body += response->GetHeaders()["Location"];
      body += "</Endpoint>"
              "</Error>";
      response->SetBody(body);
      eos_static_info("\n\n%s\n\n", response->GetBody().c_str());
    } else if (rc == SFS_ERROR) {
      if (file->error.getErrInfo() == EPERM) {
        response = S3Handler::RestErrorResponse(eos::common::HttpResponse::FORBIDDEN,
                                                "AccessDenied",
                                                "Access Denied",
                                                path, "");
      } else {
        response = S3Handler::RestErrorResponse(
                     eos::common::HttpResponse::INTERNAL_SERVER_ERROR,
                     "Internal Error",
                     "File creation currently "
                     "unavailable", path, "");
      }
    } else {
      response = S3Handler::RestErrorResponse(
                   eos::common::HttpResponse::INTERNAL_SERVER_ERROR,
                   "Internal Error",
                   "File not accessible in this way",
                   path, "");
    }

    // clean up the object
    delete file;
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
S3Store::DeleteObject(eos::common::HttpRequest* request,
                      const std::string& id,
                      const std::string& bucket,
                      const std::string& path)
{
  using namespace eos::common;
  XrdOucErrInfo error;
  VirtualIdentity vid = VirtualIdentity::Nobody();
  HttpResponse* response = 0;
  int errc = 0;
  std::string username = id;
  uid_t uid = Mapping::UserNameToUid(username, errc);

  if (errc) {
    // error mapping the s3 id to unix id
    return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                        "InvalidArgument",
                                        "Unable to map bucket id to virtual id",
                                        id.c_str(), "");
  }

  // set the bucket id as vid
  vid.uid = uid;
  vid.allowed_uids.insert(uid);
  struct stat buf;
  // build the full path for the request
  std::string objectpath = mS3ContainerPath[bucket];

  if (objectpath[objectpath.length() - 1] == '/') {
    objectpath.erase(objectpath.length() - 1);
  }

  objectpath += path;

  // stat this object
  if (gOFS->_stat(objectpath.c_str(), &buf, error, vid,
                  (const char*) 0) != SFS_OK) {
    if (error.getErrInfo() == ENOENT) {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::NOT_FOUND,
                                          "NoSuchKey",
                                          "Unable to delete requested object",
                                          id.c_str(), "");
    } else {
      return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                          "InvalidArgument",
                                          "Unable to delete requested object",
                                          id.c_str(), "");
    }
  } else {
    XrdOucString info = "mgm.cmd=rm&mgm.path=";
    info += objectpath.c_str();

    if (S_ISDIR(buf.st_mode)) {
      info += "&mgm.option=r";
    }

    ProcCommand cmd;
    cmd.open("/proc/user", info.c_str(), vid, &error);
    cmd.close();
    int rc = cmd.GetRetc();

    if (rc != SFS_OK) {
      if (error.getErrInfo() == EPERM) {
        return S3Handler::RestErrorResponse(eos::common::HttpResponse::FORBIDDEN,
                                            "AccessDenied",
                                            "Access Denied",
                                            path, "");
      } else {
        return S3Handler::RestErrorResponse(eos::common::HttpResponse::BAD_REQUEST,
                                            "InvalidArgument",
                                            "Unable to delete requested object",
                                            id.c_str(), "");
      }
    } else {
      response = new eos::common::PlainHttpResponse();
      response->AddHeader("Connection", "close");
      response->AddHeader("Server", gOFS->HostName);
      response->SetResponseCode(response->NO_CONTENT);
    }
  }

  return response;
}

EOSMGMNAMESPACE_END
