// ----------------------------------------------------------------------
// File: TapeJsonifiers.hh
// Author: Consolidated tape REST API jsonifiers
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 ************************************************************************/

#ifndef EOS_TAPE_JSONIFIERS_HH
#define EOS_TAPE_JSONIFIERS_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/model/tape/common/ErrorModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.hh"
#include "mgm/http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

namespace tape_json {

inline void appendEscapedString(std::stringstream& ss, const std::string& value)
{
  ss << '"';
  for (const char c : value) {
    switch (c) {
    case '"':
      ss << "\\\"";
      break;
    case '\\':
      ss << "\\\\";
      break;
    case '\b':
      ss << "\\b";
      break;
    case '\f':
      ss << "\\f";
      break;
    case '\n':
      ss << "\\n";
      break;
    case '\r':
      ss << "\\r";
      break;
    case '\t':
      ss << "\\t";
      break;
    default:
      ss << c;
      break;
    }
  }
  ss << '"';
}

} // namespace tape_json

// ErrorModel jsonifier
class ErrorModelJsonifier : public TapeRestApiJsonifier<ErrorModel>
{
public:
  void jsonify(const ErrorModel* obj, std::stringstream& ss) override
  {
    ss << "{\n";
    ss << "\"title\": ";
    tape_json::appendEscapedString(ss, obj->getTitle());
    ss << ",\n\"status\": " << obj->getStatus();
    if (obj->getDetail()) {
      ss << ",\n\"detail\": ";
      tape_json::appendEscapedString(ss, *obj->getDetail());
    }
    if (obj->getType()) {
      ss << ",\n\"type\": ";
      tape_json::appendEscapedString(ss, *obj->getType());
    }
    ss << "\n}";
  }
};

// CreatedStageBulkRequest jsonifier
class CreatedStageBulkRequestJsonifier : public TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>
{
public:
  void jsonify(const CreatedStageBulkRequestResponseModel* obj, std::stringstream& ss) override
  {
    ss << "{\n\"requestId\": ";
    tape_json::appendEscapedString(ss, obj->getRequestId());
    ss << "\n}";
  }
};

// GetStageBulkRequest jsonifier
class GetStageBulkRequestJsonifier : public TapeRestApiJsonifier<GetStageBulkRequestResponseModel>
{
public:
  void jsonify(const GetStageBulkRequestResponseModel* obj, std::stringstream& ss) override
  {
    ss << "{\n";
    ss << "\"id\": ";
    tape_json::appendEscapedString(ss, obj->getId());
    ss << ",\n\"createdAt\": " << static_cast<long long>(obj->getCreatedAt());
    ss << ",\n\"startedAt\": " << static_cast<long long>(obj->getStartedAt());
    ss << ",\n\"files\": [\n";
    const auto& files = obj->getFiles();
    for (size_t i = 0; i < files.size(); ++i) {
      const auto& f = files[i];
      ss << "  {\n";
      ss << "    \"path\": ";
      tape_json::appendEscapedString(ss, f->mPath);

      if (f->mState) {
        ss << ",\n    \"state\": ";
        tape_json::appendEscapedString(ss, *f->mState);
        if (f->mStartedAt) {
          ss << ",\n    \"startedAt\": " << static_cast<long long>(*f->mStartedAt);
        }
        if (f->mFinishedAt) {
          ss << ",\n    \"finishedAt\": " << static_cast<long long>(*f->mFinishedAt);
        }
      } else if (f->mOnDisk) {
        ss << ",\n    \"onDisk\": " << (*f->mOnDisk ? "true" : "false");
      }

      if (f->mError && !f->mError->empty()) {
        ss << ",\n    \"error\": ";
        tape_json::appendEscapedString(ss, *f->mError);
      }

      ss << "\n  }";
      if (i + 1 < files.size()) {
        ss << ",";
      }
      ss << "\n";
    }
    ss << "]\n}";
  }
};

// GetArchiveInfoResponse jsonifier
class GetArchiveInfoResponseJsonifier : public TapeRestApiJsonifier<GetArchiveInfoResponseModel>
{
public:
  void jsonify(const GetArchiveInfoResponseModel* obj, std::stringstream& ss) override
  {
    const auto& entries = obj->getEntries();
    ss << "[\n";
    for (size_t i = 0; i < entries.size(); ++i) {
      const auto& entry = entries[i];
      ss << "  {\n";
      ss << "    \"path\": ";
      tape_json::appendEscapedString(ss, entry.path);

      if (entry.locality) {
        ss << ",\n    \"locality\": ";
        tape_json::appendEscapedString(ss, *entry.locality);
      }

      if (entry.error) {
        ss << ",\n    \"error\": ";
        tape_json::appendEscapedString(ss, *entry.error);
      }

      ss << "\n  }";
      if (i + 1 < entries.size()) {
        ss << ",";
      }
      ss << "\n";
    }
    ss << "]";
  }
};

// GetTapeWellKnownModel jsonifier
class GetTapeWellKnownModelJsonifier : public TapeRestApiJsonifier<GetTapeWellKnownModel>
{
public:
  void jsonify(const GetTapeWellKnownModel* obj, std::stringstream& ss) override
  {
    const TapeWellKnownInfos* infos = obj->getTapeWellKnownInfos();
    ss << "{\n";
    ss << "  \"sitename\": ";
    tape_json::appendEscapedString(ss, infos->getSiteName());

    if (!infos->getDescription().empty()) {
      ss << ",\n  \"description\": ";
      tape_json::appendEscapedString(ss, infos->getDescription());
    }

    ss << ",\n  \"endpoints\": [\n";
    const auto& endpoints = infos->getEndpoints();
    for (size_t i = 0; i < endpoints.size(); ++i) {
      const auto& ep = endpoints[i];
      ss << "    {\n";
      ss << "      \"uri\": ";
      tape_json::appendEscapedString(ss, ep->getUri());
      ss << ",\n      \"version\": ";
      tape_json::appendEscapedString(ss, ep->getVersion());
      ss << "\n    }";
      if (i + 1 < endpoints.size()) {
        ss << ",";
      }
      ss << "\n";
    }
    ss << "  ]\n}";
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_JSONIFIERS_HH

