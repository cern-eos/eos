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

// ErrorModel jsonifier
class ErrorModelJsonifier : public TapeRestApiJsonifier<ErrorModel>
{
public:
  void jsonify(const ErrorModel* obj, std::stringstream& ss) override
  {
    ss << "{\n";
    ss << "\"title\": \"" << obj->getTitle() << "\",\n";
    ss << "\"status\": " << obj->getStatus();
    if (obj->getDetail()) {
      ss << ",\n\"detail\": \"" << *obj->getDetail() << "\"";
    }
    if (obj->getType()) {
      ss << ",\n\"type\": \"" << *obj->getType() << "\"";
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
    ss << "{\n\"request_id\": \"" << obj->getRequestId() << "\"\n}";
  }
};

// GetStageBulkRequest jsonifier
class GetStageBulkRequestJsonifier : public TapeRestApiJsonifier<GetStageBulkRequestResponseModel>
{
public:
  void jsonify(const GetStageBulkRequestResponseModel* obj, std::stringstream& ss) override
  {
    ss << "{\n";
    ss << "\"id\": \"" << obj->getId() << "\",\n";
    ss << "\"creation_time\": " << static_cast<long long>(obj->getCreationTime()) << ",\n";
    ss << "\"files\": [\n";
    const auto& files = obj->getFiles();
    for (size_t i = 0; i < files.size(); ++i) {
      const auto& f = files[i];
      ss << "  {\n";
      ss << "    \"path\": \"" << f->mPath << "\",\n";
      ss << "    \"on_disk\": " << (f->mOnDisk ? "true" : "false");
      if (!f->mError.empty()) {
        ss << ",\n    \"error\": \"" << f->mError << "\"";
      }
      ss << "\n  }";
      if (i + 1 < files.size()) ss << ",";
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
    auto qpr = obj->getQueryPrepareResponse();
    ss << "{\n";
    if (qpr) {
      ss << "  \"request_id\": \"" << qpr->request_id << "\",\n";
      ss << "  \"responses\": [\n";
      for (size_t i = 0; i < qpr->responses.size(); ++i) {
        const auto& r = qpr->responses[i];
        ss << "    {\n";
        ss << "      \"path\": \"" << r.path << "\",\n";
        ss << "      \"path_exists\": " << (r.is_exists ? "true" : "false") << ",\n";
        ss << "      \"on_tape\": " << (r.is_on_tape ? "true" : "false") << ",\n";
        ss << "      \"online\": " << (r.is_online ? "true" : "false") << ",\n";
        ss << "      \"requested\": " << (r.is_requested ? "true" : "false") << ",\n";
        ss << "      \"has_reqid\": " << (r.is_reqid_present ? "true" : "false") << ",\n";
        ss << "      \"req_time\": \"" << r.request_time << "\",\n";
        ss << "      \"error_text\": \"" << r.error_text << "\"\n";
        ss << "    }";
        if (i + 1 < qpr->responses.size()) ss << ",";
        ss << "\n";
      }
      ss << "  ]\n";
    }
    ss << "}";
  }
};

// GetTapeWellKnownModel jsonifier
class GetTapeWellKnownModelJsonifier : public TapeRestApiJsonifier<GetTapeWellKnownModel>
{
public:
  void jsonify(const GetTapeWellKnownModel* obj, std::stringstream& ss) override
  {
    ss << "{\n  \"versions\": [\n";
    const TapeWellKnownInfos* infos = obj->getTapeWellKnownInfos();
    const auto& endpoints = infos->getEndpoints();
    for (size_t i = 0; i < endpoints.size(); ++i) {
      const auto& ep = endpoints[i];
      ss << "    { \"version\": \"" << ep->getVersion() << "\", \"url\": \"" << ep->getUri() << "\" }";
      if (i + 1 < endpoints.size()) ss << ",";
      ss << "\n";
    }
    ss << "  ]\n}";
  }
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_JSONIFIERS_HH


