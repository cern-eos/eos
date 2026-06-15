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

class ErrorModelJsonifier : public TapeRestApiJsonifier<ErrorModel>
{
public:
  void jsonify(const ErrorModel* obj, std::stringstream& ss) override;
};

class CreatedStageBulkRequestJsonifier : public TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>
{
public:
  void jsonify(const CreatedStageBulkRequestResponseModel* obj,
               std::stringstream& ss) override;
};

class GetStageBulkRequestJsonifier : public TapeRestApiJsonifier<GetStageBulkRequestResponseModel>
{
public:
  void jsonify(const GetStageBulkRequestResponseModel* obj,
               std::stringstream& ss) override;
};

class GetArchiveInfoResponseJsonifier : public TapeRestApiJsonifier<GetArchiveInfoResponseModel>
{
public:
  void jsonify(const GetArchiveInfoResponseModel* obj, std::stringstream& ss) override;
};

class GetTapeWellKnownModelJsonifier : public TapeRestApiJsonifier<GetTapeWellKnownModel>
{
public:
  void jsonify(const GetTapeWellKnownModel* obj, std::stringstream& ss) override;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_JSONIFIERS_HH
