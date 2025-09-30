// ----------------------------------------------------------------------
// File: TapeActions.hh
// Author: Consolidated tape REST API actions
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 ************************************************************************/

#ifndef EOS_TAPE_ACTIONS_HH
#define EOS_TAPE_ACTIONS_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/action/tape/TapeAction.hh"
#include "mgm/http/rest-api/json/builder/JsonModelBuilder.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/model/tape/stage/CreateStageBulkRequestModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include "mgm/http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"

EOSMGMRESTNAMESPACE_BEGIN

// CreateStageBulkRequest
class CreateStageBulkRequest : public TapeAction
{
public:
  CreateStageBulkRequest(const std::string& accessURL,
                         const common::HttpHandler::Methods method,
                         std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,
                         std::shared_ptr<JsonModelBuilder<CreateStageBulkRequestModel>> inputJsonModelBuilder,
                         std::shared_ptr<TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>> outputObjectJsonifier,
                         const TapeRestHandler* tapeRestHandler)
    : TapeAction(accessURL, method, tapeRestApiBusiness),
      mInputJsonModelBuilder(inputJsonModelBuilder),
      mOutputObjectJsonifier(outputObjectJsonifier),
      mTapeRestHandler(tapeRestHandler) {}
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
private:
  const std::string generateAccessURL(const std::string& bulkRequestId);
  std::shared_ptr<JsonModelBuilder<CreateStageBulkRequestModel>> mInputJsonModelBuilder;
  std::shared_ptr<TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>> mOutputObjectJsonifier;
  const TapeRestHandler* mTapeRestHandler;
};

// GetStageBulkRequest
class GetStageBulkRequest : public TapeAction
{
public:
  GetStageBulkRequest(const std::string& accessURL,
                      const common::HttpHandler::Methods method,
                      std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,
                      std::shared_ptr<TapeRestApiJsonifier<GetStageBulkRequestResponseModel>> outputObjectJsonifier)
    : TapeAction(accessURL, method, tapeRestApiBusiness),
      mOutputObjectJsonifier(outputObjectJsonifier) {}
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
private:
  std::shared_ptr<TapeRestApiJsonifier<GetStageBulkRequestResponseModel>> mOutputObjectJsonifier;
};

// CancelStageBulkRequest
class CancelStageBulkRequest : public TapeAction
{
public:
  CancelStageBulkRequest(const std::string& accessURL,
                         const common::HttpHandler::Methods method,
                         std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,
                         std::shared_ptr<JsonModelBuilder<PathsModel>> inputJsonModelBuilder)
    : TapeAction(accessURL, method, tapeRestApiBusiness),
      mInputJsonModelBuilder(inputJsonModelBuilder) {}
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
private:
  std::shared_ptr<JsonModelBuilder<PathsModel>> mInputJsonModelBuilder;
};

// DeleteStageBulkRequest
class DeleteStageBulkRequest : public TapeAction
{
public:
  DeleteStageBulkRequest(const std::string& accessURL,
                         const common::HttpHandler::Methods method,
                         std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
    : TapeAction(accessURL, method, tapeRestApiBusiness) {}
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
};

// GetArchiveInfo
class GetArchiveInfo : public TapeAction
{
public:
  GetArchiveInfo(const std::string& accessURL,
                 const common::HttpHandler::Methods method,
                 std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,
                 std::shared_ptr<JsonModelBuilder<PathsModel>> inputJsonModelBuilder,
                 std::shared_ptr<TapeRestApiJsonifier<GetArchiveInfoResponseModel>> outputObjectJsonifier)
    : TapeAction(accessURL, method, tapeRestApiBusiness),
      mInputJsonModelBuilder(inputJsonModelBuilder),
      mOutputObjectJsonifier(outputObjectJsonifier) {}
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
private:
  std::shared_ptr<JsonModelBuilder<PathsModel>> mInputJsonModelBuilder;
  std::shared_ptr<TapeRestApiJsonifier<GetArchiveInfoResponseModel>> mOutputObjectJsonifier;
};

// CreateReleaseBulkRequest
class CreateReleaseBulkRequest : public TapeAction
{
public:
  CreateReleaseBulkRequest(const std::string& accessURL,
                           const common::HttpHandler::Methods method,
                           std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,
                           std::shared_ptr<JsonModelBuilder<PathsModel>> inputJsonModelBuilder)
    : TapeAction(accessURL, method, tapeRestApiBusiness),
      mInputJsonModelBuilder(inputJsonModelBuilder) {}
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
private:
  std::shared_ptr<JsonModelBuilder<PathsModel>> mInputJsonModelBuilder;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPE_ACTIONS_HH
