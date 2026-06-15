//------------------------------------------------------------------------------
//! @file GetArchiveInfoResponseModelTest.cc
//! @author Cedric Caffy - CERN
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

#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.hh"
#include <gtest/gtest.h>

using namespace eos::mgm::rest;
using eos::mgm::bulk::QueryPrepareFileResponse;
using eos::mgm::bulk::QueryPrepareResponse;

namespace {

std::shared_ptr<QueryPrepareResponse> makeQueryPrepareResponse(
  std::vector<QueryPrepareFileResponse> responses)
{
  auto response = std::make_shared<QueryPrepareResponse>();
  response->responses = std::move(responses);
  return response;
}

QueryPrepareFileResponse makeFileResponse(const std::string& path)
{
  return QueryPrepareFileResponse(path);
}

} // namespace

TEST(GetArchiveInfoResponseModelTest, prefersPrepareManagerErrorTextForMissingFile)
{
  auto file = makeFileResponse("/eos/user/missing.txt");
  file.error_text = "USER ERROR: custom prepare error";
  auto model = GetArchiveInfoResponseModel(makeQueryPrepareResponse({file}));
  ASSERT_EQ(1u, model.getEntries().size());
  ASSERT_EQ("/eos/user/missing.txt", model.getEntries()[0].path);
  ASSERT_FALSE(model.getEntries()[0].locality.has_value());
  ASSERT_TRUE(model.getEntries()[0].error.has_value());
  ASSERT_EQ("USER ERROR: custom prepare error", *model.getEntries()[0].error);
}

TEST(GetArchiveInfoResponseModelTest, usesFallbackErrorWhenMissingFileHasNoErrorText)
{
  auto file = makeFileResponse("/eos/user/missing.txt");
  auto model = GetArchiveInfoResponseModel(makeQueryPrepareResponse({file}));
  ASSERT_TRUE(model.getEntries()[0].error.has_value());
  ASSERT_EQ("USER ERROR: file does not exist or is not accessible to you",
            *model.getEntries()[0].error);
}

TEST(GetArchiveInfoResponseModelTest, errorTakesPrecedenceOverLocality)
{
  auto file = makeFileResponse("/eos/user/file.txt");
  file.is_exists = true;
  file.is_online = true;
  file.is_on_tape = true;
  file.error_text = "ERROR_ARCHIVE";
  auto model = GetArchiveInfoResponseModel(makeQueryPrepareResponse({file}));
  ASSERT_FALSE(model.getEntries()[0].locality.has_value());
  ASSERT_EQ("ERROR_ARCHIVE", *model.getEntries()[0].error);
}

TEST(GetArchiveInfoResponseModelTest, mapsLocalityValues)
{
  auto diskOnly = makeFileResponse("/disk");
  diskOnly.is_exists = true;
  diskOnly.is_online = true;

  auto tapeOnly = makeFileResponse("/tape");
  tapeOnly.is_exists = true;
  tapeOnly.is_on_tape = true;

  auto both = makeFileResponse("/both");
  both.is_exists = true;
  both.is_online = true;
  both.is_on_tape = true;

  auto unavailable = makeFileResponse("/none");
  unavailable.is_exists = true;

  auto model = GetArchiveInfoResponseModel(makeQueryPrepareResponse(
  {diskOnly, tapeOnly, both, unavailable}));
  ASSERT_EQ("DISK", *model.getEntries()[0].locality);
  ASSERT_EQ("TAPE", *model.getEntries()[1].locality);
  ASSERT_EQ("DISK_AND_TAPE", *model.getEntries()[2].locality);
  ASSERT_EQ("UNAVAILABLE", *model.getEntries()[3].locality);
}
