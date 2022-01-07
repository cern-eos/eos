// ----------------------------------------------------------------------
// File: ErrorModel.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "ErrorModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

ErrorModel::ErrorModel(){}

ErrorModel::ErrorModel(const std::string& title, const uint32_t status, const std::string& detail):mTitle(title),mStatus(status),mDetail(detail)
{}

ErrorModel::ErrorModel(const std::string& title, const uint32_t status):mTitle(title),mStatus(status)
{}

void ErrorModel::setType(const std::string& type){
  mType = type;
}

void ErrorModel::setTitle(const std::string & title){
  mTitle = title;
}

void ErrorModel::setStatus(const uint32_t status){
  mStatus = status;
}

void ErrorModel::setDetail(const std::string& detail){
  mDetail = detail;
}

const std::string ErrorModel::getType() const {
  return mType;
}

const std::string ErrorModel::getTitle() const{
  return mTitle;
}

const uint32_t ErrorModel::getStatus() const {
  return mStatus;
}

const std::optional<std::string> ErrorModel::getDetail() const{
  return mDetail;
}

EOSMGMRESTNAMESPACE_END