//------------------------------------------------------------------------------
//! @file File.hh
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

#include "File.hh"

EOSBULKNAMESPACE_BEGIN

File::File(){}

File::File(const std::string& path): mPath(path) {
}

void File::setPath(const std::string& path) {
  mPath = path;
}

const std::string File::getPath() const {
  return mPath;
}

void File::setError(const std::string& error) {
  std::optional<std::string> errorOpt(error);
  setError(errorOpt);
}

void File::setError(const std::optional<std::string> & error) {
  mError = error;
  if(error)
    mState = State::ERROR;
}

void File::setState(const std::optional<std::string> & state)
{
  if (state && STRING_TO_STATE_MAP.find(state.value()) != STRING_TO_STATE_MAP.end()) {
    mState = STRING_TO_STATE_MAP.at(*state);
  }
}

void File::setState(const State& state) {
  mState = state;
}

void File::setState(const std::string& state) {
  if(STRING_TO_STATE_MAP.find(state) != STRING_TO_STATE_MAP.end()) {
    mState = STRING_TO_STATE_MAP.at(state);
  }
  //State is not set if the string does not match any existing state
}

const std::optional<File::State> File::getState() const {
  return mState;
}

const std::optional<std::string> File::getStateStr() const {
  if(mState) {
    return STATE_TO_STRING_MAP.at(*mState);
  }
  return std::optional<std::string>();
}

const std::string File::getStateStr(const State& state) {
  return STATE_TO_STRING_MAP.at(state);
}

void File::setErrorIfNotAlreadySet(const std::string& error) {
  if(!getError()){
    setError(error);
  }
}

const std::optional<std::string> File::getError() const {
  return mError;
}

bool File::operator==(const File& other) const {
  return getPath() == other.getPath();
}

bool File::operator<(const File& other) const {
  return getPath() < other.getPath();
}

EOSBULKNAMESPACE_END
