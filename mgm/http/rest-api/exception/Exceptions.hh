// ----------------------------------------------------------------------
// File: Exceptions.hh
// Author: Simplified umbrella for REST exceptions
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) CERN/Switzerland                                       *
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

#ifndef EOS_REST_EXCEPTIONS_HH
#define EOS_REST_EXCEPTIONS_HH

#include "mgm/http/rest-api/exception/RestException.hh"
#include <string>

#include "mgm/Namespace.hh"

EOSMGMRESTNAMESPACE_BEGIN

class NotFoundException : public RestException
{
public:
  explicit NotFoundException(const std::string& exceptionMsg): RestException(exceptionMsg) {}
};

class MethodNotAllowedException : public RestException
{
public:
  explicit MethodNotAllowedException(const std::string& exceptionMsg): RestException(exceptionMsg) {}
};

class ForbiddenException : public RestException
{
public:
  explicit ForbiddenException(const std::string& exceptionMsg): RestException(exceptionMsg) {}
};

class NotImplementedException : public RestException
{
public:
  explicit NotImplementedException(const std::string& exceptionMsg): RestException(exceptionMsg) {}
};

class ObjectNotFoundException : public RestException
{
public:
  explicit ObjectNotFoundException(const std::string& exceptionMsg): RestException(exceptionMsg) {}
};

class ActionNotFoundException : public NotFoundException
{
public:
  explicit ActionNotFoundException(const std::string& exceptionMsg): NotFoundException(exceptionMsg) {}
};

class ControllerNotFoundException : public NotFoundException
{
public:
  explicit ControllerNotFoundException(const std::string& exceptionMsg): NotFoundException(exceptionMsg) {}
};

EOSMGMRESTNAMESPACE_END

#include "mgm/http/rest-api/exception/JsonValidationException.hh"

// Tape business exceptions (same namespace as RestException)
EOSMGMRESTNAMESPACE_BEGIN

class TapeRestApiBusinessException : public RestException
{
public:
  explicit TapeRestApiBusinessException(const std::string& errorMsg)
    : RestException(errorMsg) {}
};

class FileDoesNotBelongToBulkRequestException : public RestException
{
public:
  explicit FileDoesNotBelongToBulkRequestException(const std::string& errorMsg)
    : RestException(errorMsg) {}
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_REST_EXCEPTIONS_HH


