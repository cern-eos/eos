// ----------------------------------------------------------------------
// File: ErrorHandling.hh
// Author: Refactor - Centralized error to response mapping
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

#ifndef EOS_REST_ERROR_HANDLING_HH
#define EOS_REST_ERROR_HANDLING_HH

#include "common/Logging.hh"
#include "common/http/HttpResponse.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"

EOSMGMRESTNAMESPACE_BEGIN

template <typename ResponseFactory, typename Fn>
common::HttpResponse* HandleWithErrors(ResponseFactory & responseFactory, Fn fn)
{
  try {
    return fn();
  } catch (const NotFoundException& ex) {
    eos_static_info(ex.what());
    return responseFactory.NotFound().getHttpResponse();
  } catch (const MethodNotAllowedException& ex) {
    eos_static_info(ex.what());
    return responseFactory.MethodNotAllowed(ex.what()).getHttpResponse();
  } catch (const ForbiddenException& ex) {
    eos_static_info(ex.what());
    return responseFactory.Forbidden(ex.what()).getHttpResponse();
  } catch (const NotImplementedException& ex) {
    eos_static_info(ex.what());
    return responseFactory.NotImplemented().getHttpResponse();
  } catch (const RestException& ex) {
    eos_static_info(ex.what());
    return responseFactory.InternalError(ex.what()).getHttpResponse();
  } catch (...) {
    std::string errorMsg = "Unknown exception occured";
    eos_static_err(errorMsg.c_str());
    return responseFactory.InternalError(errorMsg).getHttpResponse();
  }
}

EOSMGMRESTNAMESPACE_END

#endif // EOS_REST_ERROR_HANDLING_HH


