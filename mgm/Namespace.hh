// ----------------------------------------------------------------------
// File: Namespace.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSMGM_NAMESPACE_HH__
#define __EOSMGM_NAMESPACE_HH__

#define USE_EOSMGMNAMESPACE using namespace eos::mgm;

#define EOSMGMNAMESPACE_BEGIN namespace eos { namespace mgm {
#define EOSMGMNAMESPACE_END }}


#define USE_EOSFUSESERVERNAMESPACE using namespace eos::mgm::FuseServer;

#define EOSFUSESERVERNAMESPACE_BEGIN namespace eos { namespace mgm { namespace FuseServer {
#define EOSFUSESERVERNAMESPACE_END }}}

#define EOSTGCNAMESPACE_BEGIN namespace eos { namespace mgm { namespace tgc {
#define EOSTGCNAMESPACE_END }}}

#define USE_EOSBULKNAMESPACE using namespace eos::mgm::bulk;

#define EOSBULKNAMESPACE_BEGIN namespace eos { namespace mgm { namespace bulk {
#define EOSBULKNAMESPACE_END }}}

#endif
