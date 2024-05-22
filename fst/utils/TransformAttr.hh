//------------------------------------------------------------------------------
//! @file TransformAttr.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
  * EOS - the CERN Disk Storage System                                   *
  * Copyright (C) 2022 CERN/Switzerland                           *
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


#pragma once

#include "common/Logging.hh"
#include "fst/io/local/FsIo.hh"

namespace eos::fst
{

template <typename transformFn>
bool TransformAttr(const std::string& path,
                   const std::string& attrName,
                   transformFn f)
{
  FsIo fsio{path};
  std::string attrval;
  int rc = fsio.attrGet(gFmdAttrName, attrval);

  if (rc != 0) {
    eos_static_err("msg=\"Failed to retrieve filemd attr\" path=%s",
                   path.c_str());
    return false;
  }

  rc = fsio.attrSet(gFmdAttrName, f(attrval));

  if (rc != 0) {
    eos_static_err("msg=\"Failed to set filemd attr\" path=%s",
                   path.c_str());
    return false;
  }

  return true;
}

} // namespace eos::fst
