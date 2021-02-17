/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Attribute utilities
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/IView.hh"
#include "common/StringUtils.hh"
#include "common/Logging.hh"
#include <iostream>

EOSNSNAMESPACE_BEGIN

auto constexpr kAttrLinkKey = "sys.attr.link";

//------------------------------------------------------------------------------
// Populate 'out' map with attributes found in linkedAttrs. Do not override
// existing values.
//------------------------------------------------------------------------------
inline void populateLinkedAttributes(const eos::IContainerMD::XAttrMap&
                                     linkedAttrs,
                                     eos::IContainerMD::XAttrMap& out, bool prefixLinks)
{
  for (auto it = linkedAttrs.begin(); it != linkedAttrs.end(); it++) {
    //------------------------------------------------------------------------
    // Populate any linked extended attributes which don't exist yet
    //------------------------------------------------------------------------
    if (out.find(it->first) == out.end()) {
      std::string key;

      if (prefixLinks && common::startsWith(it->first, "sys.")) {
        key = SSTR("sys.link." << it->first.substr(4));
      } else {
        key = it->first;
      }

      out[key] = it->second;
    }
  }
}

//------------------------------------------------------------------------------
// Fill out the given map with any extended attributes found in given container,
// but DO NOT override existing values.
//------------------------------------------------------------------------------
inline void
populateLinkedAttributes(IView* view, eos::IContainerMD::XAttrMap& out,
                         bool prefixLinks)
{
  try {
    auto linkedPath = out.find(kAttrLinkKey);

    if (linkedPath == out.end() || linkedPath->second.empty()) {
      return;
    }

    IContainerMDPtr dh = view->getContainer(linkedPath->second);

    if (!dh) {
      return;
    }

    populateLinkedAttributes(dh->getAttributes(), out, prefixLinks);
  } catch (eos::MDException& e) {
    //--------------------------------------------------------------------------
    // Link does not exist, or is not a directory
    //--------------------------------------------------------------------------
    out["sys.attr.link"] = SSTR(out["sys.attr.link"] << " - not found");
    eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                     e.getMessage().str().c_str());
  }
}

//------------------------------------------------------------------------------
// Retrieve list of extended attributes, including linked ones.
//
// If the same attribute exists in both the target, and the link, the most
// specific one wins, ie the one on the target itself.
//
// Target is specified as IContainerMD.
//------------------------------------------------------------------------------
inline void
listAttributes(IView* view, IContainerMD* target,
               eos::IContainerMD::XAttrMap& out, bool prefixLinks = false)
{
  out.clear();
  out = target->getAttributes();
  populateLinkedAttributes(view, out, prefixLinks);
}

//------------------------------------------------------------------------------
// Retrieve list of extended attributes, including linked ones.
//
// If the same attribute exists in both the target, and the link, the most
// specific one wins, ie the one on the target itself.
//
// Target is specified as IFileMD.
//------------------------------------------------------------------------------
inline void
listAttributes(IView* view, IFileMD* target,
               eos::IContainerMD::XAttrMap& out, bool prefixLinks = false)
{
  out.clear();
  out = target->getAttributes();
  populateLinkedAttributes(view, out, prefixLinks);
}

//------------------------------------------------------------------------------
// Retrieve list of extended attributes, including linked ones.
//
// If the same attribute exists in both the target, and the link, the most
// specific one wins, ie the one on the target itself.
//
// Target is specified as FileOrContainerMD.
//------------------------------------------------------------------------------
inline void
listAttributes(IView* view, FileOrContainerMD target,
               eos::IContainerMD::XAttrMap& out, bool prefixLinks = false)
{
  out.clear();

  if (target.file) {
    listAttributes(view, target.file.get(), out, prefixLinks);
  } else if (target.container) {
    listAttributes(view, target.container.get(), out, prefixLinks);
  }
}

EOSNSNAMESPACE_END
