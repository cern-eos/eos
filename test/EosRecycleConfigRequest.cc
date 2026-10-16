//------------------------------------------------------------------------------
//! @file EosRecycleConfigRequest.cc
//! @brief Encode literal recycle configuration paths for instance regressions.
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "common/SymKeys.hh"
#include "proto/ConsoleRequest.pb.h"
#include <iostream>
#include <string>

int
main(int argc, char* argv[])
{
  if (argc != 3 ||
      (std::string(argv[1]) != "--add-bin" && std::string(argv[1]) != "--remove-bin")) {
    std::cerr << "usage: " << argv[0] << " --add-bin|--remove-bin <literal-subtree>\n";
    return 1;
  }

  eos::console::RequestProto request;
  auto* config = request.mutable_recycle()->mutable_config();
  config->set_op(std::string(argv[1]) == "--add-bin"
                     ? eos::console::RecycleProto_ConfigProto::ADD_BIN
                     : eos::console::RecycleProto_ConfigProto::RM_BIN);
  // Do not use RecycleHelper: its legacy #AND# sealing masks the MGM bug.
  config->set_subtree(argv[2]);
  std::string encoded;

  if (!eos::common::SymKey::ProtobufBase64Encode(&request, encoded)) {
    std::cerr << "error: failed to encode recycle configuration request\n";
    return 1;
  }

  std::cout << encoded << '\n';
  return 0;
}
