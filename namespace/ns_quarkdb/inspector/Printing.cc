/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include "common/LayoutId.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/interface/IFileMD.hh"
#include <sstream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Print the given FileMd protobuf using multiple lines, full information
//------------------------------------------------------------------------------
void Printing::printMultiline(const eos::ns::FileMdProto &proto, std::ostream &stream) {
  stream << "ID: " << proto.id() << std::endl;
  stream << "Name: " << proto.name() << std::endl;
  stream << "Container ID: " << proto.cont_id() << std::endl;
  stream << "uid: " << proto.uid() << ", gid: " << proto.gid() << std::endl;
  stream << "Size: " << proto.size() << std::endl;

  std::string checksum;
  Buffer checksumBuffer(proto.checksum().size());
  checksumBuffer.putData((void*)proto.checksum().data(), proto.checksum().size());
  appendChecksumOnStringAsHexNoFmd(proto.layout_id(), checksumBuffer, checksum);

  stream << "Checksum type: " << common::LayoutId::GetChecksumString(proto.layout_id()) << ", checksum bytes: " << checksum << std::endl;
}

std::string Printing::printMultiline(const eos::ns::FileMdProto &proto) {
  std::ostringstream ss;
  printMultiline(proto, ss);
  return ss.str();
}

EOSNSNAMESPACE_END
