// ----------------------------------------------------------------------
// File: GrpcStreamReply.hh
// Author: Octavian-Mihai Matei - CERN
// ----------------------------------------------------------------------

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

#pragma once

#include "mgm/Namespace.hh"

#ifdef EOS_GRPC_GATEWAY

#include "proto/eos_rest_gateway/eos_rest_gateway_service.grpc.pb.h"
#include <string>
#include <string_view>

EOSMGMNAMESPACE_BEGIN

//! Maximum amount of stdout carried by a single streamed ReplyProto. Kept well
//! below the 4MB default gRPC message size limit so that a client using the
//! default settings can always consume the stream.
static constexpr size_t gStreamChunkSize = 1024 * 1024;

//------------------------------------------------------------------------------
//! Send a command reply to the client as a stream of ReplyProto messages
//!
//! The stdout of the reply is split on line boundaries into chunks of at most
//! chunk_size bytes. Intermediate chunks carry only stdout, while the final
//! message carries the return code and stderr, so that a client reading the
//! stream to completion observes the command status exactly once. An empty
//! reply still produces a single message.
//!
//! @note This bounds the size of each individual gRPC message, which is what
//! the message size limit applies to. It does not bound the memory used to
//! produce the reply in the first place - a command whose output must not be
//! materialised at all has to drive the writer incrementally itself, the way
//! NewfindCmd does.
//!
//! @param reply command reply to stream out
//! @param writer gRPC stream writer, or any type with a
//!        Write(const eos::console::ReplyProto&) method
//! @param chunk_size maximum stdout bytes per message
//------------------------------------------------------------------------------
template <typename Writer>
void
WriteStreamReply(const eos::console::ReplyProto& reply, Writer* writer,
                 size_t chunk_size = gStreamChunkSize)
{
  const std::string& std_out = reply.std_out();
  size_t offset = 0;

  // All but the last message carry stdout only, so that a client reading the
  // stream to completion observes the command status exactly once, at the end.
  while (std_out.size() - offset > chunk_size) {
    size_t len = chunk_size;
    // Cut on a line boundary whenever possible, which is always safe since a
    // newline is a single byte.
    const std::string_view window(std_out.data() + offset, chunk_size);
    const size_t nl = window.rfind('\n');

    if (nl != std::string_view::npos) {
      len = nl + 1;
    } else {
      // A single line longer than the chunk size has to be cut mid-line. The
      // stdout of a ReplyProto is a proto3 string, so it must stay valid UTF-8
      // and the cut may not fall inside a multi-byte sequence - EOS listings
      // are full of multi-byte box drawing characters. Back off over any
      // continuation bytes (10xxxxxx) so the chunk ends on a character.
      while ((len > 1) &&
             ((static_cast<unsigned char>(std_out[offset + len]) & 0xC0) == 0x80)) {
        --len;
      }
    }

    eos::console::ReplyProto chunk;
    chunk.set_std_out(std_out.substr(offset, len));
    writer->Write(chunk);
    offset += len;
  }

  eos::console::ReplyProto last;
  last.set_std_out(std_out.substr(offset));
  last.set_std_err(reply.std_err());
  last.set_retc(reply.retc());
  writer->Write(last);
}

EOSMGMNAMESPACE_END

#endif // EOS_GRPC_GATEWAY
