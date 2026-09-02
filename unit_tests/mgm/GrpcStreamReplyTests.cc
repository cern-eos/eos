//------------------------------------------------------------------------------
// File: GrpcStreamReplyTests.cc
// Author: Octavian-Mihai Matei - CERN
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

#include "mgm/grpc/GrpcStreamReply.hh"
#include "gtest/gtest.h"

#ifndef EOS_GRPC_GATEWAY
#error "EOS_GRPC_GATEWAY must be defined for these tests to cover anything"
#endif

#include <string>
#include <vector>

using eos::console::ReplyProto;

//------------------------------------------------------------------------------
// Collects the messages that WriteStreamReply would put on a gRPC stream
//------------------------------------------------------------------------------
class FakeWriter {
public:
  bool
  Write(const ReplyProto& msg)
  {
    mMsgs.push_back(msg);
    return true;
  }

  //! Concatenated stdout of every message, i.e. what a client reassembles
  std::string
  Joined() const
  {
    std::string out;

    for (const auto& m : mMsgs) {
      out += m.std_out();
    }

    return out;
  }

  std::vector<ReplyProto> mMsgs;
};

//------------------------------------------------------------------------------
// True if the given string is well formed UTF-8. A proto3 string field must
// be, otherwise the client refuses to unmarshal the message.
//------------------------------------------------------------------------------
static bool
IsValidUtf8(const std::string& str)
{
  size_t i = 0;

  while (i < str.size()) {
    const unsigned char c = static_cast<unsigned char>(str[i]);
    size_t extra = 0;

    if (c < 0x80) {
      extra = 0;
    } else if ((c & 0xE0) == 0xC0) {
      extra = 1;
    } else if ((c & 0xF0) == 0xE0) {
      extra = 2;
    } else if ((c & 0xF8) == 0xF0) {
      extra = 3;
    } else {
      return false; // lone continuation byte or invalid lead byte
    }

    if (i + extra >= str.size()) {
      return false; // truncated sequence
    }

    for (size_t k = 1; k <= extra; ++k) {
      if ((static_cast<unsigned char>(str[i + k]) & 0xC0) != 0x80) {
        return false;
      }
    }

    i += extra + 1;
  }

  return true;
}

//------------------------------------------------------------------------------
// Asserts the invariants that every streamed reply has to satisfy
//------------------------------------------------------------------------------
static void
CheckStreamInvariants(const FakeWriter& w, size_t chunk_size)
{
  ASSERT_FALSE(w.mMsgs.empty()) << "a reply must produce at least one message";

  for (size_t i = 0; i < w.mMsgs.size(); ++i) {
    const ReplyProto& m = w.mMsgs[i];
    // Each message is unmarshalled on its own, so each has to be valid UTF-8
    EXPECT_TRUE(IsValidUtf8(m.std_out()))
        << "message " << i << " is cut inside a multi-byte character";

    if (i + 1 < w.mMsgs.size()) {
      // Only the last message carries the command status
      EXPECT_EQ(0, m.retc()) << "message " << i << " carries a return code";
      EXPECT_TRUE(m.std_err().empty()) << "message " << i << " carries stderr";
      EXPECT_LE(m.std_out().size(), chunk_size)
          << "message " << i << " is above the chunk size";
    }
  }
}

TEST(GrpcStreamReply, EmptyReplyStillProducesOneMessage)
{
  ReplyProto reply;
  reply.set_retc(0);
  FakeWriter w;
  eos::mgm::WriteStreamReply(reply, &w, 1024);
  ASSERT_EQ(1u, w.mMsgs.size());
  EXPECT_EQ("", w.Joined());
}

TEST(GrpcStreamReply, ShortReplyIsNotSplit)
{
  ReplyProto reply;
  reply.set_std_out("one\ntwo\nthree\n");
  reply.set_std_err("a warning");
  reply.set_retc(3);
  FakeWriter w;
  eos::mgm::WriteStreamReply(reply, &w, 1024);
  ASSERT_EQ(1u, w.mMsgs.size());
  EXPECT_EQ("one\ntwo\nthree\n", w.Joined());
  EXPECT_EQ(3, w.mMsgs.back().retc());
  EXPECT_EQ("a warning", w.mMsgs.back().std_err());
}

TEST(GrpcStreamReply, StatusIsCarriedByTheLastMessageOnly)
{
  ReplyProto reply;
  std::string out;

  for (int i = 0; i < 500; ++i) {
    out += "some line of output\n";
  }

  reply.set_std_out(out);
  reply.set_std_err("the error");
  reply.set_retc(22);
  FakeWriter w;
  eos::mgm::WriteStreamReply(reply, &w, 256);
  ASSERT_GT(w.mMsgs.size(), 1u) << "the input must be large enough to split";
  CheckStreamInvariants(w, 256);
  EXPECT_EQ(out, w.Joined());
  EXPECT_EQ(22, w.mMsgs.back().retc());
  EXPECT_EQ("the error", w.mMsgs.back().std_err());
}

TEST(GrpcStreamReply, MultiLineOutputIsSplitOnLineBoundaries)
{
  ReplyProto reply;
  std::string out;

  for (int i = 0; i < 100; ++i) {
    out += "line " + std::to_string(i) + "\n";
  }

  reply.set_std_out(out);
  FakeWriter w;
  eos::mgm::WriteStreamReply(reply, &w, 64);
  ASSERT_GT(w.mMsgs.size(), 1u);
  CheckStreamInvariants(w, 64);
  EXPECT_EQ(out, w.Joined());

  // Every message but the last ends on a line boundary, because the input has
  // no line longer than the chunk size
  for (size_t i = 0; i + 1 < w.mMsgs.size(); ++i) {
    const std::string& chunk = w.mMsgs[i].std_out();
    ASSERT_FALSE(chunk.empty());
    EXPECT_EQ('\n', chunk.back()) << "message " << i << " cuts a line";
  }
}

TEST(GrpcStreamReply, OneLongLineIsSplitMidLine)
{
  ReplyProto reply;
  const std::string out(5000, 'x');
  reply.set_std_out(out);
  FakeWriter w;
  eos::mgm::WriteStreamReply(reply, &w, 512);
  ASSERT_GT(w.mMsgs.size(), 1u) << "a line above the chunk size must be split";
  CheckStreamInvariants(w, 512);
  EXPECT_EQ(out, w.Joined());
}

//------------------------------------------------------------------------------
// Regression test. An EOS listing is full of multi-byte box drawing characters
// and puts a whole table row on one line, so the mid-line cut lands inside a
// multi-byte sequence unless it backs off to a character boundary. A message
// cut that way is rejected by the client with "string field contains invalid
// UTF-8" and the whole reply is lost.
//------------------------------------------------------------------------------
TEST(GrpcStreamReply, LongLineOfMultiByteCharsIsNotCutInsideACharacter)
{
  // U+2500 BOX DRAWINGS LIGHT HORIZONTAL, three bytes each
  const std::string dash = "\xE2\x94\x80";
  std::string out;

  for (int i = 0; i < 2000; ++i) {
    out += dash;
  }

  ASSERT_EQ(6000u, out.size());
  ReplyProto reply;
  reply.set_std_out(out);
  reply.set_retc(0);

  // Sweep chunk sizes so the cut falls at every offset inside a character
  for (size_t chunk = 16; chunk <= 24; ++chunk) {
    FakeWriter w;
    eos::mgm::WriteStreamReply(reply, &w, chunk);
    ASSERT_GT(w.mMsgs.size(), 1u) << "chunk=" << chunk;
    CheckStreamInvariants(w, chunk);
    EXPECT_EQ(out, w.Joined()) << "chunk=" << chunk;
  }
}

TEST(GrpcStreamReply, MixedAsciiAndMultiByteLinesReassembleExactly)
{
  std::string out;

  for (int i = 0; i < 200; ++i) {
    // A row of a table: multi-byte borders around ascii content
    out +=
        "\xE2\x94\x82 entry " + std::to_string(i) + " \xE2\x94\x82 value \xE2\x94\x82\n";
  }

  ReplyProto reply;
  reply.set_std_out(out);
  reply.set_std_err("done");
  reply.set_retc(1);

  for (size_t chunk : {8u, 13u, 32u, 100u, 4096u}) {
    FakeWriter w;
    eos::mgm::WriteStreamReply(reply, &w, chunk);
    CheckStreamInvariants(w, chunk);
    EXPECT_EQ(out, w.Joined()) << "chunk=" << chunk;
    EXPECT_EQ(1, w.mMsgs.back().retc()) << "chunk=" << chunk;
    EXPECT_EQ("done", w.mMsgs.back().std_err()) << "chunk=" << chunk;
  }
}

TEST(GrpcStreamReply, OutputExactlyOnTheChunkBoundaryIsNotSplit)
{
  ReplyProto reply;
  const std::string out(512, 'y');
  reply.set_std_out(out);
  FakeWriter w;
  eos::mgm::WriteStreamReply(reply, &w, 512);
  // The loop runs only while more than chunk_size bytes are left
  ASSERT_EQ(1u, w.mMsgs.size());
  EXPECT_EQ(out, w.Joined());
}

TEST(GrpcStreamReply, DefaultChunkSizeIsBelowTheGrpcMessageLimit)
{
  // The default gRPC MaxCallRecvMsgSize is 4MB. The chunk size has to stay
  // well below it, otherwise a client using the defaults cannot read the
  // stream.
  EXPECT_LT(eos::mgm::gStreamChunkSize, 4u * 1024 * 1024);
}
