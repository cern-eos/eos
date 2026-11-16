//------------------------------------------------------------------------------
//! @file DocumentIndex.hh
//! @brief Document suffix gate and text extraction for the Qdrant index
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

#pragma once
#include "common/Namespace.hh"
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

//! QuarkDB keys shared by the FST scan and the MGM indexer.
//! Arrays, not pointers: qclient::QClient::exec sizes string literals by
//! array bound and rejects a const char*.
inline constexpr char kQdrantEnabledKey[] = "eos-qdrant-enabled";
inline constexpr char kQdrantMaxBytesKey[] = "eos-qdrant-maxbytes";
inline constexpr char kQdrantEndpointKey[] = "eos-qdrant-endpoint";
inline constexpr char kQdrantApiKeyKey[] = "eos-qdrant-apikey";
inline constexpr char kQdrantCollectionKey[] = "eos-qdrant-collection";
inline constexpr char kQdrantEmbedEndpointKey[] = "eos-qdrant-embed-endpoint";
inline constexpr char kQdrantEmbedModelKey[] = "eos-qdrant-embed-model";
inline constexpr char kQdrantQueueKey[] = "eos-qdrant-index";
inline constexpr char kQdrantCatalogKey[] = "eos-qdrant-catalog";
inline constexpr char kQdrantClaimKey[] = "eos-qdrant-claim";
inline constexpr char kQdrantTombstoneKey[] = "eos-qdrant-tombstone";

//! Default document size accepted into the index.
inline constexpr uint64_t kQdrantDefaultMaxBytes = 32ull * 1024ull * 1024ull;

//! Characters per embedding chunk, overlap, and the cap per file.
inline constexpr size_t kQdrantChunkChars = 1200;
inline constexpr size_t kQdrantChunkOverlap = 200;
inline constexpr size_t kQdrantMaxChunks = 64;

//------------------------------------------------------------------------------
//! True when the basename suffix is one of the document types the indexer
//! accepts. Matching is case-insensitive and uses the final suffix only.
//------------------------------------------------------------------------------
bool IsIndexableDocument(std::string_view path);

//------------------------------------------------------------------------------
//! Extract plain text from a document body. An empty string means the bytes
//! are not a document of the claimed type, or they contain no text.
//------------------------------------------------------------------------------
std::string ExtractDocumentText(std::string_view path, std::string_view bytes);

//------------------------------------------------------------------------------
//! Split extracted text into overlapping chunks. Whitespace-only pieces are
//! omitted, and the result is capped at kQdrantMaxChunks.
//------------------------------------------------------------------------------
std::vector<std::string> ChunkDocumentText(std::string_view text);

EOSCOMMONNAMESPACE_END
