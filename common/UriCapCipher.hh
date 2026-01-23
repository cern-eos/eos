//------------------------------------------------------------------------------
// File: UriCapCipher.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------
//! @file UriCapCipher.hh
//! @author Andreas-Joachim Peters
//! @brief URI capability cipher helper
//!
//! Usage overview:
//! - Construct with a secret file path (password derived from file contents),
//!   or provide a password string directly using PasswordTag.
//! - encryptToCgiFields() returns "cap.sym=...&cap.msg=..." where cap.sym holds
//!   the header (version/KDF params/salt/nonce) and cap.msg holds ciphertext+tag.
//! - Encryption uses AEAD (chacha20-poly1305), so the tag authenticates the
//!   ciphertext and the associated data; cap.sym is bound as AAD, meaning any
//!   tampering of either cap.sym or cap.msg is detected during decryption.
//! - decryptFromCgiFields() parses those fields and returns the plaintext or ""
//!   on failure.
//!
//! Performance notes:
//! - Default mode generates a fresh random salt per message and runs scrypt for
//!   each encrypt/decrypt.
//! - The FixedSaltTag constructor caches the derived key using one random salt
//!   at startup; encryption still uses a fresh nonce per message. Decryption
//!   reuses the cached key only when the message salt matches.
//------------------------------------------------------------------------------

#pragma once
#include "common/Namespace.hh"
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

class UriCapCipher
{
public:
  struct PasswordTag {
    explicit PasswordTag() = default;
  };
  struct FixedSaltTag {
    explicit FixedSaltTag() = default;
  };

  explicit UriCapCipher(const std::string& secret_file_path);
  explicit UriCapCipher(PasswordTag, std::string password);
  explicit UriCapCipher(PasswordTag, FixedSaltTag, std::string password);

  // Encrypt arbitrary string into "cap.sym=...&cap.msg=..."
  std::string encryptToCgiFields(const std::string& plaintext) const;

  // Decrypt from a string containing cap.sym=... and cap.msg=...
  // Returns decrypted string, or "" on any failure.
  std::string decryptFromCgiFields(const std::string& cgi) const;

private:
  // ----- Tunables -----
  static constexpr uint64_t kN = 1ull << 15; // scrypt N (CPU/mem cost)
  static constexpr uint64_t kR = 8;
  static constexpr uint64_t kP = 1;

  static constexpr size_t kSaltLen = 16;
  static constexpr size_t kNonceLen = 12;
  static constexpr size_t kTagLen = 16;

  // Fixed binary header (packed manually; do not rely on struct packing)
  struct Header {
    uint8_t v;       // version
    uint8_t kdf;     // 1=scrypt
    uint8_t aead;    // 1=chacha20-poly1305
    uint8_t reserved;// future use
    uint64_t N;      // scrypt N
    uint64_t r;      // scrypt r
    uint64_t p;      // scrypt p
    uint8_t salt[kSaltLen];
    uint8_t nonce[kNonceLen];
  };

  std::string pw_; // 32 bytes (binary) derived from file or provided directly
  bool has_cached_key_ = false;
  std::array<uint8_t, kSaltLen> cached_salt_{};
  std::vector<uint8_t> cached_key_;

  // ----- OpenSSL error helper -----
  static void throw_openssl(const char* what);

  // ----- Password derivation from file -----
  static std::string compute_password_from_file(const std::string& path);

  // ----- base64url encode/decode -----
  static std::string b64url_encode(const std::vector<uint8_t>& in);
  static std::vector<uint8_t> b64url_decode(const std::string& in);

  // ----- URL percent decode (minimal) -----
  static int hexval(char c);
  static std::string url_percent_decode(const std::string& s);

  // ----- Query parsing -----
  static std::string getQueryValue(const std::string& input,
                                   const std::string& key);

  // ----- Little-endian helpers -----
  static void put_u64_le(std::vector<uint8_t>& out, uint64_t x);
  static uint64_t get_u64_le(const uint8_t* p);

  // ----- Header serialize/deserialize -----
  static std::vector<uint8_t> serializeHeader(const Header& h);
  static Header deserializeHeader(const std::vector<uint8_t>& in);

  // ----- scrypt KDF -----
  static std::vector<uint8_t> kdf_scrypt(const std::string& password_bytes,
                                         const std::vector<uint8_t>& salt,
                                         uint64_t N, uint64_t r, uint64_t p,
                                         size_t key_len);

  // ----- AEAD encrypt/decrypt -----
  static void aead_encrypt_chacha20poly1305(
      const std::vector<uint8_t>& key,
      const std::vector<uint8_t>& nonce12,
      const std::string& aad,
      const std::vector<uint8_t>& plaintext,
      std::vector<uint8_t>& ciphertext_out,
      std::vector<uint8_t>& tag16_out);

  static std::vector<uint8_t> aead_decrypt_chacha20poly1305(
      const std::vector<uint8_t>& key,
      const std::vector<uint8_t>& nonce12,
      const std::string& aad,
      const std::vector<uint8_t>& ciphertext,
      const std::vector<uint8_t>& tag16);
};

EOSCOMMONNAMESPACE_END
