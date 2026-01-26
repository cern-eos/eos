//------------------------------------------------------------------------------
// File: UriCapCipher.cc
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

#include "common/UriCapCipher.hh"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iterator>
#include <sstream>
#include <stdexcept>

EOSCOMMONNAMESPACE_BEGIN

UriCapCipher::UriCapCipher(const std::string& secret_file_path)
  : pw_(compute_password_from_file(secret_file_path))
{
}

UriCapCipher::UriCapCipher(PasswordTag, std::string password)
  : pw_(std::move(password))
{
}

UriCapCipher::UriCapCipher(PasswordTag, FixedSaltTag, std::string password)
  : pw_(std::move(password))
{
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(pw_.data()), pw_.size(), digest);
  std::memcpy(cached_salt_.data(), digest, cached_salt_.size());
  std::vector<uint8_t> salt_vec(cached_salt_.begin(), cached_salt_.end());
  cached_key_ = kdf_scrypt(pw_, salt_vec, kN, kR, kP, 32);
  has_cached_key_ = true;
}

std::string
UriCapCipher::encryptToCgiFields(const std::string& plaintext) const
{
  // 1) Build header: fixed binary
  Header h{};
  h.v = 1;
  h.kdf = 1;   // 1 = scrypt
  h.aead = 1;  // 1 = chacha20-poly1305
  h.N = kN;
  h.r = kR;
  h.p = kP;

  const std::vector<uint8_t>* key_ptr = nullptr;
  std::vector<uint8_t> derived_key;
  bool wipe_key = false;

  if (has_cached_key_) {
    std::memcpy(h.salt, cached_salt_.data(), cached_salt_.size());
    key_ptr = &cached_key_;
  } else {
    if (RAND_bytes(h.salt, sizeof(h.salt)) != 1) {
      throw_openssl("RAND_bytes(salt)");
    }
    std::vector<uint8_t> salt_vec(h.salt, h.salt + sizeof(h.salt));
    derived_key = kdf_scrypt(pw_, salt_vec, h.N, h.r, h.p, 32);
    key_ptr = &derived_key;
    wipe_key = true;
  }
  if (RAND_bytes(h.nonce, sizeof(h.nonce)) != 1) {
    throw_openssl("RAND_bytes(nonce)");
  }

  // Serialize header -> cap.sym bytes
  std::vector<uint8_t> sym_bytes = serializeHeader(h);
  std::string cap_sym = b64url_encode(sym_bytes);

  // 2) AEAD encrypt with AAD = exact cap.sym string bytes
  std::vector<uint8_t> nonce_vec(h.nonce, h.nonce + sizeof(h.nonce));
  std::vector<uint8_t> pt_vec(plaintext.begin(), plaintext.end());
  std::vector<uint8_t> ct, tag;

  aead_encrypt_chacha20poly1305(*key_ptr, nonce_vec, cap_sym, pt_vec, ct, tag);

  // 4) cap.msg = b64url(ciphertext || tag)
  std::vector<uint8_t> msg_bytes;
  msg_bytes.reserve(ct.size() + tag.size());
  msg_bytes.insert(msg_bytes.end(), ct.begin(), ct.end());
  msg_bytes.insert(msg_bytes.end(), tag.begin(), tag.end());
  std::string cap_msg = b64url_encode(msg_bytes);

  // Optional: wipe key material
  if (wipe_key) {
    OPENSSL_cleanse(derived_key.data(), derived_key.size());
  }

  // 5) Emit CGI fields (base64url is URI-safe; you can still percent-encode externally if desired)
  return std::string("cap.sym=") + cap_sym + "&cap.msg=" + cap_msg;
}

std::string
UriCapCipher::decryptFromCgiFields(const std::string& cgi) const
{
  try {
    // Parse query-like string
    std::string cap_sym = getQueryValue(cgi, "cap.sym");
    std::string cap_msg = getQueryValue(cgi, "cap.msg");
    if (cap_sym.empty() || cap_msg.empty()) {
      return "";
    }

    // URL-decode in case your framework percent-encoded the query values
    cap_sym = url_percent_decode(cap_sym);
    cap_msg = url_percent_decode(cap_msg);

    // Decode and parse header
    std::vector<uint8_t> sym_bytes = b64url_decode(cap_sym);
    Header h = deserializeHeader(sym_bytes);

    // Basic sanity checks
    if (h.v != 1) return "";
    if (h.kdf != 1) return "";
    if (h.aead != 1) return "";
    if (h.N < 2 || (h.N & (h.N - 1)) != 0) return "";
    if (h.r == 0 || h.p == 0) return "";

    // Decode cap.msg -> ciphertext||tag
    std::vector<uint8_t> msg_bytes = b64url_decode(cap_msg);
    if (msg_bytes.size() < kTagLen) return "";

    const size_t ct_len = msg_bytes.size() - kTagLen;
    std::vector<uint8_t> ct(msg_bytes.begin(), msg_bytes.begin() + ct_len);
    std::vector<uint8_t> tag(msg_bytes.begin() + ct_len, msg_bytes.end());

    // Derive key (or reuse cached key if header salt matches)
    const std::vector<uint8_t>* key_ptr = nullptr;
    std::vector<uint8_t> derived_key;
    bool wipe_key = false;

    if (has_cached_key_ &&
        std::memcmp(h.salt, cached_salt_.data(), cached_salt_.size()) == 0) {
      key_ptr = &cached_key_;
    } else {
      std::vector<uint8_t> salt_vec(h.salt, h.salt + sizeof(h.salt));
      derived_key = kdf_scrypt(pw_, salt_vec, h.N, h.r, h.p, 32);
      key_ptr = &derived_key;
      wipe_key = true;
    }

    // Decrypt with AAD = exact cap.sym string bytes (must match encryption)
    std::vector<uint8_t> nonce_vec(h.nonce, h.nonce + sizeof(h.nonce));
    std::vector<uint8_t> pt = aead_decrypt_chacha20poly1305(
        *key_ptr, nonce_vec, cap_sym, ct, tag);

    if (wipe_key) {
      OPENSSL_cleanse(derived_key.data(), derived_key.size());
    }

    return std::string(pt.begin(), pt.end());
  } catch (...) {
    return "";
  }
}

void
UriCapCipher::throw_openssl(const char* what)
{
  unsigned long e = ERR_get_error();
  char buf[256];
  ERR_error_string_n(e, buf, sizeof(buf));
  throw std::runtime_error(std::string(what) + ": " + buf);
}

std::string
UriCapCipher::compute_password_from_file(const std::string& path)
{
  std::ifstream f(path, std::ios::binary);
  if (!f) {
    throw std::runtime_error("Failed to open secret file: " + path);
  }

  std::vector<uint8_t> data((std::istreambuf_iterator<char>(f)),
                            std::istreambuf_iterator<char>());
  if (data.empty()) {
    throw std::runtime_error("Secret file is empty: " + path);
  }

  // pw_ = SHA256(file_bytes) (32 bytes binary)
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(data.data(), data.size(), digest);

  return std::string(reinterpret_cast<const char*>(digest),
                     SHA256_DIGEST_LENGTH);
}

std::string
UriCapCipher::b64url_encode(const std::vector<uint8_t>& in)
{
  if (in.empty()) return "";

  std::string b64;
  b64.resize(4 * ((in.size() + 2) / 3));
  int outlen = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&b64[0]),
                               in.data(), (int)in.size());
  b64.resize(outlen);

  for (char& c : b64) {
    if (c == '+') c = '-';
    else if (c == '/') c = '_';
  }
  while (!b64.empty() && b64.back() == '=') b64.pop_back();
  return b64;
}

std::vector<uint8_t>
UriCapCipher::b64url_decode(const std::string& in)
{
  if (in.empty()) return {};

  std::string b64 = in;
  for (char& c : b64) {
    if (c == '-') c = '+';
    else if (c == '_') c = '/';
  }
  while (b64.size() % 4 != 0) b64.push_back('=');

  std::vector<uint8_t> out(3 * (b64.size() / 4));
  int len = EVP_DecodeBlock(out.data(),
                            reinterpret_cast<const unsigned char*>(b64.data()),
                            (int)b64.size());
  if (len < 0) throw std::runtime_error("Base64 decode failed");

  size_t padding = 0;
  if (!b64.empty() && b64.back() == '=') padding++;
  if (b64.size() >= 2 && b64[b64.size() - 2] == '=') padding++;
  out.resize((size_t)len - padding);
  return out;
}

int
UriCapCipher::hexval(char c)
{
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
  if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
  return -1;
}

std::string
UriCapCipher::url_percent_decode(const std::string& s)
{
  std::string out;
  out.reserve(s.size());
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%' && i + 2 < s.size()) {
      int h1 = hexval(s[i + 1]);
      int h2 = hexval(s[i + 2]);
      if (h1 >= 0 && h2 >= 0) {
        out.push_back(char((h1 << 4) | h2));
        i += 2;
      } else {
        out.push_back(s[i]);
      }
    } else if (s[i] == '+') {
      out.push_back(' ');
    } else {
      out.push_back(s[i]);
    }
  }
  return out;
}

std::string
UriCapCipher::getQueryValue(const std::string& input, const std::string& key)
{
  size_t start = 0;
  while (start <= input.size()) {
    size_t amp = input.find('&', start);
    if (amp == std::string::npos) amp = input.size();
    std::string_view part(input.data() + start, amp - start);

    size_t eq = part.find('=');
    if (eq != std::string_view::npos) {
      std::string_view k = part.substr(0, eq);
      std::string_view v = part.substr(eq + 1);
      if (k == key) return std::string(v);
    }
    start = amp + 1;
  }
  return "";
}

void
UriCapCipher::put_u64_le(std::vector<uint8_t>& out, uint64_t x)
{
  for (int i = 0; i < 8; ++i) out.push_back((uint8_t)((x >> (8 * i)) & 0xFF));
}

uint64_t
UriCapCipher::get_u64_le(const uint8_t* p)
{
  uint64_t x = 0;
  for (int i = 0; i < 8; ++i) x |= (uint64_t)p[i] << (8 * i);
  return x;
}

std::vector<uint8_t>
UriCapCipher::serializeHeader(const Header& h)
{
  std::vector<uint8_t> out;
  out.reserve(1 + 1 + 1 + 1 + 8 + 8 + 8 + kSaltLen + kNonceLen);

  out.push_back(h.v);
  out.push_back(h.kdf);
  out.push_back(h.aead);
  out.push_back(h.reserved);

  put_u64_le(out, h.N);
  put_u64_le(out, h.r);
  put_u64_le(out, h.p);

  out.insert(out.end(), h.salt, h.salt + kSaltLen);
  out.insert(out.end(), h.nonce, h.nonce + kNonceLen);
  return out;
}

UriCapCipher::Header
UriCapCipher::deserializeHeader(const std::vector<uint8_t>& in)
{
  const size_t need = 1 + 1 + 1 + 1 + 8 + 8 + 8 + kSaltLen + kNonceLen;
  if (in.size() != need) {
    throw std::runtime_error("cap.sym header wrong length");
  }

  Header h{};
  size_t off = 0;
  h.v = in[off++];
  h.kdf = in[off++];
  h.aead = in[off++];
  h.reserved = in[off++];

  h.N = get_u64_le(&in[off]); off += 8;
  h.r = get_u64_le(&in[off]); off += 8;
  h.p = get_u64_le(&in[off]); off += 8;

  std::memcpy(h.salt, &in[off], kSaltLen); off += kSaltLen;
  std::memcpy(h.nonce, &in[off], kNonceLen); off += kNonceLen;

  return h;
}

std::vector<uint8_t>
UriCapCipher::kdf_scrypt(const std::string& password_bytes,
                         const std::vector<uint8_t>& salt,
                         uint64_t N, uint64_t r, uint64_t p,
                         size_t key_len)
{
  std::vector<uint8_t> key(key_len);
  const uint64_t needed = (128ull * r * N) + (128ull * r * p) + (256ull * r);
  const uint64_t maxmem = std::max<uint64_t>(needed, 256ull * 1024 * 1024);
  if (EVP_PBE_scrypt(password_bytes.data(), password_bytes.size(),
                     salt.data(), salt.size(),
                     N, r, p,
                     maxmem,
                     key.data(), key.size()) != 1) {
    throw_openssl("EVP_PBE_scrypt");
  }
  return key;
}

void
UriCapCipher::aead_encrypt_chacha20poly1305(
    const std::vector<uint8_t>& key,
    const std::vector<uint8_t>& nonce12,
    const std::string& aad,
    const std::vector<uint8_t>& plaintext,
    std::vector<uint8_t>& ciphertext_out,
    std::vector<uint8_t>& tag16_out)
{
  if (nonce12.size() != kNonceLen) {
    throw std::runtime_error("nonce must be 12 bytes");
  }

  tag16_out.assign(kTagLen, 0);
  ciphertext_out.assign(plaintext.size(), 0);

  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (!ctx) throw std::runtime_error("EVP_CIPHER_CTX_new failed");

  int len = 0;
  if (EVP_EncryptInit_ex(ctx, EVP_chacha20_poly1305(), nullptr, nullptr, nullptr) != 1) {
    throw_openssl("EncryptInit");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, (int)nonce12.size(), nullptr) != 1) {
    throw_openssl("SET_IVLEN");
  }

  if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce12.data()) != 1) {
    throw_openssl("EncryptInit key/iv");
  }

  if (!aad.empty()) {
    if (EVP_EncryptUpdate(ctx, nullptr, &len,
                          reinterpret_cast<const unsigned char*>(aad.data()),
                          (int)aad.size()) != 1) {
      throw_openssl("EncryptUpdate AAD");
    }
  }

  if (!plaintext.empty()) {
    if (EVP_EncryptUpdate(ctx, ciphertext_out.data(), &len,
                          plaintext.data(), (int)plaintext.size()) != 1) {
      throw_openssl("EncryptUpdate PT");
    }
  }

  int finallen = 0;
  if (EVP_EncryptFinal_ex(ctx, ciphertext_out.data() + len, &finallen) != 1) {
    throw_openssl("EncryptFinal");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG, (int)kTagLen,
                          tag16_out.data()) != 1) {
    throw_openssl("GET_TAG");
  }

  EVP_CIPHER_CTX_free(ctx);
}

std::vector<uint8_t>
UriCapCipher::aead_decrypt_chacha20poly1305(
    const std::vector<uint8_t>& key,
    const std::vector<uint8_t>& nonce12,
    const std::string& aad,
    const std::vector<uint8_t>& ciphertext,
    const std::vector<uint8_t>& tag16)
{
  if (nonce12.size() != kNonceLen) {
    throw std::runtime_error("nonce must be 12 bytes");
  }
  if (tag16.size() != kTagLen) {
    throw std::runtime_error("tag must be 16 bytes");
  }

  std::vector<uint8_t> plaintext(ciphertext.size());

  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (!ctx) throw std::runtime_error("EVP_CIPHER_CTX_new failed");

  int len = 0;
  if (EVP_DecryptInit_ex(ctx, EVP_chacha20_poly1305(), nullptr, nullptr, nullptr) != 1) {
    throw_openssl("DecryptInit");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_IVLEN, (int)nonce12.size(), nullptr) != 1) {
    throw_openssl("SET_IVLEN");
  }

  if (EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce12.data()) != 1) {
    throw_openssl("DecryptInit key/iv");
  }

  if (!aad.empty()) {
    if (EVP_DecryptUpdate(ctx, nullptr, &len,
                          reinterpret_cast<const unsigned char*>(aad.data()),
                          (int)aad.size()) != 1) {
      throw_openssl("DecryptUpdate AAD");
    }
  }

  if (!ciphertext.empty()) {
    if (EVP_DecryptUpdate(ctx, plaintext.data(), &len,
                          ciphertext.data(), (int)ciphertext.size()) != 1) {
      throw_openssl("DecryptUpdate CT");
    }
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_TAG, (int)kTagLen,
                          (void*)tag16.data()) != 1) {
    throw_openssl("SET_TAG");
  }

  int finallen = 0;
  int ok = EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &finallen);
  EVP_CIPHER_CTX_free(ctx);

  if (ok != 1) throw std::runtime_error("Auth failed");

  plaintext.resize((size_t)len + (size_t)finallen);
  return plaintext;
}

EOSCOMMONNAMESPACE_END

