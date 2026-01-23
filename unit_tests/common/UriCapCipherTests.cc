//------------------------------------------------------------------------------
// File: UriCapCipherTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "common/UriCapCipher.hh"

#include <atomic>
#include <chrono>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

TEST(UriCapCipher, EncodeDecodePerf)
{
  using eos::common::UriCapCipher;
  constexpr size_t kPayloadSize = 4096;
  constexpr int kIters = 10000;

  std::mt19937_64 rng_pw(0xC0FFEEULL);
  std::uniform_int_distribution<int> dist_pw(0, 255);
  std::string password;
  password.resize(32);
  for (size_t i = 0; i < password.size(); ++i) {
    password[i] = static_cast<char>(dist_pw(rng_pw));
  }

  UriCapCipher cipher(UriCapCipher::PasswordTag{},
                      UriCapCipher::FixedSaltTag{},
                      password);

  std::mt19937_64 rng(0xBADC0DEULL);
  std::uniform_int_distribution<int> dist(0, 255);
  std::string payload;
  payload.resize(kPayloadSize);
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>(dist(rng));
  }

  std::vector<std::string> encoded;
  encoded.reserve(kIters);

  auto t0 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < kIters; ++i) {
    encoded.push_back(cipher.encryptToCgiFields(payload));
  }
  auto t1 = std::chrono::high_resolution_clock::now();

  auto enc_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0)
          .count();
  double enc_khz = (kIters / enc_sec) / 1000.0;

  auto t2 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < kIters; ++i) {
    std::string decoded = cipher.decryptFromCgiFields(encoded[i]);
    ASSERT_EQ(decoded.size(), payload.size());
    ASSERT_EQ(decoded, payload);
  }
  auto t3 = std::chrono::high_resolution_clock::now();

  auto dec_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(t3 - t2)
          .count();
  double dec_khz = (kIters / dec_sec) / 1000.0;

  std::cout << "UriCapCipher encode rate: " << enc_khz << " kHz\n";
  std::cout << "UriCapCipher decode rate: " << dec_khz << " kHz\n";

}

TEST(UriCapCipher, EncodeDecodeConcurrent)
{
  using eos::common::UriCapCipher;
  constexpr int kThreads = 100;
  constexpr int kItersPerThread = 100;
  constexpr size_t kPayloadSize = 4096;

  std::mt19937_64 rng_pw(0xC0FFEEULL);
  std::uniform_int_distribution<int> dist_pw(0, 255);
  std::string password;
  password.resize(32);
  for (size_t i = 0; i < password.size(); ++i) {
    password[i] = static_cast<char>(dist_pw(rng_pw));
  }

  UriCapCipher cipher(UriCapCipher::PasswordTag{},
                      UriCapCipher::FixedSaltTag{},
                      password);

  std::vector<std::string> payloads(kThreads);
  std::vector<std::vector<std::string>> encoded(kThreads);
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  auto t0 = std::chrono::high_resolution_clock::now();
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([t, &cipher, &failures, &payloads, &encoded]() {
      std::mt19937_64 rng(0xBADC0DEULL + static_cast<uint64_t>(t));
      std::uniform_int_distribution<int> dist(0, 255);
      std::string payload;
      payload.resize(kPayloadSize);
      for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<char>(dist(rng));
      }

      payloads[t] = std::move(payload);
      encoded[t].reserve(kItersPerThread);
      for (int i = 0; i < kItersPerThread; ++i) {
        encoded[t].push_back(cipher.encryptToCgiFields(payloads[t]));
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  auto t1 = std::chrono::high_resolution_clock::now();

  threads.clear();
  threads.reserve(kThreads);
  auto t2 = std::chrono::high_resolution_clock::now();
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([t, &cipher, &failures, &payloads, &encoded]() {
      for (int i = 0; i < kItersPerThread; ++i) {
        std::string dec = cipher.decryptFromCgiFields(encoded[t][i]);
        if (dec != payloads[t]) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
  auto t3 = std::chrono::high_resolution_clock::now();

  const double enc_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0)
          .count();
  const double dec_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(t3 - t2)
          .count();
  const double total_ops = static_cast<double>(kThreads) *
                           static_cast<double>(kItersPerThread);
  const double enc_khz = (total_ops / enc_sec) / 1000.0;
  const double dec_khz = (total_ops / dec_sec) / 1000.0;

  std::cout << "UriCapCipher concurrent encode rate: " << enc_khz << " kHz\n";
  std::cout << "UriCapCipher concurrent decode rate: " << dec_khz << " kHz\n";

  EXPECT_EQ(failures.load(), 0);
}

