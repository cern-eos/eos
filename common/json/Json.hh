// ----------------------------------------------------------------------
// File: Json.hh
// Author: Gianmaria Del Monte - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include <json/json.h>

template <typename T>
std::enable_if_t<std::is_integral_v<T>, Json::Value> inline ConvertToJson(
  const T& input);

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, Json::Value> inline ConvertToJson(
  const T& input);

template <typename T>
inline Json::Value ConvertToJson(const std::atomic<T>& input);

template <typename T, std::size_t N>
Json::Value ConvertToJson(const std::array<T, N>& input);

template <typename T, std::size_t N>
Json::Value ConvertToJson(const T(&input)[N]);

template <typename T>
Json::Value ConvertToJson(const std::vector<T>& input);

template <typename T>
Json::Value ConvertToJson(const std::set<T>& input);

template <typename K, typename V>
Json::Value ConvertToJson(const std::map<K, V>& input);

template <typename K, typename V>
Json::Value ConvertToJson(const std::unordered_map<K, V>& input);

inline Json::Value ConvertToJson(const std::string& input);

inline Json::Value ConvertToJson(const char* input);

template <typename T>
inline void ConvertFromJson(const Json::Value& val, T& out);

template <typename T>
inline void ConvertFromJson(const Json::Value& val, std::atomic<T>& out);

template <typename T, std::size_t N>
void ConvertFromJson(const Json::Value& val, std::array<T, N>& out);

template <typename T, std::size_t N>
void ConvertFromJson(const Json::Value& val, T(&out)[N]);

template <typename T>
void ConvertFromJson(const Json::Value& val, std::vector<T>& out);

template <typename V>
void ConvertFromJson(const Json::Value& val, std::set<V>& out);

template <typename K, typename V>
void ConvertFromJson(const Json::Value& val, std::map<K, V>& out);

template <typename K, typename V>
void ConvertFromJson(const Json::Value& val, std::unordered_map<K, V>& out);


template <typename T>
std::enable_if_t <
std::is_integral_v<T>,
    Json::Value >
    inline ConvertToJson(const T& input)
{
  if constexpr(std::is_signed_v<T>) {
    return Json::Int64(input);
  } else if constexpr(std::is_unsigned_v<T>) {
    return Json::UInt64(input);
  }
}

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>,
    Json::Value>
    inline ConvertToJson(const T& input)
{
  return Json::Value(input);
}

template <typename T>
inline Json::Value ConvertToJson(const std::atomic<T>& input)
{
  return ConvertToJson(input.load());
}

template <typename T, std::size_t N>
Json::Value ConvertToJson(const std::array<T, N>& input)
{
  Json::Value root(Json::arrayValue);

  for (const auto& value : input) {
    root.append(ConvertToJson(value));
  }

  return root;
}

template <typename T, std::size_t N>
Json::Value ConvertToJson(const T(&input)[N])
{
  Json::Value root(Json::arrayValue);

  for (const auto& value : input) {
    root.append(ConvertToJson(value));
  }

  return root;
}

template <typename T>
Json::Value ConvertToJson(const std::vector<T>& input)
{
  Json::Value root(Json::arrayValue);

  for (const auto& value : input) {
    root.append(ConvertToJson(value));
  }

  return root;
}

template <typename T>
Json::Value ConvertToJson(const std::set<T>& input)
{
  Json::Value root(Json::arrayValue);

  for (const auto& val : input) {
    root.append(ConvertToJson(val));
  }

  return root;
}

template <typename K, typename V>
Json::Value ConvertToJson(const std::map<K, V>& input)
{
  Json::Value root;

  for (const auto& pair : input) {
    if constexpr(std::is_same_v<K, std::string>) {
      root[pair.first] = ConvertToJson(pair.second);
    } else {
      root[std::to_string(pair.first)] = ConvertToJson(pair.second);
    }
  }

  return root;
}

template <typename K, typename V>
Json::Value ConvertToJson(const std::unordered_map<K, V>& input)
{
  Json::Value root;

  for (const auto& pair : input) {
    root[std::to_string(pair.first)] = ConvertToJson(pair.second);
  }

  return root;
}

inline Json::Value ConvertToJson(const std::string& input)
{
  return Json::Value(input);
}

inline Json::Value ConvertToJson(const char* input)
{
  return Json::Value(input);
}

template <typename T>
inline void ConvertFromJson(const Json::Value& val, T& out)
{
  if constexpr(std::is_signed_v<T>) {
    out = static_cast<T>(val.asInt64());
  } else if constexpr(std::is_unsigned_v<T>) {
    out = static_cast<T>(val.asUInt64());
  } else if constexpr(std::is_floating_point_v<T>) {
    out = static_cast<T>(val.asDouble());
  }
}


template <typename T>
inline void ConvertFromJson(const Json::Value& val, std::atomic<T>& out)
{
  T v;
  ConvertFromJson(val, v);
  out = std::move(v);
}

template <typename T, std::size_t N>
void ConvertFromJson(const Json::Value& val, std::array<T, N>& out)
{
  for (std::size_t i = 0; i < N; i++) {
    ConvertFromJson(val[Json::ArrayIndex(i)], out[i]);
  }
}

template <typename T, std::size_t N>
void ConvertFromJson(const Json::Value& val, T(&out)[N])
{
  for (std::size_t i = 0; i < N; i++) {
    auto v = val[Json::ArrayIndex(i)];
    ConvertFromJson(v, out[i]);
  }
}

template <typename T>
void ConvertFromJson(const Json::Value& val, std::vector<T>& out)
{
  out.clear();
  out.reserve(val.size());

  for (Json::ArrayIndex i = 0; i < val.size(); i++) {
    T v;
    ConvertFromJson(val[i], v);
    out.emplace_back(std::move(v));
  }
}

template <typename V>
void ConvertFromJson(const Json::Value& val, std::set<V>& out)
{
  out.clear();

  for (Json::ArrayIndex i = 0; i < val.size(); i++) {
    V v;
    ConvertFromJson(val[i], v);
    out.emplace(std::move(v));
  }
}

template <typename K, typename V>
void ConvertFromJson(const Json::Value& val, std::map<K, V>& out)
{
  for (const auto& key : val.getMemberNames()) {
    K map_key;
    V map_value;
    std::istringstream(key) >> map_key;
    auto v = val[key];
    ConvertFromJson(v, map_value);
    out.emplace(std::move(map_key), std::move(map_value));
  }
}

template <typename K, typename V>
void ConvertFromJson(const Json::Value& val, std::unordered_map<K, V>& out)
{
  for (const auto& key : val.getMemberNames()) {
    K map_key;
    V map_value;
    std::istringstream(key) >> map_key;
    ConvertFromJson(val[key], map_value);
    out.emplace(std::move(map_key), std::move(map_value));
  }
}

template <typename T>
std::string Marshal(const T& input, const char* indentation = "")
{
  Json::Value root = ConvertToJson(input);
  Json::StreamWriterBuilder writer;
  writer["indentation"] = indentation;
  return Json::writeString(writer, root);
}

template <typename T>
void Unmarshal(const std::string input, T& out)
{
  Json::Value root;
  std::istringstream(input) >> root;
  ConvertFromJson(root, out);
}
