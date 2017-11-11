/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "EosDumpProtoMd.hh"
#include "ContainerMd.pb.h"
#include "FileMd.pb.h"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/utils/DataHelper.hh"
#include "namespace/utils/StringConvertion.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "common/StringTokenizer.hh"
#include "google/protobuf/util/json_util.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include <getopt.h>
#include <iostream>

int main(int argc, char* argv[])
{
  std::string qdb_host {"localhost"};
  uint32_t qdb_port {7777};
  uint64_t id {0};
  bool is_file = true;
  int print_help = 0;

  while (true) {
    static struct option long_options[] = {
      {"help", no_argument, &print_help, 1},
      {"fid", required_argument, 0, 'f'},
      {"cid", required_argument, 0, 'c'},
      {"host", required_argument, 0, 'h'},
      {"port", required_argument, 0, 'p'},
      {0, 0, 0, 0}
    };
    // getopt_long stores the option index there
    int option_index = 0;
    int c = getopt_long(argc, argv, "f:c:h:p:", long_options, &option_index);
    std::string soptarg;

    // Detect end of the options
    if (c == -1) {
      break;
    }

    switch (c) {
    case 0: {
      // This option set a flag
      if (long_options[option_index].flag != 0) {
        break;
      }

      break;
    }

    case 'f': {
      soptarg = optarg;

      try {
        id = std::stoull(soptarg);
      } catch (const std::exception& e) {
        std::cerr << "error: fid must be a decimal numeric value" << std::endl;
        return usage_help();
      }

      break;
    }

    case 'c': {
      is_file = false;
      soptarg = optarg;

      try {
        id = std::stoull(soptarg);
      } catch (const std::exception& e) {
        std::cerr << "error: cid must be a decimal numeric value" << std::endl;
        return usage_help();
      }

      break;
    }

    case 'h': {
      qdb_host = optarg;
      break;
    }

    case 'p': {
      soptarg = optarg;

      try {
        qdb_port = std::stoul(soptarg);
      } catch (const std::exception& e) {
        std::cerr << "error: port must be a numeric value" << std::endl;
        return usage_help();
      }
    }

    default:
      return usage_help();
    }
  }

  if (print_help || !id) {
    return usage_help();
  }

  qclient::QClient* qcl = eos::BackendClient::getInstance(qdb_host, qdb_port);

  try {
    PrettyPrint(DumpProto(qcl, id, is_file));
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return usage_help();
  }

  return 0;
}

//------------------------------------------------------------------------------
// Dump metadata object information stored in QDB
//------------------------------------------------------------------------------
std::string DumpProto(qclient::QClient* qcl, uint64_t id, bool is_file)
{
  std::string blob;

  try {
    std::string sid = eos::stringify(id);
    qclient::QHash bucket_map(*qcl, eos::FileMDSvc::getBucketKey(id));
    blob = bucket_map.hget(sid);
  } catch (std::runtime_error& qdb_err) {
    eos::MDException e(ENOENT);
    e.getMessage() << "File #" << id << " not found";
    throw e;
  }

  if (blob.empty()) {
    throw std::runtime_error("no data retrieved from the backend");
  }

  std::string output;

  if (is_file) {
    eos::FileMD fmd(0, nullptr);
    eos::Buffer buff;
    buff.putData(blob.data(), blob.length());
    fmd.deserialize(buff);
    fmd.getEnv(output, false);
  } else {
  }

  return output;
}

//------------------------------------------------------------------------------
// Pretty print metadata object
//------------------------------------------------------------------------------
void PrettyPrint(const std::string& senv)
{
  std::ostringstream oss;
  using eos::common::StringTokenizer;
  auto tokens = StringTokenizer::split<std::list<std::string>>(senv, '&');

  for (const auto& elem : tokens) {
    auto kv_pair = StringTokenizer::split<std::vector<std::string>>(elem, '=');

    if (kv_pair.size() != 2) {
      std::cerr << "error: unexpected format " << elem << std::endl;
      continue;
    }

    // Convert only the seconds to printable and ignore nanoseconds
    if ((kv_pair[0] == "ctime") || (kv_pair[0] == "mtime")) {
      const time_t time = std::stoull(kv_pair[1]);
      char* ptime = ctime(&time);
      kv_pair[1] = ptime;
      size_t pos = 0;

      while ((pos = kv_pair[1].find('\n')) != std::string::npos) {
        kv_pair[1].erase(pos, 1);
      }
    } else if ((kv_pair[0] == "ctime_ns") || (kv_pair[0] == "mtime_ns")) {
      continue;
    }

    // Capitalize first letter of the key
    kv_pair[0][0] = std::toupper(kv_pair[0][0]);
    oss << kv_pair[0] << " : " << kv_pair[1] << std::endl;
  }

  std::cout << oss.str();
}

//------------------------------------------------------------------------------
// Print command useage info
//------------------------------------------------------------------------------
int usage_help()
{
  std::cerr << "Usage: eos_dump_proto_md "
            "--fid|--cid <val> [-h|--host <qdb_host>] [-p|--port <qdb_port>] "
            "[--help]" << std::endl
            << "     --fid : decimal file id" << std::endl
            << "     --cid : decimal container id" << std::endl
            << " -h|--host : QuarkDB host, default localhost" << std::endl
            << " -p|--port : QuarkDb port, default 7777" << std::endl
            << "    --help : print help message" << std::endl;
  return EINVAL;
}
