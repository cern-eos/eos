/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include <sstream>
#include <iostream>
#include <iomanip>
#include <getopt.h>
#include "client/grpc/GrpcClient.hh"

int usage(const char* name)
{
  std::ostringstream oss;
  oss << "usage: " << name
      << " [--key <ssl-key-file> --cert <ssl-cert-file> --ca <ca-cert-file>]"
      << " [--token <auth-token>]"
      << std::endl << std::setw(strlen(name) + 8) << ""
      << "[--endpoint <host:port>] [-d|--debug] [-h|--help] [--force-ssl]"
      << std::endl;
  std::cerr << oss.str();
  return -1;
}

int main(int argc, char* argv[])
{
  using eos::client::GrpcClient;
  std::string endpoint{"localhost:50051"};
  std::string keyfile;
  std::string certfile;
  std::string cafile;
  std::string token;
  bool debug = false;
  bool force_ssl = false;

  while (true) {
    static struct option long_options[] {
      {"key",      required_argument, 0, 'k'},
      {"cert",     required_argument, 0, 'c'},
      {"ca",       required_argument, 0, 'a'},
      {"endpoint", required_argument, 0, 'e'},
      {"token",    required_argument, 0, 't'},
      {"debug",    no_argument,       0, 'd'},
      {"help",     no_argument,       0, 'h'},
      {"force-ssl", no_argument,      0, 's'},
      {0, 0,                          0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "k:c:a:e:t:dhs", long_options, &option_index);

    // Detect end of the options
    if (c == -1) {
      break;
    }

    switch (c) {
    case 'k':
      keyfile = optarg;
      break;

    case 'c':
      certfile = optarg;
      break;

    case 'a':
      cafile = optarg;
      break;

    case 'e':
      endpoint = optarg;
      break;

    case 't':
      token = optarg;
      break;

    case 'd':
      debug = true;
      break;

    case 's':
      force_ssl = true;
      break;

    case 'h':
      return usage(argv[0]);

    default:
      return usage(argv[0]);
    }
  }

  // Make sure all elements are present if certificate authentication is used
  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return usage(argv[0]);
    }
  }

  std::unique_ptr<GrpcClient> eosgrpc =
    GrpcClient::Create(endpoint, token, keyfile, certfile, cafile, force_ssl);

  if (!eosgrpc) {
    std::cerr << "Failed to create grpc client object!" << std::endl;
    return -1;
  }

  auto start_time = std::chrono::steady_clock::now();
  google::protobuf::util::JsonPrintOptions options;
#if GOOGLE_PROTOBUF_VERSION >= 5027000
  options.always_print_fields_with_no_presence = true;
#else
  options.always_print_primitive_fields = true;
#endif
  options.add_whitespace = true;
  std::string jsonstring;
  eos::rpc::NsStatRequest request;
  eos::rpc::NsStatResponse reply;
  request.set_authkey(token);

  if (debug) {
    (void) google::protobuf::util::MessageToJsonString(request, &jsonstring,
        options);
    std::cout << "request: " << std::endl << jsonstring << std::endl;
  }

  if (eosgrpc->NsStat(request, reply)) {
    std::cerr << "GRPC request field" << std::endl;
    debug = true;
  }

  if (debug) {
    std::cout << "reply: " << std::endl;
  }

  jsonstring = "";
  (void) google::protobuf::util::MessageToJsonString(reply, &jsonstring, options);
  std::cout << jsonstring << std::endl;
  auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start_time);

  if (debug) {
    std::cout << "request took " << elapsed_time.count() << " microseconds"
              << std::endl;
  }

  return reply.code();
}
