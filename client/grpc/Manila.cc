#include <string>
#include <map>
#include <iostream>
#include <fstream>
#include "client/grpc/GrpcClient.hh"
#include <stdio.h>
#include "common/StringConversion.hh"
#include <google/protobuf/util/json_util.h>

int usage(const char* prog)
{
  fprintf(stderr, "usage: %s [--key <ssl-key-file> "
          "--cert <ssl-cert-file> "
          "--ca <ca-cert-file>] "
          "[--endpoint <host:port>] [--token <auth-token>] "
          "--command <command> "
          "--params <paramlist := key1:val1,key2:val2,key3:val3...> \n", prog);
  fprintf(stderr,
          "\nvalid commands: create,delete,extend,shrink,manage,unmanage,capacity\n");
  fprintf(stderr, "\n"
          "valid params:   authkey=<authkey>\n"
          "                protocol=<protocol>\n"
          "                name=<name>\n"
          "                description=<description>\n"
          "                id=<id>\n"
          "                group_id=<group_id>\n"
          "                quota=<quota>\n"
          "                creator=<creator>\n"
          "                egroup=<egroup>\n"
          "                admin_egroup=<admin_egroup>\n"
          "                location=<locaion>\n");
  return -1;
}

int main(int argc, const char* argv[])
{
  std::string endpoint = "localhost:50051";
  std::string token = "";
  std::string key;
  std::string cert;
  std::string ca;
  std::string keyfile;
  std::string certfile;
  std::string cafile;
  std::string command;
  std::string params;

  for (auto i = 1; i < argc; ++i) {
    std::string option = argv[i];

    if (option == "--key") {
      if (argc > i + 1) {
        keyfile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--cert") {
      if (argc > i + 1) {
        certfile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--ca") {
      if (argc > i + 1) {
        cafile = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--endpoint") {
      if (argc > i + 1) {
        endpoint = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--token") {
      if (argc > i + 1) {
        token = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--command") {
      if (argc > i + 1) {
        command = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    if (option == "--params") {
      if (argc > i + 1) {
        params = argv[i + 1];
        ++i;
        continue;
      } else {
        return usage(argv[0]);
      }
    }

    return usage(argv[0]);
  }

  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return usage(argv[0]);
    }
  }

  if (command.empty() || params.empty()) {
    return usage(argv[0]);
  }

  std::unique_ptr<eos::client::GrpcClient> eosgrpc =
    eos::client::GrpcClient::Create(
      endpoint,
      token,
      keyfile,
      certfile,
      cafile);

  if (!eosgrpc) {
    return usage(argv[0]);
  }

  std::cout << "=> settings: command=" << command << " params=" << params <<
            std::endl;
  /*
  enum MANILA_REQUEST_TYPE {
   CREATE_SHARE = 0;
   DELETE_SHARE = 1;
   EXTEND_SHARE = 2;
   SHRINK_SHARE = 3;
   MANAGE_EXISTING = 4;
   UNMANAGE = 5;
   GET_CAPACITIES = 6;
  }

  message ManilaRequest {
   MANILA_REQUEST_TYPE request_type = 1;
   string auth_key = 2;
   string protocol = 3;
   string share_name = 4;
   string description = 5;
   string share_id = 6;
   string share_group_id = 7;
   int32 quota = 8;
   string creator = 9;
   string egroup = 10;
   string admin_egroup = 11;
   string share_location = 12;
  }

  message ManilaResponse {
   string msg = 1; //for generic messages
   int32 code = 2; // < 1 is an error -- > 1 is OK
   int64 total_used = 3;
   int64 total_capacity = 4;
   int64 new_share_quota = 5;
   string new_share_path = 6;

  }
  */
  eos::rpc::ManilaRequest request;
  eos::rpc::ManilaResponse reply;

  if (command == "create") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::CREATE_SHARE);
  } else if (command == "delete") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::DELETE_SHARE);
  } else if (command == "extend") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::EXTEND_SHARE);
  } else if (command == "shrink") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::SHRINK_SHARE);
  } else if (command == "manage") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::MANAGE_EXISTING);
  } else if (command == "unmanage") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::UNMANAGE);
  } else if (command == "capacity") {
    request.set_request_type(eos::rpc::MANILA_REQUEST_TYPE::GET_CAPACITIES);
  } else {
    std::cerr << "Invalid command: " << command << std::endl;
    return usage(argv[0]);
  }

  std::map<std::string, std::string> pmap;
  eos::common::StringConversion::GetKeyValueMap(params.c_str(),
      pmap);

  for (auto it = pmap.begin(); it != pmap.end(); ++it) {
    if (it->first == "authkey") {
      request.set_auth_key(it->second);
    } else if (it->first == "protocol") {
      request.set_protocol(it->second);
    } else if (it->first == "name") {
      request.set_share_name(it->second);
    } else if (it->first == "description") {
      request.set_description(it->second);
    } else if (it->first == "id") {
      request.set_share_id(it->second);
    } else if (it->first == "group_id") {
      request.set_share_group_id(it->second);
    } else if (it->first == "quota") {
      request.set_quota(strtoull(it->second.c_str(), 0, 10));
    } else if (it->first == "creator") {
      request.set_creator(it->second);
    } else if (it->first == "egroup") {
      request.set_egroup(it->second);
    } else if (it->first == "admin_egroup") {
      request.set_admin_egroup(it->second);
    } else if (it->first == "location") {
      request.set_share_location(it->second);
    } else {
      std::cerr << "param:" << it->first << " is not valid " << std::endl;
      return usage(argv[0]);
    }
  }

  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
#if GOOGLE_PROTOBUF_VERSION >= 5027000
  options.always_print_fields_with_no_presence = true;
#else
  options.always_print_primitive_fields = true;
#endif
  std::string jsonstring;
  (void) google::protobuf::util::MessageToJsonString(request,
      &jsonstring, options);
  std::cout << "# sending request " << std::endl << jsonstring << std::endl;
  eosgrpc->ManilaRequest(request,
                         reply);
  (void0 google::protobuf::util::MessageToJsonString(reply,
      &jsonstring, options);
   std::cout << "# got response " << std::endl << jsonstring << std::endl;
   std::chrono::steady_clock::time_point watch_global =
     std::chrono::steady_clock::now();
   std::chrono::microseconds elapsed_global =
     std::chrono::duration_cast<std::chrono::microseconds>
     (std::chrono::steady_clock::now() - watch_global);
   std::cout << "Request took " << elapsed_global.count() <<
   " micro seconds" << std::endl;
   return reply.code();
}
