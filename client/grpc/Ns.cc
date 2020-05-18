#include <string>
#include <iostream>
#include "client/grpc/GrpcClient.hh"
#include <stdio.h>
#include "common/StringConversion.hh"
#include <google/protobuf/util/json_util.h>

int usage(const char* prog)
{
  fprintf(stderr, "usage: %s [--key <ssl-key-file> "
          "--cert <ssl-cert-file> "
          "--ca <ca-cert-file>] "
          "[--endpoint <host:port>] [--token <auth-token>] [--xattr <key:val>] [--mode <mode>] [--username <username>] [ [--groupname <groupname>] [--uid <uid>] [--gid <gid>] [--owner-uid <uid>] [--owner-gid <gid>] [--acl <acl>] [--sysacl] [--norecycle] [-r] [--max-version <max-version>] [--target <target>] [--year <year>] [--month <month>] [--day <day>] -p <path> <command>\n", prog);

  fprintf(stderr,
	  "                                     -p <path> mkdir \n"
	  "                                [-r] -p <path> rmdir \n"
	  "                                     -p <path> touch \n"
	  "                       [--norecycle] -p <path> rm \n"
	  "                   --target <target> -p <path> rename \n"
	  "                   --target <target> -p <path> symlink \n"
	  "                   --xattr <key=val> -p <path> setxattr # sets key=val \n"
	  "                     --xattr <!key=> -p <path> setxattr # deletes key\n"
	  " --owner-uid <uid> --owner-gid <gid> -p <path> chown \n"
	  "                       --mode <mode> -p <path> chmod \n"
	  "       [--sysacl] [-r] [--acl <acl>] -p <path> acl \n"
	  "     --ztoken <token> | [--acl] [-r] -p <path> token\n"
	  "                [--max-version <max> -p <path> create-version \n"
	  "                                     -p <path> list-version \n"
	  "                [--max-version <max> -p <path> purge-version \n"
	  "                                               recycle ls\n"
	  "                                     -p <key>  recycle restore\n"
          " --year <year> [--month <month> [--day <day>]] recycle purge\n"
	  "[--username <u> | --groupname <g>] [-p <path>] quota \n \n");
	  
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
  std::string cmd = "";
  std::string subcmd = "";
  std::string path = "";
  std::string target = "";
  std::string xattr = "";
  std::string acl = "";
  mode_t mode = 0775;
  int64_t max_version = -1;
  uid_t uid = 0;
  gid_t gid = 0;
  uint32_t day = 0;
  uint32_t month = 0;
  uint32_t year = 0;

  std::string username;
  std::string groupname;

  uid_t owner_uid = 0;
  gid_t owner_gid = 0;
  bool recursive = false;
  bool norecycle = false;
  bool sysacl = false;
  std::string eostoken = "";

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

    if (option == "--uid") {
      if (argc > i + 1) {
	uid = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--gid") {
      if (argc > i + 1) {
	gid = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--username") {
      if (argc > i + 1) {
	username= argv[i + 1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--year") {
      if (argc > i + 1) {
	year = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--month") {
      if (argc > i + 1) {
        month = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--day") {
      if (argc > i + 1) {
        day = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--groupname") {
      if (argc > i + 1) {
	groupname= argv[i + 1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--owner-uid") {
      if (argc > i + 1) {
	owner_uid = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--owner-gid") {
      if (argc > i + 1) {
	owner_gid = strtoul(argv[i + 1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "-p") {
      if (argc > i + 1) {
	path = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--target") {
      if (argc > i + 1) {
	target = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--acl") {
      if (argc > i + 1) {
	acl = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--mode") {
      if (argc > i + 1) {
	mode = strtol(argv[i+1],0,8);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--max-version") {
      if (argc > i + 1) {
	max_version = strtol(argv[i+1],0,10);
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "--xattr") {
      if (argc > i + 1) {
	xattr = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    if (option == "-r") {
      recursive = true;
      continue;
    }

    if (option == "--sysacl") {
      sysacl = true;
      continue;
    }

    if (option == "--norecycle") {
      norecycle = true;
      continue;
    }

    if (option == "--ztoken") {
      if (argc > i + 1) {
	eostoken = argv[i+1];
	++i;
	continue;
      } else {
	return usage(argv[0]);
      }
    }

    cmd = option;

    if (argc > (i + 1)) {
      if ( cmd == "recycle" ) {
	subcmd = argv[i+1];
	if ( (subcmd != "ls" ) &&
	     (subcmd != "restore") &&
	     (subcmd != "purge") ) {
	  return usage(argv[0]);
	}
	break;
      } else {
	return usage(argv[0]);
      }
    }
  }

  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return usage(argv[0]);
    }
  }

  if (cmd.empty() || ((cmd != "quota") && (cmd != "recycle") && path.empty() && eostoken.empty())) {
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

  std::chrono::steady_clock::time_point watch_global =
    std::chrono::steady_clock::now();

  eos::rpc::NSRequest request;
  eos::rpc::NSResponse reply;
  request.set_authkey(token);
  if (uid) {
    request.mutable_role()->set_uid(uid);
  }
  if (gid) {
    request.mutable_role()->set_gid(gid);
  }

  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  
  if (cmd == "mkdir") {
    request.mutable_mkdir()->mutable_id()->set_path(path);

    if (recursive) {
      request.mutable_mkdir()->set_recursive(true);
    }
    request.mutable_mkdir()->set_mode(mode);
  } else if (cmd == "rmdir") {
    request.mutable_rmdir()->mutable_id()->set_path(path);
  } else if (cmd == "touch") {
    request.mutable_touch()->mutable_id()->set_path(path);
  } else if (cmd == "unlink") {
    request.mutable_unlink()->mutable_id()->set_path(path);
    if (norecycle) {
      request.mutable_unlink()->set_norecycle(norecycle);
    }
  } else if (cmd == "rm") {
    request.mutable_rm()->mutable_id()->set_path(path);
    if (norecycle) {
      request.mutable_rm()->set_norecycle(norecycle);
    }

    if (recursive) {
      request.mutable_rm()->set_recursive(recursive);
    }
  } else if (cmd == "rename") {
    request.mutable_rename()->mutable_id()->set_path(path);
    request.mutable_rename()->set_target(target);
  } else if (cmd == "symlink") {
    request.mutable_symlink()->mutable_id()->set_path(path);
    request.mutable_symlink()->set_target(target);
  } else if (cmd == "setxattr") {
    request.mutable_xattr()->mutable_id()->set_path(path);
    std::string key, val;
    eos::common::StringConversion::SplitKeyValue(xattr, key, val, "=");
    if (key.front() == '!') {
      // add as a deletion key
      auto x = request.mutable_xattr()->add_keystodelete();
      *x = key.substr(1);
    } else {
      // add as a new attribute key
      (*(request.mutable_xattr()->mutable_xattrs()))[key] = val;
    }
  } else if (cmd == "chown") {
    // run as root
    request.mutable_chown()->mutable_id()->set_path(path);
    request.mutable_chown()->mutable_owner()->set_uid(owner_uid);
    request.mutable_chown()->mutable_owner()->set_gid(owner_gid);
  } else if (cmd == "chmod") {
    request.mutable_chmod()->mutable_id()->set_path(path);
    request.mutable_chmod()->set_mode(mode);
  } else if (cmd == "create-version") {
    request.mutable_version()->set_cmd(eos::rpc::NSRequest::VersionRequest::CREATE);
    request.mutable_version()->mutable_id()->set_path(path);
    request.mutable_version()->set_maxversion(max_version);
  } else if (cmd == "list-version") {
    request.mutable_version()->set_cmd(eos::rpc::NSRequest::VersionRequest::LIST);
    request.mutable_version()->mutable_id()->set_path(path);
  } else if (cmd == "purge-version") {
    request.mutable_version()->set_cmd(eos::rpc::NSRequest::VersionRequest::PURGE);
    request.mutable_version()->mutable_id()->set_path(path);
    request.mutable_version()->set_maxversion(max_version);
  } else if (cmd == "acl") {
    if (acl.empty()) {
      // list acl
      request.mutable_acl()->set_cmd(eos::rpc::NSRequest::AclRequest::LIST);
    } else {
      // modify acl
      request.mutable_acl()->set_cmd(eos::rpc::NSRequest::AclRequest::MODIFY);
      request.mutable_acl()->set_rule(acl);
    }
    request.mutable_acl()->mutable_id()->set_path(path);
    if (recursive) {
      request.mutable_acl()->set_recursive(true);
    }
    if (sysacl) {
      request.mutable_acl()->set_type(eos::rpc::NSRequest::AclRequest::SYS_ACL);
    } else {
      request.mutable_acl()->set_type(eos::rpc::NSRequest::AclRequest::USER_ACL);
    }
  } else if (cmd == "token") {
    request.mutable_token()->mutable_token()->mutable_token()->set_expires(time(NULL) + 300);
    if (!path.empty()) {
      request.mutable_token()->mutable_token()->mutable_token()->set_path(path);
    }
    if (recursive) {
      request.mutable_token()->mutable_token()->mutable_token()->set_allowtree(true);
    }
    if (acl.empty()) {
      request.mutable_token()->mutable_token()->mutable_token()->set_permission("rx");
    } else {
      request.mutable_token()->mutable_token()->mutable_token()->set_permission(acl);
    }
    if (!eostoken.empty()) {
      request.mutable_token()->mutable_token()->mutable_token()->set_vtoken(eostoken);
    }
  } else if (cmd == "quota") {
    if (username.length()) {
      request.mutable_quota()->mutable_id()->set_username(username);
    }
    if (groupname.length()) {
      request.mutable_quota()->mutable_id()->set_groupname(groupname);
    }
    request.mutable_quota()->set_path(path);
  } else if (cmd == "recycle") {

    if ( (subcmd == "")  ||
	 (subcmd == "ls") ) {
      request.mutable_recycle()->set_cmd(eos::rpc::NSRequest::RecycleRequest::LIST);
    } else if (subcmd == "purge") {
      if (year) {
	request.mutable_recycle()->mutable_purgedate()->set_year(year);
      }
      if (month) {
	request.mutable_recycle()->mutable_purgedate()->set_month(month);
      }
      if (day) {
	request.mutable_recycle()->mutable_purgedate()->set_day(day);
      }
      request.mutable_recycle()->set_cmd(eos::rpc::NSRequest::RecycleRequest::PURGE);
    } else if (subcmd == "restore") {
      request.mutable_recycle()->set_cmd(eos::rpc::NSRequest::RecycleRequest::RESTORE);
      request.mutable_recycle()->set_key(path);
    } else {
      std::cerr << "invalid recycle request" << std::endl;
      return EINVAL;
    }
  }

  google::protobuf::util::MessageToJsonString(request,
					      &jsonstring, options);
  
  std::cout << "request: " << std::endl << jsonstring << std::endl;
  
  int retc = EIO;
  if (eosgrpc->Exec(request, reply)) {
    std::cerr << "grpc request failed" << std::endl;
  } else {
    retc = reply.error().code();
  }
  
  jsonstring = "";
  google::protobuf::util::MessageToJsonString(reply,
					      &jsonstring, options);
  
  std::cout << "reply: " << std::endl << jsonstring << std::endl;
  
  std::chrono::microseconds elapsed_global =
    std::chrono::duration_cast<std::chrono::microseconds>
    (std::chrono::steady_clock::now() - watch_global);
  std::cout << "request took " << elapsed_global.count() <<
            " micro seconds" << std::endl;
  return retc;
}
