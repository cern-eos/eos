#!/usr/bin/env python3

import re
import sys
from pathlib import Path


MAIN_GRPC_EXCEPTIONS = {
    "TrafficShapingRate": "admin/sudoer monitoring stream",
}

UNIMPLEMENTED_MAIN_GRPC = {
    "Notify",
    "Notification",
}


def read(root, relpath):
    return (root / relpath).read_text(encoding="utf-8")


def fail(errors):
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if errors else 0


def extract_service_methods(proto_text, service_name):
    service = re.search(
        rf"service\s+{re.escape(service_name)}\s*\{{(?P<body>.*?)\n\}}",
        proto_text,
        re.S,
    )

    if not service:
        raise RuntimeError(f"service {service_name} not found")

    return set(re.findall(r"\brpc\s+([A-Za-z0-9_]+)\s*\(", service.group("body")))


def extract_overrides(source_text, class_name):
    class_match = re.search(
        rf"class\s+{re.escape(class_name)}\b.*?\{{(?P<body>.*?)\n\}};",
        source_text,
        re.S,
    )

    if not class_match:
        raise RuntimeError(f"class {class_name} not found")

    body = class_match.group("body")
    matches = list(
        re.finditer(
            r"\bStatus\s+([A-Za-z0-9_]+)\s*\([^;{]*?\)\s*(?:override\s*)?\{",
            body,
            re.S,
        )
    )
    methods = {}

    for index, match in enumerate(matches):
        name = match.group(1)
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        methods[name] = body[start:end]

    return methods


def extract_function_body(source_text, function_name):
    match = re.search(rf"\b{re.escape(function_name)}\s*\([^)]*\)\s*\{{", source_text)

    if not match:
        raise RuntimeError(f"function {function_name} not found")

    depth = 1
    pos = match.end()

    while pos < len(source_text) and depth:
        if source_text[pos] == "{":
            depth += 1
        elif source_text[pos] == "}":
            depth -= 1
        pos += 1

    return source_text[match.end():pos - 1]


def extract_oneof_fields(proto_text, message_name, oneof_name):
    message = re.search(
        rf"\bmessage\s+{re.escape(message_name)}\s*\{{(?P<body>.*?)\n\}}",
        proto_text,
        re.S,
    )

    if not message:
        raise RuntimeError(f"message {message_name} not found")

    oneof = re.search(
        rf"\boneof\s+{re.escape(oneof_name)}\s*\{{(?P<body>.*?)\n\s*\}}",
        message.group("body"),
        re.S,
    )

    if not oneof:
        raise RuntimeError(f"oneof {oneof_name} not found in {message_name}")

    fields = set()

    for line in oneof.group("body").splitlines():
        line = re.sub(r"//.*", "", line).strip()

        if not line:
            continue

        match = re.match(r"(?:[\w.]+)\s+(\w+)\s*=", line)

        if match:
            fields.add(match.group(1))

    return fields


def field_to_case(field):
    return "".join(part.capitalize() for part in field.split("_"))


def extract_enum_values(proto_text, enum_name):
    enum = re.search(
        rf"\benum\s+{re.escape(enum_name)}\s*\{{(?P<body>.*?)\n\}}",
        proto_text,
        re.S,
    )

    if not enum:
        raise RuntimeError(f"enum {enum_name} not found")

    values = set()

    for line in enum.group("body").splitlines():
        line = re.sub(r"//.*", "", line).strip()

        if not line:
            continue

        match = re.match(r"([A-Z0-9_]+)\s*=", line)

        if match:
            values.add(match.group(1))

    return values


def extract_cases(source_text, enum_prefix):
    return set(re.findall(rf"\bcase\s+{re.escape(enum_prefix)}::k([A-Za-z0-9_]+)\s*:", source_text))


def extract_upper_cases(source_text, enum_prefix):
    return set(re.findall(rf"\bcase\s+{re.escape(enum_prefix)}::([A-Z0-9_]+)\s*:", source_text))


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    errors = []

    rpc_proto = read(root, "common/grpc-proto/Rpc.proto")
    rest_gateway_proto = read(root, "proto/eos_rest_gateway/eos_rest_gateway_service.proto")
    wnc_proto = read(root, "proto/eos-protobuf-spec/EosWnc.proto")
    console_request_proto = read(root, "proto/eos-protobuf-spec/ConsoleRequest.proto")
    quota_proto = read(root, "proto/eos-protobuf-spec/Quota.proto")
    grpc_server = read(root, "mgm/grpc/GrpcServer.cc")
    grpc_ns = read(root, "mgm/grpc/GrpcNsInterface.cc")
    grpc_rest_gateway = read(root, "mgm/grpc/GrpcRestGwServer.cc")
    grpc_wnc = read(root, "mgm/grpc/GrpcWncInterface.cc")
    grpc_auth = read(root, "mgm/grpc/GrpcAuth.cc")

    main_methods = extract_service_methods(rpc_proto, "Eos")
    implemented_methods = extract_overrides(grpc_server, "RequestServiceImpl")

    unknown_main = main_methods - set(implemented_methods) - UNIMPLEMENTED_MAIN_GRPC
    if unknown_main:
        errors.append(
            "main gRPC RPC methods are not implemented or explicitly exempted: "
            + ", ".join(sorted(unknown_main))
        )

    for method_name, body in sorted(implemented_methods.items()):
        if method_name in MAIN_GRPC_EXCEPTIONS:
            continue

        if method_name == "Exec":
            if "GrpcNsInterface::Exec" not in body:
                errors.append("main gRPC Exec must delegate to GrpcNsInterface::Exec")
            continue

        if "GrpcAuth::Authorize" not in body:
            errors.append(f"main gRPC method {method_name} does not call GrpcAuth::Authorize")

    if "GrpcAuth::Authorize" not in extract_function_body(grpc_ns, "GrpcNsInterface::Exec"):
        errors.append("GrpcNsInterface::Exec does not call GrpcAuth::Authorize")

    supported_exec_cases = extract_cases(
        extract_function_body(grpc_ns, "GrpcNsInterface::Exec"),
        "eos::rpc::NSRequest",
    )
    mapped_exec_cases = extract_cases(
        extract_function_body(grpc_auth, "GrpcAuth::ExecScope"),
        "eos::rpc::NSRequest",
    )
    missing_exec_cases = supported_exec_cases - mapped_exec_cases

    if missing_exec_cases:
        errors.append(
            "supported NSRequest cases are missing gRPC scope mappings: "
            + ", ".join(sorted(missing_exec_cases))
        )

    rpc_quota_ops = extract_enum_values(rpc_proto, "QUOTAOP")
    mapped_rpc_quota_ops = extract_upper_cases(
        extract_function_body(grpc_auth, "GrpcAuth::ExecScope"),
        "eos::rpc::QUOTAOP",
    )
    missing_rpc_quota_ops = rpc_quota_ops - mapped_rpc_quota_ops

    if missing_rpc_quota_ops:
        errors.append(
            "Rpc.proto QUOTAOP values are missing gRPC scope mappings: "
            + ", ".join(sorted(missing_rpc_quota_ops))
        )

    rest_gateway_methods = extract_service_methods(rest_gateway_proto, "EosRestGatewayService")
    implemented_rest_gateway_methods = extract_overrides(
        grpc_rest_gateway, "EosRestGatewayServiceImpl"
    )
    missing_rest_gateway_methods = rest_gateway_methods - set(implemented_rest_gateway_methods)

    if missing_rest_gateway_methods:
        errors.append(
            "REST gateway RPC methods are not implemented: "
            + ", ".join(sorted(missing_rest_gateway_methods))
        )

    for method_name, body in sorted(implemented_rest_gateway_methods.items()):
        if "AuthorizeRestGateway" not in body:
            errors.append(f"REST gateway method {method_name} does not authorize gRPC scope")

        if method_name == "QuotaRequest":
            if "GrpcAuth::QuotaScope(*request)" not in body:
                errors.append("REST gateway QuotaRequest must use shared quota scope mapping")
        elif "GrpcAuth::RestScope(__func__)" not in body:
            errors.append(f"REST gateway method {method_name} does not use RestScope")

    if "GrpcAuth::Authorize" not in extract_function_body(
        grpc_rest_gateway, "AuthorizeRestGateway"
    ):
        errors.append("REST gateway scope helper does not call GrpcAuth::Authorize")

    if extract_service_methods(wnc_proto, "EosWnc") != {"ProcessSingle", "ProcessStream"}:
        errors.append("EosWnc service methods changed; update gRPC scope coverage guard")

    for method_name in ("ProcessSingle", "ProcessStream"):
        body = extract_function_body(read(root, "mgm/grpc/GrpcWncServer.cc"), method_name)
        if "GrpcAuth::Authorize" not in body:
            errors.append(f"WNC method {method_name} does not call GrpcAuth::Authorize")

    supported_wnc_cases = (
        extract_cases(extract_function_body(grpc_wnc, "GrpcWncInterface::ExecCmd"),
                      "eos::console::RequestProto") |
        extract_cases(extract_function_body(grpc_wnc, "GrpcWncInterface::ExecStreamCmd"),
                      "eos::console::RequestProto")
    )
    mapped_wnc_cases = extract_cases(
        extract_function_body(grpc_auth, "WncCommandName"),
        "eos::console::RequestProto",
    )
    missing_wnc_cases = supported_wnc_cases - mapped_wnc_cases

    if missing_wnc_cases:
        errors.append(
            "supported WNC RequestProto cases are missing gRPC scope mappings: "
            + ", ".join(sorted(missing_wnc_cases))
        )

    console_request_cases = {field_to_case(field)
                             for field in extract_oneof_fields(console_request_proto,
                                                               "RequestProto", "command")}
    unsupported_but_mapped = mapped_wnc_cases - console_request_cases

    if unsupported_but_mapped:
        errors.append(
            "WNC scope mappings reference RequestProto cases not present in protobuf: "
            + ", ".join(sorted(unsupported_but_mapped))
        )

    wnc_quota_cases = {field_to_case(field)
                       for field in extract_oneof_fields(quota_proto, "QuotaProto", "subcmd")}
    mapped_wnc_quota_cases = extract_cases(
        extract_function_body(grpc_auth, "GrpcAuth::QuotaScope"),
        "eos::console::QuotaProto",
    )
    missing_wnc_quota_cases = wnc_quota_cases - mapped_wnc_quota_cases

    if missing_wnc_quota_cases:
        errors.append(
            "WNC quota subcommands are missing gRPC scope mappings: "
            + ", ".join(sorted(missing_wnc_quota_cases))
        )

    if "UnknownAction(action)" not in grpc_auth:
        errors.append("GrpcAuth::ScopeListAllows must reject unknown actions before wildcard matching")

    if 'wildcard_suffix = ".*"' not in grpc_auth:
        errors.append("GrpcAuth wildcard scopes must only wildcard whole scope components")

    if "RestGatewayPeerIsLocal(context)" not in extract_function_body(
        grpc_rest_gateway, "AuthorizeRestGateway"
    ):
        errors.append("REST gateway scope helper must reject non-loopback peers before auth")

    return fail(errors)


if __name__ == "__main__":
    sys.exit(main())
