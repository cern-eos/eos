#include <stdexcept>
#include <string>
#include <tuple>

#ifndef __EOSCOMMON_WFEENDPOINT__HH__
#define __EOSCOMMON_WFEENDPOINT__HH__

// Parse the endpoint in scheme://host[:port] format
inline std::tuple<std::string, std::string, int>
parseURI(const std::string& endpointUrl)
{
  size_t scheme_end = endpointUrl.find("://");
  std::string scheme;
  std::string host_port;

  if (scheme_end == std::string::npos) {
    // Assume it's an XRootD SSI URL (legacy)
    scheme = "root";
    host_port = endpointUrl;
  } else {
    scheme = endpointUrl.substr(0, scheme_end);
    host_port = endpointUrl.substr(scheme_end + 3); // Skip "://"
  }
  size_t colon_pos = host_port.find(':');
  int port = (scheme == "root") ? 1094 : 50051;
  std::string host = host_port;

  if (colon_pos != std::string::npos) {
    try {
      port = std::stoi(host_port.substr(colon_pos + 1));
      host = host_port.substr(0, colon_pos);
    } catch (const std::exception& ex) {
      throw std::invalid_argument("Invalid port in endpoint URL: " + endpointUrl);
    }
  }

  return std::make_tuple(scheme, host, port);
}

struct WFEndpoint {
  enum class ClientType { GRPC_JWT, GRPCS_JWT, GRPCS_MTLS, XROOTD_SSI };

  static WFEndpoint
  from_config(const std::string& endpointUrl, const std::string& certPath,
              const std::string& keyPath, const std::string& jwtTokenPath)
  {
    const bool hasMtls = !certPath.empty() && !keyPath.empty();
    const bool hasJwt = !jwtTokenPath.empty();

    if (hasMtls && hasJwt) {
      throw std::invalid_argument(
          "mTLS and JWT authentication cannot be configured at the same time.");
    }

    if (hasMtls) {
      return WFEndpoint(endpointUrl, certPath, keyPath);
    }

    if (hasJwt) {
      return WFEndpoint(endpointUrl, jwtTokenPath);
    }

    // XRootD SSI endpoint (legacy)
    return WFEndpoint(endpointUrl);
  }

  /**
   * Build an endpoint object from a string in the format scheme://hostname[:port].
   */
  WFEndpoint(std::string endpointUrl)
  {
    auto [scheme, hostname_, port_] = parseURI(endpointUrl);

    hostname = hostname_;
    port = port_;

    if (scheme == "root") {
      type = ClientType::XROOTD_SSI;
    } else if (scheme == "grpc" || scheme == "grpcs") {
      throw std::invalid_argument("You cannot use a grpc(s) endpoint without specifying "
                                  "either JWT or mTLS authentication.");
    } else {
      throw std::invalid_argument("Invalid endpoint URL scheme: " + scheme + ".");
    }
  }

  WFEndpoint(std::string endpointUrl, std::string jwtTokenPath)
  {
    auto [scheme, hostname_, port_] = parseURI(endpointUrl);

    hostname = hostname_;
    port = port_;

    if (scheme == "grpc") {
      type = ClientType::GRPC_JWT;
    } else if (scheme == "grpcs") {
      type = ClientType::GRPCS_JWT;
    } else {
      throw std::invalid_argument("Invalid JWT endpoint URL scheme: " + scheme +
                                  ". Expected 'grpc' or 'grpcs'.");
    }
  }

  WFEndpoint(std::string endpointUrl, std::string certPath, std::string keyPath)
  {
    // Parse the endpoint in scheme://host[:port] format
    auto [scheme, hostname_, port_] = parseURI(endpointUrl);

    hostname = hostname_;
    port = port_;

    if (scheme == "grpcs") {
      type = ClientType::GRPCS_MTLS;
    } else {
      throw std::invalid_argument("Invalid mTLS endpoint URL scheme: " + scheme +
                                  ". Expected 'grpcs'.");
    }
  }

  /**
   * Return the endpoint as a string in the format scheme://hostname:port
   */
  std::string
  uri() const
  {
    std::string scheme;
    switch (type) {
    case ClientType::GRPC_JWT:
      scheme = "grpc";
      break;
    case ClientType::GRPCS_JWT:
    case ClientType::GRPCS_MTLS:
      scheme = "grpcs";
      break;
    case ClientType::XROOTD_SSI:
      scheme = "root";
      break;
    }
    return scheme + "://" + hostname + ":" + std::to_string(port);
  }

  /**
   * Return the address as a string in the format hostname:port
   */
  std::string
  address() const
  {
    return hostname + ":" + std::to_string(port);
  }

  std::string hostname;
  int port;
  ClientType type;
};

#endif // __EOSCOMMON_WFEENDPOINT__HH__
