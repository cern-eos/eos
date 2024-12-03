#pragma once
#include <openssl/err.h>
#include <string>
#include <stdexcept>

namespace jwk_generator
{
struct openssl_error: public std::runtime_error {
private:
  static inline std::string open_ssl_last_error()
  {
    int err = ERR_get_error();
    char errStr[256];
    ERR_error_string(err, errStr);
    return std::string(errStr);
  }
public:
  openssl_error(std::string what) : std::runtime_error(what +
        open_ssl_last_error()) {}
};
};

