#pragma once

#include <sstream>
#include <stdexcept>
#include <string>
#include <memory>
#include <tuple>
#include <stdint.h>

#include <openssl/bio.h>
#include <openssl/pem.h>

#include "jwk_generator//libs/base64_url.hpp"
#include "jwk_generator/libs/uuid.hpp"
#include "jwk_generator/libs/json.hpp"
#include "jwk_generator/errors.hpp"
#include "jwk_generator/keyspecs/ec_key.hpp"
#include "jwk_generator/keyspecs/rsa_key.hpp"

namespace jwk_generator
{
template <typename KeySpec>
class JwkGenerator
{
public:
  KeySpec key;
  std::string kid;

private:
  std::string to_pem(std::function<int(BIO*, EVP_PKEY*)> writeKeyToBIO) const
  {
    using namespace detail;
    auto pemKeyBIO = std::shared_ptr<BIO>(BIO_new(BIO_s_secmem()), BIO_free);

    if (!pemKeyBIO) {
      throw openssl_error("Unable to retrieve public key: ");
    }

    EVP_PKEY* tmpEVP = key.keyPair.get();
    int result = writeKeyToBIO(pemKeyBIO.get(), tmpEVP);

    if (!result) {
      throw openssl_error("Unable to convert key to pem: ");
    }

    char* buffer;
    auto len = BIO_get_mem_data(pemKeyBIO.get(), &buffer);

    if (!len) {
      throw openssl_error("Unable to retrieve key from bio: ");
    }

    std::string pem;
    pem.resize(len);
    std::memcpy(pem.data(), buffer, len);
    return pem;
  }

public:
  JwkGenerator(const JwkGenerator&) = delete;
  JwkGenerator& operator = (const JwkGenerator&) = delete;
  JwkGenerator(JwkGenerator&&) = default;
  JwkGenerator& operator = (JwkGenerator&&) = default;

  JwkGenerator()
  {
    kid = detail::generate_uuid_v4();
  }

  JwkGenerator(const std::string& kid_uuid,
               const std::string& fn_public = "",
               const std::string& fn_private = ""):
    kid(kid_uuid)
  {
    if (!fn_public.empty() && !fn_private.empty()) {
      key = KeySpec(fn_public, fn_private);
    }
  }

  std::string private_to_pem() const
  {
    return to_pem([](auto bio, auto key) {
      return PEM_write_bio_PrivateKey(bio, key, NULL, NULL, 0, 0, NULL);
    });
  }

  std::string public_to_pem() const
  {
    return to_pem([](auto bio, auto key) {
      return PEM_write_bio_PUBKEY(bio, key);
    });
  }

  nlohmann::json to_json() const
  {
    nlohmann::json json;
    key.insert_json(json);
    json["kid"] = kid;
    return json;
  }

  std::string to_pretty_string() const
  {
    return to_json().dump(true);
  }

  operator std::string() const
  {
    return to_json().dump();
  }

  friend std::ostream& operator<< (std::ostream& out, const JwkGenerator& e)
  {
    out << std::string(e);
    return out;
  }
};

template <typename... KeySpec>
class JwkSetGenerator
{
private:
  JwkSetGenerator(JwkSetGenerator&) = delete;

public:
  JwkSetGenerator() = default;
  JwkSetGenerator(std::tuple<JwkGenerator<KeySpec>...>&& keys) : keys{keys} { }

  const std::tuple<JwkGenerator<KeySpec>...> keys;

  nlohmann::json to_json() const
  {
    using namespace nlohmann;
    nlohmann::json jwks;
    std::apply([&jwks](const auto & ... jwk) {
      jwks["keys"] = std::array < nlohmann::json, std::tuple_size<decltype(keys)> {} > {jwk.to_json()...};
    }, keys);
    return jwks;
  }

  template <size_t idx>
  const auto& get() const
  {
    return std::get<idx>(keys);
  }

  operator std::string() const
  {
    return to_json().dump();
  }

  friend std::ostream& operator<< (std::ostream& out, const JwkSetGenerator& e)
  {
    out << std::string(e);
    return out;
  }
};

template <typename KeySpec, template < class ... > class Container, class ... Args >
class JwkSetSingleSpecGenerator
{
private:
  JwkSetSingleSpecGenerator(JwkSetSingleSpecGenerator&) = delete;
public:
  JwkSetSingleSpecGenerator() = default;
  JwkSetSingleSpecGenerator(Container<JwkGenerator<KeySpec>, Args...>&& keys) :
    keys{std::move(keys)} { }
  Container<JwkGenerator<KeySpec>, Args...> keys;

  nlohmann::json to_json() const
  {
    using namespace nlohmann;
    nlohmann::json jwks;
    jwks["keys"] = json::array();

    for (const auto& jwk : keys) {
      jwks["keys"].push_back(jwk.to_json());
    }

    return jwks;
  }

  const JwkGenerator<KeySpec>& operator[](size_t idx) const
  {
    return keys[idx];
  }

  template <size_t idx>
  const JwkGenerator<KeySpec>& get() const
  {
    return keys[idx];
  }

  operator std::string() const
  {
    return to_json().dump();
  }

  friend std::ostream& operator<< (std::ostream& out,
                                   const JwkSetSingleSpecGenerator& e)
  {
    out << std::string(e);
    return out;
  }
};

template<typename KeySpec>
static auto make_jwks(size_t nKeys)
{
  std::vector<JwkGenerator<KeySpec>> keys;
  keys.resize(nKeys);
  return JwkSetSingleSpecGenerator(std::move(keys));
}

};
