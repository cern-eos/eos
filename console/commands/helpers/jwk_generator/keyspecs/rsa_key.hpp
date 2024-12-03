#pragma once

#include "jwk_generator/libs/json.hpp"
#include "jwk_generator/errors.hpp"
#include "jwk_generator/openssl_wrapper.hpp"

namespace jwk_generator
{
template<size_t shaBits>
struct RSAKey {
public:
  static constexpr const size_t nBits = 2048;
  openssl::EVPPKey keyPair;
  std::string modulous;
  std::string exponent;

  RSAKey(const RSAKey&) = delete;
  RSAKey& operator = (const RSAKey&) = delete;
  RSAKey(RSAKey&&) = default;
  RSAKey& operator = (RSAKey&&) = default;
  RSAKey()
  {
    using namespace detail;
#ifdef JWKGEN_OPENSSL_3_0
    keyPair = {[]()
    {
      return EVP_RSA_gen(nBits);
    }
              };

    if (!keyPair) {
      throw openssl_error("Unable to generate rsa key: ");
    }

    BIGNUM* modBN = nullptr;

    if (!EVP_PKEY_get_bn_param(keyPair, OSSL_PKEY_PARAM_RSA_N, &modBN)) {
      throw openssl_error("Unable to retrieve public key: ");
    }

    BIGNUM* exBN = nullptr;

    if (!EVP_PKEY_get_bn_param(keyPair, OSSL_PKEY_PARAM_RSA_E, &exBN)) {
      throw openssl_error("Unable to retrieve public key: ");
    }

#else
    auto exBN = openssl::BigNum::allocate();

    if (!exBN) {
      throw openssl_error("Unable to allocate BN: ");
    }

    BN_set_word(exBN, 65537);
    auto rsa = openssl::RSA::allocate();

    if (!RSA_generate_key_ex(rsa, nBits, exBN, NULL)) {
      throw openssl_error("Unable to generate rsa key: ");
    }

    keyPair = openssl::EVPPKey::allocate();

    if (!keyPair) {
      throw openssl_error("Unable to generate rsa key: ");
    }

    RSA* rsaPtr = rsa.release();

    if (!EVP_PKEY_assign_RSA(keyPair, rsaPtr)) {
      throw openssl_error("Unable to generate rsa key: ");
    }

    const BIGNUM* modBN = RSA_get0_n(rsaPtr);
#endif
    size_t len = BN_num_bytes(modBN);
    std::vector<uint8_t> modBin;
    modBin.resize(len);
    BN_bn2bin(modBN, modBin.data());
    modulous = base64_url_encode(modBin);
    len = BN_num_bytes(exBN);
    std::vector<uint8_t> exBin;
    exBin.resize(len);
    BN_bn2bin(exBN, exBin.data());
    exponent = base64_url_encode(exBin);
  }

  void insert_json(nlohmann::json& json) const
  {
    json["alg"] = "RS" + std::to_string(shaBits);
    json["kty"] = "RSA";
    json["e"] = exponent;
    json["n"] = modulous;
  }
};

using RS256 = RSAKey<256>;
using RS384 = RSAKey<384>;
using RS512 = RSAKey<512>;
};
