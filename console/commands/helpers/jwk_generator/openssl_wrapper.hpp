#pragma once
#include "jwk_generator/c_resource.hpp"

#include "openssl/evp.h"
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>

#if OPENSSL_VERSION_NUMBER >= 0x30000000L // 3.0.0
#define JWKGEN_OPENSSL_3_0
#include <openssl/types.h>
#include <openssl/core_names.h>
#elif OPENSSL_VERSION_NUMBER < 0x10100000L // 1.1.0
#define JWKGEN_OPENSSL_1_0
#endif

namespace jwk_generator
{
namespace openssl
{
using EVPPKey = c_resource<EVP_PKEY, EVP_PKEY_free, EVP_PKEY_new>;
using ECGroup = c_resource<EC_GROUP, EC_GROUP_free>;
using BigNum = c_resource<BIGNUM, BN_free, BN_new>;

#ifndef JWKGEN_OPENSSL_3_0
using ECKey = c_resource<EC_KEY, EC_KEY_free, EC_KEY_new>;
using RSA = c_resource<::RSA, RSA_free, RSA_new>;
#endif
};
};
