# bats file_tags=checksum
load ../instance

@test "checksums of a known string: adler32, crc32, crc32c, crc64, md5" {
  runtest "### adler32 " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum adler /dev/stdin | grep 091e01de"
  runtest "### crc32   " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum crc32 /dev/stdin | grep cbf43926"
  runtest "### crc32c  " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum crc32c /dev/stdin | grep e3069283"
  runtest "### crc64   " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum crc64 /dev/stdin | grep 6c40df5f0b497347"
  runtest "### md5     " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum md5 /dev/stdin  | grep 25f9e794323b453885f5181f1b624d0b"
  runtest "### xxhash64" unix SUCCESS "" shell echo -n "123456789 |  eos-checksum xxhash64 /dev/stdin | grep 8cb841db40e6ae83"
  runtest "### sha1    " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum sha /dev/stdin | grep f7c3bc1d808e04732adf679965ccc34ca7ae3441"
  runtest "### sha256  " unix SUCCESS "" shell echo -n "123456789 |  eos-checksum sha256 /dev/stdin | grep 15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225"
}
