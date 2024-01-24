{
  const char* mod = AY_OBFUSCATE("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");

  for (size_t i = 0; i < key.length(); ++i)
  {
    key[i] ^= mod[i];
  }
}



