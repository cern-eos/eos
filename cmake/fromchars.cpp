#include <charconv>
#include <string_view>

int main()
{
  std::string_view s("123456789");
  uint64_t result;
  std::from_chars(s.data(), s.data() + s.size(), result);
  return 0;
}
