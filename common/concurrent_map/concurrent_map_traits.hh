#include <type_traits>

namespace eos::common {
namespace detail {
  // This is a basic expreession SFINAE example, read more at
  // https://en.cppreference.com/w/cpp/language/sfinae C++ templates do not fail
  // compilation when there is a substitution failure as far as another overload
  // can be picked. So this can be used to introspect types at compile time

  // For detecting for eg. whether the underlying map implements the try_emplace
  // method, we first write the base case which should catch all expressions given a type
  // as the second argument cannot be overriden, so this will evaluate to false
  template <typename T, typename = void>
  struct has_try_emplace : std::false_type {};

  // Now comes the interesting overload, so by default the compiler picks this
  // overload as you'd write has_try_emplace<std::map<int,int>> which has no
  // second argument, so the compiler tries to evaluate the second std::void_t
  // expr, which will only be well formed when the underlying map has a
  // try_emplace method, if this expr. is not well formed we fallback to the
  // second overload which will evaluate to false type
  // This overload technically works without a value args overload for try_emplace due to the
  // variadic Args still matching a null arugment
  template <typename T>
  struct has_try_emplace<T,
    std::void_t<decltype(std::declval<T&>().try_emplace(std::declval<typename T::key_type>()))>>
    : std::true_type {};

  template <typename T, typename = void>
  struct has_emplace : std::false_type {};

  template <typename T>
  struct has_emplace<T,
    std::void_t<decltype(std::declval<T&>().emplace(std::declval<typename T::key_type>()))>>
    : std::true_type {};


} // namespace detail
} // namespace eos::common
