#pragma once
#include <memory>
#include <functional>
#include <type_traits>
#include <stdexcept>

template <typename T, auto deleter, auto allocator = nullptr>
class c_resource
{
private:
  std::unique_ptr<T, decltype(deleter)> ptr;
public:
  c_resource(std::function<T*()> creater) : ptr{creater(), deleter} {}
  c_resource(T* ptr) : ptr{ptr, deleter} {}
  c_resource() : ptr {nullptr, deleter} {}

  static c_resource allocate()
  {
    static_assert(not std::is_null_pointer_v<decltype(allocator)>,
                  "allocate should not be used without a defined allocator");
    return c_resource(allocator);
  }

  operator bool() const
  {
    return ptr;
  }

  auto release()
  {
    return ptr.release();
  }

  auto get()
  {
    return ptr.get();
  }

  auto get() const
  {
    return ptr.get();
  }

  operator T* ()
  {
    return get();
  }

  operator const T* () const
  {
    return get();
  }
};
