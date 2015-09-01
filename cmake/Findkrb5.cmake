# - Try to find Kerberos5 
#  Check for libkrb5.a
#
#  KRB5_INCLUDE_DIR - where to find krb5.h, etc.
#  KRB5_LIBRARIES   - List of libraries when using ....
#  KRB5_FOUND       - True if Kerberos 5 libraries found.

include(FindPackageHandleStandardArgs)

if(KRB5_INCLUDE_DIRS AND KRB5_LIBRARIES)
  set(KRB5_FIND_QUIETLY TRUE)
else()
  find_path(KRB5_INCLUDE_DIR NAMES krb5.h 
    /usr/kerberos/include
    /usr/krb5/include
    /usr/local/kerberos/include
    /usr/include 
    /usr/include/kerberosV 
    /usr/local/include
    $ENV{KRB5_DIR}/include) 

  set(KRB5_INCLUDE_DIRS ${KRB5_INCLUDE_DIR})

  find_library(KRB5_MIT_LIBRARY NAMES k5crypto
    /usr/kerberos/lib
    /usr/krb5/lib 
    /usr/local/kerberos/lib
    /usr/lib64 
    /usr/lib 
    /usr/local/lib
    $ENV{KRB5_DIR}/lib)

  set(KRB5_LIBRARIES ${KRB5_LIBRARIES} ${KRB5_MIT_LIBRARY})
 
  find_library(KRB5_LIBRARY NAMES krb5
    /usr/kerberos/lib
    /usr/krb5/lib 
    /usr/local/kerberos/lib
    /usr/lib64 
    /usr/lib 
    /usr/local/lib
    $ENV{KRB5_DIR}/lib)


  set(KRB5_LIBRARIES ${KRB5_LIBRARIES} ${KRB5_LIBRARY})

  find_program(KRB5_INIT NAMES kinit
    /usr/kerberos/bin
    /usr/krb5/bin 
    /usr/local/kerberos/bin
    /usr/bin
    /usr/local/bin
    $ENV{KRB5_DIR}/bin)
  
  find_package_handle_standard_args(
    krb5 DEFAULT_MSG KRB5_LIBRARY KRB5_INCLUDE_DIR KRB5_MIT_LIBRARY KRB5_INIT)

  mark_as_advanced(KRB5_INCLUDE_DIR KRB5_MIT_LIBRARY KRB5_LIBRARY KRB5_INIT)
endif()
