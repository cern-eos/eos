/*
 * FuseException.hh
 *
 *  Created on: Nov 16, 2016
 *      Author: simonm
 */

#ifndef FUSEEXCEPTION_HH_
#define FUSEEXCEPTION_HH_

#include <string.h>
#include <exception>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

class FuseException : public std::exception
{
public:

  FuseException(int rc) : code(rc) { }

  FuseException(const FuseException& ex): code(ex.code) { }

  FuseException& operator=(const FuseException& ex)
  {
    code = ex.code;
    return *this;
  }

  virtual ~FuseException() { }

  virtual const char* what() const throw()
  {
    return strerror(code);
  }

private:

  int code;
};



#endif /* FUSEEXCEPTION_HH_ */
