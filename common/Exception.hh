#ifndef EOSCOMMON_EXCEPTION_HH
#define EOSCOMMON_EXCEPTION_HH

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include <stdexcept>
#include <sstream>
#include <cerrno>
#include <cstring>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//!  Exception handling errno + error text
//----------------------------------------------------------------------------
class Exception: public std::exception
{
public:
  //------------------------------------------------------------------------
  // Constructor
  //------------------------------------------------------------------------
  Exception( int errorNo = ENODATA ) throw():
    mErrorNo( errorNo ), mTmpMessage( 0 ) {}
  
  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  virtual ~Exception() throw()
  {
    delete [] mTmpMessage;
  }
  
  //------------------------------------------------------------------------
  //! Copy constructor - this is actually required because we cannot copy
  //! stringstreams
  //------------------------------------------------------------------------
  Exception( Exception &e )
  {
    mMessage << e.getMessage().str();
    mErrorNo = e.getErrno();
    mTmpMessage = 0;
  }
  
  //------------------------------------------------------------------------
  //! Get errno assosiated with the exception
  //------------------------------------------------------------------------
  int getErrno() const
  {
    return mErrorNo;
  }
  
  //------------------------------------------------------------------------
  //! Get the message stream
  //------------------------------------------------------------------------
  std::ostringstream &getMessage()
  {
    return mMessage;
  }
  
  //------------------------------------------------------------------------
  // Get the message
  //------------------------------------------------------------------------
  virtual const char *what() const throw()
  {
    // we could to that instead: return (mMessage.str()+" ").c_str();
    // but it's ugly and probably not portable
    
    if( mTmpMessage )
      delete [] mTmpMessage;
    
    std::string msg = mMessage.str();
    mTmpMessage = new char[msg.length()+1];
    mTmpMessage[msg.length()] = 0;
    strcpy( mTmpMessage, msg.c_str() );
    return mTmpMessage;
  }
  
private:
  //------------------------------------------------------------------------
  // Data members
  //------------------------------------------------------------------------
  std::ostringstream  mMessage;
  int                 mErrorNo;
  mutable char       *mTmpMessage;
};

EOSCOMMONNAMESPACE_END

#endif
