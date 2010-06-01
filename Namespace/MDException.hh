//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Metadata exception
//------------------------------------------------------------------------------

#ifndef EOS_MD_EXCEPTION_HH
#define EOS_MD_EXCEPTION_HH

#include <stdexcept>
#include <sstream>
#include <cerrno>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Metadata exception
  //----------------------------------------------------------------------------
  class MDException: public std::exception
  {
    public:
      //------------------------------------------------------------------------
      // Constructor
      //------------------------------------------------------------------------
      MDException( int errorNo = 0 ) throw(): pErrorNo( errorNo ) {}

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~MDException() throw() {}

      //------------------------------------------------------------------------
      //! Copy constructor - this is actually required because we cannot copy
      //! stringstreams
      //------------------------------------------------------------------------
      MDException( MDException &e )
      {
        pMessage << e.getMessage().str();
        pErrorNo = e.pErrorNo;
      }

      //------------------------------------------------------------------------
      //! Get errno assosiated with the exception
      //------------------------------------------------------------------------
      int getErrno() const
      {
        return pErrorNo;
      }

      //------------------------------------------------------------------------
      //! Get the message stream
      //------------------------------------------------------------------------
      std::ostringstream &getMessage()
      {
        return pMessage;
      }

    private:
      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      std::ostringstream pMessage;
      int                pErrorNo;
  };
}

#endif // EOS_MD_EXCEPTION_HH
