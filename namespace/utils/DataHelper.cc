//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Checksumming, data conversion and other stuff
//------------------------------------------------------------------------------

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cerrno>
#include "Namespace/utils/DataHelper.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  // Copy file ownership information
  //----------------------------------------------------------------------------
  void DataHelper::copyOwnership( const std::string &target,
                                  const std::string &source,
                                  bool ignoreNoPerm )
        throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Check the root-ness
    //--------------------------------------------------------------------------
    uid_t uid = getuid();
    if( uid != 0 && ignoreNoPerm )
      return;

    if( uid != 0 )
    {
      MDException e( EFAULT );
      e.getMessage() << "Only root can change ownership";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Get the thing done
    //--------------------------------------------------------------------------
    struct stat st;
    if( stat( source.c_str(), &st ) != 0 )
    {
      MDException e( errno );
      e.getMessage() << "Unable to stat source: " << source;
      throw e;
    }

    if( chown( target.c_str(), st.st_uid, st.st_gid ) != 0 )
    {
      MDException e( errno );
      e.getMessage() << "Unable to change the ownership of the target: ";
      e.getMessage() << target;
      throw e;
    }
  }
}

