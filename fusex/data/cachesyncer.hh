/*
 * cachesyncer.hh
 *
 *  Created on: May 10, 2017
 *      Author: simonm
 */

#ifndef FUSEX_CACHESYNCER_HH_
#define FUSEX_CACHESYNCER_HH_

#include "interval_tree.hh"

#include "XrdCl/XrdClFile.hh"

class cachesyncer
{
public:

  /**
   * We expect a file that has been already opened
   */
  cachesyncer( XrdCl::File &file ) : file( file )
  {
  }

  virtual ~cachesyncer()
  {
  }

  int sync( int fd, interval_tree<uint64_t, uint64_t> &journal,
           size_t offshift,
           off_t truncatesize=0 );

private:

  XrdCl::File &file;
} ;

#endif /* FUSEX_CACHESYNCER_HH_ */
