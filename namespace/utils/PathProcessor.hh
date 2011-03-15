//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#ifndef EOS_PATH_PROCESSOR_HH
#define EOS_PATH_PROCESSOR_HH

#include <cstring>
#include <vector>
#include <string>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Helper class responsible for spliting the path
  //----------------------------------------------------------------------------
  class PathProcessor
  {
    public:
      //------------------------------------------------------------------------
      //! Split the path and put its elements in a vector, the tokens are
      //! copied, the buffer is not overwritten
      //------------------------------------------------------------------------
      static void splitPath( std::vector<std::string> &elements,
                             const std::string &path )
      {
        elements.clear();
        std::vector<char*> elems;
        char buffer[path.length()+1];
        strcpy( buffer, path.c_str() );
        splitPath( elems, buffer );
        for( size_t i = 0; i < elems.size(); ++i )
          elements.push_back( elems[i] );
      }

      //------------------------------------------------------------------------
      //! Split the path and put its element in a vector, the split is done
      //! in-place and the buffer is overwritten
      //------------------------------------------------------------------------
      static void splitPath( std::vector<char*> &elements, char *buffer )
      {
        elements.clear();
        elements.reserve( 10 );

        char *cursor = buffer;
        char *beg    = buffer;

        //----------------------------------------------------------------------
        // Go by the characters one by one
        //----------------------------------------------------------------------
        while( *cursor )
        {
          if( *cursor == '/' )
          {
            *cursor = 0;
            if( beg != cursor )
              elements.push_back( beg );
            beg = cursor+1;
          }
          ++cursor;
        }

        if( beg != cursor )
          elements.push_back( beg );
      }
  };
}

#endif // EOS_PATH_PROCESSOR_HH
