/*
 * RBTreeTest.cc
 *
 *  Created on: Mar 22, 2017
 *      Author: Michal Simon
 *
 ************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <cppunit/extensions/HelperMacros.h>
#include <unistd.h>
#include <climits>
#include "fusex/data/rbtree.hh"

#ifndef __EOS_FUSEX_RBTREETEST_HH__
#define __EOS_FUSEX_RBTREETEST_HH__

class RBTreeTest : public CppUnit::TestCase
{
    CPPUNIT_TEST_SUITE( RBTreeTest );
      CPPUNIT_TEST( TestRBInvariant );
      CPPUNIT_TEST( TestBSTInvariant );
      CPPUNIT_TEST( TestIterator );
    CPPUNIT_TEST_SUITE_END();

  public:

    void TestRBInvariant()
    {
      tree.clear();
      Populate();
      auto ret = TestRBInvariant( tree.tree_root );
      CPPUNIT_ASSERT( ret.first );
    }

    void TestBSTInvariant()
    {
      tree.clear();
      Populate();
      CPPUNIT_ASSERT( TestBSTInvariant( tree.tree_root ) );
    }

    void TestIterator()
    {
      tree.clear();
      tree.insert( 1, "1" );
      tree.insert( 2, "2" );
      tree.insert( 3, "3" );
      tree.insert( 4, "4" );
      tree.insert( 5, "5" );
      tree.insert( 6, "6" );
      tree.insert( 7, "7" );
      tree.insert( 8, "8" );
      tree.insert( 9, "9" );

      int i = 1;
      rbtree<int, std::string>::iterator itr;
      for( itr = tree.begin() ; itr != tree.end(); ++itr )
      {
        CPPUNIT_ASSERT( itr->key == i );
        ++i;
      }
   }

  private:

    void Populate()
    {
      srand( time( NULL ) );

      for( int i = 0; i < 1000; ++i )
      {
        int k = rand() % 1000 + 1;
        std::stringstream ss;
        ss << k;
        tree.insert( k, ss.str() );
      }

      for( int i = 0; i < 200; ++i )
      {
        int k = rand() % 1000 + 1;
        tree.erase( k );
      }
    }

    static std::pair<bool, int> TestRBInvariant( const std::unique_ptr< node_t<int, std::string> > &root )
    {
      // base case
      if( !root )
        return std::make_pair( true, 0 );

      int black = 0;
      if( root->colour == RED )
      {
        // RED node cannot have RED children
        if( ( root->left && root->left->colour == RED ) || ( root->right && root->right->colour == RED ) )
          return std::make_pair( false, -1 );
      }
      else
        black += 1;

      std::pair<bool, int> l = TestRBInvariant( root->left );
      std::pair<bool, int> r = TestRBInvariant( root->right );

      // both sub trees have to be valid red-black trees
      if( !l.first || !r.first )
        return std::make_pair( false, -1 );

      // the 'black' high of both sub-trees has to be the same
      if( l.second != r.second )
        return std::make_pair( false, -1 );

      return std::make_pair( true, l.second + black );
    }

    static std::pair<bool, int> GetMax( const std::unique_ptr< node_t<int, std::string> > &root )
    {
      if( !root )
        return std::make_pair( false, 0 );

      node_t<int, std::string>* node = root.get();
      while( node->right )
        node = node->right.get();

      return std::make_pair( true, node->key );
    }

    static std::pair<bool, int> GetMin( const std::unique_ptr< node_t<int, std::string> > &root )
    {
      if( !root )
        return std::make_pair( false, 0 );

      node_t<int, std::string>* node = root.get();
      while( node->left )
        node = node->left.get();

      return std::make_pair( true, node->key );
    }

    static bool TestBSTInvariant( const std::unique_ptr< node_t<int, std::string> > &root )
    {
      if( !root ) return true;

      if( !TestBSTInvariant( root->left ) || !TestBSTInvariant( root->right ) )
        return false;

      auto right_min = GetMin( root->right );
      // check if the right-sub tree exists
      if( right_min.first )
        // all the items in right sub-tree have to be greater than root
        if( right_min.second <= root->key )
          return false;

      auto left_max  = GetMax( root->left );
      // check if the left sub-tree exists
      if( left_max.first )
        // all the items in left sub-tree have to be smaller than root
        if( left_max.second >= root->key )
          return false;

      return true;
    }

    rbtree<int, std::string> tree;
};

CPPUNIT_TEST_SUITE_REGISTRATION( RBTreeTest );

#endif
