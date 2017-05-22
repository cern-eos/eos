/*
 * interval_tree.hh
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
#include <list>
#include "fusex/data/interval_tree.hh"

#ifndef __EOS_FUSEX_INTERVALTREETEST_HH__
#define __EOS_FUSEX_INTERVALTREETEST_HH__

class IntervalTreeTest : public CppUnit::TestCase
{
    CPPUNIT_TEST_SUITE( IntervalTreeTest );
      CPPUNIT_TEST( TestInvariant );
      CPPUNIT_TEST( TestQuery );
    CPPUNIT_TEST_SUITE_END();

  public:

    void TestInvariant()
    {
      tree.clear();
      Populate();
      CPPUNIT_ASSERT( TestInvariant( tree.tree_root ) );
    }

    void TestQuery()
    {
      tree.clear();
      tree.insert( 5, 10, "(5, 10)" );
      tree.insert( 1, 12, "(1, 12)" );
      tree.insert( 2, 8, "(2, 8)" );
      tree.insert( 15, 25, "(15, 25)" );
      tree.insert( 8, 16, "(8, 16)" );
      tree.insert( 14, 20, "(14, 20)" );
      tree.insert( 18, 21, "(18, 21)" );

      auto result = tree.query( 26, 28 );
      CPPUNIT_ASSERT( result.size() == 0 );

      result = tree.query( 12, 14 );
      CPPUNIT_ASSERT( result.size() == 1 );

      result = tree.query( 10, 12 );
      CPPUNIT_ASSERT( result.size() == 2 );

      result = tree.query( 18, 19 );
      CPPUNIT_ASSERT( result.size() == 3 );

      result = tree.query( 6, 9 );
      CPPUNIT_ASSERT( result.size() == 4 );

      result = tree.query( 7, 15 );
      CPPUNIT_ASSERT( result.size() == 5 );

      result = tree.query( 6, 16 );
      CPPUNIT_ASSERT( result.size() == 6 );

      result = tree.query( 0, 26 );
      CPPUNIT_ASSERT( result.size() == 7 );
    }

    void Populate()
    {
      std::list< std::pair<int, int> > intervals;

      srand( time( NULL ) );

      for( int i = 0; i < 1000; ++i )
      {
        int m = rand() % 999 + 1;
        int l = rand() % m + 1;
        int h = m + rand() % ( 1000 - m ) + 1;
        std::stringstream ss;
        ss << "(" << l << ", " << h << ")";
        tree.insert( l, h, ss.str() );
        intervals.push_back( std::make_pair( l, h ) );
      }

      for( int i = 0; i < 200; ++i )
      {
        int index = rand() % intervals.size();
        auto itr = intervals.begin();
        std::advance( itr, index );
        tree.erase( itr->first, itr->second );
        intervals.erase( itr );
      }
    }

  private:

    static bool TestInvariant( const std::unique_ptr< interval_node_t<int, std::string> > &root )
    {
      // base case
      if( !root )
        return true;

      // max has to be >= high
      if( root->max < root->high )
        return false;

      // max has to be >= left->max
      if( root->left && root->max < root->left->max )
        return false;

      // max has to be >= right->max
      if( root->right && root->max < root->right->max )
        return false;

      // test children
      return TestInvariant( root->left ) && TestInvariant( root->right );
    }

    interval_tree<int, std::string> tree;
};

CPPUNIT_TEST_SUITE_REGISTRATION( IntervalTreeTest );

#endif
