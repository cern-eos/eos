// ----------------------------------------------------------------------
// File: DbMapCommon.cc
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
/*----------------------------------------------------------------------------*/
#include "common/DbMapCommon.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <iomanip>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

bool operator == (const DbMapTypes::Tlogentry &l, const DbMapTypes::Tlogentry &r)
{
  return !l.timestampstr.compare(r.timestampstr)
      && !l.seqid.compare(r.seqid)
      && !l.writer.compare(r.writer)
      && !l.key.compare(r.key)
      && !l.value.compare(r.value)
      && !l.comment.compare(r.comment);
}

bool operator == (const DbMapTypes::Tval &l, const DbMapTypes::Tval &r)
                                                                                                                                    {
  return !l.timestampstr.compare(r.timestampstr)
      && l.seqid==r.seqid
      && !l.writer.compare(r.writer)
      && !l.value.compare(r.value)
      && !l.comment.compare(r.comment);
                                                                                                                                    }

void
Tlogentry2Tval (const DbMapTypes::Tlogentry &tle, DbMapTypes::Tval *tval)
{
  tval->timestampstr = tle.timestampstr;
  tval->seqid = atol(tle.seqid.c_str());
  tval->value = tle.value;
  tval->writer = tle.writer;
  tval->comment = tle.comment;
}

ostream& operator << (ostream &os, const DbMapTypes::Tval &val)
{
  os << setprecision(20) << "\t" << val.timestampstr << "\t" << val.seqid << "\t" << val.writer << "\t" << val.value << "\t" << val.comment;
  return os;
}

istream& operator >> (istream &is, DbMapTypes::Tval &val)
{
  string buffer;
  //is >> val.timestamp;
  getline(is, val.timestampstr, '\t');
  getline(is, buffer, '\t');
  sscanf(buffer.c_str(), "%lu", &val.seqid);
  getline(is, val.value, '\t');
  getline(is, val.comment, '\t');
  return is;
}

ostream& operator << (ostream &os, const DbMapTypes::Tlogentry &entry)
{
  os << setprecision(20)  << "\ttimestampstr=" << entry.timestampstr << "\tseqid=" << entry.seqid << "\twriter=" << entry.writer << "\tkey=" << entry.key << "\tvalue=" << entry.value << "\tcomment=" << entry.comment;
  return os;
}

ostream& operator << (ostream &os, const DbMapTypes::TlogentryVec &entryvec)
{
  for (DbMapTypes::TlogentryVec::const_iterator it = entryvec.begin(); it != entryvec.end(); it++)
    os << (*it) << endl;
  return os;
}

void TimeToStr(const time_t t, char *tstr ) {
  struct tm ptm;
  localtime_r(&t,&ptm);
  size_t offset=strftime(tstr, 64, "%Y-%m-%d %H:%M:%S", &ptm);
  tstr[offset++]='#';

  sprintf(tstr+offset, "%9.9lu",(time_t)0);
}

EOSCOMMONNAMESPACE_END
