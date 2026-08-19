#ifndef __XRDMETRICS_VALUE_HH__
#define __XRDMETRICS_VALUE_HH__
/******************************************************************************/
/*                                                                            */
/*                    X r d M e t r i c s V a l u e . h h                     */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/* XRootD is free software: you can redistribute it and/or modify it under    */
/* the terms of the GNU Lesser General Public License as published by the     */
/* Free Software Foundation, either version 3 of the License, or (at your     */
/* option) any later version.                                                 */
/*                                                                            */
/* XRootD is distributed in the hope that it will be useful, but WITHOUT      */
/* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or      */
/* FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public       */
/* License for more details.                                                  */
/*                                                                            */
/* You should have received a copy of the GNU Lesser General Public License   */
/* along with XRootD in a file called COPYING.LESSER (LGPL license) and file  */
/* COPYING (GPL license).  If not, see <http://www.gnu.org/licenses/>.        */
/******************************************************************************/

#include <cstdint>
#include <string>

//-----------------------------------------------------------------------------
//! Number-to-text helpers for the metrics serializers. These append the value
//! directly to a caller-owned buffer so a scrape allocates nothing in steady
//! state.
//!
//! Integer values always go through std::to_chars, which is available since
//! GCC 8 and is allocation- and locale-free. Doubles use std::to_chars(double)
//! when the build probe HAVE_FLOAT_TO_CHARS is set (libstdc++ >= GCC 11), and
//! otherwise fall back to a locale-guarded snprintf (the AlmaLinux 8 / GCC 8
//! path). Both paths render whole numbers without a decimal point and emit the
//! Prometheus tokens "+Inf", "-Inf" and "NaN" for non-finite values.
//-----------------------------------------------------------------------------

namespace XrdMetrics {
void appendValue(std::string& out, std::uint64_t v);
void appendValue(std::string& out, std::int64_t v);
void appendValue(std::string& out, double v);
} // namespace XrdMetrics
#endif
