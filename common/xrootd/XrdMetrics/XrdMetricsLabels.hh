#ifndef __XRDMETRICS_LABELS_HH__
#define __XRDMETRICS_LABELS_HH__
/******************************************************************************/
/*                                                                            */
/*                   X r d M e t r i c s L a b e l s . h h                    */
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

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

//-----------------------------------------------------------------------------
//! Label model for the metrics module. Labels come in three tiers with very
//! different mutation profiles, and the storage layout follows that:
//!
//!  - Variable label *names* are fixed per metric family and identical across
//!    every series in it. They live once in a LabelSchema owned by the family.
//!  - Const labels (registry-global plus per-family) are name and value, fixed
//!    at construction and immutable forever.
//!  - Variable label *values* are the only thing that differs between series and
//!    the only thing a dynamic lookup keys on (see LabelValues).
//!
//! Because everything that feeds a series' label rendering is immutable once the
//! series exists, SeriesLabels builds its Prometheus text prefix exactly once
//! and never invalidates it; a scrape only appends the changing value after it.
//-----------------------------------------------------------------------------

namespace XrdMetrics {
//! A fixed (name,value) label, e.g. a registry-global instance label.
using ConstLabel = std::pair<std::string, std::string>;

/******************************************************************************/
/*                         L a b e l S c h e m a                              */
/******************************************************************************/

//! Ordered, immutable variable-label names. One per family, shared by all of
//! its series. Declared order is canonical, so per-series value lookup is
//! positional and needs no per-call sorting.

class LabelSchema {
public:
  LabelSchema() = default;
  explicit LabelSchema(std::vector<std::string> names)
      : names_(std::move(names))
  {
  }

  std::size_t
  size() const noexcept
  {
    return names_.size();
  }
  const std::vector<std::string>&
  names() const noexcept
  {
    return names_;
  }

private:
  std::vector<std::string> names_;
};

/******************************************************************************/
/*                        L a b e l C o n t e x t                            */
/******************************************************************************/

//! Immutable label bundle shared by every series of a family; owned by the
//! family. global points at the registry-wide const labels (by reference, so
//! they must be frozen before any series is built).

struct LabelContext {
  const std::vector<ConstLabel>* global = nullptr; // registry-wide, by reference
  std::vector<ConstLabel> constLabels;             // family-level const labels
  LabelSchema schema;                              // variable label names
};

/******************************************************************************/
/*                         L a b e l V a l u e s                             */
/******************************************************************************/

//! A series' variable label values, in schema order. This tuple is the series
//! identity and the key of the family's child cache.

struct LabelValues {
  std::vector<std::string> v;

  bool
  operator==(const LabelValues& o) const noexcept
  {
    return v == o.v;
  }
};

//! FNV-1a hash over the value tuple, with a delimiter between elements so that
//! {"ab","c"} and {"a","bc"} do not collide.

struct LabelValuesHash {
  std::size_t
  operator()(const LabelValues& lv) const noexcept
  {
    std::uint64_t h = 1469598103934665603ull;
    auto mix = [&](unsigned char c) {
      h ^= c;
      h *= 1099511628211ull;
    };
    for (const auto& s : lv.v) {
      for (unsigned char c : s) {
        mix(c);
      }
      mix(0xff);
    }
    return static_cast<std::size_t>(h);
  }
};

/******************************************************************************/
/*                        S e r i e s L a b e l s                            */
/******************************************************************************/

//! The per-series label holder. It caches the immutable Prometheus text prefix
//! (everything that precedes the value, e.g. name{a="1",b="2"} with a trailing
//! space) for the hot scrape path, and exposes the live tiers structurally via
//! forEachLabel() for the other (JSON/XML/OTel) serializers, which escape
//! differently and so cannot share the cached string.

class SeriesLabels {
public:
  //! @param ctx        the family's immutable label context.
  //! @param metricName the resolved full metric name (prefix_subsystem_metric).
  //! @param vals       this series' variable label values, in schema order.
  SeriesLabels(const LabelContext& ctx, const std::string& metricName, LabelValues vals);

  //! Bytes that precede the value in the text exposition format.
  const std::string&
  prometheusPrefix() const noexcept
  {
    return prefix_;
  }

  //! This series' variable label values, in schema order.
  const LabelValues&
  values() const noexcept
  {
    return values_;
  }

  //! Visit every label (global const, family const, then variable) in order,
  //! passing raw, unescaped name/value to f. Format-agnostic: each serializer
  //! escapes as its format requires.
  template <class F>
  void
  forEachLabel(F&& f) const
  {
    if (ctx_->global) {
      for (auto& kv : *ctx_->global) {
        f(kv.first, kv.second);
      }
    }
    for (auto& kv : ctx_->constLabels) {
      f(kv.first, kv.second);
    }
    const auto& n = ctx_->schema.names();
    for (std::size_t i = 0; i < n.size(); ++i) {
      f(n[i], values_.v[i]);
    }
  }

private:
  void buildPrefix(const std::string& metricName);

  const LabelContext* ctx_;
  LabelValues values_;
  std::string prefix_; // built once; all inputs immutable
};

/******************************************************************************/
/*                            H e l p e r s                                  */
/******************************************************************************/

//! Join two name parts with an underscore, skipping empty parts.
std::string joinName(const std::string& a, const std::string& b);

//! Validate a Prometheus metric name ([a-zA-Z_:][a-zA-Z0-9_:]*).
bool validMetricName(const std::string& name);

//! Validate a Prometheus label name ([a-zA-Z_][a-zA-Z0-9_]*).
bool validLabelName(const std::string& name);
} // namespace XrdMetrics
#endif
