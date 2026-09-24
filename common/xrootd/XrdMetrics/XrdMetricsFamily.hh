#ifndef __XRDMETRICS_FAMILY_HH__
#define __XRDMETRICS_FAMILY_HH__
/******************************************************************************/
/*                                                                            */
/*                   X r d M e t r i c s F a m i l y . h h                    */
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
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "XrdMetrics/XrdMetricsInstrument.hh"
#include "XrdMetrics/XrdMetricsLabels.hh"
#include "XrdMetrics/XrdMetricsSerializer.hh"

//-----------------------------------------------------------------------------
//! A metric family: one name, one help string, one type, and a set of series
//! keyed by their variable label values. The family owns a two-tier child
//! cache. The first time a label combination is seen it is created under a
//! short write lock; thereafter the caller caches the returned handle and hits
//! it lock-free forever. Scraping snapshots child pointers under a brief read
//! lock and serializes outside it, so a scrape never blocks metric producers.
//-----------------------------------------------------------------------------

namespace XrdMetrics {
/******************************************************************************/
/*                             I F a m i l y                                 */
/******************************************************************************/

//! Type-erased family interface so the registry can hold counters and gauges
//! together and drive any serializer over them.

class IFamily {
public:
  virtual ~IFamily() = default;
  virtual void serialize(Serializer& s) const = 0;
};

/******************************************************************************/
/*                        L a b e l e d F a m i l y                          */
/******************************************************************************/

//! @tparam Child  Counter<T> or Gauge<T>.

template <class Child>
class LabeledFamily : public IFamily {
public:
  //! @param fullName resolved name (prefix_subsystem_metric), embedded in every
  //!                 series prefix.
  //! @param ctx      immutable label context shared by all series.
  //! @param help     family HELP text.
  //! @param maxKids  cardinality cap; 0 means unlimited. Label combinations past
  //!                 the cap fold into a single overflow series rather than
  //!                 growing the series count without bound.
  LabeledFamily(std::string fullName, LabelContext ctx, std::string help,
                std::size_t maxKids = 0)
      : name_(std::move(fullName))
      , help_(std::move(help))
      , ctx_(std::move(ctx))
      , maxKids_(maxKids)
  {
  }

  //! Positional lookup; vals order matches the schema. Returns a stable handle
  //! the caller should cache. This is the only locked step on the update path,
  //! amortized to once per call site.
  Child&
  labels(std::vector<std::string> vals)
  {
    LabelValues key{std::move(vals)};

    // Normalize to the schema arity so the cached prefix and forEachLabel can
    // walk names and values in lockstep: missing values become empty, extras
    // are dropped. The argument is meant to be positional in schema order.
    key.v.resize(ctx_.schema.size());

    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      auto it = children_.find(key);
      if (it != children_.end()) {
        return *it->second;
      }
    }

    std::unique_lock<std::shared_mutex> wr(mutex_);
    auto it = children_.find(key); // re-check under write
    if (it != children_.end()) {
      return *it->second;
    }

    if (maxKids_ && children_.size() >= maxKids_) {
      return overflow();
    }

    auto child = std::make_unique<Child>(SeriesLabels(ctx_, name_, key));
    Child& ref = *child;
    children_.emplace(std::move(key), std::move(child));
    return ref;
  }

  //! The full metric name (prefix_subsystem_metric).
  const std::string&
  name() const noexcept
  {
    return name_;
  }

  void
  serialize(Serializer& s) const override
  {
    s.beginFamily(name_, Child::kind(), help_);
    for (const Child* c : collect()) {
      s.series(c->labels(), c->value());
    }
    s.endFamily();
  }

private:
  //! Collect child pointers under a short read lock. Node-based unordered_map
  //! keeps Child pointers stable across rehash, so serialization outside the
  //! lock is safe.
  std::vector<const Child*>
  collect() const
  {
    std::shared_lock<std::shared_mutex> rd(mutex_);
    std::vector<const Child*> out;
    out.reserve(children_.size() + (overflow_ ? 1 : 0));
    for (auto& kv : children_) {
      out.push_back(kv.second.get());
    }
    if (overflow_) {
      out.push_back(overflow_.get());
    }
    return out;
  }

  //! The shared series that over-cap label combinations fold into. Built lazily
  //! under the held write lock, labelled with a placeholder per schema slot.
  Child&
  overflow()
  {
    if (!overflow_) {
      LabelValues key;
      key.v.assign(ctx_.schema.size(), "__over_cardinality_limit__");
      overflow_ = std::make_unique<Child>(SeriesLabels(ctx_, name_, key));
    }
    return *overflow_;
  }

  std::string name_;
  std::string help_;
  LabelContext ctx_;
  std::size_t maxKids_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<LabelValues, std::unique_ptr<Child>, LabelValuesHash> children_;
  std::unique_ptr<Child> overflow_;
};

/******************************************************************************/
/*                       O b s e r v e d F a m i l y                         */
/******************************************************************************/

//! A read-only metric family whose series values are produced by reader
//! functions called at scrape time, rather than stored as atomics. Use it to
//! expose values that some other subsystem owns and updates (e.g. a scheduler
//! thread count, a getrusage figure, or an existing protocol counter) without
//! making the metric the source of truth. Readers must be cheap and
//! thread-safe; they run while the registry is being serialized.
//!
//! A family may hold several series distinguished by their variable label
//! values (in schema order); add them with add(). For an unlabelled metric,
//! add a single series with empty values.
//!
//! @tparam T  the value type (uint64_t, int64_t or double) selecting the
//!            Serializer::series overload, hence the exposition numeric type.

template <class T>
class ObservedFamily : public IFamily {
public:
  ObservedFamily(std::string fullName, MetricKind kind, std::string help,
                 LabelContext ctx)
      : name_(std::move(fullName))
      , help_(std::move(help))
      , kind_(kind)
      , ctx_(std::move(ctx))
  {
  }

  ObservedFamily(const ObservedFamily&) = delete; // series_ point at ctx_
  ObservedFamily& operator=(const ObservedFamily&) = delete;

  //! Add a series. values are the variable label values in schema order; reader
  //! produces the value at scrape time. Returns *this for chaining.
  ObservedFamily&
  add(std::vector<std::string> values, std::function<T()> reader)
  {
    LabelValues v{std::move(values)};
    v.v.resize(ctx_.schema.size()); // normalize to the schema arity
    series_.push_back(Series{SeriesLabels(ctx_, name_, std::move(v)), std::move(reader)});
    return *this;
  }

  void
  serialize(Serializer& s) const override
  {
    s.beginFamily(name_, kind_, help_);
    for (const Series& sr : series_) {
      s.series(sr.labels, sr.reader());
    }
    s.endFamily();
  }

  const std::string&
  name() const noexcept
  {
    return name_;
  }

private:
  struct Series {
    SeriesLabels labels;
    std::function<T()> reader;
  };

  std::string name_;
  std::string help_;
  MetricKind kind_;
  LabelContext ctx_; // owns the labels every Series points into
  std::vector<Series> series_;
};

/******************************************************************************/
/*                      H i s t o g r a m F a m i l y                        */
/******************************************************************************/

//! A family of cumulative histograms sharing one set of bucket bounds, keyed by
//! variable label values. Mirrors LabeledFamily's two-tier child cache but its
//! children take the bounds at construction and serialize as _bucket/_sum/_count.

class HistogramFamily : public IFamily {
public:
  HistogramFamily(std::string fullName, LabelContext ctx, std::vector<double> bounds,
                  std::string help, std::size_t maxKids = 0)
      : name_(std::move(fullName))
      , help_(std::move(help))
      , ctx_(std::move(ctx))
      , bounds_(std::move(bounds))
      , maxKids_(maxKids)
  {
  }

  Histogram&
  labels(std::vector<std::string> vals)
  {
    LabelValues key{std::move(vals)};
    key.v.resize(ctx_.schema.size());

    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      auto it = children_.find(key);
      if (it != children_.end()) {
        return *it->second;
      }
    }
    std::unique_lock<std::shared_mutex> wr(mutex_);
    auto it = children_.find(key);
    if (it != children_.end()) {
      return *it->second;
    }
    if (maxKids_ && children_.size() >= maxKids_) {
      return overflow();
    }

    auto child = std::unique_ptr<Histogram>(
        new Histogram(SeriesLabels(ctx_, name_, key), bounds_));
    Histogram& ref = *child;
    children_.emplace(std::move(key), std::move(child));
    return ref;
  }

  const std::string&
  name() const noexcept
  {
    return name_;
  }

  void
  serialize(Serializer& s) const override
  {
    s.beginFamily(name_, MetricKind::Histogram, help_);
    for (const Histogram* h : collect()) {
      std::vector<std::uint64_t> cum = h->cumulative();
      s.histogram(name_, h->labels(), HistogramData{h->bounds(), cum, h->value_sum()});
    }
    s.endFamily();
  }

private:
  std::vector<const Histogram*>
  collect() const
  {
    std::shared_lock<std::shared_mutex> rd(mutex_);
    std::vector<const Histogram*> out;
    out.reserve(children_.size() + (overflow_ ? 1 : 0));
    for (auto& kv : children_) {
      out.push_back(kv.second.get());
    }
    if (overflow_) {
      out.push_back(overflow_.get());
    }
    return out;
  }

  Histogram&
  overflow()
  {
    if (!overflow_) {
      LabelValues key;
      key.v.assign(ctx_.schema.size(), "__over_cardinality_limit__");
      overflow_ = std::unique_ptr<Histogram>(
          new Histogram(SeriesLabels(ctx_, name_, key), bounds_));
    }
    return *overflow_;
  }

  std::string name_;
  std::string help_;
  LabelContext ctx_;
  std::vector<double> bounds_;
  std::size_t maxKids_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<LabelValues, std::unique_ptr<Histogram>, LabelValuesHash> children_;
  std::unique_ptr<Histogram> overflow_;
};

/******************************************************************************/
/*                        S u m m a r y F a m i l y                          */
/******************************************************************************/

//! A family of summaries (count + sum, no quantiles) keyed by variable label
//! values. Identical two-tier child cache to LabeledFamily; children serialize
//! as _sum/_count through Serializer::summary.

class SummaryFamily : public IFamily {
public:
  SummaryFamily(std::string fullName, LabelContext ctx, std::string help,
                std::size_t maxKids = 0)
      : name_(std::move(fullName))
      , help_(std::move(help))
      , ctx_(std::move(ctx))
      , maxKids_(maxKids)
  {
  }

  Summary&
  labels(std::vector<std::string> vals)
  {
    LabelValues key{std::move(vals)};
    key.v.resize(ctx_.schema.size());

    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      auto it = children_.find(key);
      if (it != children_.end()) {
        return *it->second;
      }
    }
    std::unique_lock<std::shared_mutex> wr(mutex_);
    auto it = children_.find(key);
    if (it != children_.end()) {
      return *it->second;
    }
    if (maxKids_ && children_.size() >= maxKids_) {
      return overflow();
    }

    auto child = std::make_unique<Summary>(SeriesLabels(ctx_, name_, key));
    Summary& ref = *child;
    children_.emplace(std::move(key), std::move(child));
    return ref;
  }

  const std::string&
  name() const noexcept
  {
    return name_;
  }

  void
  serialize(Serializer& s) const override
  {
    s.beginFamily(name_, MetricKind::Summary, help_);
    for (const Summary* m : collect()) {
      s.summary(name_, m->labels(), SummaryData{m->value_sum(), m->count()});
    }
    s.endFamily();
  }

private:
  std::vector<const Summary*>
  collect() const
  {
    std::shared_lock<std::shared_mutex> rd(mutex_);
    std::vector<const Summary*> out;
    out.reserve(children_.size() + (overflow_ ? 1 : 0));
    for (auto& kv : children_) {
      out.push_back(kv.second.get());
    }
    if (overflow_) {
      out.push_back(overflow_.get());
    }
    return out;
  }

  Summary&
  overflow()
  {
    if (!overflow_) {
      LabelValues key;
      key.v.assign(ctx_.schema.size(), "__over_cardinality_limit__");
      overflow_ = std::make_unique<Summary>(SeriesLabels(ctx_, name_, key));
    }
    return *overflow_;
  }

  std::string name_;
  std::string help_;
  LabelContext ctx_;
  std::size_t maxKids_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<LabelValues, std::unique_ptr<Summary>, LabelValuesHash> children_;
  std::unique_ptr<Summary> overflow_;
};
} // namespace XrdMetrics
#endif
