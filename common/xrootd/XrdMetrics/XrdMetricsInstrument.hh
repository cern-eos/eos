#ifndef __XRDMETRICS_INSTRUMENT_HH__
#define __XRDMETRICS_INSTRUMENT_HH__
/******************************************************************************/
/*                                                                            */
/*               X r d M e t r i c s I n s t r u m e n t . h h                */
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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

#include "XrdMetrics/XrdMetricsLabels.hh"

//-----------------------------------------------------------------------------
//! The metric instruments: Counter and Gauge. Each is just a SeriesLabels plus
//! one atomic payload and carries no label logic of its own. The mutators are
//! operators (++, +=, =, --) and all return void: these objects are write-only
//! sinks, and "write-only" is made visible in the type. Reading is the explicit
//! value() used only by serializers at scrape time.
//!
//! There are deliberately no virtuals here: the hot path is a non-virtual
//! relaxed atomic, and at high cardinality a per-series vtable pointer is waste.
//! The heterogeneity the registry needs lives one level up, at the family.
//-----------------------------------------------------------------------------

namespace XrdMetrics {
//! Instrument kind. Richer than a text token: an OTel serializer maps Counter
//! to a monotonic Sum, Gauge to a Gauge, Histogram to a Histogram and Summary
//! to an OTLP Summary.
enum class MetricKind { Counter, Gauge, Histogram, Summary };

/******************************************************************************/
/*                          S e r i e s B a s e                              */
/******************************************************************************/

//! Holds the SeriesLabels so the family can inject labels uniformly and the hot
//! path stays free of any label logic.

class SeriesBase {
public:
  explicit SeriesBase(SeriesLabels lbl)
      : labels_(std::move(lbl))
  {
  }

  const SeriesLabels&
  labels() const noexcept
  {
    return labels_;
  }

protected:
  SeriesLabels labels_;
};

/******************************************************************************/
/*                             C o u n t e r                                 */
/******************************************************************************/

//! A monotonically increasing counter, templated over uint64_t (the common
//! integral case) or double (e.g. accumulated seconds). There is no operator=,
//! no -=, and no reset: the only way to move a Counter is up, enforced by the
//! type system (a decrement fails to compile). Counter resets at process restart
//! are handled on the Prometheus server side via rate(). The double variant uses
//! the same portable compare-exchange loop as Gauge<double> because
//! std::atomic<double>::fetch_add is C++20 (libstdc++ >= GCC 10) and we still
//! support AlmaLinux 8 / GCC 8.

template <class T>
class Counter : public SeriesBase {
  static_assert(std::is_same<T, std::uint64_t>::value || std::is_same<T, double>::value,
                "Counter supports uint64_t or double only");

public:
  explicit Counter(SeriesLabels lbl)
      : SeriesBase(std::move(lbl))
  {
  }
  Counter(const Counter&) = delete;
  Counter& operator=(const Counter&) = delete;

  void
  operator++() noexcept
  {
    add(T{1});
  }
  void
  operator++(int) noexcept
  {
    add(T{1});
  }

  //! Note: for the unsigned variant a negative argument silently wraps to a huge
  //! value. There is no clean compile-time guard once the type is unsigned.
  void
  operator+=(T n) noexcept
  {
    add(n);
  }

  T
  value() const noexcept
  {
    return val_.load(std::memory_order_relaxed);
  }

  static constexpr MetricKind
  kind() noexcept
  {
    return MetricKind::Counter;
  }

private:
  void
  add(T v) noexcept
  {
    if constexpr (std::is_integral<T>::value) {
      val_.fetch_add(v, std::memory_order_relaxed);
    } else {
      T cur = val_.load(std::memory_order_relaxed);
      while (!val_.compare_exchange_weak(cur, cur + v, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
      }
    }
  }

  std::atomic<T> val_{T{}}; // explicit init: pre-C++20 default is UB
};

/******************************************************************************/
/*                               G a u g e                                   */
/******************************************************************************/

//! A value that may go up or down. Templated over int64_t or double. The only
//! operation that differs between them is the read-modify-write: integral uses
//! fetch_add, while double uses a portable compare-exchange loop because
//! std::atomic<double>::fetch_add is C++20 (libstdc++ >= GCC 10) and we still
//! support AlmaLinux 8 / GCC 8.

template <class T>
class Gauge : public SeriesBase {
  static_assert(std::is_same<T, std::int64_t>::value || std::is_same<T, double>::value,
                "Gauge supports int64_t or double only");

public:
  explicit Gauge(SeriesLabels lbl)
      : SeriesBase(std::move(lbl))
  {
  }
  Gauge(const Gauge&) = delete;
  Gauge& operator=(const Gauge&) = delete; // no gauge-to-gauge copy

  void
  operator=(T v) noexcept
  {
    val_.store(v, std::memory_order_relaxed);
  }

  void
  operator+=(T v) noexcept
  {
    add(v);
  }
  void
  operator-=(T v) noexcept
  {
    add(static_cast<T>(-v));
  }
  void
  operator++() noexcept
  {
    add(T{1});
  }
  void
  operator++(int) noexcept
  {
    add(T{1});
  }
  void
  operator--() noexcept
  {
    add(static_cast<T>(-1));
  }
  void
  operator--(int) noexcept
  {
    add(static_cast<T>(-1));
  }

  T
  value() const noexcept
  {
    return val_.load(std::memory_order_relaxed);
  }

  static constexpr MetricKind
  kind() noexcept
  {
    return MetricKind::Gauge;
  }

private:
  void
  add(T v) noexcept
  {
    if constexpr (std::is_integral<T>::value) {
      val_.fetch_add(v, std::memory_order_relaxed);
    } else {
      T cur = val_.load(std::memory_order_relaxed);
      while (!val_.compare_exchange_weak(cur, cur + v, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
      }
    }
  }

  std::atomic<T> val_{T{}}; // explicit init: pre-C++20 default is UB
};

/******************************************************************************/
/*                           H i s t o g r a m                               */
/******************************************************************************/

//! A cumulative histogram with fixed bucket upper bounds. observe(v) is the
//! lock-free hot path; the cumulative per-bucket counts, total count and sum
//! are computed at scrape time.

class Histogram : public SeriesBase {
public:
  Histogram(SeriesLabels lbl, std::vector<double> bounds)
      : SeriesBase(std::move(lbl))
      , bounds_(std::move(bounds))
      , buckets_(bounds_.size() + 1) // one extra for the +Inf bucket
  {
    std::sort(bounds_.begin(), bounds_.end());
    for (auto& b : buckets_) {
      b.store(0, std::memory_order_relaxed); // pre-C++20 UB guard
    }
  }
  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;

  void
  observe(double v) noexcept
  {
    std::size_t i = bounds_.size(); // default: +Inf bucket
    for (std::size_t k = 0; k < bounds_.size(); ++k) {
      if (v <= bounds_[k]) {
        i = k;
        break;
      }
    }
    buckets_[i].fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    double cur = sum_.load(std::memory_order_relaxed);
    while (!sum_.compare_exchange_weak(cur, cur + v, std::memory_order_relaxed)) {
    }
  }

  //! Bucket upper bounds (ascending); the implicit +Inf bucket is not included.
  const std::vector<double>&
  bounds() const noexcept
  {
    return bounds_;
  }

  //! Cumulative counts, size bounds().size()+1; the last element is the +Inf
  //! bucket and equals the total observation count.
  std::vector<std::uint64_t>
  cumulative() const
  {
    std::vector<std::uint64_t> c(buckets_.size());
    std::uint64_t run = 0;
    for (std::size_t i = 0; i < buckets_.size(); ++i) {
      run += buckets_[i].load(std::memory_order_relaxed);
      c[i] = run;
    }
    return c;
  }

  double
  value_sum() const noexcept
  {
    return sum_.load(std::memory_order_relaxed);
  }

  static constexpr MetricKind
  kind() noexcept
  {
    return MetricKind::Histogram;
  }

private:
  std::vector<double> bounds_;
  std::vector<std::atomic<std::uint64_t>> buckets_;
  std::atomic<std::uint64_t> count_{0};
  std::atomic<double> sum_{0.0};
};

/******************************************************************************/
/*                              S u m m a r y                                */
/******************************************************************************/

//! A Prometheus Summary without quantiles: a lock-free running aggregate of the
//! observation count and the sum of the observed values, exposed as _count and
//! _sum (TYPE summary). observe(v) is two relaxed atomic updates; the sum uses
//! the same portable compare-exchange loop as the double Gauge because
//! std::atomic<double>::fetch_add is C++20.
//!
//! Client-side quantiles are deliberately not computed: they would force a lock
//! on this hot path and yield non-aggregatable series. Use this for an average
//! (sum/count) of a distribution where bucket histograms are unwanted; for
//! server-side quantiles use a Histogram instead.

class Summary : public SeriesBase {
public:
  explicit Summary(SeriesLabels lbl)
      : SeriesBase(std::move(lbl))
  {
  }
  Summary(const Summary&) = delete;
  Summary& operator=(const Summary&) = delete;

  void
  observe(double v) noexcept
  {
    count_.fetch_add(1, std::memory_order_relaxed);
    double cur = sum_.load(std::memory_order_relaxed);
    while (!sum_.compare_exchange_weak(cur, cur + v, std::memory_order_relaxed)) {
    }
  }

  std::uint64_t
  count() const noexcept
  {
    return count_.load(std::memory_order_relaxed);
  }
  double
  value_sum() const noexcept
  {
    return sum_.load(std::memory_order_relaxed);
  }

  static constexpr MetricKind
  kind() noexcept
  {
    return MetricKind::Summary;
  }

private:
  std::atomic<std::uint64_t> count_{0};
  std::atomic<double> sum_{0.0};
};
} // namespace XrdMetrics
#endif
