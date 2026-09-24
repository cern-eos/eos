#ifndef __XRDMETRICS_REGISTRY_HH__
#define __XRDMETRICS_REGISTRY_HH__
/******************************************************************************/
/*                                                                            */
/*                 X r d M e t r i c s R e g i s t r y . h h                  */
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
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "XrdMetrics/XrdMetricsFamily.hh"
#include "XrdMetrics/XrdMetricsLabels.hh"
#include "XrdMetrics/XrdMetricsSerializer.hh"

//-----------------------------------------------------------------------------
//! The three-level hierarchy: Collector owns Subsystems, a Subsystem owns
//! Families, a Family owns series. Naming context flows down at registration
//! time (the registry's prefix and the group's subsystem are resolved into each
//! family's full name once, when the family is created, and baked into the
//! cached series prefixes); iteration flows down at scrape time.
//!
//! Lifetime is strictly nested and that is what keeps the back-pointers valid:
//! Collector outlives groups outlive families outlive series. The registry's
//! global const labels must be frozen before the first family is created, since
//! they are baked into the text prefixes.
//-----------------------------------------------------------------------------

namespace XrdMetrics {
class Collector; // referenced by Subsystem; defined below

/******************************************************************************/
/*                             S u b s y s t e m                             */
/******************************************************************************/

//! A named subsystem (e.g. "scheduler", "ops") that owns a set of families and
//! is the factory injecting the resolved full name and label context downward.

class Subsystem {
public:
  Subsystem(Collector& collector, std::string subsystem)
      : collector_(collector)
      , subsystem_(std::move(subsystem))
  {
  }

  //! Create an UNLABELLED counter (T = uint64_t or double) and return the series
  //! itself, so the common case is just counter<T>("name", "help") with nothing
  //! more to call. constLabels are fixed labels. For a metric with variable labels
  //! use counterFamily() and pick a series with .labels({...}). Throws
  //! std::invalid_argument on an invalid metric or label name.
  template <class T>
  Counter<T>& counter(const std::string& name, std::string help = {},
                      std::vector<ConstLabel> constLabels = {});

  //! Create a labelled counter family. varLabels are the variable label names
  //! (schema); pick a series with .labels({...}). maxKids caps series cardinality
  //! (0 = unlimited); label combinations past the cap fold into one overflow series.
  template <class T>
  LabeledFamily<Counter<T>>& counterFamily(const std::string& name, std::string help,
                                           std::vector<std::string> varLabels,
                                           std::vector<ConstLabel> constLabels = {},
                                           std::size_t maxKids = 0);

  //! Unlabelled gauge (T = int64_t or double); returns the series itself.
  template <class T>
  Gauge<T>& gauge(const std::string& name, std::string help = {},
                  std::vector<ConstLabel> constLabels = {});

  //! Labelled gauge family; pick a series with .labels({...}).
  template <class T>
  LabeledFamily<Gauge<T>>& gaugeFamily(const std::string& name, std::string help,
                                       std::vector<std::string> varLabels,
                                       std::vector<ConstLabel> constLabels = {},
                                       std::size_t maxKids = 0);

  //! Unlabelled histogram with fixed bucket upper bounds (the implicit +Inf bucket
  //! is added automatically); returns the series itself. Observe values on it.
  Histogram& histogram(const std::string& name, std::vector<double> bounds,
                       std::string help = {}, std::vector<ConstLabel> constLabels = {});

  //! Labelled histogram family; pick a series with .labels({...}).
  HistogramFamily& histogramFamily(const std::string& name, std::vector<double> bounds,
                                   std::string help, std::vector<std::string> varLabels,
                                   std::vector<ConstLabel> constLabels = {},
                                   std::size_t maxKids = 0);

  //! Unlabelled summary (count + sum, no quantiles); returns the series itself.
  //! Renders as _sum/_count under TYPE summary.
  Summary& summary(const std::string& name, std::string help = {},
                   std::vector<ConstLabel> constLabels = {});

  //! Labelled summary family; pick a series with .labels({...}).
  SummaryFamily& summaryFamily(const std::string& name, std::string help,
                               std::vector<std::string> varLabels,
                               std::vector<ConstLabel> constLabels = {},
                               std::size_t maxKids = 0);

  //! Register a read-only metric family whose series values are produced by
  //! reader functions at scrape time. Use these to surface a value owned and
  //! updated by another subsystem (the source of truth stays there) as typed
  //! series; add the series with ObservedFamily::add(). varLabels are the
  //! variable label names; readers must be cheap and thread-safe.
  //! observeCounter: T = uint64_t or double (a monotonic counter, e.g. CPU
  //! seconds; rendered with TYPE counter).
  template <class T>
  ObservedFamily<T>& observeCounter(const std::string& name, std::string help = {},
                                    std::vector<ConstLabel> constLabels = {},
                                    std::vector<std::string> varLabels = {});

  //! Like observeCounter but for a gauge value (T = int64_t or double).
  template <class T>
  ObservedFamily<T>& observeGauge(const std::string& name, std::string help = {},
                                  std::vector<ConstLabel> constLabels = {},
                                  std::vector<std::string> varLabels = {});

  //! Dynamic get-or-create-series convenience: one call returns the series for a
  //! metric name plus label values, creating the family (deduplicated by name) on
  //! first use. The label NAMES must be consistent across calls for a given
  //! metric name. Intended for callers that build labelled metrics on the fly
  //! (e.g. a monitoring collector), trading a per-call map lookup for that
  //! convenience; prefer the cached-handle factories on hot server paths.
  Counter<std::uint64_t>& counterSeries(const std::string& name, const std::string& help,
                                        std::vector<ConstLabel> labels = {});
  Gauge<double>& gaugeSeries(const std::string& name, const std::string& help,
                             std::vector<ConstLabel> labels = {});
  Histogram& histogramSeries(const std::string& name, const std::string& help,
                             std::vector<double> bounds,
                             std::vector<ConstLabel> labels = {});
  Summary& summarySeries(const std::string& name, const std::string& help,
                         std::vector<ConstLabel> labels = {});

  const std::string&
  name() const noexcept
  {
    return subsystem_;
  }

  void
  serialize(Serializer& s) const
  {
    std::vector<const IFamily*> snap;
    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      snap.reserve(families_.size());
      for (auto& f : families_) {
        snap.push_back(f.get());
      }
    }
    for (auto* f : snap) {
      f->serialize(s);
    }
  }

private:
  template <class Child>
  LabeledFamily<Child>& add(const std::string& name, std::string help,
                            std::vector<ConstLabel> constLabels,
                            std::vector<std::string> varNames, std::size_t maxKids);

  template <class T>
  ObservedFamily<T>& addObserved(MetricKind kind, const std::string& name,
                                 std::string help, std::vector<ConstLabel> constLabels,
                                 std::vector<std::string> varNames);

  template <class Child>
  LabeledFamily<Child>& getOrAddLabeled(const std::string& full,
                                        const std::vector<std::string>& names,
                                        const std::string& help);

  HistogramFamily& getOrAddHistogram(const std::string& full,
                                     const std::vector<std::string>& names,
                                     std::vector<double> bounds, const std::string& help);

  SummaryFamily& getOrAddSummary(const std::string& full,
                                 const std::vector<std::string>& names,
                                 const std::string& help);

  Collector& collector_;
  std::string subsystem_;

  mutable std::shared_mutex mutex_;
  std::vector<std::unique_ptr<IFamily>> families_;
  std::unordered_map<std::string, IFamily*> byName_; // dedup index for *Series
};

/******************************************************************************/
/*                             C o l l e c t o r                             */
/******************************************************************************/

//! Owns the global prefix, the frozen global const labels, and the groups.

class Collector {
public:
  //! @param prefix       leading name component for every metric (e.g. "xrootd").
  //! @param globalLabels const labels added to every series. Reserve these for
  //!                     things the Prometheus server cannot know (an XRootD
  //!                     instance name); instance/job are usually set at scrape
  //!                     time. They must not be mutated once a family exists.
  explicit Collector(std::string prefix, std::vector<ConstLabel> globalLabels = {})
      : prefix_(std::move(prefix))
      , globalLabels_(std::move(globalLabels))
  {
  }

  //! Drop this Collector from the process-wide directory (see CollectorRegistry).
  //! Safe whether or not it was ever registered.
  ~Collector();

  //! Replace the global const labels. Succeeds only while the registry is still
  //! empty (no family created yet), because the labels are baked into each
  //! series' cached text prefix at family-creation time; returns false otherwise
  //! and leaves the labels unchanged. Used by the early config load to seed
  //! cluster/program/role labels before any subsystem registers.
  bool
  setGlobalLabels(std::vector<ConstLabel> labels)
  {
    std::unique_lock<std::shared_mutex> wr(mutex_);
    if (!subsystems_.empty()) {
      return false;
    }
    globalLabels_ = std::move(labels);
    return true;
  }

  //! Obtain (creating on first use) the subsystem group by name.
  Subsystem&
  subsystem(const std::string& name)
  {
    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      auto it = subsystems_.find(name);
      if (it != subsystems_.end()) {
        return *it->second;
      }
    }
    std::unique_lock<std::shared_mutex> wr(mutex_);
    auto it = subsystems_.find(name);
    if (it != subsystems_.end()) {
      return *it->second;
    }
    auto g = std::unique_ptr<Subsystem>(new Subsystem(*this, name));
    auto& ref = *g;
    subsystems_.emplace(name, std::move(g));
    return ref;
  }

  const std::string&
  prefix() const noexcept
  {
    return prefix_;
  }
  const std::vector<ConstLabel>&
  globalLabels() const noexcept
  {
    return globalLabels_;
  }

  //! A predicate selecting which subsystems (groups) to emit, by group name.
  //! An empty (null) filter emits every group.
  using GroupFilter = std::function<bool(const std::string& subsystem)>;

  //! Drive a serializer over the registry's groups without the document framing
  //! (no begin()/end()), so several registries can be serialized into one
  //! document (see CollectorRegistry::serialize). Groups are snapshotted under a brief
  //! read lock and serialized outside it; a group is skipped when @p filter rejects it.
  void
  serializeBody(Serializer& s, const GroupFilter& filter = {}) const
  {
    std::vector<const Subsystem*> snap;
    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      snap.reserve(subsystems_.size());
      for (auto& kv : subsystems_) {
        snap.push_back(kv.second.get());
      }
    }
    for (auto* g : snap) {
      if (!filter || filter(g->name())) {
        g->serialize(s);
      }
    }
  }

  //! Drive a serializer over every group in the registry, with the document
  //! framing. Equivalent to begin(); serializeBody(s); end().
  void
  serialize(Serializer& s) const
  {
    s.begin();
    serializeBody(s);
    s.end();
  }

  //! Register a Prometheus-text collector, appended after the registry's own
  //! series on a text scrape (see runTextCollectors). This is an escape hatch for
  //! counters kept outside the registry (e.g. legacy plugin counter sets bridged
  //! as raw exposition text); it has no structured representation, so it only
  //! participates in Prometheus text output, not the other Serializer formats.
  void
  addTextCollector(std::function<void(std::string&)> c)
  {
    std::unique_lock<std::shared_mutex> wr(mutex_);
    collectors_.push_back(std::move(c));
  }

  //! Append every text collector's output to out. Call this after serializing the
  //! registry with a PrometheusTextSerializer into the same buffer.
  void
  runTextCollectors(std::string& out) const
  {
    std::vector<std::function<void(std::string&)>> snap;
    {
      std::shared_lock<std::shared_mutex> rd(mutex_);
      snap = collectors_;
    }
    for (auto& c : snap) {
      c(out);
    }
  }

private:
  std::string prefix_;
  std::vector<ConstLabel> globalLabels_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<Subsystem>> subsystems_;
  std::vector<std::function<void(std::string&)>> collectors_;
};

/******************************************************************************/
/*          S u b s y s t e m   f a c t o r i e s   ( need Collector )         */
/******************************************************************************/

template <class Child>
LabeledFamily<Child>&
Subsystem::add(const std::string& name, std::string help,
               std::vector<ConstLabel> constLabels, std::vector<std::string> varNames,
               std::size_t maxKids)
{
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);

  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : varNames) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }
  for (auto& cl : constLabels) {
    if (!validLabelName(cl.first)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + cl.first + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.constLabels = std::move(constLabels);
  ctx.schema = LabelSchema(std::move(varNames));

  auto fam = std::unique_ptr<LabeledFamily<Child>>(new LabeledFamily<Child>(
      std::move(full), std::move(ctx), std::move(help), maxKids));
  auto& ref = *fam;
  std::unique_lock<std::shared_mutex> wr(mutex_);
  families_.push_back(std::move(fam));
  return ref;
}

template <class T>
LabeledFamily<Counter<T>>&
Subsystem::counterFamily(const std::string& name, std::string help,
                         std::vector<std::string> varLabels,
                         std::vector<ConstLabel> constLabels, std::size_t maxKids)
{
  static_assert(std::is_same<T, std::uint64_t>::value || std::is_same<T, double>::value,
                "counter<T>: T must be uint64_t or double");
  return add<Counter<T>>(name, std::move(help), std::move(constLabels),
                         std::move(varLabels), maxKids);
}

template <class T>
Counter<T>&
Subsystem::counter(const std::string& name, std::string help,
                   std::vector<ConstLabel> constLabels)
{
  return counterFamily<T>(name, std::move(help), {}, std::move(constLabels)).labels({});
}

template <class T>
LabeledFamily<Gauge<T>>&
Subsystem::gaugeFamily(const std::string& name, std::string help,
                       std::vector<std::string> varLabels,
                       std::vector<ConstLabel> constLabels, std::size_t maxKids)
{
  static_assert(std::is_same<T, std::int64_t>::value || std::is_same<T, double>::value,
                "gauge<T>: T must be int64_t or double");
  return add<Gauge<T>>(name, std::move(help), std::move(constLabels),
                       std::move(varLabels), maxKids);
}

template <class T>
Gauge<T>&
Subsystem::gauge(const std::string& name, std::string help,
                 std::vector<ConstLabel> constLabels)
{
  return gaugeFamily<T>(name, std::move(help), {}, std::move(constLabels)).labels({});
}

inline HistogramFamily&
Subsystem::histogramFamily(const std::string& name, std::vector<double> bounds,
                           std::string help, std::vector<std::string> varLabels,
                           std::vector<ConstLabel> constLabels, std::size_t maxKids)
{
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);

  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : varLabels) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }
  for (auto& cl : constLabels) {
    if (!validLabelName(cl.first)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + cl.first + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.constLabels = std::move(constLabels);
  ctx.schema = LabelSchema(std::move(varLabels));

  auto fam = std::unique_ptr<HistogramFamily>(new HistogramFamily(
      std::move(full), std::move(ctx), std::move(bounds), std::move(help), maxKids));
  auto& ref = *fam;
  std::unique_lock<std::shared_mutex> wr(mutex_);
  families_.push_back(std::move(fam));
  return ref;
}

inline Histogram&
Subsystem::histogram(const std::string& name, std::vector<double> bounds,
                     std::string help, std::vector<ConstLabel> constLabels)
{
  return histogramFamily(name, std::move(bounds), std::move(help), {},
                         std::move(constLabels), 0)
      .labels({});
}

inline SummaryFamily&
Subsystem::summaryFamily(const std::string& name, std::string help,
                         std::vector<std::string> varLabels,
                         std::vector<ConstLabel> constLabels, std::size_t maxKids)
{
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);

  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : varLabels) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }
  for (auto& cl : constLabels) {
    if (!validLabelName(cl.first)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + cl.first + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.constLabels = std::move(constLabels);
  ctx.schema = LabelSchema(std::move(varLabels));

  auto fam = std::unique_ptr<SummaryFamily>(
      new SummaryFamily(std::move(full), std::move(ctx), std::move(help), maxKids));
  auto& ref = *fam;
  std::unique_lock<std::shared_mutex> wr(mutex_);
  families_.push_back(std::move(fam));
  return ref;
}

inline Summary&
Subsystem::summary(const std::string& name, std::string help,
                   std::vector<ConstLabel> constLabels)
{
  return summaryFamily(name, std::move(help), {}, std::move(constLabels), 0).labels({});
}

template <class T>
ObservedFamily<T>&
Subsystem::observeCounter(const std::string& name, std::string help,
                          std::vector<ConstLabel> constLabels,
                          std::vector<std::string> varLabels)
{
  static_assert(std::is_same<T, std::uint64_t>::value || std::is_same<T, double>::value,
                "observeCounter<T>: T must be uint64_t or double");
  return addObserved<T>(MetricKind::Counter, name, std::move(help),
                        std::move(constLabels), std::move(varLabels));
}

template <class T>
ObservedFamily<T>&
Subsystem::observeGauge(const std::string& name, std::string help,
                        std::vector<ConstLabel> constLabels,
                        std::vector<std::string> varLabels)
{
  static_assert(std::is_same<T, std::int64_t>::value || std::is_same<T, double>::value,
                "observeGauge<T>: T must be int64_t or double");
  return addObserved<T>(MetricKind::Gauge, name, std::move(help), std::move(constLabels),
                        std::move(varLabels));
}

// Shared private helper behind observeCounter/observeGauge: build a read-only
// ObservedFamily<T> of the given kind (the value type T is validated by the
// public wrappers above).
template <class T>
ObservedFamily<T>&
Subsystem::addObserved(MetricKind kind, const std::string& name, std::string help,
                       std::vector<ConstLabel> constLabels,
                       std::vector<std::string> varNames)
{
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);

  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : varNames) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }
  for (auto& cl : constLabels) {
    if (!validLabelName(cl.first)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + cl.first + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.constLabels = std::move(constLabels);
  ctx.schema = LabelSchema(std::move(varNames));

  auto fam = std::unique_ptr<ObservedFamily<T>>(
      new ObservedFamily<T>(std::move(full), kind, std::move(help), std::move(ctx)));
  auto& ref = *fam;
  std::unique_lock<std::shared_mutex> wr(mutex_);
  families_.push_back(std::move(fam));
  return ref;
}

/******************************************************************************/
/*        D y n a m i c   g e t - o r - c r e a t e   s e r i e s            */
/******************************************************************************/

//! Cardinality cap for families created through the dynamic get-or-create
//! (*Series) helpers. Those build series from label values that are often
//! remote-controlled (e.g. a monitoring collector labelling by reporting
//! server, request method or error category), so an unbounded family would let
//! a buggy or hostile source grow the series count without limit. Label
//! combinations past the cap fold into a single __over_cardinality_limit__
//! overflow series. The cached-handle factories (counter/histogram/...) are not
//! capped by default since their label sets are fixed in the instrumenting code.
inline constexpr std::size_t kDynamicSeriesCap = 8192;

template <class Child>
LabeledFamily<Child>&
Subsystem::getOrAddLabeled(const std::string& full, const std::vector<std::string>& names,
                           const std::string& help)
{
  {
    std::shared_lock<std::shared_mutex> rd(mutex_); // fast path: family exists
    auto it = byName_.find(full);
    if (it != byName_.end()) {
      auto* fam = dynamic_cast<LabeledFamily<Child>*>(it->second);
      if (!fam) {
        throw std::invalid_argument("XrdMetrics: metric '" + full +
                                    "' redefined with a different type");
      }
      return *fam;
    }
  }
  std::unique_lock<std::shared_mutex> wr(mutex_);
  auto it = byName_.find(full); // re-check under write lock
  if (it != byName_.end()) {
    auto* fam = dynamic_cast<LabeledFamily<Child>*>(it->second);
    if (!fam) {
      throw std::invalid_argument("XrdMetrics: metric '" + full +
                                  "' redefined with a different type");
    }
    return *fam;
  }
  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : names) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.schema = LabelSchema(names);
  auto fam = std::unique_ptr<LabeledFamily<Child>>(
      new LabeledFamily<Child>(full, std::move(ctx), help, kDynamicSeriesCap));
  auto& ref = *fam;
  byName_.emplace(full, fam.get());
  families_.push_back(std::move(fam));
  return ref;
}

inline HistogramFamily&
Subsystem::getOrAddHistogram(const std::string& full,
                             const std::vector<std::string>& names,
                             std::vector<double> bounds, const std::string& help)
{
  {
    std::shared_lock<std::shared_mutex> rd(mutex_); // fast path: family exists
    auto it = byName_.find(full);
    if (it != byName_.end()) {
      auto* fam = dynamic_cast<HistogramFamily*>(it->second);
      if (!fam) {
        throw std::invalid_argument("XrdMetrics: metric '" + full +
                                    "' redefined with a different type");
      }
      return *fam;
    }
  }
  std::unique_lock<std::shared_mutex> wr(mutex_);
  auto it = byName_.find(full); // re-check under write lock
  if (it != byName_.end()) {
    auto* fam = dynamic_cast<HistogramFamily*>(it->second);
    if (!fam) {
      throw std::invalid_argument("XrdMetrics: metric '" + full +
                                  "' redefined with a different type");
    }
    return *fam;
  }
  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : names) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.schema = LabelSchema(names);
  auto fam = std::unique_ptr<HistogramFamily>(new HistogramFamily(
      full, std::move(ctx), std::move(bounds), help, kDynamicSeriesCap));
  auto& ref = *fam;
  byName_.emplace(full, fam.get());
  families_.push_back(std::move(fam));
  return ref;
}

inline SummaryFamily&
Subsystem::getOrAddSummary(const std::string& full, const std::vector<std::string>& names,
                           const std::string& help)
{
  {
    std::shared_lock<std::shared_mutex> rd(mutex_); // fast path: family exists
    auto it = byName_.find(full);
    if (it != byName_.end()) {
      auto* fam = dynamic_cast<SummaryFamily*>(it->second);
      if (!fam) {
        throw std::invalid_argument("XrdMetrics: metric '" + full +
                                    "' redefined with a different type");
      }
      return *fam;
    }
  }
  std::unique_lock<std::shared_mutex> wr(mutex_);
  auto it = byName_.find(full); // re-check under write lock
  if (it != byName_.end()) {
    auto* fam = dynamic_cast<SummaryFamily*>(it->second);
    if (!fam) {
      throw std::invalid_argument("XrdMetrics: metric '" + full +
                                  "' redefined with a different type");
    }
    return *fam;
  }
  if (!validMetricName(full)) {
    throw std::invalid_argument("XrdMetrics: invalid metric name '" + full + "'");
  }
  for (auto& ln : names) {
    if (!validLabelName(ln)) {
      throw std::invalid_argument("XrdMetrics: invalid label name '" + ln + "'");
    }
  }

  LabelContext ctx;
  ctx.global = &collector_.globalLabels();
  ctx.schema = LabelSchema(names);
  auto fam = std::unique_ptr<SummaryFamily>(
      new SummaryFamily(full, std::move(ctx), help, kDynamicSeriesCap));
  auto& ref = *fam;
  byName_.emplace(full, fam.get());
  families_.push_back(std::move(fam));
  return ref;
}

namespace detail {
inline void
splitLabels(const std::vector<ConstLabel>& labels, std::vector<std::string>& names,
            std::vector<std::string>& values)
{
  names.reserve(labels.size());
  values.reserve(labels.size());
  for (auto& kv : labels) {
    names.push_back(kv.first);
    values.push_back(kv.second);
  }
}
} // namespace detail

inline Counter<std::uint64_t>&
Subsystem::counterSeries(const std::string& name, const std::string& help,
                         std::vector<ConstLabel> labels)
{
  std::vector<std::string> names, values;
  detail::splitLabels(labels, names, values);
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);
  return getOrAddLabeled<Counter<std::uint64_t>>(full, names, help)
      .labels(std::move(values));
}

inline Gauge<double>&
Subsystem::gaugeSeries(const std::string& name, const std::string& help,
                       std::vector<ConstLabel> labels)
{
  std::vector<std::string> names, values;
  detail::splitLabels(labels, names, values);
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);
  return getOrAddLabeled<Gauge<double>>(full, names, help).labels(std::move(values));
}

inline Histogram&
Subsystem::histogramSeries(const std::string& name, const std::string& help,
                           std::vector<double> bounds, std::vector<ConstLabel> labels)
{
  std::vector<std::string> names, values;
  detail::splitLabels(labels, names, values);
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);
  return getOrAddHistogram(full, names, std::move(bounds), help)
      .labels(std::move(values));
}

inline Summary&
Subsystem::summarySeries(const std::string& name, const std::string& help,
                         std::vector<ConstLabel> labels)
{
  std::vector<std::string> names, values;
  detail::splitLabels(labels, names, values);
  std::string full = joinName(joinName(collector_.prefix(), subsystem_), name);
  return getOrAddSummary(full, names, help).labels(std::move(values));
}

/******************************************************************************/
/*                              D e f a u l t                                */
/******************************************************************************/

//! The process-wide registry shared by the server and all loaded plugins.
//! Prefixed "xrootd"; plugins should register into this so all metrics land in
//! the same scrape. Auto-joins the registry directory on first use.
Collector& Default();

/******************************************************************************/
/*                 C o l l e c t o r   R e g i s t r y                        */
/******************************************************************************/

//! The process-wide directory of Collectors the exporter aggregates into one
//! scrape/push. The xrootd Default() Collector joins automatically; a foreign
//! owner (e.g. EOS) or a plugin that keeps its own Collector adds it here so
//! its series appear alongside the xrootd_* ones. Registration is idempotent
//! and a Collector removes itself on destruction.
class CollectorRegistry {
public:
  //! The process-wide directory. Leaked (never destroyed) so it outlives every
  //! Collector, including the static Default() one that unregisters at exit.
  static CollectorRegistry& instance();

  //! Register/unregister a Collector. add() is idempotent; a Collector calls
  //! remove() from its own destructor.
  void add(Collector& c);
  void remove(Collector& c);

  //! A snapshot of the currently registered Collectors.
  std::vector<Collector*> collectors();

  //! Serialize every registered Collector into one document: frames once
  //! (begin()/end()) and walks each Collector's body, marking a per-Collector
  //! resource boundary so envelope formats (OTLP) emit one resource block per
  //! Collector. @p filter selects which subsystems (groups) to emit, by name.
  void serialize(Serializer& s, const Collector::GroupFilter& filter = {});

private:
  CollectorRegistry() = default;
  CollectorRegistry(const CollectorRegistry&) = delete;
  CollectorRegistry& operator=(const CollectorRegistry&) = delete;

  std::mutex mutex_;
  std::vector<Collector*> collectors_;
};
} // namespace XrdMetrics
#endif
