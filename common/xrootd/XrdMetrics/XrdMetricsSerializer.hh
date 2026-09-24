#ifndef __XRDMETRICS_SERIALIZER_HH__
#define __XRDMETRICS_SERIALIZER_HH__
/******************************************************************************/
/*                                                                            */
/*              X r d M e t r i c s S e r i a l i z e r . h h                 */
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
#include <unordered_map>
#include <vector>

#include "XrdMetrics/XrdMetricsInstrument.hh" // MetricKind
#include "XrdMetrics/XrdMetricsLabels.hh"     // SeriesLabels

//-----------------------------------------------------------------------------
//! The serialization seam. A family is serialized by accepting an Serializer
//! and calling beginFamily, one series() per child (with the value's static
//! type preserved so a uint64 counter is never coerced through double), then
//! endFamily. Adding an output format (OTel JSON, XRootD XML) is a new
//! Serializer subclass and never a change to families or instruments.
//-----------------------------------------------------------------------------

namespace XrdMetrics {
//! A histogram series' scrape-time snapshot. bounds are the upper bounds;
//! cumulative has bounds.size()+1 entries (the last is the +Inf bucket and
//! equals the total observation count).
struct HistogramData {
  const std::vector<double>& bounds;
  const std::vector<std::uint64_t>& cumulative;
  double sum;
};

//! A summary series' scrape-time snapshot: the observation count and the sum of
//! the observed values (no quantiles).
struct SummaryData {
  double sum;
  std::uint64_t count;
};

/******************************************************************************/
/*                          I S e r i a l i z e r                            */
/******************************************************************************/

class Serializer {
public:
  Serializer() = default;
  Serializer(const Serializer&) = default;
  Serializer(Serializer&&) = default;
  Serializer& operator=(const Serializer&) = default;
  Serializer& operator=(Serializer&&) = default;
  virtual ~Serializer() = default;

  //! Document framing, called once by Collector::serialize() around the whole
  //! traversal. Formats with an envelope (JSON, XML) open/close it here; the
  //! line-oriented Prometheus text format leaves both as no-ops.
  virtual void
  begin()
  {
  }
  virtual void
  end()
  {
  }

  //! Per-Collector boundary, called by CollectorRegistry::serialize() around each
  //! Collector's body so envelope formats can emit one resource block per
  //! Collector (OTLP maps a Collector to a resourceMetrics entry, using its global labels
  //! as resource attributes). The single-registry Collector::serialize() path does not
  //! call these; formats that need a resource still open one lazily. No-ops for the
  //! Prometheus text format, which concatenates by distinct metric-name prefix.
  virtual void
  beginResource(const std::string& /*scope*/, const std::vector<ConstLabel>& /*attrs*/)
  {
  }
  virtual void
  endResource()
  {
  }

  virtual void
  beginFamily(const std::string& /*fullName*/, MetricKind /*kind*/,
              const std::string& /*help*/)
  {
  }
  virtual void
  endFamily()
  {
  }

  //! Typed series emit; the family's static Child type selects the overload.
  virtual void series(const SeriesLabels& labels, std::uint64_t value) = 0;
  virtual void series(const SeriesLabels& labels, std::int64_t value) = 0;
  virtual void series(const SeriesLabels& labels, double value) = 0;

  //! A histogram series (emits _bucket/_sum/_count in the text format).
  virtual void histogram(const std::string& fullName, const SeriesLabels& labels,
                         const HistogramData& data) = 0;

  //! A summary series (emits _sum/_count in the text format).
  virtual void summary(const std::string& fullName, const SeriesLabels& labels,
                       const SummaryData& data) = 0;
};

/******************************************************************************/
/*             P r o m e t h e u s T e x t S e r i a l i z e r               */
/******************************************************************************/

//! Emits the Prometheus text exposition format into a caller-owned string. The
//! same buffer can be reused across scrapes (the registry traversal clears
//! nothing, so the caller clears once before serialize()), giving an
//! allocation-free steady state. Per series the work is a memcpy of the cached
//! prefix plus one number append and a newline.

class PrometheusTextSerializer : public Serializer {
public:
  explicit PrometheusTextSerializer(std::string& out)
      : out_(out)
  {
  }

  void beginFamily(const std::string& name, MetricKind kind,
                   const std::string& help) override;

  void series(const SeriesLabels& labels, std::uint64_t value) override;
  void series(const SeriesLabels& labels, std::int64_t value) override;
  void series(const SeriesLabels& labels, double value) override;

  void histogram(const std::string& fullName, const SeriesLabels& labels,
                 const HistogramData& data) override;

  void summary(const std::string& fullName, const SeriesLabels& labels,
               const SummaryData& data) override;

private:
  template <class T>
  void emit(const SeriesLabels& labels, T value);

  std::string& out_;
};

/******************************************************************************/
/*                   O t e l J s o n S e r i a l i z e r                     */
/******************************************************************************/

//! Emits the OpenTelemetry OTLP/JSON encoding of an ExportMetricsServiceRequest
//! into a caller-owned string: a single resourceMetrics -> scopeMetrics ->
//! metrics document that can be POSTed to an OTLP/HTTP receiver
//! (Content-Type: application/json). Each family maps to one metric: a Counter
//! becomes a monotonic cumulative Sum, a Gauge a Gauge, and a Histogram a
//! cumulative Histogram (bucketCounts are de-cumulated as OTLP requires). Per
//! proto3 JSON, 64-bit integers are encoded as strings. Every data point is
//! stamped with the scrape time captured in begin().
//!
//! Unlike the Prometheus text format this carries an envelope, so it must be
//! driven through Collector::serialize() (which calls begin()/end()); appending
//! raw text collectors into the same buffer would corrupt the JSON.

class OtelJsonSerializer : public Serializer {
public:
  //! @param out           buffer the JSON document is appended to.
  //! @param scope         instrumentation scope name (e.g. the registry prefix).
  //! @param resourceAttrs optional OTLP Resource attributes (e.g. an instance id).
  //! @param out           buffer the JSON document is appended to.
  //! @param scope         instrumentation scope name; also the scope/fallback for
  //!                      the single-Collector path. CollectorRegistry::serialize()
  //!                      overrides the scope per Collector with the Collector prefix.
  //! @param resourceAttrs base OTLP Resource attributes (e.g. service.name,
  //!                      service.instance.id) emitted on every resource block,
  //!                      ahead of any per-registry global labels.
  explicit OtelJsonSerializer(std::string& out, std::string scope = "xrootd",
                              std::vector<ConstLabel> resourceAttrs = {})
      : out_(out)
      , scope_(std::move(scope))
      , resource_(std::move(resourceAttrs))
  {
  }

  void begin() override;
  void end() override;

  void beginResource(const std::string& scope,
                     const std::vector<ConstLabel>& attrs) override;
  void endResource() override;

  void beginFamily(const std::string& name, MetricKind kind,
                   const std::string& help) override;
  void endFamily() override;

  void series(const SeriesLabels& labels, std::uint64_t value) override;
  void series(const SeriesLabels& labels, std::int64_t value) override;
  void series(const SeriesLabels& labels, double value) override;

  void histogram(const std::string& fullName, const SeriesLabels& labels,
                 const HistogramData& data) override;

  void summary(const std::string& fullName, const SeriesLabels& labels,
               const SummaryData& data) override;

private:
  void beginPoint(const SeriesLabels& labels); // comma, "{", attributes array

  //! Open a resourceMetrics block (base resource_ attributes followed by @p extra)
  //! and close the current one; used by begin/beginResource/beginFamily/end to
  //! frame each registry. @p scope names the instrumentation scope.
  void openResource(const std::string& scope, const std::vector<ConstLabel>* extra);
  void closeResource();

  std::string& out_;
  std::string scope_;
  std::vector<ConstLabel> resource_;
  std::string ts_; // timeUnixNano, set in begin()
  MetricKind kind_ = MetricKind::Counter;
  bool firstMetric_ = true;
  bool firstPoint_ = true;
  bool firstResource_ = true; // no comma before first block
  bool resourceOpen_ = false; // a resource block is open
};

/******************************************************************************/
/*                       M e t r i c S n a p s h o t                         */
/******************************************************************************/

//! Collects the registry's series values into a by-name lookup, so a consumer
//! can pull values for an output layout that does not follow the flat metric
//! model (e.g. the legacy XrdStats <stats id=...> XML). Drive it with
//! Collector::serialize(), then read values back by key.
//!
//! The key is the series identity "name{labels}" — the cached Prometheus prefix
//! without its trailing space; an unlabelled series is keyed by its bare metric
//! name. Histograms and summaries are skipped (the legacy formats predate them).

class MetricSnapshot : public Serializer {
public:
  void series(const SeriesLabels& labels, std::uint64_t value) override;
  void series(const SeriesLabels& labels, std::int64_t value) override;
  void series(const SeriesLabels& labels, double value) override;
  void
  histogram(const std::string&, const SeriesLabels&, const HistogramData&) override
  {
  }
  void
  summary(const std::string&, const SeriesLabels&, const SummaryData&) override
  {
  }

  //! Look up a series value by key, returning dflt when it is absent. getInt
  //! truncates a double-typed series toward zero (the legacy fields are integers).
  std::int64_t getInt(const std::string& key, std::int64_t dflt = 0) const;
  double getDouble(const std::string& key, double dflt = 0) const;
  bool has(const std::string& key) const;

  //! Number of distinct series captured.
  std::size_t
  size() const noexcept
  {
    return values_.size();
  }

private:
  struct Value {
    std::int64_t i;
    double d;
    bool isDouble;
  };
  void put(const SeriesLabels& labels, std::int64_t i, double d, bool isDouble);

  std::unordered_map<std::string, Value> values_;
};
} // namespace XrdMetrics
#endif
