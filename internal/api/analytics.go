package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
)

func (s *Server) requireAnalytics(w http.ResponseWriter) analytics.Querier {
	if s.analytics == nil {
		writeErr(w, http.StatusServiceUnavailable, "analytics unavailable (ClickHouse not configured)")
		return nil
	}
	return s.analytics
}

func (s *Server) parseRange(r *http.Request, def time.Duration) (analytics.TimeRange, error) {
	return analytics.ParseRange(r.URL.Query().Get("from"), r.URL.Query().Get("to"), def)
}

func (s *Server) analyticsThroughput(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	grain := r.URL.Query().Get("grain")
	if grain == "" {
		grain = "minute"
	}
	res, err := q.Throughput(r.Context(), tr, grain)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsLatency(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := q.Latency(r.Context(), tr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsFailures(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := q.Failures(r.Context(), tr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsWorkers(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := q.Workers(r.Context(), tr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsQueue(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := q.Queue(r.Context(), tr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsScheduler(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, 24*time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := q.Scheduler(r.Context(), tr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsCapacity(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	tr, err := s.parseRange(r, 24*time.Hour)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	target := 30.0
	if v := r.URL.Query().Get("target_per_worker"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			target = f
		}
	}
	res, err := q.Capacity(r.Context(), tr, target)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) analyticsAnomalies(w http.ResponseWriter, r *http.Request) {
	q := s.requireAnalytics(w)
	if q == nil {
		return
	}
	now := time.Now().UTC()
	window := 15 * time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			window = d
		}
	}
	current := analytics.TimeRange{From: now.Add(-window), To: now}
	baseline := analytics.TimeRange{From: now.Add(-2 * window), To: now.Add(-window)}
	if r.URL.Query().Get("from") != "" {
		if tr, err := s.parseRange(r, window); err == nil {
			current = tr
			span := tr.To.Sub(tr.From)
			baseline = analytics.TimeRange{From: tr.From.Add(-span), To: tr.From}
		}
	}
	res, err := q.Anomalies(r.Context(), current, baseline)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}
