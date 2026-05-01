package controller

import "github.com/prometheus/client_golang/prometheus"

var (
	publishDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "csi_publish_duration_seconds",
			Help:    "Duration of CSI ControllerPublishVolume calls.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"result"},
	)

	publishErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "csi_publish_errors_total",
			Help: "Total number of failed CSI ControllerPublishVolume calls.",
		},
	)

	publishLUNRetries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "csi_publish_lun_retries_total",
			Help: "Total number of ControllerPublishVolume LUN retry attempts.",
		},
	)

	publishAlreadyMapped = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "csi_publish_already_mapped_total",
			Help: "Total number of ControllerPublishVolume mappings already present on the backend.",
		},
	)

	unpublishTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "csi_unpublish_total",
			Help: "Total number of CSI ControllerUnpublishVolume calls.",
		},
	)

	unpublishErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "csi_unpublish_errors_total",
			Help: "Total number of failed CSI ControllerUnpublishVolume calls.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		publishDuration,
		publishErrors,
		publishLUNRetries,
		publishAlreadyMapped,
		unpublishTotal,
		unpublishErrors,
	)
}
