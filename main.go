package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/distribution"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
)

type KeyValue struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Range struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type LinearBuckets struct {
	NumFiniteBuckets int32   `json:"num_finite_buckets"`
	Width            float64 `json:"width"`
	Offset           float64 `json:"offset"`
}

type ExponentialBuckets struct {
	NumFiniteBuckets int32   `json:"num_finite_buckets"`
	GrowthFactor     float64 `json:"growth_factor"`
	Scale            float64 `json:"scale"`
}

type ExplicitBuckets struct {
	Bounds []float64 `json:"bounds"`
}

type OptionsUnion struct {
	LinearBuckets      *LinearBuckets      `json:"linear_buckets"`
	ExponentialBuckets *ExponentialBuckets `json:"exponential_buckets"`
	ExplicitBuckets    *ExplicitBuckets    `json:"explicit_buckets"`
}

type BucketOptions struct {
	Options *OptionsUnion `json:"options"`
}

type DistributionValue struct {
	Count                 int64          `json:"count"`
	Mean                  float64        `json:"mean"`
	SumOfSquaredDeviation float64        `json:"sum_of_squared_deviation"`
	Range                 *Range         `json:"range"`
	BucketOptions         *BucketOptions `json:"bucket_options"`
	BucketCounts          []int64        `json:"bucket_counts"`
}

type Point struct {
	Timestamp         time.Time          `json:"timestamp"`
	Labels            []KeyValue         `json:"labels"`
	BoolValue         *bool              `json:"bool_value"`
	Int64Value        *int64             `json:"int64_value"`
	DoubleValue       *float64           `json:"double_value"`
	StringValue       *string            `json:"string_value"`
	DistributionValue *DistributionValue `json:"distribution_value"`
}

func convertKeyValuePairs(labels map[string]string, _type string) []KeyValue {
	kvs := make([]KeyValue, 0)
	for k, v := range labels {
		kvs = append(kvs, KeyValue{Type: _type, Key: k, Value: v})
	}
	return kvs
}

func readAndPrintTimeSeriesFields(
	ctx context.Context,
	projectID string,
	metricType string,
	resourceType string,
	startTime time.Time,
	endTime time.Time,
) error {
	client, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		return fmt.Errorf("NewMetricClient: %v", err)
	}
	defer client.Close()

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/" + projectID,
		Filter: fmt.Sprintf("metric.type=\"%s\" resource.type=\"%s\"", metricType, resourceType),
		Interval: &monitoringpb.TimeInterval{
			StartTime: &timestamp.Timestamp{
				Seconds: startTime.Unix(),
			},
			EndTime: &timestamp.Timestamp{
				Seconds: endTime.Unix(),
			},
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	}

	it := client.ListTimeSeries(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("could not read time series value: %v", err)
		}

		labels := make([]KeyValue, 0)
		labels = append(labels, convertKeyValuePairs(resp.GetResource().GetLabels(), "resource")...)
		labels = append(labels, convertKeyValuePairs(resp.GetMetric().GetLabels(), "metric")...)

		points := make([]Point, 0)
		for _, p := range resp.GetPoints() {
			point := Point{
				Timestamp: p.GetInterval().StartTime.AsTime(),
				Labels:    labels,
			}
			switch v := p.GetValue().GetValue().(type) {
			case *monitoringpb.TypedValue_BoolValue:
				point.BoolValue = &v.BoolValue
			case *monitoringpb.TypedValue_Int64Value:
				point.Int64Value = &v.Int64Value
			case *monitoringpb.TypedValue_DoubleValue:
				point.DoubleValue = &v.DoubleValue
			case *monitoringpb.TypedValue_StringValue:
				point.StringValue = &v.StringValue
			case *monitoringpb.TypedValue_DistributionValue:
				dv := v.DistributionValue
				point.DistributionValue = &DistributionValue{
					Count:                 dv.GetCount(),
					Mean:                  dv.GetMean(),
					SumOfSquaredDeviation: dv.GetSumOfSquaredDeviation(),
				}
				if r := dv.GetRange(); r != nil {
					point.DistributionValue.Range = &Range{
						Min: r.GetMin(),
						Max: r.GetMax(),
					}
				}
				if bo := dv.GetBucketOptions(); bo != nil {
					var ou OptionsUnion
					switch o := bo.GetOptions().(type) {
					case *distribution.Distribution_BucketOptions_LinearBuckets:
						ou.LinearBuckets = &LinearBuckets{
							NumFiniteBuckets: o.LinearBuckets.GetNumFiniteBuckets(),
							Width:            o.LinearBuckets.GetWidth(),
							Offset:           o.LinearBuckets.GetOffset(),
						}
					case *distribution.Distribution_BucketOptions_ExponentialBuckets:
						ou.ExponentialBuckets = &ExponentialBuckets{
							NumFiniteBuckets: o.ExponentialBuckets.GetNumFiniteBuckets(),
							GrowthFactor:     o.ExponentialBuckets.GetGrowthFactor(),
							Scale:            o.ExponentialBuckets.GetScale(),
						}
					case *distribution.Distribution_BucketOptions_ExplicitBuckets:
						ou.ExplicitBuckets = &ExplicitBuckets{
							Bounds: o.ExplicitBuckets.GetBounds(),
						}
					}
					point.DistributionValue.BucketOptions = &BucketOptions{
						Options: &ou,
					}
				}
				point.DistributionValue.BucketCounts = v.DistributionValue.GetBucketCounts()
			default:
				return fmt.Errorf("Not supported metric type: %s", v)
			}
			points = append(points, point)
		}

		outputJson, err := json.Marshal(&points)
		if err != nil {
			return err
		}
		fmt.Println(string(outputJson))
	}
	return nil
}

func main() {
	var (
		project      = flag.String("project", "", "GCP project")
		metricType   = flag.String("metricType", "", "Type of metric")
		resourceType = flag.String("resourceType", "", "Type of resource")
		start        = flag.Int64("start", time.Now().Add(time.Duration(-10)*time.Minute).Unix(), "Start time (unix time)")
		end          = flag.Int64("end", time.Now().Unix(), "End time (unix time)")
	)
	flag.Parse()
	ctx := context.Background()

	if err := readAndPrintTimeSeriesFields(ctx, *project, *metricType, *resourceType, time.Unix(*start, 0), time.Unix(*end, 0)); err != nil {
		log.Fatal(err)
	}
}
