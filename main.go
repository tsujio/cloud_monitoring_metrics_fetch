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

type Point struct {
	Timestamp         time.Time                  `json:"timestamp"`
	Labels            []KeyValue                 `json:"labels"`
	BoolValue         *bool                      `json:"bool_value"`
	Int64Value        *int64                     `json:"int64_value"`
	DoubleValue       *float64                   `json:"double_value"`
	StringValue       *string                    `json:"string_value"`
	DistributionValue *distribution.Distribution `json:"distribution_value"`
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
			switch t := p.GetValue().GetValue().(type) {
			case *monitoringpb.TypedValue_BoolValue:
				v := p.GetValue().GetBoolValue()
				point.BoolValue = &v
			case *monitoringpb.TypedValue_Int64Value:
				v := p.GetValue().GetInt64Value()
				point.Int64Value = &v
			case *monitoringpb.TypedValue_DoubleValue:
				v := p.GetValue().GetDoubleValue()
				point.DoubleValue = &v
			case *monitoringpb.TypedValue_StringValue:
				v := p.GetValue().GetStringValue()
				point.StringValue = &v
			case *monitoringpb.TypedValue_DistributionValue:
				v := p.GetValue().GetDistributionValue()
				point.DistributionValue = v
			default:
				return fmt.Errorf("Not supported metric type: %s", t)
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
