# APIs

## Dashboard APIs

### Get all dashboards
    endpoint: api/dashboards/listall
    method: GET

    Example:
    request: http://localhost:5122/api/dashboards/listall
    response: 
        {
            "1546954169045081977": "dashboard-y",
            "4729811385114799544": "dashboard-2",
            "5025894940373832739": "new-dashboard",
            "9089097379643921334": "test-dashboard",
            "9352623799225230043": "test-new"
        }

### Get dashboard by id
    endpoint: api/dashboards/{id}
    method: GET

    Example:
    request: http://localhost:5122/api/dashboards/9352623799225230043
    response:   
        {
            "description": "this is test dashboard",
            "name": "test",
            "panels": [
                {
                    "chartType": "line",
                    "gridpos": {
                        "h": 23,
                        "w": 35,
                        "x": 23,
                        "y": 44
                    },
                    "querySrr": "batch AND iOS...",
                    "queryType": "logs"
                }
            ]
        }


### Create dashboard
    endpoint: api/dashboards/create
    method: POST

    Example:
    request: http://localhost:5122/api/dashboards/create
    body: test-new
    response: 
        {
            "5902803467278423946": "db-1"
        }


### Update dashboard
    endpoint: api/dashboards/update
    method: POST

    Example:
    request: http://localhost:5122/api/dashboards/update
    body: 
        {
        "id": "9089097379643921334",
        "details": {
                "name": "test",
                "description": "this is test dashboard",
                "panels": [ {
                                    "queryType" : "logs",
                                    "querySrr" : "batch AND iOS...",
                                    "gridpos" : {
                                                "h": 23,
                                                "w": 35,
                                                "x": 23,
                                                "y": 44
                                                },
                                    "chartType": "line"
                            }
                ]                        
                }
    }

    response: "Dashboard updated successfully"

### Delete dashboard
    endpoint: api/dashboards/delete/{id}
    method: GET

    Example:
    request: http://localhost:5122/api/dashboards/delete/9352623799225230043
    response: "Dashboard deleted successfully"

## Alerting APIs
### Create a Contact Point
    endpoint: api/alerts/createContact
    method: POST

    Example:
    request: http://localhost:5122/api/alerts/createContact
    body:  
        {
            "contact_name":"team",
            "email": ["email1@example.com", "email2@example.com"],
            "slack": ["channelId1", "channelId2"],
            "pager_duty": "string"
        }
    response: 
        {
            "message": "Successfully created a contact point"
        }
        
### Create an Alert
    endpoint: api/alerts/create
    method: POST

    Example:
    request: http://localhost:5122/api/alerts/create
    body:  
       {
            "alert_name": "ExampleMetricAlert-3",
            "alert_type": 2,   // For logs=1, Metrics=2
            "labels": [
                {
                "label_name": "severity",
                "label_value": "high"
                },
                {
                "label_name": "environment",
                "label_value": "production"
                }
            ],
            "condition": 0,
            "value": 50,
            "eval_for": 15,
            "eval_interval": 10,
            "message": "Test Example Metric Alert",
            "contact_id": "9f7cadc7-650c-469f-9629-75c17dda3f17",
            "queryParams": {   // This is required when the Alert is Logs.
                "data_source": "Logs",
                "queryLanguage": "Splunk QL",
                "queryText": "http_status=404 | stats count(*)",
                "startTime": "now-3h",
                "endTime": "now"
            },
            "metricsQueryParams": "{\"start\": \"now-24h\", \"end\": \"now\", \"queries\": [{\"name\": \"a\", \"query\": \"avg by (car_type) (testmetric0{car_type=\\\"Passenger car heavy\\\"})\", \"qlType\": \"promql\"}, {\"name\": \"b\", \"query\": \"avg by (car_type) (testmetric1{car_type=\\\"Passenger car heavy\\\"})\", \"qlType\": \"promql\"}], \"formulas\": [{\"formula\": \"a+b\"}]}" // This is only required if the Alert is for Metrics
        }

    response: 
        {
            "message": "Successfully created an alert"
        }
    
### Get All Alerts
    endpoint: api/allalerts
    method: GET

    Example:
    request: http://localhost:5122/api/allalerts
    body:
    response: 
        {
            "alerts": [
                {
                    "alert_id": "e5dabddc-56cb-40a0-a375-eb6147a9fad7",
                    "alert_type": 1,
                    "alert_name": "RuleFireFor404",
                    "state": 0,
                    "create_timestamp": "0001-01-01T00:00:00Z",
                    "contact_id": "9f7cadc7-650c-469f-9629-75c17dda3f17",
                    "contact_name": "SigScalrSlack",
                    "labels": [
                        {
                            "label_name": "TestSlackBok",
                            "label_value": "Yes"
                        },
                        {
                            "label_name": "alerting",
                            "label_value": "true"
                        }
                    ],
                    "silence_minutes": 0,
                    "queryParams": {
                        "data_source": "Logs",
                        "queryLanguage": "Splunk QL",
                        "queryText": "http_status=404 | stats count(*)",
                        "startTime": "now-3h",
                        "endTime": "now"
                    },
                    "metricsQueryParams": "",
                    "condition": 0,
                    "value": 10,
                    "eval_for": 1,
                    "eval_interval": 1,
                    "message": "404. Alert",
                    "cron_job": {},
                    "node_id": 0,
                    "notification_id": "",
                    "org_id": 0
                },
                {
                    "alert_id": "9e43ee7a-1fed-4c3b-aa3f-93ab9ce5abb1",
                    "alert_type": 1,
                    "alert_name": "ExampleMetricAlert-2",
                    "state": 2,
                    "create_timestamp": "2024-06-20T12:45:09.14442+05:30",
                    "contact_id": "9f7cadc7-650c-469f-9629-75c17dda3f17",
                    "contact_name": "SigScalrSlack",
                    "labels": [
                        {
                            "label_name": "environment",
                            "label_value": "production"
                        },
                        {
                            "label_name": "severity",
                            "label_value": "high"
                        }
                    ],
                    "silence_minutes": 0,
                    "queryParams": {
                        "data_source": "",
                        "queryLanguage": "",
                        "queryText": "",
                        "startTime": "",
                        "endTime": ""
                    },
                    "metricsQueryParams": "{\"start\": \"now-24h\", \"end\": \"now\", \"queries\": [{\"name\": \"a\", \"query\": \"avg by (car_type) (testmetric0{car_type=\\\"Passenger car heavy\\\"})\", \"qlType\": \"promql\"}, {\"name\": \"b\", \"query\": \"avg by (car_type) (testmetric1{car_type=\\\"Passenger car heavy\\\"})\", \"qlType\": \"promql\"}], \"formulas\": [{\"formula\": \"a+b\"}]}",
                    "condition": 0,
                    "value": 50,
                    "eval_for": 5,
                    "eval_interval": 1,
                    "message": "Test Example Metric Alert",
                    "cron_job": {},
                    "node_id": 0,
                    "notification_id": "",
                    "org_id": 0
                }
            ]
        }
        
### Get An Alert By ID
    endpoint: api/alerts/{alertID}
    method: GET

    Example:
    request: http://localhost:5122/api/alerts/{alertID}
    body:
    response: 
        {
            "alert_id": "e5dabddc-56cb-40a0-a375-eb6147a9fad7",
            "alert_type": 1,
            "alert_name": "RuleFireFor404",
            "state": 0,
            "create_timestamp": "0001-01-01T00:00:00Z",
            "contact_id": "9f7cadc7-650c-469f-9629-75c17dda3f17",
            "contact_name": "SigScalrSlack",
            "labels": [
                {
                    "label_name": "TestSlackBok",
                    "label_value": "Yes"
                },
                {
                    "label_name": "alerting",
                    "label_value": "true"
                }
            ],
            "silence_minutes": 0,
            "queryParams": {
                "data_source": "Logs",
                "queryLanguage": "Splunk QL",
                "queryText": "http_status=404 | stats count(*)",
                "startTime": "now-3h",
                "endTime": "now"
            },
            "metricsQueryParams": "",
            "condition": 0,
            "value": 10,
            "eval_for": 1,
            "eval_interval": 1,
            "message": "404. Alert",
            "cron_job": {},
            "node_id": 0,
            "notification_id": "",
            "org_id": 0
        },

### Update An Alert By ID
    endpoint: api/alerts/update
    method: POST

    Example:
    request: http://localhost:5122/api/alerts/update
    body:
        {
            "alert_id": "e5dabddc-56cb-40a0-a375-eb6147a9fad7",
            "alert_type": 1,
            "alert_name": "RuleFireFor404",
            "state": 0,
            "create_timestamp": "0001-01-01T00:00:00Z",
            "contact_id": "9f7cadc7-650c-469f-9629-75c17dda3f17",
            "contact_name": "SigScalrSlack",
            "labels": [
                {
                    "label_name": "TestSlackBok",
                    "label_value": "Yes"
                },
                {
                    "label_name": "alerting",
                    "label_value": "true"
                }
            ],
            "silence_minutes": 0,
            "queryParams": {
                "data_source": "Logs",
                "queryLanguage": "Splunk QL",
                "queryText": "http_status=404 | stats count(*)",
                "startTime": "now-3h",
                "endTime": "now"
            },
            "metricsQueryParams": "",
            "condition": 0,
            "value": 10,
            "eval_for": 1,
            "eval_interval": 1,
            "message": "404. Alert",
            "cron_job": {},
            "node_id": 0,
            "notification_id": "",
            "org_id": 0
        }
    note: Send the Entire Alert Data in every update request, with the required fields updated with new Values.
          - Fields like alert_id, alert_type cannot be updated.
          - Send either the `queryParams` or `metricQueryParams` field based on the Alert type.
    response: 
        {
            "message": "Alert updated successfully"
        }

### Delete An Alert By ID
    endpoint: api/alerts/delete
    method: DELETE

    Example:
    request: http://localhost:5122/api/alerts/delete
    body:
        {
            "alert_id": "fc952d2a-f2f4-4c08-acf2-228c1fbc7583"
        }
    response: 
        {
            "message": "Alert deleted successfully"
        }

### Get Alert History For An Alert by AlertID
    endpoint: /api/alerts/{alert_id}/history
    method: GET

    Query Params:
        - sort_order: "DESC" / "ASC" (optional)
        - limit: uint (optional)  // limit the number of results
        - offset: uint (optional) // skip these number of results

    Example:
    request: http://localhost:5122/api/alerts/3a4a7c72-5195-4ab0-bf2f-0b8cac7a5bee/history?sort_order=DESC&limit=10&offset=5
    response: 
        {
            "alertHistory": [
                {
                    "ID": 256,
                    "alert_id": "3a4a7c72-5195-4ab0-bf2f-0b8cac7a5bee",
                    "alert_type": 2,
                    "alert_state": 2,
                    "event_description": "Alert Pending",
                    "user_name": "System Generated",
                    "event_triggered_at": "2024-06-21T23:28:04.933045Z"
                }
            ],
            "count": 1
        }

### Get All Contacts
    endpoint: api/alerts/allContacts
    method: GET

    Example:
    request: http://localhost:5122/api/alerts/allContacts
    body:
    response: 
        {
            "contacts": [
                {
                    "contact_name":"team",
                    "email": ["email1@example.com", "email2@example.com"],
                    "slack": ["channelId1", "channelId2"],
                    "pager_duty": "string"
                }.
                {
                    "contact_name": "team head",
                    "contact_id": "30c90735-6da5-472f-8fec-a8f1798c0db4",
                    "email": ["backend@gmail.com"],
                    "slack": ["channel_id"],
                    "pager_duty": "string"
                },
            ]
        }

### Update Contact By ID
    endpoint: api/alerts/updateContact
    method: POST

    Example:
    request: http://localhost:5122/api/alerts/updateContact
    body:
        {
            "contact_id": "30c90735-6da5-472f-8fec-a8f1798c0db4",
            "contact_name":"team",
            "email": ["newemail@example.com", "email2@example.com"],
            "slack": ["newchannelId", "channelId2"],
            "pager_duty": "string"  
        }
    response: 
        {
            "message": "Contact details updated successfully"
        }

### Delete Contact By ID
    endpoint: api/alerts/deleteContact
    method: DELETE

    Example:
    request: http://localhost:5122/api/alerts/deleteContact
    body:
        {
            "contact_id": "d1187d7f-a079-4280-b54f-ed1f55fa0a28"
        }
    response: 
        {
            "message": "Contact point deleted successfully"
        }
# Traces API
## 1. Retrieve Ingested Data
    endpoint: api/search
    method: POST

    Example:
    request:http://localhost:5122/api/search
    body:
        {
            "startEpoch": "now-1h",
            "endEpoch": "now",
            "searchText": "*",
            "indexName": "traces",
            "queryLanguage": "Splunk QL"
        }
    response:  
        {
            "hits": {
                "totalMatched": {
                    "value": 23321,
                    "relation": "gte"
                },
                "records": [
                    {
                        "_index": "traces",
                        "app.ads.ad_request_type": null,
                        "app.ads.ad_response_type": null,
                        "app.ads.category": null,
                        "app.ads.contextKeys": null,
                        "app.ads.contextKeys.count": null,
                        "app.ads.count": null,
                        "app.cart.items.count": null,
                        "app.currency.conversion.from": null,
                        "app.currency.conversion.to": null,
                        "app.email.recipient": null,
                        "app.featureflag.enabled": null,
                        "app.featureflag.name": null,
                        "app.filtered_products.count": null,
                        "app.filtered_products.list.0": null,
                        "app.filtered_products.list.1": null,
                        "app.filtered_products.list.2": null,
                        "app.filtered_products.list.3": null,
                        "app.filtered_products.list.4": null,
                        "app.order.amount": null,
                        "app.order.id": null,
                        "app.order.items.count": null,
                        "app.payment.amount": null,
                        "app.payment.card_type": null,
                        "app.payment.card_valid": null,
                        "app.payment.charged": null,
                        "app.product.id": null,
                        "app.product.name": null,
                        "app.product.quantity": null,
                        "app.products.count": null,
                        "app.products_recommended.count": null,
                        "app.quote.cost.total": null,
                        "app.quote.items.count": null,
                        "app.recommendation.cache_enabled": null,
                        "app.shipping.amount": null,
                        "app.shipping.cost.total": null,
                        "app.shipping.items.count": null,
                        "app.shipping.tracking.id": null,
                        "app.shipping.zip_code": null,
                        "app.synthetic_request": null,
                        "app.user.currency": null,
                        "app.user.id": null,
                        "busy_ns": null,
                        "canceled": null,
                        "code.filepath": null,
                        "code.function": null,
                        "code.lineno": null,
                        "code.namespace": null,
                        "component": "proxy",
                        "db.instance": null,
                        "db.name": null,
                        "db.redis.database_index": null,
                        "db.redis.flags": null,
                        "db.statement": null,
                        "db.system": null,
                        "db.type": null,
                        "db.url": null,
                        "decode_time_microseconds": null,
                        "downstream_cluster": null,
                        "dropped_attributes_count": 0,
                        "dropped_events_count": 0,
                        "dropped_links_count": 0,
                        "duration": 60669000,
                        "end_time": 1701741403012683000,
                        "error": null,
                        "events": "null",
                        "guid:x-request-id": null,
                        "http.client_ip": null,
                        "http.flavor": null,
                        "http.host": null,
                        "http.method": null,
                        "http.protocol": "HTTP/1.1",
                        "http.request_content_length": null,
                        "http.request_content_length_uncompressed": null,
                        "http.response_content_length": null,
                        "http.route": null,
                        "http.scheme": null,
                        "http.status_code": 200,
                        "http.status_text": null,
                        "http.target": null,
                        "http.url": null,
                        "http.user_agent": null,
                        "idle_ns": null,
                        "idle_time_microseconds": null,
                        "kind": "SPAN_KIND_CLIENT",
                        "links": "[]",
                        "messaging.client_id": null,
                        "messaging.destination.kind": null,
                        "messaging.destination.name": null,
                        "messaging.kafka.consumer.group": null,
                        "messaging.kafka.destination.partition": null,
                        "messaging.kafka.message.offset": null,
                        "messaging.message.payload_size_bytes": null,
                        "messaging.operation": null,
                        "messaging.system": null,
                        "name": "router frontend egress",
                        "net.host.ip": null,
                        "net.host.name": null,
                        "net.host.port": null,
                        "net.peer.ip": null,
                        "net.peer.name": null,
                        "net.peer.port": null,
                        "net.sock.host.addr": null,
                        "net.sock.peer.addr": null,
                        "net.sock.peer.port": null,
                        "net.transport": null,
                        "node_id": null,
                        "parent_span_id": "efd4f4939e0c2d69",
                        "peer.address": "172.18.0.24:8080",
                        "peer.service": null,
                        "phoenix.action": null,
                        "phoenix.plug": null,
                        "query_time_microseconds": null,
                        "queue_time_microseconds": null,
                        "request_size": null,
                        "response_flags": "-",
                        "response_size": null,
                        "rpc.grpc.status_code": null,
                        "rpc.method": null,
                        "rpc.service": null,
                        "rpc.system": null,
                        "rpc.user_agent": null,
                        "service": "frontend-proxy",
                        "sinatra.template_name": null,
                        "source": null,
                        "span_id": "36bd1a491a6a764e",
                        "start_time": 1701741402952014000,
                        "status": "STATUS_CODE_UNSET",
                        "thread.id": null,
                        "thread.name": null,
                        "timestamp": 1701741405424,
                        "total_time_microseconds": null,
                        "trace_id": "2a874c870c3b2ff3bccd135c6375ae3f",
                        "trace_state": "",
                        "upstream_address": "172.18.0.24:8080",
                        "upstream_cluster": "frontend",
                        "upstream_cluster.name": "frontend",
                        "user_agent": null,
                        "zone": null
                    },
                    // Additional records...
                ]
            },
            "aggregations": {},
            "elapedTimeMS": 410,
            "allColumns": [
                "_index",
                // Additional columns...
            ],
            "qtype": "logs-query",
            "can_scroll_more": true,
            "total_rrc_count": 100,
            "dashboardPanelId": ""
        }

## 2. Gantt Chart Data
For Gantt chart data specific to a trace ID, modify the request body accordingly. The response body will be similar to the one provided above, but it will be filtered to include data specific to the desired trace ID.

    endpoint: api/search
    method: POST

    Example:
    request:http://localhost:5122/api/search
    body:
        {
            "startEpoch": "now-1h",
            "endEpoch": "now",
            "searchText": "trace_id=9f3239f9e01f9f648d188de4767c9a36",
            "indexName": "traces",
            "queryLanguage": "Splunk QL"
        }

## 3. Red-metrics Data
    endpoint: api/search
    method: POST

    Example:
    request:http://localhost:5122/api/search
    body:
        {
            "startEpoch": "now-1h",
            "endEpoch": "now",
            "searchText": "*",
            "indexName": "red-traces",
            "queryLanguage": "Splunk QL"
        }
    respone:       
        {
            "hits": {
                "totalMatched": {
                    "value": 35,
                    "relation": "eq"
                },
                "records": [
                    {
                        "_index": "red-traces",
                        "error_rate": 0,
                        "p50": 49792000,
                        "p90": 49792000,
                        "p95": 49792000,
                        "p99": 49792000,
                        "rate": 0.03333333333333333,
                        "service": "frontend-proxy",
                        "timestamp": 1701744228514
                    },
                    {
                        "_index": "red-traces",
                        "error_rate": 0,
                        "p50": 2043042,
                        "p90": 2043042,
                        "p95": 2043042,
                        "p99": 2043042,
                        "rate": 0.016666666666666666,
                        "service": "adservice",
                        "timestamp": 1701744228514
                    },
                    // ... more records ...
                ]
            },
            "aggregations": {},
            "elapedTimeMS": 6,
            "allColumns": [
                "_index",
                "error_rate",
                "p50",
                "p90",
                "p95",
                "p99",
                "rate",
                "service",
                "timestamp"
            ],
            "qtype": "logs-query",
            "can_scroll_more": false,
            "total_rrc_count": 35,
            "dashboardPanelId": ""
        }

## 3. Searching all Traces Data
    endpoint: api/traces/search
    method: POST

    Example:
    request: http://localhost:5122/api/traces/search
    body:
        {
            "startEpoch": "now-1h",
            "endEpoch": "now",
            "searchText": "*",
            "indexName": "traces",
            "queryLanguage": "Splunk QL"
        }
    response:
        {
            "traces":[
                {
                    "trace_id": "497021dac34126ec34bb671b235013f8",
                    "start_time": 1701740537332844544,
                    "end_time": 1701740537365339648,
                    "span_count": 4,
                    "span_errors_count": 0,
                    "service_name": "frontend",
                    "operation_name" : "GET"
                },
                {
                    "trace_id": "ce926f2dc509e61591d62734945ab781",
                    "start_time": 1701739970657999872,
                    "end_time": 1701739976741989376,
                    "span_count": 37,
                    "span_errors_count": 0,
                    "service_name": "productcatalogservice",
                    "operation_name" : "oteldemo.ProductCatalogService/GetProduct"
                },
                {
                    // ... more records ...
                }
            ]   
        }

## 4. Dependency Graph Data
    endpoint: api/traces/dependencies
    method: POST

    Example:
    request: http://localhost:5122/api/traces/dependencies
    body:
        {
            "startEpoch": "now-1h",
            "endEpoch": "now",
            "searchText": "*",
            "indexName": "service-dependency",
            "queryLanguage": "Splunk QL"
        }
    response:
        {
            "_index": "service-dependency",
            "frontend": {
                "adservice": 1,
                "cartservice": 1,
                "productcatalogservice": 4
            },
            "frontend-proxy": {
                "frontend": 7
            },
            "loadgenerator": {
                "frontend-proxy": 5
            },
            "timestamp": 1701745060056
        }

## 5. Gantt Chart Data
    endpoint: api/traces/ganttchart
    method: POST

    Example:
    request: http://localhost:5122/api/traces/ganttchart
    body:
        {
            "startEpoch": "now-1h",
            "endEpoch": "now",
            "searchText": "trace_id=95db2d5796f8986dbeccec3d1582ee85"
        }
    response:
        {
            "span_id": "a98166653c6f7f44",
            "actual_start_time": 1702310799070961452,
            "start_time": 0,
            "end_time": 2088875,
            "duration": 2088875,
            "service_name": "featureflagservice",
            "operation_name": "/",
            "is_anomalous": false,
            "tags": {
                "http.client_ip": "127.0.0.1",
                "http.flavor": "1.1",
                "http.method": "GET",
                "http.route": "/",
                "http.scheme": "http",
                "http.status_code": 200,
                "http.target": "/",
                "http.user_agent": "curl/7.64.0",
                "net.host.name": "localhost",
                "net.host.port": 8081,
                "net.peer.port": 60928,
                "net.sock.host.addr": "127.0.0.1",
                "net.sock.peer.addr": "127.0.0.1",
                "net.transport": "IP.TCP",
                "phoenix.action": "index",
                "phoenix.plug": "Elixir.FeatureflagserviceWeb.PageController"
            },
            "children": [
                // ... more records ...
            ]
        }

## Metric APIs
### Total number of unique series
    Endpoint: /metrics-explorer/api/v1/series-cardinality
    Method: POST
    Inputs:
        - startEpoch: unix seconds or e.g. "now-1h"
        - endEpoch: unix seconds or e.g. "now"
    Outputs:
        - seriesCardinality: int

### Tag-keys with highest number of series
    Endpoint: /metrics-explorer/api/v1/tag-keys-with-most-series
    Method: POST
    Inputs:
        - startEpoch: unix seconds or e.g. "now-1h"
        - endEpoch: unix seconds or e.g. "now"
        - limit: non-negative int (default 10; 0 means no limit)
    Outputs:
        - tagKeys: []{key: string, numSeries: int}

### Tag key-value pairs with highest number of series
    Endpoint: /metrics-explorer/api/v1/tag-pairs-with-most-series
    Method: POST
    Inputs:
        - startEpoch: unix seconds or e.g. "now-1h"
        - endEpoch: unix seconds or e.g. "now"
        - limit: non-negative int (default 10; 0 means no limit)
    Outputs:
        - tagPairs: []{key: string, value: string, numSeries: int}
    
### Tag keys with highest number of unique values
    Endpoint: /metrics-explorer/api/v1/tag-keys-with-most-values
    Method: POST
    Inputs:
        - startEpoch: unix seconds or e.g. "now-1h"
        - endEpoch: unix seconds or e.g. "now"
        - limit: non-negative int (default 10; 0 means no limit)
    Outputs:
        - tagKeys: []{key: string, numValues: int}
