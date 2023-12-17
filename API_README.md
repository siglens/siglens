# APIs

## Dashboard APIs

### Get all dashboards
    endpoint: api/dashboards/listall
    method: GET

    Example:
    request: http://localhost:80/api/dashboards/listall
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
    request: http://localhost:80/api/dashboards/9352623799225230043
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
    request: http://localhost:80/api/dashboards/create
    body: test-new
    response: 
        {
            "5902803467278423946": "db-1"
        }


### Update dashboard
    endpoint: api/dashboards/update
    method: POST

    Example:
    request: http://localhost:80/api/dashboards/update
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
    request: http://localhost:80/api/dashboards/delete/9352623799225230043
    response: "Dashboard deleted successfully"

## Alerting APIs
### Create a Contact Point
    endpoint: api/alerts/createContact
    method: POST

    Example:
    request: http://localhost:80/api/alerts/createContact
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
    request: http://localhost:80/api/alerts/create
    body:  
       {
            "alertInfo": {
                "alert_name": "alert 1"
            },
            "query": "cardinality(zip)",
            "condition": 0,
            "value": 1,
            "eval_for": 2,
            "eval_interval": 1,
            "message": "Alert triggered!",
            "contact_id": "c1"
        }
    response: 
        {
            "message": "Successfully created an alert"
        }
    
### Get All Alerts
    endpoint: api/allalerts
    method: GET

    Example:
    request: http://localhost:80/api/allalerts
    body:
    response: 
        {
            "alerts": [
                    {
                        "alert_id": "b1e5452411-651c5-480b-8850-6766765",
                        "alert_name": "alert 1",
                        "state": 1,
                        "create_timestamp": "2023-07-07T15:49:51.784393Z"
                    },
                    {
                        "alert_id": "a2877ee3348-f1d4-4e534f-b55f-34553",
                        "alert_name": "alert 2",
                        "state": 1,
                        "create_timestamp": "2023-07-07T15:53:20.012422Z"
                    },
                ]
        }
        
### Get An Alert By ID
    endpoint: api/alerts/{alertID}
    method: GET

    Example:
    request: http://localhost:80/api/alerts/{alertID}
    body:
    response: 
      {
        "alert": {
            "alertInfo": {
                "alert_id": "fc952d2a-f2f4-4c08-acf2-228c1fbc7583",
                "alert_name": "alert 1",
                "state": 0,
                "create_timestamp": "2023-07-11T17:54:57.835539Z"
            },
            "query": "cardinality(zip)",
            "condition": 0,
            "value": 5,
            "eval_for": 10,
            "eval_interval": 5,
            "message": "new message",
            "contact_id": "c1"
        }
    }

### Update An Alert By ID
    endpoint: api/alerts/update
    method: POST

    Example:
    request: http://localhost:80/api/alerts/update
    body:
        {
            "alertInfo": {
                "alert_name": "my alert",
                "alert_id": "fc952d2a-f2f4-4c08-acf2-228c1fbc7583"
                },
            "query": "cardinality(zip)",
            "condition": 1,
            "value": 0,
            "eval_for": 1,
            "eval_interval": 1,
            "message": "alerting!",
            "contact_id": "c1"
        }
    note: alert_id, alert_name, query, and contact_id fields are required in body.
    response: 
        {
            "message": "Alert updated successfully"
        }

### Delete An Alert By ID
    endpoint: api/alerts/delete
    method: DELETE

    Example:
    request: http://localhost:80/api/alerts/delete
    body:
        {
            "alert_id": "fc952d2a-f2f4-4c08-acf2-228c1fbc7583"
        }
    response: 
        {
            "message": "Alert deleted successfully"
        }

### Get All Contacts
    endpoint: api/alerts/allContacts
    method: GET

    Example:
    request: http://localhost:80/api/alerts/allContacts
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
    request: http://localhost:80/api/alerts/updateContact
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
    request: http://localhost:80/api/alerts/deleteContact
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
    request:http://localhost:80/api/search
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
    request:http://localhost:80/api/search
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
    request:http://localhost:80/api/search
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
    request: http://localhost:80/api/traces/search
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
    request: http://localhost:80/api/traces/dependencies
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
    request: http://localhost:80/api/traces/ganttchart
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


        




            


                

