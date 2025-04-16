from http.server import BaseHTTPRequestHandler, HTTPServer

# Static metrics with labels and different timestamps
METRICS = """
# TYPE temperature_gauge gauge
temperature_gauge{city="New York",sensor="A"} 22.5 1700000000000
temperature_gauge{city="New York",sensor="A"} 23.0 1700000010000
temperature_gauge{city="New York",sensor="A"} 33.0 1700000020000
temperature_gauge{city="New York",sensor="A"} 43.0 1700000030000
temperature_gauge{city="New York",sensor="A"} 25.0 1700000040000
temperature_gauge{city="New York",sensor="A"} 10.0 1700000050000
temperature_gauge{city="Portland",sensor="B"} 18.7 1700000020000
"""

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.end_headers()
            self.wfile.write(METRICS.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def run(server_class=HTTPServer, handler_class=MetricsHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Serving metrics on http://localhost:{port}/metrics")
    httpd.serve_forever()

if __name__ == '__main__':
    run()
