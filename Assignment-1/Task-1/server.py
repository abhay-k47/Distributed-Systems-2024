import http.server
import socketserver
import os

PORT = 5000
server_id = os.environ.get('SERVER_ID', 'Unknown')

class MyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/home':
            self.handle_home()
        elif self.path == '/heartbeat':
            self.handle_heartbeat()
        else:
            super().do_GET()

    def handle_home(self):
        message = f"Hello from Server: {server_id}"
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = f'{{ "message": "{message}", "status": "successful" }}'
        self.wfile.write(response.encode())

    def handle_heartbeat(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

if __name__ == '__main__':
    with socketserver.TCPServer(("", PORT), MyHandler) as httpd:
        print(f"Serving on port {PORT} as Server: {server_id}")
        httpd.serve_forever()
