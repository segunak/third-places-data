#!/usr/bin/env python3
"""
Simple HTTP server for the Third Places Photo Viewer
Serves static files and provides an API endpoint for listing JSON files.
"""

import http.server
import socketserver
import os
import json
import mimetypes
import shutil
import sys

# Default port, can be overridden by command line argument
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
# Go up one level to the 'data' directory, where 'places' folder is located
DATA_ROOT = os.path.abspath(os.path.join(DIRECTORY, '..'))


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        if self.path == '/api/list-places':
            places_dir = os.path.join(DATA_ROOT, 'places', 'charlotte')
            try:
                files = [f for f in os.listdir(places_dir) if f.endswith('.json') and os.path.isfile(os.path.join(places_dir, f))]
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(files).encode('utf-8'))
            except FileNotFoundError:
                self.send_error(404, f"Places directory not found at: {places_dir}")
            except Exception as e:
                self.send_error(500, f"Error listing places: {e}")
            return
        elif self.path.startswith('/places/charlotte/'):
            # Extract the filename from the path
            filename = self.path.replace('/places/charlotte/', '')
            # Construct the full path to the file
            actual_file_path = os.path.abspath(os.path.join(DATA_ROOT, 'places', 'charlotte', filename))
            # Security check: ensure the file is within the allowed directory
            allowed_dir = os.path.abspath(os.path.join(DATA_ROOT, 'places', 'charlotte'))

            if not actual_file_path.startswith(allowed_dir) or not os.path.isfile(actual_file_path):
                self.send_error(404, f"File not found: {filename}")
                return

            try:
                with open(actual_file_path, 'rb') as f:
                    self.send_response(200)
                    mime_type, _ = mimetypes.guess_type(actual_file_path)
                    self.send_header('Content-type', mime_type if mime_type else 'application/json')
                    fs = os.fstat(f.fileno())
                    self.send_header("Content-Length", str(fs.st_size))
                    self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
                    self.end_headers()
                    shutil.copyfileobj(f, self.wfile)
            except Exception as e:
                self.send_error(500, f"Error serving file: {e}")
            return

        # For other paths (index.html, css, js), use the default handler
        super().do_GET()


if __name__ == "__main__":
    # Ensure the current working directory is where serve.py is, for SimpleHTTPRequestHandler
    os.chdir(DIRECTORY)
    print(f"Serving HTTP on http://localhost:{PORT} from {DIRECTORY}")
    print(f"Serving place data from {os.path.join(DATA_ROOT, 'places', 'charlotte')}")

    # Check if the places directory exists
    places_dir = os.path.join(DATA_ROOT, 'places', 'charlotte')
    if os.path.exists(places_dir):
        print(f"✓ Places directory found with {len([f for f in os.listdir(places_dir) if f.endswith('.json')])} JSON files")
    else:
        print(f"✗ Places directory not found at: {places_dir}")

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print("Server is ready. Press Ctrl+C to stop.")
        httpd.serve_forever()
