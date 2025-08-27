#!/usr/bin/env python3
"""
Dual Server Launcher for CSV Data Analysis

Launches two separate servers in different threads:
1. MCP Server (server.py) - For query, list, and delete operations
2. HTTP Server (http.py) - For file operations (analyze, transform, load)

Both servers run completely isolated with different ports.
"""

import os
import sys
import threading
import logging
import time
import signal
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("dual-server-launcher")

def run_mcp_server():
    """Run the MCP server in a separate thread"""
    try:
        logger.info("Starting MCP Server thread...")
        
        # Import and run MCP server
        import server
        
        # Configure MCP server environment
        os.environ["TRANSPORT"] = os.getenv("MCP_TRANSPORT", "http")
        os.environ["HOST"] = os.getenv("MCP_HOST", "0.0.0.0")
        os.environ["PORT"] = str(os.getenv("MCP_PORT", "8000"))
        
        host = os.environ["HOST"]
        port = int(os.environ["PORT"])
        transport = os.environ["TRANSPORT"]
        
        logger.info(f"MCP Server starting on {transport}://{host}:{port}")
        
        if transport.lower() == "http":
            server.mcp.run(transport="http", host=host, port=port)
        else:
            server.mcp.run()
            
    except Exception as e:
        logger.error(f"MCP Server error: {e}")
        sys.exit(1)

def run_http_server():
    """Run the HTTP server in a separate thread"""
    try:
        logger.info("Starting HTTP Server thread...")
        
        # Import and configure HTTP server
        import http_server
        
        # Configure HTTP server environment  
        host = os.getenv("HTTP_HOST", "0.0.0.0")
        port = int(os.getenv("HTTP_PORT", "8001"))
        
        logger.info(f"HTTP Server starting on http://{host}:{port}")
        
        # Run the HTTP server
        http_server.app.run(host=host, port=port, debug=False, threaded=True)
        
    except Exception as e:
        logger.error(f"HTTP Server error: {e}")
        sys.exit(1)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down servers...")
    sys.exit(0)

def main():
    """Main launcher function"""
    logger.info("=== CSV Data Analysis Dual Server Launcher ===")
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Display configuration
    mcp_host = os.getenv("MCP_HOST", "0.0.0.0")
    mcp_port = os.getenv("MCP_PORT", "8000")
    mcp_transport = os.getenv("MCP_TRANSPORT", "http")
    
    http_host = os.getenv("HTTP_HOST", "0.0.0.0")
    http_port = os.getenv("HTTP_PORT", "8001")
    
    logger.info("Server Configuration:")
    logger.info(f"  MCP Server:  {mcp_transport}://{mcp_host}:{mcp_port}")
    logger.info(f"  HTTP Server: http://{http_host}:{http_port}")
    logger.info("")
    
    try:
        # Create and start threads
        mcp_thread = threading.Thread(target=run_mcp_server, name="MCP-Server", daemon=False)
        http_thread = threading.Thread(target=run_http_server, name="HTTP-Server", daemon=False)
        
        # Start both servers
        logger.info("Starting both servers...")
        mcp_thread.start()
        time.sleep(1)  # Small delay to avoid port conflicts
        http_thread.start()
        
        logger.info("Both servers started successfully!")
        logger.info("Press Ctrl+C to shutdown both servers")
        
        # Wait for threads to complete
        mcp_thread.join()
        http_thread.join()
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Launcher error: {e}")
    finally:
        logger.info("Launcher shutting down...")

if __name__ == "__main__":
    main()