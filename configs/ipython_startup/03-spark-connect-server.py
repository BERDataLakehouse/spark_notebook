"""
Auto-start Spark Connect server for BERDL notebook environments.

This IPython startup script automatically starts a Spark Connect server when the notebook kernel launches,
enabling users to create remote Spark sessions. The server is reused across multiple notebook tabs.
"""

import logging

logger = logging.getLogger("berdl.startup")
logger.info("🚀 Starting Spark Connect server...")
try:
    server_info = start_spark_connect_server()  # noqa: F821
    logger.info(f"✅ Spark Connect server ready at {server_info['url']}")
except Exception as e:
    logger.error(f"❌ Failed to start Spark Connect server: {e}")
