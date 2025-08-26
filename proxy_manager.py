"""
Proxy Configuration Manager for BlakeGUI
Handles selective proxy usage - proxies only for ZabaSearch, direct connection for AI APIs
"""
import os
import random
import logging
from typing import Dict, List, Optional, Union
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class ProxyManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.proxies = []
        self.current_proxy_index = 0
        self.load_proxy_config()

    def load_proxy_config(self):
        """Load proxy configuration from environment variables"""
        try:
            # Load from environment variables (format: host:port:username:password)
            proxy_list = os.getenv('BLAKE_PROXIES', '').strip()

            if proxy_list:
                for proxy_str in proxy_list.split(','):
                    proxy_str = proxy_str.strip()
                    if ':' in proxy_str:
                        parts = proxy_str.split(':')
                        if len(parts) >= 2:
                            proxy_config = {
                                'server': f"http://{parts[0]}:{parts[1]}"
                            }

                            # Add authentication if provided
                            if len(parts) >= 4:
                                proxy_config['username'] = parts[2]
                                proxy_config['password'] = parts[3]

                            self.proxies.append(proxy_config)

                self.logger.info(f"✅ Loaded {len(self.proxies)} proxies from environment")
            else:
                self.logger.info("ℹ️ No proxies configured - ZabaSearch will use direct connection")

        except Exception as e:
            self.logger.error(f"❌ Error loading proxy config: {e}")

    def get_random_proxy(self) -> Optional[Dict]:
        """Get a random proxy for ZabaSearch operations"""
        if not self.proxies:
            self.logger.info("🔄 No proxies available - using direct connection")
            return None

        proxy = random.choice(self.proxies)
        self.logger.info(f"🔒 Selected proxy: {proxy['server']}")
        return proxy

    def get_next_proxy(self) -> Optional[Dict]:
        """Get next proxy in rotation for ZabaSearch operations"""
        if not self.proxies:
            return None

        proxy = self.proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)

        self.logger.info(f"🔄 Rotating to proxy: {proxy['server']}")
        return proxy

    def get_proxy_count(self) -> int:
        """Get number of configured proxies"""
        return len(self.proxies)

    def is_proxy_enabled(self) -> bool:
        """Check if any proxies are configured"""
        return len(self.proxies) > 0

# Global proxy manager instance
proxy_manager = ProxyManager()

def get_proxy_for_zabasearch() -> Optional[Dict]:
    """Get proxy specifically for ZabaSearch operations with unique session"""
    proxy = proxy_manager.get_random_proxy()
    if proxy:
        # Create a unique session ID for each batch to prevent conflicts
        import random
        import time
        session_id = f"batch_{int(time.time())}_{random.randint(1000, 9999)}"

        # Add session to password for unique proxy sessions
        if 'password' in proxy:
            original_password = proxy['password']
            # If password already has session, replace it
            if '_session-' in original_password:
                base_password = original_password.split('_session-')[0]
            else:
                base_password = original_password
            proxy['password'] = f"{base_password}_session-{session_id}"

        proxy_manager.logger.info(f"🔒 Created unique proxy session: {session_id}")
    return proxy

def get_proxy_count() -> int:
    """Get number of configured proxies"""
    return proxy_manager.get_proxy_count()

def is_proxy_enabled() -> bool:
    """Check if proxies are configured"""
    return proxy_manager.is_proxy_enabled()
