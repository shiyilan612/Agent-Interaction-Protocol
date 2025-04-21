import time
from typing import Dict, Optional


class GatewaySession:
    def __init__(self):
        self.session_map: Dict[str, str] = {}  # session_id: original_sender
        self.reverse_map: Dict[str, str] = {}  # response_session_id: original_session_id

    def create_route_session(self, gw_id, original_session_id: str) -> str:
        """generate a new session ID for the forwarding request"""
        pass

    def resolve_response_session(self, route_session_id: str) -> Optional[str]:
        """parse the returned original session ID"""
        pass
