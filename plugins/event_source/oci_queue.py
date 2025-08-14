"""
Oracle OCI Queue event source plugin for Ansible Event Driven Automation.

This plugin allows receiving messages from Oracle Cloud Infrastructure (OCI) Queue service.
"""

import asyncio
import logging
import time
from typing import Any, Dict

try:
    import oci
    from oci.queue import QueueClient
    from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
    HAS_OCI = True
except ImportError:
    HAS_OCI = False

# Plugin metadata
DOCUMENTATION = r"""
---
name: oci_queue
version_added: "1.0.0"
short_description: Receive events from Oracle OCI Queue
description:
  - Receive messages from Oracle Cloud Infrastructure Queue service
  - Supports both user principal and instance principal authentication
  - Automatically handles message acknowledgment after successful processing
  - Configurable polling intervals and batch sizes
requirements:
  - oci>=2.0.0
author:
  - Ansible Event Driven Team
options:
  queue_id:
    description:
      - The OCID of the OCI Queue to receive messages from
    required: true
    type: str
  compartment_id:
    description:
      - The OCID of the compartment containing the queue
    required: true
    type: str
  region:
    description:
      - The OCI region where the queue is located
    required: true
    type: str
  config_profile:
    description:
      - The profile name to use from the OCI config file
      - If not specified, uses the DEFAULT profile
    required: false
    type: str
    default: DEFAULT
  config_file:
    description:
      - Path to the OCI config file
      - If not specified, uses the default location (~/.oci/config)
    required: false
    type: str
  use_instance_principal:
    description:
      - Use instance principal authentication instead of user principal
      - Useful when running on OCI compute instances
    required: false
    type: bool
    default: false
  timeout_seconds:
    description:
      - Timeout in seconds for long polling when receiving messages
      - Valid range is 0-20 seconds
    required: false
    type: int
    default: 20
  visibility_timeout_seconds:
    description:
      - Visibility timeout for received messages in seconds
      - Messages will be hidden from other consumers for this duration
    required: false
    type: int
    default: 30
  limit:
    description:
      - Maximum number of messages to receive in a single request
      - Valid range is 1-20
    required: false
    type: int
    default: 1
  polling_interval:
    description:
      - Time to wait between polling attempts when no messages are available
    required: false
    type: float
    default: 5.0
  auto_acknowledge:
    description:
      - Automatically acknowledge messages after successful processing
      - If false, messages must be manually acknowledged
    required: false
    type: bool
    default: true
"""

EXAMPLES = r"""
# Basic usage with user principal authentication
- name: Listen for OCI Queue messages
  hosts: localhost
  sources:
    - name: oci_queue_source
      oci_queue:
        queue_id: "ocid1.queue.oc1.us-phoenix-1.example"
        compartment_id: "ocid1.compartment.oc1..example"
        region: "us-phoenix-1"
        config_profile: "DEFAULT"
        timeout_seconds: 20
        limit: 5

# Using instance principal authentication
- name: Listen for OCI Queue messages with instance principal
  hosts: localhost  
  sources:
    - name: oci_queue_source
      oci_queue:
        queue_id: "ocid1.queue.oc1.us-phoenix-1.example"
        compartment_id: "ocid1.compartment.oc1..example"
        region: "us-phoenix-1"
        use_instance_principal: true
        polling_interval: 10.0
        auto_acknowledge: false

# Custom configuration
- name: Listen for OCI Queue messages with custom settings
  hosts: localhost
  sources:
    - name: oci_queue_source
      oci_queue:
        queue_id: "ocid1.queue.oc1.us-phoenix-1.example"
        compartment_id: "ocid1.compartment.oc1..example"
        region: "us-phoenix-1"
        config_file: "/path/to/custom/oci/config"
        config_profile: "MYPROFILE"
        timeout_seconds: 15
        visibility_timeout_seconds: 60
        limit: 10
        polling_interval: 2.0
"""

logger = logging.getLogger(__name__)


class OCIQueueEventSource:
    """Oracle OCI Queue event source for Ansible EDA."""

    def __init__(self, queue, args: Dict[str, Any]):
        """Initialize the OCI Queue event source."""
        self.queue = queue
        self.args = args
        self._client = None
        self._should_stop = False
        
        # Validate required parameters
        self._validate_args()
        
        # Extract configuration
        self.queue_id = args["queue_id"]
        self.compartment_id = args["compartment_id"]
        self.region = args["region"]
        self.config_profile = args.get("config_profile", "DEFAULT")
        self.config_file = args.get("config_file")
        self.use_instance_principal = args.get("use_instance_principal", False)
        self.timeout_seconds = args.get("timeout_seconds", 20)
        self.visibility_timeout_seconds = args.get("visibility_timeout_seconds", 30)
        self.limit = args.get("limit", 1)
        self.polling_interval = args.get("polling_interval", 5.0)
        self.auto_acknowledge = args.get("auto_acknowledge", True)
        
        # Validate parameter ranges
        if not (0 <= self.timeout_seconds <= 20):
            raise ValueError("timeout_seconds must be between 0 and 20")
        if not (1 <= self.limit <= 20):
            raise ValueError("limit must be between 1 and 20")

    def _validate_args(self) -> None:
        """Validate required arguments."""
        if not HAS_OCI:
            raise ImportError("oci library is required for OCI Queue event source")
        
        required_args = ["queue_id", "compartment_id", "region"]
        for arg in required_args:
            if arg not in self.args:
                raise ValueError(f"Required argument '{arg}' is missing")

    def _create_client(self) -> QueueClient:
        """Create and configure the OCI Queue client."""
        try:
            # Construct the Queue service endpoint
            queue_endpoint = f"https://cell-1.queue.messaging.{self.region}.oci.oraclecloud.com"
            
            if self.use_instance_principal:
                # Use instance principal authentication
                signer = InstancePrincipalsSecurityTokenSigner()
                config = {"region": self.region}
                client = QueueClient(config, signer=signer, service_endpoint=queue_endpoint)
                logger.info("Created OCI Queue client with instance principal authentication")
            else:
                # Use user principal authentication
                if self.config_file:
                    config = oci.config.from_file(
                        file_location=self.config_file,
                        profile_name=self.config_profile
                    )
                else:
                    config = oci.config.from_file(profile_name=self.config_profile)
                
                # Override region if specified
                config["region"] = self.region
                oci.config.validate_config(config)
                
                client = QueueClient(config, service_endpoint=queue_endpoint)
                logger.info(f"Created OCI Queue client with user principal authentication (profile: {self.config_profile})")
            
            logger.debug(f"Queue service endpoint: {queue_endpoint}")
            return client
            
        except Exception as e:
            logger.error(f"Failed to create OCI Queue client: {e}")
            raise

    async def _get_messages(self) -> list:
        """Get messages from the OCI Queue."""
        try:
            response = self._client.get_messages(
                queue_id=self.queue_id,
                timeout_in_seconds=self.timeout_seconds,
                limit=self.limit,
                visibility_in_seconds=self.visibility_timeout_seconds
            )
            
            messages = response.data.messages if response.data else []
            if messages:
                logger.debug(f"Retrieved {len(messages)} messages from queue")
            return messages
            
        except Exception as e:
            logger.error(f"Failed to get messages from OCI Queue: {e}")
            return []

    async def _acknowledge_message(self, receipt: str) -> bool:
        """Acknowledge a message to remove it from the queue."""
        try:
            self._client.delete_message(
                queue_id=self.queue_id,
                message_receipt=receipt
            )
            logger.debug(f"Acknowledged message with receipt: {receipt[:20]}...")
            return True
            
        except Exception as e:
            logger.error(f"Failed to acknowledge message: {e}")
            return False

    def _format_event(self, message) -> Dict[str, Any]:
        """Format an OCI Queue message as an event."""
        event = {
            "oci_queue": {
                "message_id": message.id,
                "receipt": message.receipt,
                "content": message.content,
                "delivery_count": message.delivery_count,
                "visible_after": message.visible_after.isoformat() if message.visible_after else None,
                "expire_after": message.expire_after.isoformat() if message.expire_after else None,
                "created_at": message.created_at.isoformat() if message.created_at else None,
            },
            "meta": {
                "source": {
                    "name": "oci_queue",
                    "type": "oci_queue",
                    "queue_id": self.queue_id,
                    "region": self.region,
                },
                "received_at": time.time(),
            }
        }
        
        # Add custom metadata if present
        if hasattr(message, 'metadata') and message.metadata:
            event["oci_queue"]["metadata"] = message.metadata
            
        return event

    async def _handle_message(self, message) -> None:
        """Handle a single message from the queue."""
        try:
            # Format and send the event
            event = self._format_event(message)
            await self.queue.put(event)
            
            # Auto-acknowledge if enabled
            if self.auto_acknowledge:
                await self._acknowledge_message(message.receipt)
                
        except Exception as e:
            logger.error(f"Failed to handle message {message.id}: {e}")

    async def listen(self) -> None:
        """Main event listening loop."""
        logger.info(f"Starting OCI Queue event source for queue: {self.queue_id}")
        
        # Initialize the OCI client
        self._client = self._create_client()
        
        try:
            while not self._should_stop:
                try:
                    # Get messages from the queue
                    messages = await self._get_messages()
                    
                    if messages:
                        # Process each message
                        for message in messages:
                            if self._should_stop:
                                break
                            await self._handle_message(message)
                    else:
                        # No messages available, wait before polling again
                        if self.polling_interval > 0:
                            await asyncio.sleep(self.polling_interval)
                            
                except Exception as e:
                    logger.error(f"Error in message processing loop: {e}")
                    if self.polling_interval > 0:
                        await asyncio.sleep(self.polling_interval)
                    
        except asyncio.CancelledError:
            logger.info("OCI Queue event source cancelled")
            self._should_stop = True
        except Exception as e:
            logger.error(f"Fatal error in OCI Queue event source: {e}")
            raise
        finally:
            logger.info("OCI Queue event source stopped")

    def stop(self) -> None:
        """Stop the event source."""
        logger.info("Stopping OCI Queue event source")
        self._should_stop = True


async def main(queue, args: Dict[str, Any]) -> None:
    """Main entry point for the OCI Queue event source plugin."""
    event_source = OCIQueueEventSource(queue, args)
    
    try:
        await event_source.listen()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        event_source.stop()


# Entry point for ansible-rulebook
if __name__ == "__main__":
    import sys
    import json
    
    # This would typically be called by ansible-rulebook
    # For testing purposes, you can provide arguments via command line
    if len(sys.argv) > 1:
        args = json.loads(sys.argv[1])
    else:
        # Example configuration for testing
        args = {
            "queue_id": "ocid1.queue.oc1.us-phoenix-1.example",
            "compartment_id": "ocid1.compartment.oc1..example", 
            "region": "us-phoenix-1",
            "timeout_seconds": 20,
            "limit": 5
        }
    
    # Mock queue for testing
    class MockQueue:
        async def put(self, item):
            print(f"Event received: {json.dumps(item, indent=2)}")
    
    asyncio.run(main(MockQueue(), args))