### Ansible Collection - zchef2k.eda_oci_queue

Event Driven Ansible event source plugin to read messages from Oracle OCI Queues- similar to `azure_service_bus` and `kafka`.

Dependencies:

* `pip install ansible-rulebook oci`
*  valid `~/oci/config` file

Example rulebook:

```
---
- name: Test OCI Queue Event Source
  hosts: localhost
  sources:
    - name: oci_queue_listener
      ansible.eda.oci_queue:
        queue_id: "ocid1.queue.oc1.iad...."
        compartment_id: "ocid1.compartment.oc1......"
        region: "us-<redacted>-1"
        config_profile: "DEFAULT"
        timeout_seconds: 20
        limit: 5
        polling_interval: 10.0
        auto_acknowledge: true
  rules:
    - name: Process OCI Queue Message
      condition: event.oci_queue is defined
      action:
        debug:
          msg: "Received message: {{ event.oci_queue.content }}"
```

Fork it or ask to collaborate here to improve it.

Developed in conjunction with Claude.ai
