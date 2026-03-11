# API Reference

## Environment

<!-- prettier-ignore -->
::: dynamic_des.core.environment.DynamicRealtimeEnvironment
    options:
      heading_level: 3
      show_root_heading: true
      members:
        - setup_ingress
        - setup_egress
        - publish_telemetry
        - publish_event

## Registry & Parameters

<!-- prettier-ignore -->
::: dynamic_des.core.registry.SimulationRegistry
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: dynamic_des.models.params.SimParameter
    options:
      heading_level: 3

## Resources

<!-- prettier-ignore -->
::: dynamic_des.resources.resource.DynamicResource
    options:
      heading_level: 3
      members:
        - request
        - release
        - in_use

## Admin & Infrastructure

<!-- prettier-ignore -->
::: dynamic_des.connectors.admin.kafka.KafkaAdminConnector
    options:
      heading_level: 3
      members:
        - create_topics
        - send_config
        - collect_events
        - get_state

## Connectors (Ingress)

<!-- prettier-ignore -->
::: dynamic_des.connectors.ingress.kafka.KafkaIngress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: dynamic_des.connectors.ingress.local.LocalIngress
    options:
      heading_level: 3

## Connectors (Egress)

<!-- prettier-ignore -->
::: dynamic_des.connectors.egress.kafka.KafkaEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: dynamic_des.connectors.egress.local.ConsoleEgress
    options:
      heading_level: 3
