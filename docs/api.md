# API Reference

This page provides technical documentation for all classes and methods in **Dynamic DES**, automatically generated from the source code.

## Environment

<!-- prettier-ignore -->
::: dynamic_des.core.environment.DynamicRealtimeEnvironment
    options:
      show_root_heading: true
      members:
        - setup_ingress
        - setup_egress
        - publish_telemetry
        - publish_event

## Registry & Parameters

<!-- prettier-ignore -->
::: dynamic_des.core.registry.SimulationRegistry

<!-- prettier-ignore -->
::: dynamic_des.models.params.SimParameter

<!-- prettier-ignore -->
::: dynamic_des.models.params.DistributionConfig

<!-- prettier-ignore -->
::: dynamic_des.models.params.CapacityConfig

## Resources

<!-- prettier-ignore -->
::: dynamic_des.resources.resource.DynamicResource
    options:
      members:
        - request
        - release
        - in_use

## Connectors (Ingress)

<!-- prettier-ignore -->
::: dynamic_des.connectors.ingress.kafka.KafkaIngress

<!-- prettier-ignore -->
::: dynamic_des.connectors.ingress.local.LocalIngress

## Connectors (Egress)

<!-- prettier-ignore -->
::: dynamic_des.connectors.egress.kafka.KafkaEgress

<!-- prettier-ignore -->
::: dynamic_des.connectors.egress.local.ConsoleEgress
