# Connecting to Secure Kafka Clusters

Dynamic DES natively supports enterprise Kafka security protocols (SASL, mTLS, OAuth, AWS IAM) without requiring custom code.

Because our connectors (`KafkaIngress`, `KafkaEgress`, and `KafkaAdminConnector`) wrap the `aiokafka` and `kafka-python` libraries, they utilize a `**kwargs` passthrough pattern. This means you can inject any standard connection argument from those libraries directly into your Dynamic DES classes. `KafkaIngress` and `KafkaEgress` use `aiokafka`. `KafkaAdminConnector` uses `kafka-python` for `create_topics` and `aiokafka` for `send_config` and `collect_data`, so its arguments must suit the library each method uses.

Below are examples of how to connect to various secure enterprise environments.

Every example here connects to a managed or secured cluster, so no local container is involved. To try the connectors locally without security instead, start a broker with `odctl up kafka-lite` and see [Getting Started](../getting-started.md).

## Confluent Cloud (SASL PLAIN)

To connect to Confluent Cloud (or any cluster using standard SASL PLAIN/SCRAM), pass the `security_protocol`, `sasl_mechanism`, and credentials as keyword arguments.

```python
from dynamic_des import KafkaEgress

egress = KafkaEgress(
    bootstrap_servers="pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    topic_router=my_router,
    # These kwargs are passed straight down to the underlying library
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN", # Or SCRAM-SHA-512
    sasl_plain_username="<YOUR_API_KEY>",
    sasl_plain_password="<YOUR_API_SECRET>"
)
```

## On-Premise Secure Cluster (Strict mTLS)

For internally secured clusters requiring mutual TLS authentication, build an SSL context from your certificate files. `aiokafka` takes only `ssl_context`, not the certificate paths, and `kafka-python` accepts the same context, so one context serves every connector.

```python
from aiokafka.helpers import create_ssl_context
from dynamic_des import KafkaIngress

ssl_context = create_ssl_context(
    cafile="/path/to/ca.pem",
    certfile="/path/to/service.cert",
    keyfile="/path/to/service.key",
)

ingress = KafkaIngress(
    topic="sim-commands",
    bootstrap_servers="secure-broker.internal.company.com:9093",
    # mTLS configurations
    security_protocol="SSL",
    ssl_context=ssl_context,
)
```

---

## AWS MSK (IAM Roles & OAuthBearer)

AWS Managed Streaming for Kafka (MSK) utilizes IAM access control. To authenticate natively via IAM, you use `sasl_mechanism="OAUTHBEARER"` and provide an AWS token provider.

_(Note: You will need the `aws-msk-iam-sasl-signer-python` package installed.)_

Each library checks the provider's type. `aiokafka` requires a subclass of `aiokafka.abc.AbstractTokenProvider` with an `async` `token()`. `kafka-python` requires a subclass of `kafka.sasl.oauth.AbstractTokenProvider` with a plain `token()`. One object cannot be both, so the connectors that use `aiokafka` get one provider, and `create_topics` gets the other.

```python
from aiokafka.abc import AbstractTokenProvider as AsyncTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.sasl.oauth import AbstractTokenProvider as SyncTokenProvider
from dynamic_des import KafkaAdminConnector, KafkaEgress

def msk_token() -> str:
    # Uses standard boto3/AWS credentials from your environment or EC2/EKS role
    token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
    return token

class AsyncMSKTokenProvider(AsyncTokenProvider):
    async def token(self):
        return msk_token()

class SyncMSKTokenProvider(SyncTokenProvider):
    def token(self):
        return msk_token()

msk = dict(
    bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
)

# KafkaIngress and KafkaEgress use aiokafka
egress = KafkaEgress(**msk, topic_router=my_router, sasl_oauth_token_provider=AsyncMSKTokenProvider())

# create_topics uses kafka-python
admin = KafkaAdminConnector(**msk, sasl_oauth_token_provider=SyncMSKTokenProvider())
admin.create_topics([{"name": "sim-events", "partitions": 3}])
```

> **Tip:** You can use this exact same `OAUTHBEARER` pattern with custom token provider classes to authenticate against Okta, Auth0, or other enterprise SSO providers!
