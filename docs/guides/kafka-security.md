# Connecting to Secure Kafka Clusters

Dynamic DES natively supports enterprise Kafka security protocols (SASL, mTLS, OAuth, AWS IAM) without requiring custom code.

Because our connectors (`KafkaIngress`, `KafkaEgress`, and `KafkaAdminConnector`) wrap the `aiokafka` and `kafka-python` libraries, they utilize a `**kwargs` passthrough pattern. This means you can inject any standard connection argument from those libraries directly into your Dynamic DES classes.

Below are examples of how to connect to various secure enterprise environments.

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

For internally secured clusters requiring mutual TLS authentication, provide the paths to your certificate files.

```python
from dynamic_des import KafkaIngress

ingress = KafkaIngress(
    topic="sim-commands",
    bootstrap_servers="secure-broker.internal.company.com:9093",
    # mTLS configurations
    security_protocol="SSL",
    ssl_cafile="/path/to/ca.pem",
    ssl_certfile="/path/to/service.cert",
    ssl_keyfile="/path/to/service.key"
)
```

---

## AWS MSK (IAM Roles & OAuthBearer)

AWS Managed Streaming for Kafka (MSK) utilizes IAM access control. To authenticate natively via IAM, you use `sasl_mechanism="OAUTHBEARER"` and provide an AWS token provider.

_(Note: You will need the `aws-msk-iam-sasl-signer-python` package installed.)_

```python
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from dynamic_des import KafkaAdminConnector

class MSKTokenProvider:
    def token(self):
        # Uses standard boto3/AWS credentials from your environment or EC2/EKS role
        token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
        return token

admin = KafkaAdminConnector(
    bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider()
)
```

> **Tip:** You can use this exact same `OAUTHBEARER` pattern with custom token provider classes to authenticate against Okta, Auth0, or other enterprise SSO providers\!
