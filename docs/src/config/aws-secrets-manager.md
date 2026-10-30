# AWS Secrets Manager (`type: aws-secrets-manager`)

A **provider**: resolves `{{ secret('<instance>', '<ref>') }}` values from
[AWS Secrets Manager](https://docs.aws.amazon.com/secretsmanager/) before the
run starts, so passwords and keys never sit in the pipeline file. Requests are
signed with pure-Rust SigV4 and sent over rustls (no AWS CLI, no SDK, no C
toolchain).

- Provider binary: `loadsmith-provider-aws-secrets-manager`
- Install: `loadsmith plugin install aws-secrets-manager`

```yaml
providers:
  aws:
    type: aws-secrets-manager          # → binary loadsmith-provider-aws-secrets-manager
    config:
      region: sa-east-1

source:
  type: postgres
  config:
    host: "{{ secret('aws', 'prod/orders-db#host') }}"
    user: "{{ secret('aws', 'prod/orders-db#username') }}"
    password: "{{ secret('aws', 'prod/orders-db#password') }}"
    # …
```

## References

A reference is `secret-id[#key]`:

- `secret-id` is the secret's **name** or **ARN** (`prod/orders-db`,
  `arn:aws:secretsmanager:sa-east-1:123456789012:secret:prod/orders-db-AbCdEf`);
- without `#key`, the value is the whole `SecretString`;
- with `#key`, the `SecretString` is parsed as JSON and `key` — a dotted path,
  where a numeric segment indexes an array (`hosts.0`) — selects one string,
  number or boolean. That is the shape the AWS console gives a database secret
  (`username`, `password`, `host`, `port`, …).

Each secret is fetched **once per run** (`GetSecretValue`), however many keys
the pipeline takes from it. Binary secrets (`SecretBinary`) are not supported.

## Config

| Field | Type | Default | Description |
|---|---|---|---|
| `region` | string | `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `us-east-1` | The secrets' region. |
| `endpoint` | string | `AWS_ENDPOINT_URL_SECRETS_MANAGER`, then `https://secretsmanager.<region>.amazonaws.com` | Another endpoint: a VPC interface endpoint's DNS name, LocalStack. |
| `version_stage` | string | `AWSCURRENT` | The staging label to read (`AWSPREVIOUS` reads the value before the last rotation). |
| `access_key_id` / `secret_access_key` | string | — | Static credentials; both or neither. Leave them out to use the AWS credential chain ([Credentials](#credentials)). They cannot come from a `secret()` (providers resolve before anything else); use `env()`. |
| `session_token` | string | — | For temporary credentials (STS). |
| `request_timeout_seconds` | integer | `30` | Limit for one request. |
| `max_attempts` | integer | `4` | Tries per request on network errors, throttling and `5xx`, with 1s, 2s, 4s, … between them. |

No `config:` block at all is every default. Unknown keys are rejected.

## Credentials

The same chain as the [S3 sink](./s3.md#credentials): keys in the config,
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, the shared credentials file, EKS
IRSA, the ECS task role / EKS Pod Identity container endpoint, and the EC2
instance profile. The credentials are fetched at `configure`, so a run without
any fails before a secret is asked for.

The identity needs `secretsmanager:GetSecretValue` on the secrets it reads
(and `kms:Decrypt` on their key when it is a customer-managed KMS key).

> **On ECS, prefer the task definition.** A task definition can inject a
> secret as an environment variable by itself (`secrets:` → `valueFrom`), read
> by the pipeline with `{{ env('DB_PASSWORD') }}` — no provider involved. This
> provider is for everything else: a laptop, a VM, Kubernetes, a scheduler that
> has no such feature, or secrets chosen by the pipeline file.

## Errors

A failed reference fails the run before any data plugin starts (exit `3`),
with one of these codes. **No message ever contains a value** (nor a JSON
parser error, which could quote one):

| Code | When |
|---|---|
| `NOT_FOUND` | The secret does not exist (`ResourceNotFoundException`), it has no such key, or the key is `null`. |
| `ACCESS_DENIED` | `AccessDeniedException`, `DecryptionFailure`, invalid or expired credentials. |
| `INVALID_REF` | A malformed reference, a key into a secret that is not JSON, a key that selects an object or a list, a binary secret. |
| `RESOLVE_FAILED` | Anything else (network, throttling after the last attempt, `5xx`). |
