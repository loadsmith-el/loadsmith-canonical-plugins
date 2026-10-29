//! Where the sink's AWS credentials come from: the AWS SDKs' default chain,
//! minus what needs a browser or another process (SSO, `credential_process`,
//! a profile's `role_arn`).
//!
//! 1. `access_key_id` / `secret_access_key` (/ `session_token`) in the config;
//! 2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (/ `AWS_SESSION_TOKEN`);
//! 3. the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, default
//!    `~/.aws/credentials`), profile `AWS_PROFILE` (default `default`);
//! 4. a web identity (EKS IRSA): `AWS_WEB_IDENTITY_TOKEN_FILE` +
//!    `AWS_ROLE_ARN`, exchanged at STS (`AssumeRoleWithWebIdentity`);
//! 5. the container endpoint (ECS task roles, EKS Pod Identity):
//!    `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or `…_FULL_URI`, with
//!    `AWS_CONTAINER_AUTHORIZATION_TOKEN(_FILE)`;
//! 6. the EC2 instance profile, through IMDSv2 (unless
//!    `AWS_EC2_METADATA_DISABLED=true`).
//!
//! The first that applies is used for the whole run. Temporary credentials
//! (4–6) are fetched again five minutes before they expire, so a long load
//! outlives them.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use reqwest::Client;
use rusty_s3::Credentials;
use tokio::sync::Mutex;

/// How long before their expiry temporary credentials are fetched again.
const REFRESH_AHEAD: Duration = Duration::from_secs(300);
/// The container endpoint for a relative URI (ECS).
const ECS_HOST: &str = "http://169.254.170.2";
/// IMDS, unless `AWS_EC2_METADATA_SERVICE_ENDPOINT`.
const IMDS_ENDPOINT: &str = "http://169.254.169.254";
/// How long one metadata request may take: off EC2 the address does not
/// answer, and the chain must not hang on it.
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Keys and the moment they stop working (`None`: never).
#[derive(Clone)]
struct Keys {
    credentials: Credentials,
    expires: Option<SystemTime>,
}

impl Keys {
    fn fresh(&self) -> bool {
        self.expires.is_none_or(|t| SystemTime::now() + REFRESH_AHEAD < t)
    }
}

/// A source of temporary credentials, asked again when they near expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Refreshing {
    WebIdentity { token_file: PathBuf, role_arn: String, session: String, sts: String },
    Container { url: String, token: Option<String>, token_file: Option<PathBuf> },
    Imds { endpoint: String },
}

impl Refreshing {
    fn name(&self) -> &'static str {
        match self {
            Refreshing::WebIdentity { .. } => "web identity (AWS_WEB_IDENTITY_TOKEN_FILE)",
            Refreshing::Container { .. } => "container credentials endpoint",
            Refreshing::Imds { .. } => "EC2 instance profile (IMDS)",
        }
    }
}

/// The run's credentials: fixed keys, or a [`Refreshing`] source and the
/// keys it last gave.
pub struct Provider {
    source: Option<Refreshing>,
    keys: Mutex<Option<Keys>>,
    http: Client,
}

/// What the config says about credentials.
pub struct Explicit<'a> {
    pub access_key_id: Option<&'a str>,
    pub secret_access_key: Option<&'a str>,
    pub session_token: Option<&'a str>,
}

impl Provider {
    /// Picks the first source of the chain that applies and fetches its
    /// credentials, so a run without any fails at `configure`.
    pub async fn resolve(explicit: Explicit<'_>, region: &str) -> Result<Self> {
        let http = Client::builder()
            .tls_backend_preconfigured(loadsmith_tls::public_web_config(&[b"h2", b"http/1.1"])?)
            .timeout(METADATA_TIMEOUT)
            .no_proxy()
            .build()
            .context("failed to build the credentials HTTP client")?;
        let fixed = |credentials| Ok(Self { source: None, keys: Mutex::new(Some(Keys { credentials, expires: None })), http: http.clone() });
        match (explicit.access_key_id, explicit.secret_access_key) {
            (Some(key), Some(secret)) => return fixed(make(key, secret, explicit.session_token)),
            (Some(_), None) | (None, Some(_)) => {
                bail!("set both `access_key_id` and `secret_access_key`, or neither (to use the AWS credential chain)")
            }
            (None, None) => {}
        }
        if let (Some(key), Some(secret)) = (env_var("AWS_ACCESS_KEY_ID"), env_var("AWS_SECRET_ACCESS_KEY")) {
            let token = explicit.session_token.map(str::to_string).or_else(|| env_var("AWS_SESSION_TOKEN"));
            return fixed(make(&key, &secret, token.as_deref()));
        }
        if let Some(credentials) = from_profile_file()? {
            return fixed(credentials);
        }
        let Some(source) = refreshing_source(region) else {
            bail!(
                "no S3 credentials: set `access_key_id` and `secret_access_key` (e.g. from `secret()`), \
                 AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, a profile in ~/.aws/credentials, or run with \
                 a role (EKS IRSA / Pod Identity, ECS task role, EC2 instance profile)"
            );
        };
        let provider = Self { source: Some(source), keys: Mutex::new(None), http };
        provider.get().await.map_err(|e| {
            let source = provider.source.as_ref().map(Refreshing::name).unwrap_or_default();
            anyhow::anyhow!("no S3 credentials from the {source}: {e:#}")
        })?;
        Ok(provider)
    }

    /// The credentials to sign the next request with.
    pub async fn get(&self) -> Result<Credentials> {
        let mut keys = self.keys.lock().await;
        if let Some(k) = keys.as_ref().filter(|k| k.fresh()) {
            return Ok(k.credentials.clone());
        }
        let source = self.source.as_ref().context("credentials expired")?;
        let fetched = fetch(&self.http, source).await?;
        tracing::debug!(source = source.name(), "fetched temporary credentials");
        let credentials = fetched.credentials.clone();
        *keys = Some(fetched);
        Ok(credentials)
    }
}

fn make(key: &str, secret: &str, token: Option<&str>) -> Credentials {
    match token {
        Some(token) => Credentials::new_with_token(key, secret, token),
        None => Credentials::new(key, secret),
    }
}

/// Static keys of the profile in the shared credentials file; `None` when
/// there is no file or no such profile.
fn from_profile_file() -> Result<Option<Credentials>> {
    let path = match env_var("AWS_SHARED_CREDENTIALS_FILE") {
        Some(p) => PathBuf::from(p),
        None => match env_var("HOME") {
            Some(home) => PathBuf::from(home).join(".aws/credentials"),
            None => return Ok(None),
        },
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    let profile = env_var("AWS_PROFILE").unwrap_or_else(|| "default".to_string());
    let Some(entries) = ini_section(&text, &profile) else {
        return Ok(None);
    };
    let get = |k: &str| entries.iter().find(|(key, _)| key == k).map(|(_, v)| v.as_str());
    match (get("aws_access_key_id"), get("aws_secret_access_key")) {
        (Some(key), Some(secret)) => Ok(Some(make(key, secret, get("aws_session_token")))),
        _ => bail!(
            "profile `{profile}` in {} has no aws_access_key_id/aws_secret_access_key (role_arn, SSO and \
             credential_process profiles are not supported)",
            path.display()
        ),
    }
}

/// The `key = value` lines of `[name]` in an INI text.
fn ini_section(text: &str, name: &str) -> Option<Vec<(String, String)>> {
    let mut found = None;
    let mut current: Option<&mut Vec<(String, String)>> = None;
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(section) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            current = None;
            if section.trim() == name {
                current = Some(found.insert(Vec::new()));
            }
            continue;
        }
        if let (Some(entries), Some((k, v))) = (current.as_deref_mut(), line.split_once('=')) {
            entries.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    found
}

/// The first role-based source the environment points at.
fn refreshing_source(region: &str) -> Option<Refreshing> {
    if let (Some(token_file), Some(role_arn)) = (env_var("AWS_WEB_IDENTITY_TOKEN_FILE"), env_var("AWS_ROLE_ARN")) {
        let sts = env_var("AWS_ENDPOINT_URL_STS").unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));
        let session = env_var("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| "loadsmith-s3".to_string());
        return Some(Refreshing::WebIdentity { token_file: token_file.into(), role_arn, session, sts });
    }
    let container = env_var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        .map(|uri| format!("{ECS_HOST}{uri}"))
        .or_else(|| env_var("AWS_CONTAINER_CREDENTIALS_FULL_URI"));
    if let Some(url) = container {
        return Some(Refreshing::Container {
            url,
            token: env_var("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
            token_file: env_var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE").map(PathBuf::from),
        });
    }
    if env_var("AWS_EC2_METADATA_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return None;
    }
    let endpoint = env_var("AWS_EC2_METADATA_SERVICE_ENDPOINT").unwrap_or_else(|| IMDS_ENDPOINT.to_string());
    Some(Refreshing::Imds { endpoint: endpoint.trim_end_matches('/').to_string() })
}

async fn fetch(http: &Client, source: &Refreshing) -> Result<Keys> {
    match source {
        Refreshing::WebIdentity { token_file, role_arn, session, sts } => {
            let token = std::fs::read_to_string(token_file)
                .with_context(|| format!("reading the web identity token {}", token_file.display()))?;
            let resp = http
                .get(sts.as_str())
                .timeout(Duration::from_secs(30))
                .query(&[
                    ("Action", "AssumeRoleWithWebIdentity"),
                    ("Version", "2011-06-15"),
                    ("RoleArn", role_arn.as_str()),
                    ("RoleSessionName", session.as_str()),
                    ("WebIdentityToken", token.trim()),
                ])
                .send()
                .await
                .context("STS AssumeRoleWithWebIdentity")?;
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                bail!("STS AssumeRoleWithWebIdentity: HTTP {status}: {}", crate::s3_error(&body));
            }
            let field = |name: &str| {
                crate::xml_element(&body, name).with_context(|| format!("STS answered without <{name}>"))
            };
            Ok(Keys {
                credentials: make(&field("AccessKeyId")?, &field("SecretAccessKey")?, Some(&field("SessionToken")?)),
                expires: Some(expiry(&field("Expiration")?)?),
            })
        }
        Refreshing::Container { url, token, token_file } => {
            let auth = match (token_file, token) {
                (Some(file), _) => Some(
                    std::fs::read_to_string(file)
                        .with_context(|| format!("reading the container authorization token {}", file.display()))?
                        .trim()
                        .to_string(),
                ),
                (None, token) => token.clone(),
            };
            let mut req = http.get(url.as_str());
            if let Some(auth) = auth {
                req = req.header("Authorization", auth);
            }
            json_keys(req.send().await.context("the container credentials endpoint")?).await
        }
        Refreshing::Imds { endpoint } => {
            let token = http
                .put(format!("{endpoint}/latest/api/token"))
                .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .context("IMDSv2 token (not on EC2? set AWS_EC2_METADATA_DISABLED=true to skip it)")?
                .text()
                .await?;
            let base = format!("{endpoint}/latest/meta-data/iam/security-credentials/");
            let role = http
                .get(&base)
                .header("X-aws-ec2-metadata-token", &token)
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .context("IMDS: the instance has no IAM role")?
                .text()
                .await?;
            let role = role.lines().next().map(str::trim).filter(|r| !r.is_empty()).context("IMDS: no IAM role")?;
            let resp = http.get(format!("{base}{role}")).header("X-aws-ec2-metadata-token", &token).send().await?;
            json_keys(resp).await
        }
    }
}

/// Keys from the JSON the container endpoint and IMDS answer with.
async fn json_keys(resp: reqwest::Response) -> Result<Keys> {
    let status = resp.status();
    if !status.is_success() {
        bail!("HTTP {status}: {}", resp.text().await.unwrap_or_default().trim());
    }
    let body = resp.text().await.context("reading the credentials")?;
    let v: serde_json::Value = serde_json::from_str(&body).context("credentials are not JSON")?;
    let field = |name: &str| v[name].as_str().with_context(|| format!("credentials without `{name}`"));
    Ok(Keys {
        credentials: make(field("AccessKeyId")?, field("SecretAccessKey")?, v["Token"].as_str()),
        expires: v["Expiration"].as_str().map(expiry).transpose()?,
    })
}

/// An ISO-8601 expiry (`2026-10-29T12:00:00Z`).
fn expiry(text: &str) -> Result<SystemTime> {
    let at = chrono::DateTime::parse_from_rfc3339(text.trim()).with_context(|| format!("expiry {text:?}"))?;
    Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(at.timestamp().max(0) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    #[test]
    fn profiles_are_read_from_their_section() {
        let text = "[default]\naws_access_key_id = A\naws_secret_access_key=B\n\n# c\n[prod]\naws_access_key_id = C\n";
        assert_eq!(ini_section(text, "default").unwrap(), [("aws_access_key_id".into(), "A".into()), ("aws_secret_access_key".into(), "B".into())]);
        assert_eq!(ini_section(text, "prod").unwrap().len(), 1);
        assert!(ini_section(text, "dev").is_none());
    }

    #[test]
    fn expiry_is_iso_8601() {
        let t = expiry("2026-10-29T12:00:00Z").unwrap();
        assert_eq!(t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(), 1_792_670_400 + 7 * 86400);
        assert!(expiry("tomorrow").is_err());
    }

    fn provider(source: Refreshing) -> Provider {
        loadsmith_tls::install_provider();
        Provider { source: Some(source), keys: Mutex::new(None), http: Client::new() }
    }

    #[tokio::test]
    async fn imds_v2_gives_the_role_credentials_and_they_are_cached() {
        let server = MockServer::start_async().await;
        let token = server
            .mock_async(|when, then| {
                when.method(PUT).path("/latest/api/token").header_exists("X-aws-ec2-metadata-token-ttl-seconds");
                then.status(200).body("tok");
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(GET).path("/latest/meta-data/iam/security-credentials/").header("X-aws-ec2-metadata-token", "tok");
                then.status(200).body("loader-role\n");
            })
            .await;
        let creds = server
            .mock_async(|when, then| {
                when.method(GET).path("/latest/meta-data/iam/security-credentials/loader-role");
                then.status(200).body(r#"{"Code":"Success","AccessKeyId":"ASIA1","SecretAccessKey":"s","Token":"t","Expiration":"2999-01-01T00:00:00Z"}"#);
            })
            .await;
        let p = provider(Refreshing::Imds { endpoint: server.base_url() });
        let c = p.get().await.unwrap();
        assert_eq!((c.key(), c.secret(), c.token()), ("ASIA1", "s", Some("t")));
        p.get().await.unwrap();
        token.assert_hits_async(1).await;
        creds.assert_hits_async(1).await;
    }

    #[tokio::test]
    async fn credentials_near_expiry_are_fetched_again() {
        let server = MockServer::start_async().await;
        let soon = chrono::Utc::now() + chrono::Duration::seconds(60);
        let creds = server
            .mock_async(|when, then| {
                when.method(GET).path("/creds").header("Authorization", "secret-token");
                then.status(200).body(format!(
                    r#"{{"AccessKeyId":"ASIA2","SecretAccessKey":"s","Token":"t","Expiration":"{}"}}"#,
                    soon.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                ));
            })
            .await;
        let p = provider(Refreshing::Container { url: server.url("/creds"), token: Some("secret-token".into()), token_file: None });
        p.get().await.unwrap();
        p.get().await.unwrap();
        creds.assert_hits_async(2).await;
    }

    #[tokio::test]
    async fn web_identity_is_exchanged_at_sts() {
        let server = MockServer::start_async().await;
        let dir = std::env::temp_dir().join(format!("s3-wid-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let token_file = dir.join("token");
        std::fs::write(&token_file, "jwt\n").unwrap();
        let sts = server
            .mock_async(|when, then| {
                when.method(GET)
                    .query_param("Action", "AssumeRoleWithWebIdentity")
                    .query_param("RoleArn", "arn:aws:iam::1:role/r")
                    .query_param("WebIdentityToken", "jwt");
                then.status(200).body(
                    "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
                     <AccessKeyId>ASIA3</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
                     <Expiration>2999-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult>\
                     </AssumeRoleWithWebIdentityResponse>",
                );
            })
            .await;
        let p = provider(Refreshing::WebIdentity {
            token_file,
            role_arn: "arn:aws:iam::1:role/r".into(),
            session: "loadsmith-s3".into(),
            sts: server.base_url(),
        });
        assert_eq!(p.get().await.unwrap().key(), "ASIA3");
        sts.assert_async().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_role_that_does_not_answer_is_reported() {
        let p = provider(Refreshing::Container { url: "http://127.0.0.1:9/creds".into(), token: None, token_file: None });
        let e = format!("{:#}", p.get().await.unwrap_err());
        assert!(e.contains("container credentials endpoint"), "{e}");
    }
}
