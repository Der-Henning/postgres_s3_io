use pgrx::prelude::*;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

::pgrx::pg_module_magic!(name, version);

// One Tokio runtime per backend (session), built lazily.
fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

#[pg_extern]
fn s3_object_exists_lazy(
    bucket: &str,
    object_key: &str,
    endpoint_url: default!(Option<&str>, "NULL"),
    access_key: default!(Option<&str>, "NULL"),
    secret_key: default!(Option<&str>, "NULL"),
    session_token: default!(Option<&str>, "NULL"),
    region: default!(Option<&str>, "NULL"), // used ONLY on the first call in this backend
) -> bool {
    let client = get_or_init_client(endpoint_url, access_key, secret_key, session_token, region);
    let fut = async move {
        match client
            .head_object()
            .bucket(bucket)
            .key(object_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                use aws_smithy_types::error::metadata::ProvideErrorMetadata;
                let code = err.code().unwrap_or_default();
                if matches!(code, "NotFound" | "NoSuchKey" | "404")
                    || err.to_string().contains("NotFound")
                    || err.to_string().contains("NoSuchKey")
                    || err.to_string().contains("404")
                {
                    Ok(false)
                } else if code == "AccessDenied" {
                    Err(format!(
                        "AccessDenied for s3://{}/{} (check credentials/policy)",
                        bucket, object_key
                    ))
                } else {
                    Err(format!("S3 HeadObject error: {}", err))
                }
            }
        }
    };

    match rt().block_on(fut) {
        Ok(b) => b,
        Err(e) => pgrx::error!("{e}"),
    }
}

#[pg_extern]
fn s3_create_bucket(
    bucket: &str,
    endpoint_url: default!(Option<&str>, "NULL"),
    access_key: default!(Option<&str>, "NULL"),
    secret_key: default!(Option<&str>, "NULL"),
    session_token: default!(Option<&str>, "NULL"),
    region: default!(Option<&str>, "NULL"),
) -> bool {
    let client = get_or_init_client(endpoint_url, access_key, secret_key, session_token, region);

    let fut = async move {
        match client.create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(true),
            Err(aws_sdk_s3::error::SdkError::DispatchFailure(e)) => {
                Err(format!("Dispatch failure: {e:?}"))
            }
            Err(other) => Err(format!("CreateBucket failed: {other:?}")),
        }
    };

    match rt().block_on(fut) {
        Ok(v) => v,
        Err(e) => pgrx::error!("{e}"),
    }
}

#[pg_extern]
fn s3_put_object(
    bucket: &str,
    object_key: &str,
    data: Vec<u8>,
    endpoint_url: default!(Option<&str>, "NULL"),
    access_key: default!(Option<&str>, "NULL"),
    secret_key: default!(Option<&str>, "NULL"),
    session_token: default!(Option<&str>, "NULL"),
    region: default!(Option<&str>, "NULL"),
    content_type: default!(Option<&str>, "NULL"),
) -> String {
    let client = get_or_init_client(endpoint_url, access_key, secret_key, session_token, region);
    let body = aws_sdk_s3::primitives::ByteStream::from(data);

    let fut = async move {
        let mut req = client
            .put_object()
            .bucket(bucket)
            .key(object_key)
            .body(body);

        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }

        match req.send().await {
            Ok(out) => {
                let etag = out
                    .e_tag()
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string();
                Ok(etag)
            }
            Err(aws_sdk_s3::error::SdkError::DispatchFailure(e)) => {
                Err(format!("Dispatch failure: {e:?}"))
            }
            Err(other) => Err(format!("PutObject failed: {other:?}")),
        }
    };

    match rt().block_on(fut) {
        Ok(etag) => etag,
        Err(e) => pgrx::error!("{e}"),
    }
}

#[pg_extern]
fn s3_get_object(
    bucket: &str,
    object_key: &str,
    endpoint_url: default!(Option<&str>, "NULL"),
    access_key: default!(Option<&str>, "NULL"),
    secret_key: default!(Option<&str>, "NULL"),
    session_token: default!(Option<&str>, "NULL"),
    region: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    let client = get_or_init_client(endpoint_url, access_key, secret_key, session_token, region);

    let fut = async move {
        let req = client.get_object().bucket(bucket).key(object_key);

        match req.send().await {
            Ok(out) => out
                .body
                .collect()
                .await
                .map_err(|e| format!("Collect error: {e:?}")),
            Err(aws_sdk_s3::error::SdkError::DispatchFailure(e)) => {
                Err(format!("Dispatch failure: {e:?}"))
            }
            Err(other) => Err(format!("PutObject failed: {other:?}")),
        }
    };

    match rt().block_on(fut) {
        Ok(data) => data.to_vec(),
        Err(e) => pgrx::error!("{e}"),
    }
}

#[derive(Eq, PartialEq, Hash)]
struct ClientKey {
    endpoint_url: String,
    access_key: String,
    secret_key: String,
    region: String,
}

impl ClientKey {
    fn new(endpoint_url: &str, access_key: &str, secret_key: &str, region: &str) -> Self {
        Self {
            endpoint_url: endpoint_url.to_owned(),
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
            region: region.to_owned(),
        }
    }
}

fn get_or_init_client(
    endpoint_url: Option<&str>,
    access_key: Option<&str>,
    secret_key: Option<&str>,
    session_token: Option<&str>,
    region: Option<&str>,
) -> aws_sdk_s3::Client {
    // Session-lifetime S3 client, initialized on first use.
    // static S3_CLIENT: OnceLock<aws_sdk_s3::Client> = OnceLock::new();
    static S3_CLIENTS: OnceLock<Mutex<HashMap<ClientKey, aws_sdk_s3::Client>>> = OnceLock::new();

    let ep = normalize_endpoint(
        endpoint_url.unwrap_or(
            &std::env::var("S3_ENDPOINT_URL")
                .map_err(|_| pgrx::error!("AWS_SECRET_ACCESS_KEY not set"))
                .unwrap(),
        ),
    );
    let ak = access_key
        .unwrap_or(
            &std::env::var("AWS_ACCESS_KEY_ID")
                .map_err(|_| pgrx::error!("AWS_ACCESS_KEY_ID not set"))
                .unwrap(),
        )
        .to_string();
    let sk = secret_key
        .unwrap_or(
            &std::env::var("AWS_SECRET_ACCESS_KEY")
                .map_err(|_| pgrx::error!("AWS_SECRET_ACCESS_KEY not set"))
                .unwrap(),
        )
        .to_string();
    let st = session_token
        .map(|x| x.to_string())
        .or(std::env::var("AWS_SESSION_TOKEN").ok());
    let rg = region.unwrap_or("us-east-1").to_string();

    let client_key = ClientKey::new(&ep, &ak, &sk, &rg);

    S3_CLIENTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .entry(client_key)
        .or_insert(rt().block_on(async {
            use aws_config::meta::region::RegionProviderChain;
            use aws_config::BehaviorVersion;
            use aws_credential_types::Credentials;
            use aws_sdk_s3::{
                config::{Builder, SharedCredentialsProvider},
                Client,
            };
            use aws_types::region::Region;

            let region_provider = RegionProviderChain::first_try(Region::new(rg))
                .or_default_provider()
                .or_else(Region::new("us-east-1"));

            let base = aws_config::defaults(BehaviorVersion::latest())
                .region(region_provider)
                .load()
                .await;

            let mut cfg = Builder::from(&base).force_path_style(true);
            cfg = cfg.endpoint_url(ep);

            let creds = Credentials::from_keys(ak, sk, st);
            cfg = cfg.credentials_provider(SharedCredentialsProvider::new(creds));

            Client::from_conf(cfg.build())
        }))
        .clone()
}

fn normalize_endpoint(ep: &str) -> String {
    if ep.starts_with("http://") || ep.starts_with("https://") {
        ep.to_string()
    } else {
        format!("https://{ep}")
    }
}

mod testutils;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use core::time;
    use std::thread;

    use crate::testutils::minio_test::MinioServer;
    use pgrx::prelude::*;

    #[pg_test]
    fn exists_true_and_false() {
        let _minio = MinioServer::start().expect("minio up");

        let bucket = "test-bucket";
        crate::s3_create_bucket(bucket, None, None, None, None, None);
        crate::s3_put_object(
            bucket,
            "hello.txt",
            "Hi".into(),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        thread::sleep(time::Duration::from_secs(1));
        assert!(crate::s3_object_exists_lazy(
            bucket,
            "hello.txt",
            None,
            None,
            None,
            None,
            None
        ));
        assert!(!crate::s3_object_exists_lazy(
            bucket, "nope.txt", None, None, None, None, None
        ));

        log!("tests done");
    }

    #[pg_test]
    fn create_bucket() {
        let _minio = MinioServer::start().expect("minio up");

        let bucket = "tbk";
        assert!(crate::s3_create_bucket(
            bucket, None, None, None, None, None
        ));
        log!("tests done");
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        let _ = crate::testutils::minio_test::ensure_minio_binary();
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
