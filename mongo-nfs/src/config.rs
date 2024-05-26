use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use thiserror::Error;
use tokio::fs;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(
    name = "mongo-nfs",
    version = "0.1.0",
    author = "AsPulse",
    about = "export Mofu FileSystem as NFS server"
)]
pub(crate) struct AppArgs {
    #[clap(short = 'p', long, env, default_value = "31128")]
    pub port: u16,

    #[clap(default_value = "config.yaml")]
    pub config: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct Config {
    pub sources: BTreeMap<String, SourceConfig>,
    pub mountpoints: Vec<MountpointConfig>,
}

#[derive(Error, Debug)]
pub enum ConfigFromFileError {
    #[error("failed to open configuration file {0}: {1}")]
    FileCannotOpen(PathBuf, std::io::Error),

    #[error("failed to parse configuration file: {0}")]
    FailedToParse(#[from] serde_yaml::Error),

    #[error("source `{0}` is not defined in the configuration file.")]
    MissingSourceDefinition(String),
}

impl Config {
    pub async fn from_file(path: &PathBuf) -> Result<Self, ConfigFromFileError> {
        let file = fs::read(path)
            .await
            .map_err(|e| ConfigFromFileError::FileCannotOpen(path.to_path_buf(), e))?;

        let config: Config = serde_yaml::from_slice(&file)?;

        for mountpoint in &config.mountpoints {
            if !config.sources.contains_key(&mountpoint.source) {
                return Err(ConfigFromFileError::MissingSourceDefinition(
                    mountpoint.source.clone(),
                ));
            }
        }

        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct MountpointConfig {
    pub path: PathBuf,
    pub source: String,
    pub bucket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct SourceConfig {
    pub uri: String,
    pub db: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_config_file() {
        let yaml = r#"
          sources:
            test:
              uri: mongodb://localhost:27017
              db: test
          mountpoints:
            - path: /mongo
              source: test
              bucket: test
        "#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            config,
            Config {
                sources: vec![(
                    "test".into(),
                    SourceConfig {
                        uri: "mongodb://localhost:27017".into(),
                        db: "test".into()
                    }
                )]
                .into_iter()
                .collect(),
                mountpoints: vec![MountpointConfig {
                    path: "/mongo".into(),
                    source: "test".into(),
                    bucket: "test".into()
                }]
            }
        );
    }
}
