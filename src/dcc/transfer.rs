//! Bounded, streamed DCC SEND file I/O.

use std::{
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom},
    sync::mpsc,
};
use uuid::Uuid;

use super::{
    confine::{Occupant, ReceiveDestination},
    session::DccSessionId,
};

/// Caller-selected behavior when a destination already exists.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConflict {
    /// Leave the existing path untouched and reject the transfer.
    #[default]
    Fail,
    /// Commit the completed temporary file over the existing destination.
    Replace,
    /// Select a non-existing sibling name and report it.
    Rename,
}

/// Progress that may be coalesced in resource notifications.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct TransferProgress {
    /// Owning DCC session.
    pub session_id: DccSessionId,
    /// Bytes read or written so far, including a resume offset.
    pub transferred_bytes: u64,
    /// Expected total when known.
    pub total_bytes: Option<u64>,
}

/// Completed receive result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedFile {
    /// Actual committed destination on the gateway host, including any
    /// generated rename.
    pub path: PathBuf,
    /// Configured receive root the file was written beneath.
    pub root: String,
    /// Committed destination relative to that root.
    pub relative_path: PathBuf,
    /// Final file length.
    pub bytes: u64,
}

/// Shared bounded transfer runtime settings.
#[derive(Clone, Debug)]
pub struct TransferOptions {
    /// Resume position already present or acknowledged.
    pub resume_offset: u64,
    /// Streaming buffer size.
    pub buffer_bytes: usize,
    /// Maximum time without peer socket progress.
    pub idle_timeout: Duration,
    /// Optional bounded progress sink.
    pub progress: Option<mpsc::Sender<TransferProgress>>,
}

/// Receive-specific transfer settings.
#[derive(Clone, Debug)]
pub struct ReceiveOptions {
    /// Existing destination behavior.
    pub conflict: DestinationConflict,
    /// Advertised total size, when known.
    pub expected_size: Option<u64>,
    /// Hard ceiling for bytes written, including unknown-size offers.
    pub max_bytes: u64,
    /// Shared streaming and timeout settings.
    pub transfer: TransferOptions,
}

/// Stream a local file to a connected DCC peer with cumulative ACK validation.
pub async fn send_file<S>(
    stream: S,
    session_id: DccSessionId,
    source: &Path,
    options: TransferOptions,
) -> Result<u64, TransferError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    validate_options(&options)?;
    let mut file = File::open(source)
        .await
        .map_err(|source| TransferError::Io {
            operation: "open DCC source_path on the gateway host",
            source,
        })?;
    let metadata = file.metadata().await.map_err(|source| TransferError::Io {
        operation: "read DCC source_path metadata on the gateway host",
        source,
    })?;
    if !metadata.is_file() {
        return Err(TransferError::SourceNotFile(source.to_path_buf()));
    }
    let total = metadata.len();
    if options.resume_offset > total {
        return Err(TransferError::InvalidResume {
            offset: options.resume_offset,
            length: total,
        });
    }
    file.seek(SeekFrom::Start(options.resume_offset))
        .await
        .map_err(|source| TransferError::Io {
            operation: "seek DCC source_path on the gateway host",
            source,
        })?;

    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut buffer = vec![0; options.buffer_bytes];
    let mut transferred = options.resume_offset;
    let mut acknowledged = options.resume_offset;
    while transferred < total {
        let wanted = usize::try_from((total - transferred).min(options.buffer_bytes as u64))
            .expect("bounded by usize buffer length");
        let read = file
            .read(&mut buffer[..wanted])
            .await
            .map_err(|source| TransferError::Io {
                operation: "read DCC source_path on the gateway host",
                source,
            })?;
        if read == 0 {
            return Err(TransferError::UnexpectedSourceEof {
                expected: total,
                actual: transferred,
            });
        }
        peer_io(
            options.idle_timeout,
            "write DCC transfer",
            writer.write_all(&buffer[..read]),
        )
        .await?;
        transferred = transferred.saturating_add(read as u64);

        while acknowledged < transferred {
            let mut ack = [0; 4];
            peer_io(
                options.idle_timeout,
                "read DCC transfer acknowledgement",
                reader.read_exact(&mut ack),
            )
            .await?;
            let raw = u32::from_be_bytes(ack);
            let expanded = expand_acknowledgement(raw, acknowledged);
            if expanded == acknowledged {
                continue;
            }
            if expanded > transferred {
                return Err(TransferError::InvalidAcknowledgement {
                    expected: transferred,
                    actual: expanded,
                });
            }
            acknowledged = expanded;
        }
        report_progress(&options.progress, &session_id, transferred, Some(total)).await;
    }
    peer_io(options.idle_timeout, "flush DCC transfer", writer.flush()).await?;
    Ok(transferred)
}

/// Lift a wrapped 32-bit cumulative DCC ACK onto the nearest monotonic u64
/// position at or after the previously acknowledged byte.
fn expand_acknowledgement(raw: u32, previous: u64) -> u64 {
    const WRAP: u64 = 1_u64 << 32;
    let base = previous & !(WRAP - 1);
    let candidate = base | u64::from(raw);
    if candidate < previous {
        candidate.saturating_add(WRAP)
    } else {
        candidate
    }
}

/// Receive a DCC stream and commit it according to explicit conflict behavior.
///
/// `destination` is a directory this process already holds open plus a name
/// inside it, so every operation below — the conflict probe, the temporary file,
/// the commit, the cleanup — addresses an entry within that descriptor. Nothing
/// here re-resolves a path, which is what keeps a link or a swapped directory
/// from redirecting the write after the destination was confined. See
/// [`crate::dcc::confine`].
pub async fn receive_file<S>(
    stream: S,
    session_id: DccSessionId,
    mut destination: ReceiveDestination,
    options: ReceiveOptions,
) -> Result<ReceivedFile, TransferError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    validate_options(&options.transfer)?;
    if options.max_bytes == 0 {
        return Err(TransferError::InvalidSizeLimit);
    }
    if options
        .expected_size
        .is_some_and(|total| total > options.max_bytes)
    {
        return Err(TransferError::SizeLimit {
            limit: options.max_bytes,
        });
    }
    if options
        .expected_size
        .is_some_and(|total| options.transfer.resume_offset > total)
    {
        return Err(TransferError::InvalidResume {
            offset: options.transfer.resume_offset,
            length: options.expected_size.unwrap_or_default(),
        });
    }

    let (mut file, temporary) = if options.transfer.resume_offset == 0 {
        select_destination(&mut destination, options.conflict).await?;
        let temporary = temporary_name(destination.file_name());
        let file =
            destination
                .create_new(&temporary)
                .await
                .map_err(|source| TransferError::Io {
                    operation: "create temporary DCC destination",
                    source,
                })?;
        (file, Some(temporary))
    } else {
        let file = destination
            .open_append(destination.file_name())
            .await
            .map_err(|source| TransferError::Io {
                operation: "open partial DCC destination",
                source,
            })?;
        let length = file
            .metadata()
            .await
            .map_err(|source| TransferError::Io {
                operation: "read partial DCC destination metadata",
                source,
            })?
            .len();
        if length != options.transfer.resume_offset {
            return Err(TransferError::InvalidResume {
                offset: options.transfer.resume_offset,
                length,
            });
        }
        (file, None)
    };

    let transfer = receive_into(stream, &mut file, session_id, &options).await;
    let transferred = match transfer {
        Ok(value) => value,
        Err(error) => {
            drop(file);
            if let Some(temporary) = temporary {
                let _ = destination.remove(&temporary).await;
            }
            return Err(error);
        }
    };

    file.flush().await.map_err(|source| TransferError::Io {
        operation: "flush DCC destination",
        source,
    })?;
    file.sync_data().await.map_err(|source| TransferError::Io {
        operation: "sync DCC destination",
        source,
    })?;
    drop(file);
    if let Some(temporary) = temporary {
        commit_temporary(&destination, &temporary, options.conflict).await?;
    }
    Ok(ReceivedFile {
        path: destination.path(),
        root: destination.root().to_owned(),
        relative_path: destination.relative_path(),
        bytes: transferred,
    })
}

async fn receive_into<S>(
    stream: S,
    file: &mut File,
    session_id: DccSessionId,
    options: &ReceiveOptions,
) -> Result<u64, TransferError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut buffer = vec![0; options.transfer.buffer_bytes];
    let mut transferred = options.transfer.resume_offset;
    loop {
        let remaining_limit = options.max_bytes.saturating_sub(transferred);
        let wanted = options
            .expected_size
            .map_or(options.transfer.buffer_bytes, |total| {
                usize::try_from((total - transferred).min(options.transfer.buffer_bytes as u64))
                    .expect("bounded by usize buffer length")
            })
            .min(usize::try_from(remaining_limit.min(usize::MAX as u64)).unwrap_or(usize::MAX));
        if wanted == 0 {
            if options.expected_size.is_none() {
                let mut probe = [0_u8; 1];
                let read = peer_io(
                    options.transfer.idle_timeout,
                    "check DCC transfer size limit",
                    reader.read(&mut probe),
                )
                .await?;
                if read != 0 {
                    return Err(TransferError::SizeLimit {
                        limit: options.max_bytes,
                    });
                }
            }
            break;
        }
        let read = peer_io(
            options.transfer.idle_timeout,
            "read DCC transfer",
            reader.read(&mut buffer[..wanted]),
        )
        .await?;
        if read == 0 {
            if let Some(expected) = options.expected_size
                && transferred != expected
            {
                return Err(TransferError::UnexpectedPeerEof {
                    expected,
                    actual: transferred,
                });
            }
            break;
        }
        file.write_all(&buffer[..read])
            .await
            .map_err(|source| TransferError::Io {
                operation: "write DCC destination",
                source,
            })?;
        transferred = transferred.saturating_add(read as u64);
        peer_io(
            options.transfer.idle_timeout,
            "write DCC acknowledgement",
            writer.write_all(&(transferred as u32).to_be_bytes()),
        )
        .await?;
        report_progress(
            &options.transfer.progress,
            &session_id,
            transferred,
            options.expected_size,
        )
        .await;
    }
    Ok(transferred)
}

async fn report_progress(
    progress: &Option<mpsc::Sender<TransferProgress>>,
    session_id: &DccSessionId,
    transferred_bytes: u64,
    total_bytes: Option<u64>,
) {
    if let Some(progress) = progress {
        let _ = progress
            .send(TransferProgress {
                session_id: session_id.clone(),
                transferred_bytes,
                total_bytes,
            })
            .await;
    }
}

fn validate_options(options: &TransferOptions) -> Result<(), TransferError> {
    if options.buffer_bytes == 0 {
        Err(TransferError::InvalidBuffer)
    } else if options.idle_timeout.is_zero() {
        Err(TransferError::InvalidIdleTimeout)
    } else {
        Ok(())
    }
}

async fn peer_io<T>(
    timeout: Duration,
    operation: &'static str,
    future: impl Future<Output = std::io::Result<T>>,
) -> Result<T, TransferError> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| TransferError::IdleTimeout { operation })?
        .map_err(|source| TransferError::Io { operation, source })
}

/// Settle which name inside the confined directory this transfer commits to.
///
/// The probe never follows a link, so a link planted at the destination name is
/// an occupied name to resolve rather than a route out of the receive root: with
/// `fail` the transfer stops, with `replace` the commit rename replaces the link
/// itself, and with `rename` the transfer moves aside.
async fn select_destination(
    destination: &mut ReceiveDestination,
    conflict: DestinationConflict,
) -> Result<(), TransferError> {
    let occupant = destination
        .occupant(destination.file_name())
        .await
        .map_err(|source| TransferError::Io {
            operation: "inspect DCC destination",
            source,
        })?;
    match occupant {
        None => Ok(()),
        Some(Occupant::Directory) => Err(TransferError::InvalidDestination(destination.path())),
        Some(Occupant::Entry) => match conflict {
            DestinationConflict::Fail => Err(TransferError::DestinationExists(destination.path())),
            DestinationConflict::Replace => Ok(()),
            DestinationConflict::Rename => {
                let available = available_sibling(destination).await?;
                destination.set_file_name(available);
                Ok(())
            }
        },
    }
}

async fn available_sibling(destination: &ReceiveDestination) -> Result<String, TransferError> {
    let taken = Path::new(destination.file_name());
    let stem = taken
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| TransferError::InvalidDestination(destination.path()))?;
    let extension = taken.extension().and_then(|value| value.to_str());
    for number in 2..=10_000 {
        let candidate = extension.map_or_else(
            || format!("{stem} ({number})"),
            |extension| format!("{stem} ({number}).{extension}"),
        );
        match destination.occupant(&candidate).await {
            Ok(None) => return Ok(candidate),
            Ok(Some(_)) => {}
            Err(source) => {
                return Err(TransferError::Io {
                    operation: "inspect renamed DCC destination",
                    source,
                });
            }
        }
    }
    Err(TransferError::RenameExhausted(destination.path()))
}

fn temporary_name(file_name: &str) -> String {
    format!(".{file_name}.{}.part", Uuid::new_v4())
}

async fn commit_temporary(
    destination: &ReceiveDestination,
    temporary: &str,
    conflict: DestinationConflict,
) -> Result<(), TransferError> {
    let committed = destination.file_name();
    if conflict != DestinationConflict::Replace {
        return destination
            .rename_within(temporary, committed)
            .await
            .map_err(|source| TransferError::Io {
                operation: "commit DCC destination",
                source,
            });
    }

    let backup = format!(".{committed}.{}.backup", Uuid::new_v4());
    let had_existing = match destination.rename_within(committed, &backup).await {
        Ok(()) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(source) => {
            return Err(TransferError::Io {
                operation: "stage existing DCC destination",
                source,
            });
        }
    };
    if let Err(source) = destination.rename_within(temporary, committed).await {
        if had_existing {
            let _ = destination.rename_within(&backup, committed).await;
        }
        return Err(TransferError::Io {
            operation: "commit replacement DCC destination",
            source,
        });
    }
    if had_existing && let Err(error) = destination.remove(&backup).await {
        tracing::warn!(backup, %error, "could not remove replaced DCC backup");
    }
    Ok(())
}

/// Stream or destination failure.
#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    /// Configured streaming buffer is zero.
    #[error("DCC transfer buffer must be greater than zero")]
    InvalidBuffer,
    /// Configured peer inactivity deadline is zero.
    #[error("DCC transfer idle timeout must be greater than zero")]
    InvalidIdleTimeout,
    /// Configured receive ceiling is zero.
    #[error("DCC transfer byte limit must be greater than zero")]
    InvalidSizeLimit,
    /// Peer tried to transfer more data than the configured ceiling.
    #[error("DCC transfer exceeds the configured {limit}-byte limit")]
    SizeLimit {
        /// Maximum accepted byte count.
        limit: u64,
    },
    /// Peer socket made no progress before the inactivity deadline.
    #[error("DCC transfer timed out while attempting to {operation}")]
    IdleTimeout {
        /// Socket operation that stalled.
        operation: &'static str,
    },
    /// Source is not a regular file.
    #[error("DCC source_path on the gateway host is not a regular file: {0}")]
    SourceNotFile(PathBuf),
    /// Destination path cannot identify a file.
    #[error("invalid DCC destination: {0}")]
    InvalidDestination(PathBuf),
    /// Conflict behavior is fail and the path exists.
    #[error("DCC destination already exists: {0}")]
    DestinationExists(PathBuf),
    /// Could not find a non-existing sibling name.
    #[error("could not generate an available DCC destination beside {0}")]
    RenameExhausted(PathBuf),
    /// Resume offset disagrees with available file data.
    #[error("invalid DCC resume offset {offset}; available length is {length}")]
    InvalidResume {
        /// Requested offset.
        offset: u64,
        /// Available/expected length.
        length: u64,
    },
    /// Local source ended before its metadata length.
    #[error("DCC source ended at {actual} bytes; expected {expected}")]
    UnexpectedSourceEof {
        /// Expected total.
        expected: u64,
        /// Actual bytes read.
        actual: u64,
    },
    /// Peer ended before the advertised total.
    #[error("DCC peer ended at {actual} bytes; expected {expected}")]
    UnexpectedPeerEof {
        /// Expected total.
        expected: u64,
        /// Actual bytes received.
        actual: u64,
    },
    /// Peer acknowledgement does not match cumulative bytes.
    #[error("invalid DCC acknowledgement {actual}; expected progress through {expected}")]
    InvalidAcknowledgement {
        /// Highest cumulative byte count written so far.
        expected: u64,
        /// Expanded peer-provided cumulative count.
        actual: u64,
    },
    /// Filesystem or direct socket failed.
    #[error("{operation}: {source}")]
    Io {
        /// Stable operation context.
        operation: &'static str,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DccReceiveRoot;

    /// Resolve one destination inside a temporary receive root.
    async fn destination(root: &Path, relative: &str) -> ReceiveDestination {
        ReceiveDestination::resolve(
            &DccReceiveRoot {
                name: "downloads".into(),
                path: root.to_path_buf(),
            },
            Path::new(relative),
        )
        .await
        .expect("resolve destination")
    }

    #[test]
    fn cumulative_acknowledgements_expand_across_the_u32_boundary() {
        assert_eq!(expand_acknowledgement(9, 4_294_967_290), 4_294_967_305);
        assert_eq!(expand_acknowledgement(100, 90), 100);
    }

    #[tokio::test]
    async fn streamed_transfer_round_trips_over_duplex() {
        let source_dir = tempfile::tempdir().expect("source tempdir");
        let destination_dir = tempfile::tempdir().expect("destination tempdir");
        let source = source_dir.path().join("source.bin");
        let received = destination(destination_dir.path(), "nested/destination.bin").await;
        let committed = received.path();
        let data = vec![0x5a; 70_000];
        tokio::fs::write(&source, &data)
            .await
            .expect("write source");
        let (left, right) = tokio::io::duplex(8 * 1024);

        let send_id = DccSessionId::new();
        let receive_id = DccSessionId::new();
        let (sent, outcome) = tokio::join!(
            send_file(
                left,
                send_id,
                &source,
                TransferOptions {
                    resume_offset: 0,
                    buffer_bytes: 4096,
                    idle_timeout: Duration::from_secs(1),
                    progress: None,
                },
            ),
            receive_file(
                right,
                receive_id,
                received,
                ReceiveOptions {
                    conflict: DestinationConflict::Fail,
                    expected_size: Some(data.len() as u64),
                    max_bytes: data.len() as u64,
                    transfer: TransferOptions {
                        resume_offset: 0,
                        buffer_bytes: 4096,
                        idle_timeout: Duration::from_secs(1),
                        progress: None,
                    },
                },
            )
        );
        assert_eq!(sent.expect("send"), data.len() as u64);
        let outcome = outcome.expect("receive");
        assert_eq!(outcome.path, committed);
        assert_eq!(outcome.root, "downloads");
        assert_eq!(
            outcome.relative_path,
            PathBuf::from("nested/destination.bin")
        );
        assert_eq!(
            tokio::fs::read(committed).await.expect("read destination"),
            data
        );
    }

    #[tokio::test]
    async fn source_lookup_errors_name_the_gateway_host() {
        let directory = tempfile::tempdir().expect("tempdir");
        let source = directory.path().join("missing.bin");
        let (stream, _peer) = tokio::io::duplex(64);
        let error = send_file(
            stream,
            DccSessionId::new(),
            &source,
            TransferOptions {
                resume_offset: 0,
                buffer_bytes: 16,
                idle_timeout: Duration::from_secs(1),
                progress: None,
            },
        )
        .await
        .expect_err("missing source must fail");

        assert!(
            error
                .to_string()
                .contains("source_path on the gateway host")
        );
    }

    #[tokio::test]
    async fn failed_conflict_never_touches_existing_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let existing = directory.path().join("existing");
        tokio::fs::write(&existing, b"original")
            .await
            .expect("seed");
        let (_left, right) = tokio::io::duplex(64);
        let error = receive_file(
            right,
            DccSessionId::new(),
            destination(directory.path(), "existing").await,
            ReceiveOptions {
                conflict: DestinationConflict::Fail,
                expected_size: Some(0),
                max_bytes: 1,
                transfer: TransferOptions {
                    resume_offset: 0,
                    buffer_bytes: 16,
                    idle_timeout: Duration::from_secs(1),
                    progress: None,
                },
            },
        )
        .await
        .expect_err("must reject");
        assert!(matches!(error, TransferError::DestinationExists(_)));
        assert_eq!(tokio::fs::read(existing).await.expect("read"), b"original");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_link_at_the_destination_is_replaced_rather_than_written_through() {
        // Following the link would place a peer's bytes anywhere the link
        // addressed, which is the whole hole this confinement closes. Replacing
        // it keeps the write inside the receive root and leaves the target of
        // the link untouched.
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("downloads");
        let outside = directory.path().join("outside.txt");
        tokio::fs::create_dir_all(&root).await.expect("root");
        tokio::fs::write(&outside, b"untouched")
            .await
            .expect("outside file");
        std::os::unix::fs::symlink(&outside, root.join("report.txt")).expect("link");

        let (mut sender, receiver) = tokio::io::duplex(64);
        let send = tokio::spawn(async move {
            sender.write_all(b"peer").await.expect("write");
            let mut acknowledgement = [0_u8; 4];
            sender
                .read_exact(&mut acknowledgement)
                .await
                .expect("acknowledgement");
            drop(sender);
        });
        let outcome = receive_file(
            receiver,
            DccSessionId::new(),
            destination(&root, "report.txt").await,
            ReceiveOptions {
                conflict: DestinationConflict::Replace,
                expected_size: Some(4),
                max_bytes: 16,
                transfer: TransferOptions {
                    resume_offset: 0,
                    buffer_bytes: 16,
                    idle_timeout: Duration::from_secs(1),
                    progress: None,
                },
            },
        )
        .await
        .expect("receive");
        send.await.expect("sender");

        assert_eq!(outcome.relative_path, PathBuf::from("report.txt"));
        assert_eq!(
            tokio::fs::read(&outside).await.expect("read outside"),
            b"untouched"
        );
        assert_eq!(
            tokio::fs::read(root.join("report.txt"))
                .await
                .expect("read destination"),
            b"peer"
        );
        assert!(
            !tokio::fs::symlink_metadata(root.join("report.txt"))
                .await
                .expect("metadata")
                .is_symlink()
        );
    }

    #[tokio::test]
    async fn unknown_size_receive_stops_at_the_configured_limit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let bounded = directory.path().join("bounded.bin");
        let (mut sender, receiver) = tokio::io::duplex(64);
        let send = tokio::spawn(async move {
            sender.write_all(b"12345").await.expect("write");
            let mut acknowledgement = [0_u8; 4];
            sender
                .read_exact(&mut acknowledgement)
                .await
                .expect("acknowledgement");
        });
        let error = receive_file(
            receiver,
            DccSessionId::new(),
            destination(directory.path(), "bounded.bin").await,
            ReceiveOptions {
                conflict: DestinationConflict::Fail,
                expected_size: None,
                max_bytes: 4,
                transfer: TransferOptions {
                    resume_offset: 0,
                    buffer_bytes: 16,
                    idle_timeout: Duration::from_secs(1),
                    progress: None,
                },
            },
        )
        .await
        .expect_err("limit must be enforced");
        send.await.expect("sender");
        assert!(matches!(error, TransferError::SizeLimit { limit: 4 }));
        assert!(!bounded.exists());
    }
}
