//! Confining every accepted DCC file to one configured receive root.
//!
//! A peer offers a filename and a caller names a destination; neither is
//! trusted, and the configured roots are the only filesystem authority in the
//! system. Resolving that safely is harder than comparing strings, because two
//! things can move while the comparison is being made:
//!
//! * **A symbolic link at any component.** Canonicalizing the destination's
//!   parent and checking that it starts with the root says nothing about the
//!   leaf. A link *named* as the destination is not a path to compare — it is a
//!   redirection applied at open time, and an ordinary `metadata` call follows
//!   it without saying so.
//! * **A swap between the check and the open.** Even a fully canonicalized path
//!   is a statement about the past. Anything that resolves a path once and then
//!   opens the same path again has agreed to be redirected in between, which is
//!   the classic time-of-check/time-of-use race.
//!
//! Both disappear if the resolution stops producing *paths* and starts producing
//! *capabilities*. This module walks the destination one component at a time,
//! opening each directory relative to the descriptor of the directory above it
//! and refusing to traverse a symbolic link, and hands the transfer a directory
//! it holds open plus a leaf name to create inside it. Every later
//! operation — the existence check, the temporary file, the commit rename, the
//! cleanup — names an entry *within that descriptor*. There is no second walk to
//! redirect: replacing a directory after it was opened leaves this process
//! holding the inode it verified, and the write lands where it was confined to
//! or nowhere.
//!
//! The policy — which path shapes are acceptable at all — lives here once and
//! runs identically everywhere. Only the primitives differ by platform: on Unix
//! they are `openat`/`mkdirat`/`statat`/`renameat`/`unlinkat` against a held
//! descriptor. Elsewhere the standard library offers no directory-relative
//! operation, so the fallback opens by path and *verifies afterwards* that the
//! directory it reached is still the one beneath the root, which closes the same
//! hole without the capability guarantee.

use std::{
    io,
    path::{Component, Path, PathBuf},
};

use tokio::fs::File;

use crate::config::DccReceiveRoot;

/// Where one accepted incoming file is to be written.
///
/// Both halves are explicit and replayable: the root is a configured name, and
/// the path is relative to it. An absolute path cannot appear here, because the
/// name is what carries the authority and a path that supplied its own root
/// would be claiming authority the configuration never granted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiveChoice {
    /// Configured root name from `dcc.receive_roots`.
    pub root: String,
    /// Destination relative to that root.
    pub path: PathBuf,
}

/// Why a destination could not be confined to its root.
#[derive(Debug, thiserror::Error)]
pub enum ConfinementError {
    /// The destination is absolute, or walks out of the root by name.
    #[error("DCC destination must be a relative path beneath the receive root, without '..': {0}")]
    NotRelative(PathBuf),
    /// The destination names no file.
    #[error("DCC destination must name a file: {0}")]
    NoFileName(PathBuf),
    /// The destination cannot be spelled in the naming this gateway uses for
    /// temporary and committed files.
    #[error("DCC destination must be valid UTF-8: {0}")]
    NotUtf8(PathBuf),
    /// A component of the destination is a symbolic link, so following it would
    /// place the file wherever the link points instead of beneath the root.
    #[error("DCC destination component is a link out of the receive root: {0}")]
    Link(PathBuf),
    /// A component of the destination exists and is not a directory.
    #[error("DCC destination path descends through something that is not a directory: {0}")]
    NotADirectory(PathBuf),
    /// The filesystem refused an operation.
    #[error("{operation}: {source}")]
    Io {
        /// Stable operation context.
        operation: &'static str,
        /// Underlying error.
        #[source]
        source: io::Error,
    },
}

impl ConfinementError {
    /// Attach operation context to a filesystem failure.
    fn io(operation: &'static str) -> impl FnOnce(io::Error) -> Self {
        move |source| Self::Io { operation, source }
    }
}

/// What a name inside a resolved destination's directory already refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Occupant {
    /// A directory. No transfer can be received onto one.
    Directory,
    /// Anything else that exists, including a symbolic link — reported as
    /// itself and never as whatever it points at, so an existing link is a
    /// conflict to resolve rather than a hole to write through.
    Entry,
}

/// One resolved receive destination: a directory this process holds open, plus
/// the name of the file to create inside it.
///
/// Constructing one is the whole confinement decision. Everything reachable
/// through the result is inside the configured root by construction, so the code
/// that streams bytes does not repeat — or weaken — the check.
#[derive(Debug)]
pub struct ReceiveDestination {
    directory: platform::Directory,
    file_name: String,
    root: String,
    /// Root-relative directory holding `file_name`; empty at the root itself.
    relative_directory: PathBuf,
}

impl ReceiveDestination {
    /// Resolve `relative` beneath `root`, creating the root and any intermediate
    /// directories the destination names.
    ///
    /// # Errors
    ///
    /// Refuses anything that is not a relative path of ordinary components, a
    /// destination that names no file, and any component that turns out to be a
    /// symbolic link.
    pub async fn resolve(root: &DccReceiveRoot, relative: &Path) -> Result<Self, ConfinementError> {
        let (directories, file_name) = split_relative(relative)?;
        let mut directory = platform::Directory::open_root(&root.path)
            .await
            .map_err(ConfinementError::io("open DCC receive root"))?;
        let mut relative_directory = PathBuf::new();
        for part in directories {
            directory = directory
                .open_child(part)
                .await
                .map_err(|error| match error {
                    ChildError::Link => ConfinementError::Link(relative_directory.join(part)),
                    ChildError::NotADirectory => {
                        ConfinementError::NotADirectory(relative_directory.join(part))
                    }
                    ChildError::Io(source) => ConfinementError::Io {
                        operation: "open DCC destination directory",
                        source,
                    },
                })?;
            relative_directory.push(part);
        }
        Ok(Self {
            directory,
            file_name: file_name.to_owned(),
            root: root.name.clone(),
            relative_directory,
        })
    }

    /// Configured root name this destination is confined to.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Name of the file inside the held directory.
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Destination relative to its configured root.
    pub fn relative_path(&self) -> PathBuf {
        self.relative_directory.join(&self.file_name)
    }

    /// Absolute destination, for reporting only.
    ///
    /// Deliberately not used to reopen anything: reopening by path is what this
    /// module exists to avoid.
    pub fn path(&self) -> PathBuf {
        self.directory.path().join(&self.file_name)
    }

    /// Point this destination at a different name in the same directory.
    ///
    /// Used only by conflict resolution, which chooses among names it has
    /// already probed inside the directory it holds.
    pub fn set_file_name(&mut self, file_name: String) {
        self.file_name = file_name;
    }

    /// What `name` inside the held directory currently refers to, if anything.
    pub async fn occupant(&self, name: &str) -> io::Result<Option<Occupant>> {
        self.directory.occupant(name).await
    }

    /// Create `name` inside the held directory, failing if it already exists.
    ///
    /// Exclusive creation is also what refuses a symbolic link planted at the
    /// name: the link exists, so the create fails rather than resolving through
    /// it.
    pub async fn create_new(&self, name: &str) -> io::Result<File> {
        self.directory.create_new(name).await
    }

    /// Open `name` inside the held directory for appending.
    pub async fn open_append(&self, name: &str) -> io::Result<File> {
        self.directory.open_append(name).await
    }

    /// Rename one name to another inside the held directory.
    ///
    /// Both ends name entries in the same held descriptor, so the commit cannot
    /// be re-routed by anything that changed the path it was resolved from.
    pub async fn rename_within(&self, from: &str, to: &str) -> io::Result<()> {
        self.directory.rename_within(from, to).await
    }

    /// Remove `name` inside the held directory.
    pub async fn remove(&self, name: &str) -> io::Result<()> {
        self.directory.remove(name).await
    }
}

/// Why opening a child directory failed.
enum ChildError {
    /// The child is a symbolic link.
    Link,
    /// The child exists and is not a directory.
    NotADirectory,
    /// The filesystem refused.
    Io(io::Error),
}

/// Split a destination into the directories it descends and the file it names.
///
/// Only ordinary components are accepted. That refuses an absolute path, a
/// platform prefix, and `..` in one place instead of trusting a later
/// canonicalization to notice — and `..` in particular cannot be judged by
/// inspection at all, because whether it escapes depends on what the earlier
/// components turn out to be at the moment they are opened. A leading `.` is
/// dropped rather than refused: it selects the directory the walk already starts
/// from and can never move it.
fn split_relative(relative: &Path) -> Result<(Vec<&str>, &str), ConfinementError> {
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| ConfinementError::NotUtf8(relative.to_path_buf()))?,
            ),
            Component::RootDir | Component::Prefix(_) | Component::ParentDir => {
                return Err(ConfinementError::NotRelative(relative.to_path_buf()));
            }
        }
    }
    let file_name = parts
        .pop()
        .ok_or_else(|| ConfinementError::NoFileName(relative.to_path_buf()))?;
    Ok((parts, file_name))
}

#[cfg(unix)]
mod platform {
    //! Directory-relative primitives against a descriptor this process holds.

    use std::{
        fs::File as StdFile,
        io,
        os::fd::OwnedFd,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use rustix::fs::{AtFlags, FileType, Mode, OFlags};
    use tokio::fs::File;

    use super::{ChildError, Occupant};

    /// Run one blocking filesystem call off the runtime threads.
    ///
    /// Every primitive here is a single syscall, but a single syscall on a
    /// network filesystem can block for a long time, and the `tokio::fs` calls
    /// this replaced were off-thread for exactly that reason.
    async fn blocking<T, F>(operation: F) -> io::Result<T>
    where
        F: FnOnce() -> io::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(io::Error::other)?
    }

    /// Permissions for a directory this gateway creates along a destination.
    const DIRECTORY_MODE: Mode = Mode::RWXU
        .union(Mode::RGRP)
        .union(Mode::XGRP)
        .union(Mode::ROTH)
        .union(Mode::XOTH);

    /// Permissions for a received file.
    const FILE_MODE: Mode = Mode::RUSR
        .union(Mode::WUSR)
        .union(Mode::RGRP)
        .union(Mode::ROTH);

    /// A directory held open as a capability.
    #[derive(Debug)]
    pub struct Directory {
        /// Shared so each blocking call can hold the descriptor alive without
        /// duplicating it.
        handle: Arc<OwnedFd>,
        path: PathBuf,
    }

    impl Directory {
        /// Open one directory by path, creating it and its parents when absent.
        ///
        /// This is the only path-based open in the module. The configured root
        /// is an operator decision, so resolving it through links is correct;
        /// everything *below* it is resolved from the descriptor this returns.
        pub async fn open_root(path: &Path) -> io::Result<Self> {
            tokio::fs::create_dir_all(path).await?;
            let path = tokio::fs::canonicalize(path).await?;
            let opened = path.clone();
            let handle = blocking(move || {
                rustix::fs::open(
                    &opened,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(io::Error::from)
            })
            .await?;
            Ok(Self {
                handle: Arc::new(handle),
                path,
            })
        }

        /// Absolute path of this directory, for reporting only.
        pub fn path(&self) -> &Path {
            &self.path
        }

        /// Open one child directory, creating it when absent and refusing to
        /// follow a link.
        pub async fn open_child(&self, name: &str) -> Result<Self, ChildError> {
            let handle = Arc::clone(&self.handle);
            let child = name.to_owned();
            let opened = tokio::task::spawn_blocking(move || open_child_blocking(&handle, &child))
                .await
                .map_err(|error| ChildError::Io(io::Error::other(error)))??;
            Ok(Self {
                handle: Arc::new(opened),
                path: self.path.join(name),
            })
        }

        /// What `name` inside this directory refers to, without following a link
        /// at that name.
        pub async fn occupant(&self, name: &str) -> io::Result<Option<Occupant>> {
            let handle = Arc::clone(&self.handle);
            let name = name.to_owned();
            blocking(move || {
                match rustix::fs::statat(&*handle, name.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
                    Ok(stat) => Ok(Some(
                        if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
                            Occupant::Directory
                        } else {
                            Occupant::Entry
                        },
                    )),
                    Err(error) if error == rustix::io::Errno::NOENT => Ok(None),
                    Err(error) => Err(io::Error::from(error)),
                }
            })
            .await
        }

        /// Create `name` inside this directory, failing if the name is taken.
        pub async fn create_new(&self, name: &str) -> io::Result<File> {
            let handle = Arc::clone(&self.handle);
            let name = name.to_owned();
            let opened = blocking(move || {
                rustix::fs::openat(
                    &*handle,
                    name.as_str(),
                    OFlags::WRONLY
                        | OFlags::CREATE
                        | OFlags::EXCL
                        | OFlags::NOFOLLOW
                        | OFlags::CLOEXEC,
                    FILE_MODE,
                )
                .map_err(io::Error::from)
            })
            .await?;
            Ok(File::from_std(StdFile::from(opened)))
        }

        /// Open `name` inside this directory for appending.
        pub async fn open_append(&self, name: &str) -> io::Result<File> {
            let handle = Arc::clone(&self.handle);
            let name = name.to_owned();
            let opened = blocking(move || {
                rustix::fs::openat(
                    &*handle,
                    name.as_str(),
                    OFlags::WRONLY | OFlags::APPEND | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(io::Error::from)
            })
            .await?;
            Ok(File::from_std(StdFile::from(opened)))
        }

        /// Rename one name to another inside this directory.
        pub async fn rename_within(&self, from: &str, to: &str) -> io::Result<()> {
            let handle = Arc::clone(&self.handle);
            let from = from.to_owned();
            let to = to.to_owned();
            blocking(move || {
                rustix::fs::renameat(&*handle, from.as_str(), &*handle, to.as_str())
                    .map_err(io::Error::from)
            })
            .await
        }

        /// Remove `name` inside this directory.
        pub async fn remove(&self, name: &str) -> io::Result<()> {
            let handle = Arc::clone(&self.handle);
            let name = name.to_owned();
            blocking(move || {
                rustix::fs::unlinkat(&*handle, name.as_str(), AtFlags::empty())
                    .map_err(io::Error::from)
            })
            .await
        }
    }

    /// Create and open one child directory relative to a held descriptor.
    ///
    /// `O_NOFOLLOW` is what makes the refusal reliable: the kernel resolves the
    /// final component itself and fails rather than handing back whatever a link
    /// addressed, so there is no window in which this process holds a descriptor
    /// it did not verify.
    fn open_child_blocking(parent: &OwnedFd, name: &str) -> Result<OwnedFd, ChildError> {
        if let Err(error) = rustix::fs::mkdirat(parent, name, DIRECTORY_MODE)
            && error != rustix::io::Errno::EXIST
        {
            return Err(ChildError::Io(error.into()));
        }
        rustix::fs::openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            // Linux answers `O_DIRECTORY | O_NOFOLLOW` on a link with `ENOTDIR`
            // rather than `ELOOP`, so the errno alone cannot say whether a link
            // or an ordinary file is in the way. The refusal has already
            // happened; this second look only decides which of the two an
            // operator is told about, and cannot weaken it.
            if error == rustix::io::Errno::LOOP {
                ChildError::Link
            } else if error == rustix::io::Errno::NOTDIR {
                match rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
                    Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::Symlink => {
                        ChildError::Link
                    }
                    _ => ChildError::NotADirectory,
                }
            } else {
                ChildError::Io(error.into())
            }
        })
    }
}

#[cfg(not(unix))]
mod platform {
    //! Path-based primitives that verify what they reached.
    //!
    //! Without directory-relative calls the best available protocol is
    //! open-then-verify: create the directory, then confirm that resolving it
    //! still lands exactly where the walk expected. That refuses a link planted
    //! at any component, and the exclusive create refuses one planted at the
    //! leaf. It cannot offer the Unix guarantee that no second resolution
    //! happens at all, so the window it leaves is the interval between the
    //! verification and the next call rather than the whole transfer.

    use std::{
        io,
        path::{Path, PathBuf},
    };

    use tokio::fs::{File, OpenOptions};

    use super::{ChildError, Occupant};

    /// A directory addressed by a path that has been verified to be the one
    /// beneath its root.
    #[derive(Debug)]
    pub struct Directory {
        path: PathBuf,
    }

    impl Directory {
        /// Open one directory by path, creating it and its parents when absent.
        pub async fn open_root(path: &Path) -> io::Result<Self> {
            tokio::fs::create_dir_all(path).await?;
            Ok(Self {
                path: tokio::fs::canonicalize(path).await?,
            })
        }

        /// Absolute path of this directory, for reporting only.
        pub fn path(&self) -> &Path {
            &self.path
        }

        /// Create and enter one child directory, refusing a link.
        pub async fn open_child(&self, name: &str) -> Result<Self, ChildError> {
            let expected = self.path.join(name);
            match tokio::fs::create_dir(&expected).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(ChildError::Io(error)),
            }
            // This directory's own path is already canonical, so a canonical
            // child that is not the name joined onto it was reached through a
            // link and is not the directory the walk asked for.
            let reached = tokio::fs::canonicalize(&expected)
                .await
                .map_err(ChildError::Io)?;
            if reached != expected {
                return Err(ChildError::Link);
            }
            if !tokio::fs::metadata(&reached)
                .await
                .map_err(ChildError::Io)?
                .is_dir()
            {
                return Err(ChildError::NotADirectory);
            }
            Ok(Self { path: reached })
        }

        /// What `name` inside this directory refers to, without following a link
        /// at that name.
        pub async fn occupant(&self, name: &str) -> io::Result<Option<Occupant>> {
            match tokio::fs::symlink_metadata(self.path.join(name)).await {
                Ok(metadata) if metadata.is_dir() => Ok(Some(Occupant::Directory)),
                Ok(_) => Ok(Some(Occupant::Entry)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error),
            }
        }

        /// Create `name` inside this directory, failing if the name is taken.
        pub async fn create_new(&self, name: &str) -> io::Result<File> {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.path.join(name))
                .await
        }

        /// Open `name` inside this directory for appending.
        pub async fn open_append(&self, name: &str) -> io::Result<File> {
            OpenOptions::new()
                .append(true)
                .open(self.path.join(name))
                .await
        }

        /// Rename one name to another inside this directory.
        pub async fn rename_within(&self, from: &str, to: &str) -> io::Result<()> {
            tokio::fs::rename(self.path.join(from), self.path.join(to)).await
        }

        /// Remove `name` inside this directory.
        pub async fn remove(&self, name: &str) -> io::Result<()> {
            tokio::fs::remove_file(self.path.join(name)).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(path: &Path) -> DccReceiveRoot {
        DccReceiveRoot {
            name: "downloads".into(),
            path: path.to_path_buf(),
        }
    }

    #[test]
    fn a_destination_must_be_a_relative_path_that_names_a_file() {
        assert!(matches!(
            split_relative(Path::new("../escape.txt")),
            Err(ConfinementError::NotRelative(_))
        ));
        assert!(matches!(
            split_relative(Path::new("nested/../escape.txt")),
            Err(ConfinementError::NotRelative(_))
        ));
        assert!(matches!(
            split_relative(Path::new("")),
            Err(ConfinementError::NoFileName(_))
        ));
        assert_eq!(
            split_relative(Path::new("./nested/report.txt")).expect("relative"),
            (vec!["nested"], "report.txt")
        );
        assert_eq!(
            split_relative(Path::new("report.txt")).expect("relative"),
            (Vec::new(), "report.txt")
        );
    }

    #[test]
    fn an_absolute_destination_is_refused_whatever_it_points_at() {
        // The root name carries the authority. A destination that supplied its
        // own root would be claiming authority the configuration never granted,
        // even when the path happens to sit inside a configured root.
        let absolute = if cfg!(windows) {
            Path::new(r"C:\srv\downloads\report.txt")
        } else {
            Path::new("/srv/downloads/report.txt")
        };
        assert!(matches!(
            split_relative(absolute),
            Err(ConfinementError::NotRelative(_))
        ));
    }

    #[tokio::test]
    async fn a_nested_relative_destination_resolves_beneath_its_root() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root_path = directory.path().join("downloads");
        let destination =
            ReceiveDestination::resolve(&root(&root_path), Path::new("reports/august/report.txt"))
                .await
                .expect("resolve");

        assert_eq!(destination.root(), "downloads");
        assert_eq!(destination.file_name(), "report.txt");
        assert_eq!(
            destination.relative_path(),
            PathBuf::from("reports/august/report.txt")
        );
        assert!(
            destination.path().starts_with(
                tokio::fs::canonicalize(&root_path)
                    .await
                    .expect("canonical root")
            )
        );
        assert!(root_path.join("reports/august").is_dir());
    }

    #[tokio::test]
    async fn traversal_out_of_the_root_is_refused_before_any_directory_is_made() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root_path = directory.path().join("downloads");
        let error = ReceiveDestination::resolve(&root(&root_path), Path::new("../escape.txt"))
            .await
            .expect_err("traversal must be refused");

        assert!(matches!(error, ConfinementError::NotRelative(_)));
        assert!(!directory.path().join("escape.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_directory_replaced_by_a_link_before_resolution_does_not_escape() {
        // The link is planted exactly where the walk will look, which is the
        // strongest form of the race a check-then-open resolution loses: by the
        // time the destination is opened, the name it was verified under no
        // longer refers to a directory inside the root.
        let directory = tempfile::tempdir().expect("tempdir");
        let root_path = directory.path().join("downloads");
        let outside = directory.path().join("outside");
        tokio::fs::create_dir_all(&root_path).await.expect("root");
        tokio::fs::create_dir_all(&outside).await.expect("outside");
        std::os::unix::fs::symlink(&outside, root_path.join("reports")).expect("link");

        let error = ReceiveDestination::resolve(&root(&root_path), Path::new("reports/report.txt"))
            .await
            .expect_err("a link component must be refused");

        assert!(
            matches!(error, ConfinementError::Link(_)),
            "unexpected error: {error}"
        );
        assert!(!outside.join("report.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_link_at_the_destination_name_is_a_conflict_and_never_a_hole() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root_path = directory.path().join("downloads");
        let outside = directory.path().join("outside.txt");
        tokio::fs::create_dir_all(&root_path).await.expect("root");
        tokio::fs::write(&outside, b"original")
            .await
            .expect("outside file");
        std::os::unix::fs::symlink(&outside, root_path.join("report.txt")).expect("link");

        let destination = ReceiveDestination::resolve(&root(&root_path), Path::new("report.txt"))
            .await
            .expect("resolve");

        // The link is reported as itself, so conflict handling sees an occupied
        // name rather than following it and reporting the target.
        assert_eq!(
            destination.occupant("report.txt").await.expect("occupant"),
            Some(Occupant::Entry)
        );
        assert_eq!(
            destination
                .create_new("report.txt")
                .await
                .expect_err("exclusive creation must refuse an existing link")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            tokio::fs::read(&outside).await.expect("read outside"),
            b"original"
        );
    }

    #[tokio::test]
    async fn a_committed_file_lands_inside_the_held_directory() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root_path = directory.path().join("downloads");
        let destination =
            ReceiveDestination::resolve(&root(&root_path), Path::new("nested/report.txt"))
                .await
                .expect("resolve");

        destination.create_new("temporary").await.expect("create");
        assert_eq!(
            destination.occupant("temporary").await.expect("occupant"),
            Some(Occupant::Entry)
        );
        destination
            .rename_within("temporary", "report.txt")
            .await
            .expect("commit");
        assert!(root_path.join("nested/report.txt").is_file());

        destination.remove("report.txt").await.expect("remove");
        assert_eq!(
            destination.occupant("report.txt").await.expect("gone"),
            None
        );
    }

    #[tokio::test]
    async fn a_directory_in_the_way_of_the_destination_is_reported_as_one() {
        let directory = tempfile::tempdir().expect("tempdir");
        let root_path = directory.path().join("downloads");
        let destination = ReceiveDestination::resolve(&root(&root_path), Path::new("report.txt"))
            .await
            .expect("resolve");
        tokio::fs::create_dir_all(root_path.join("report.txt"))
            .await
            .expect("directory in the way");

        assert_eq!(
            destination.occupant("report.txt").await.expect("occupant"),
            Some(Occupant::Directory)
        );
    }
}
