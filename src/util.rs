use bytes::Bytes;
use rand::RngExt;

use crate::{codec, index::IndexType};

#[cfg(target_os = "linux")]
fn cgroup_v2_memory_limit(root: &std::path::Path, membership: &str) -> Option<u64> {
    let group = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?;
    let relative = std::path::Path::new(group.strip_prefix('/')?);
    if relative
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return None;
    }
    root.join(relative)
        .ancestors()
        .take_while(|path| path.starts_with(root))
        .filter_map(|path| std::fs::read_to_string(path.join("memory.max")).ok())
        .filter_map(|limit| limit.trim().parse::<u64>().ok())
        .min()
}

pub(crate) fn cgroup_memory_limit(detected: Option<u64>) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        // sysinfo reads the mount root; systemd scopes can have tighter nested limits.
        let nested = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|membership| {
                cgroup_v2_memory_limit(std::path::Path::new("/sys/fs/cgroup"), &membership)
            });
        detected.into_iter().chain(nested).min()
    }
    #[cfg(not(target_os = "linux"))]
    detected
}

pub(crate) fn rss_budget(total_memory: u64, cgroup_memory: Option<u64>) -> Option<u64> {
    let memory = cgroup_memory.map_or(total_memory, |limit| limit.min(total_memory));
    let budget = memory / 4 * 3;
    (budget > 0).then_some(budget)
}

pub fn random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();

    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

pub fn concat_bytes(parts: &[&[u8]]) -> Vec<u8> {
    let mut result = Vec::new();
    for part in parts {
        result.extend(*part);
    }
    result
}

pub trait GetSlice {
    fn get_slice(&self, start: usize, end: usize) -> Self;
}

impl GetSlice for &[u8] {
    fn get_slice(&self, start: usize, end: usize) -> Self {
        &self[start..end]
    }
}

impl GetSlice for Vec<u8> {
    fn get_slice(&self, start: usize, end: usize) -> Self {
        self[start..end].to_vec()
    }
}

impl GetSlice for Bytes {
    fn get_slice(&self, start: usize, end: usize) -> Self {
        self.slice(start..end)
    }
}

pub fn get_slice<T: GetSlice>(bytes: T, start: usize, end: usize) -> T {
    bytes.get_slice(start, end)
}

pub trait StripPrefix {
    fn strip_prefix(&self, prefix_len: usize) -> Self;
}

impl<T: GetSlice + AsRef<[u8]>> StripPrefix for T {
    fn strip_prefix(&self, prefix_len: usize) -> Self {
        self.get_slice(prefix_len, self.as_ref().len())
    }
}

pub fn strip_prefix<T: StripPrefix>(bytes: T, prefix_len: usize) -> T {
    bytes.strip_prefix(prefix_len)
}

pub trait StripSuffix {
    fn strip_suffix(&self, suffix_len: usize) -> Self;
}

impl<T: GetSlice + AsRef<[u8]>> StripSuffix for T {
    fn strip_suffix(&self, suffix_len: usize) -> Self {
        self.get_slice(0, self.as_ref().len() - suffix_len)
    }
}

pub fn strip_suffix<T: StripSuffix>(bytes: T, suffix_len: usize) -> T {
    bytes.strip_suffix(suffix_len)
}

fn check_position(position: usize, index: IndexType) {
    match index {
        IndexType::EAV | IndexType::AVE | IndexType::AEV | IndexType::VAE => {
            assert!(position < 3, "Position must be less than 3");
        }
        IndexType::AE | IndexType::AV => {
            assert!(position < 2, "Position must be less than 2");
        }
    }
}

// TODO  refactor this and prefix_len
pub fn make_extractor<T: GetSlice + AsRef<[u8]>>(
    position: usize,
    index: IndexType,
) -> impl Fn(T) -> T {
    move |bytes: T| {
        check_position(position, index);
        let total_length = bytes.as_ref().len();

        let (start, end) = match index {
            IndexType::EAV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                2.. => (
                    codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
                    total_length - codec::TX_EID_OP_SUFFIX,
                ),
            },
            IndexType::AVE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    total_length - codec::ENTITY_LENGTH - codec::TX_EID_OP_SUFFIX,
                ),
                2.. => (
                    total_length - codec::ENTITY_LENGTH - codec::TX_EID_OP_SUFFIX,
                    total_length - codec::TX_EID_OP_SUFFIX,
                ),
            },
            IndexType::AEV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1 => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
                ),
                2.. => (
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
                    total_length - codec::TX_EID_OP_SUFFIX,
                ),
            },
            IndexType::VAE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    total_length
                        - codec::ATTRIBUTE_LENGTH
                        - codec::ENTITY_LENGTH
                        - codec::TX_EID_OP_SUFFIX,
                ),
                1 => (
                    total_length
                        - codec::ATTRIBUTE_LENGTH
                        - codec::ENTITY_LENGTH
                        - codec::TX_EID_OP_SUFFIX,
                    total_length - codec::ENTITY_LENGTH - codec::TX_EID_OP_SUFFIX,
                ),
                2.. => (
                    total_length - codec::ENTITY_LENGTH - codec::TX_EID_OP_SUFFIX,
                    total_length - codec::TX_EID_OP_SUFFIX,
                ),
            },
            // AE/AV are atemporal — no T+op suffix
            IndexType::AE => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1.. => (codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH, total_length),
            },
            IndexType::AV => match position {
                0 => (
                    codec::CODEC_LENGTH,
                    codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                ),
                1.. => (codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH, total_length),
            },
        };
        bytes.get_slice(start, end)
    }
}

pub fn extract_value<T: GetSlice + AsRef<[u8]>>(bytes: T, position: usize, index: IndexType) -> T {
    make_extractor(position, index)(bytes)
}

pub fn prefix_extractor<T: GetSlice + AsRef<[u8]>>(
    position: usize,
    index: IndexType,
) -> impl Fn(T) -> T {
    move |bytes: T| {
        check_position(position, index);
        let total_length = bytes.as_ref().len();

        let end = match index {
            IndexType::EAV => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ENTITY_LENGTH,
                2.. => codec::CODEC_LENGTH + codec::ENTITY_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::AVE => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                2.. => total_length - codec::ENTITY_LENGTH - codec::TX_EID_OP_SUFFIX,
            },
            IndexType::AEV => match position {
                0 => codec::CODEC_LENGTH,
                1 => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
                2.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH + codec::ENTITY_LENGTH,
            },
            IndexType::VAE => match position {
                0 => codec::CODEC_LENGTH,
                1 => {
                    total_length
                        - codec::ATTRIBUTE_LENGTH
                        - codec::ENTITY_LENGTH
                        - codec::TX_EID_OP_SUFFIX
                }
                2.. => total_length - codec::ENTITY_LENGTH - codec::TX_EID_OP_SUFFIX,
            },
            IndexType::AE => match position {
                0 => codec::CODEC_LENGTH,
                1.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
            IndexType::AV => match position {
                0 => codec::CODEC_LENGTH,
                1.. => codec::CODEC_LENGTH + codec::ATTRIBUTE_LENGTH,
            },
        };
        bytes.get_slice(0, end)
    }
}

pub fn extract_prefix<T: GetSlice + AsRef<[u8]>>(bytes: T, position: usize, index: IndexType) -> T {
    prefix_extractor(position, index)(bytes)
}

/// Compute the lexicographic successor of a byte string.
/// Returns None if all bytes are 0xFF (no successor exists).
///
/// Note: this duplicates `BytesRange::increment_prefix` in SlateDB
/// (`slatedb/src/bytes_range.rs`), which is not publicly exposed.
pub fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    for i in (0..next.len()).rev() {
        if next[i] < 0xFF {
            next[i] += 1;
            next.truncate(i + 1);
            return Some(next);
        }
    }
    None
}

#[cfg(all(test, target_os = "linux"))]
mod memory_tests {
    use super::*;

    #[test]
    fn nested_scope_sets_rss_budget() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("user.slice/server.scope");
        std::fs::create_dir_all(&scope).unwrap();
        std::fs::write(root.path().join("memory.max"), "max\n").unwrap();
        std::fs::write(scope.join("memory.max"), (12_u64 << 30).to_string()).unwrap();

        let limit = cgroup_v2_memory_limit(root.path(), "0::/user.slice/server.scope\n");
        assert_eq!(limit, Some(12 << 30));
        assert_eq!(rss_budget(32 << 30, limit), Some(9 << 30));
        assert_eq!(rss_budget(8 << 30, limit), Some(6 << 30));
    }

    #[test]
    fn tighter_parent_limit_applies_to_unlimited_child() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("user.slice");
        let scope = parent.join("server.scope");
        std::fs::create_dir_all(&scope).unwrap();
        std::fs::write(parent.join("memory.max"), (8_u64 << 30).to_string()).unwrap();
        std::fs::write(scope.join("memory.max"), "max\n").unwrap();

        assert_eq!(
            cgroup_v2_memory_limit(root.path(), "0::/user.slice/server.scope\n"),
            Some(8 << 30)
        );
        std::fs::write(scope.join("memory.max"), (12_u64 << 30).to_string()).unwrap();
        assert_eq!(
            cgroup_v2_memory_limit(root.path(), "0::/user.slice/server.scope\n"),
            Some(8 << 30)
        );
    }

    #[test]
    fn unlimited_or_unavailable_cgroup_uses_host_budget() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("memory.max"), "max\n").unwrap();
        assert_eq!(cgroup_v2_memory_limit(root.path(), "0::/\n"), None);
        assert_eq!(cgroup_v2_memory_limit(root.path(), "1:memory:/\n"), None);
        assert_eq!(cgroup_v2_memory_limit(root.path(), "0::/../\n"), None);
        assert_eq!(rss_budget(16 << 30, None), Some(12 << 30));
        assert_eq!(rss_budget(0, None), None);
    }
}
