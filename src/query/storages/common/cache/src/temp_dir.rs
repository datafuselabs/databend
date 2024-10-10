// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::create_dir;
use std::fs::create_dir_all;
use std::fs::remove_dir_all;
use std::hash::Hash;
use std::io::ErrorKind;
use std::ops::Deref;
use std::ops::Drop;
use std::path::Path;
use std::path::PathBuf;
use std::ptr::Alignment;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;

use databend_common_base::base::GlobalInstance;
use databend_common_base::base::GlobalUniqName;
use databend_common_config::SpillConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use rustix::fs::statvfs;

pub struct TempDirManager {
    root: Option<Box<Path>>,

    // Global limit in bytes
    global_limit: usize,
    // Reserved disk space in blocks
    reserved: u64,
    alignment: Alignment,

    group: Mutex<Group>,
}

impl TempDirManager {
    pub fn init(config: &SpillConfig, tenant_id: &str) -> Result<()> {
        let (root, reserved, alignment) = if config.path.is_empty() {
            (None, 0, Alignment::MIN)
        } else {
            let path = PathBuf::from(&config.path)
                .join(tenant_id)
                .into_boxed_path();

            if let Err(e) = remove_dir_all(&path) {
                if !matches!(e.kind(), ErrorKind::NotFound) {
                    return Err(ErrorCode::StorageUnavailable(format!(
                        "can't clean temp dir: {e}",
                    )));
                }
            }

            if create_dir_all(&path).is_err() {
                (None, 0, Alignment::MIN)
            } else {
                let stat =
                    statvfs(path.as_ref()).map_err(|e| ErrorCode::StorageOther(e.to_string()))?;

                (
                    Some(path),
                    (stat.f_blocks as f64 * *config.reserved_disk_ratio) as u64,
                    Alignment::new(stat.f_bsize.max(512) as usize).unwrap(),
                )
            }
        };

        GlobalInstance::set(Arc::new(Self {
            root,
            global_limit: config.global_bytes_limit as usize,
            reserved,
            alignment,
            group: Mutex::new(Group {
                dirs: HashMap::new(),
            }),
        }));
        Ok(())
    }

    pub fn instance() -> Arc<TempDirManager> {
        GlobalInstance::get()
    }

    pub fn get_disk_spill_dir(
        self: &Arc<TempDirManager>,
        limit: usize,
        query_id: &str,
    ) -> Option<Arc<TempDir>> {
        if limit == 0 {
            return None;
        }

        let path = self.root.as_ref()?.join(query_id).into_boxed_path();
        let mut group = self.group.lock().unwrap();
        let dir = match group.dirs.entry(path.clone()) {
            Entry::Occupied(o) => TempDir {
                path,
                dir_info: o.get().clone(),
                manager: self.clone(),
            },
            Entry::Vacant(v) => {
                let dir_info = Arc::new(DirInfo {
                    limit,
                    count: Default::default(),
                    size: Default::default(),
                    inited: Once::new(),
                });
                v.insert(dir_info.clone());
                TempDir {
                    path,
                    dir_info,
                    manager: self.clone(),
                }
            }
        };
        Some(Arc::new(dir))
    }

    pub fn drop_disk_spill_dir(self: &Arc<TempDirManager>, query_id: &str) -> Result<bool> {
        let path = match self.root.as_ref() {
            None => return Ok(false),
            Some(root) => root.join(query_id).into_boxed_path(),
        };

        let mut group = self.group.lock().unwrap();
        if group.dirs.remove(&path).is_some() {
            match remove_dir_all(&path) {
                Ok(_) => return Ok(true),
                Err(e) if matches!(e.kind(), ErrorKind::NotFound) => {}
                res => res?,
            }
        }
        Ok(false)
    }

    pub fn drop_disk_spill_dir_unknown(
        self: &Arc<TempDirManager>,
        limit: usize,
    ) -> Result<Vec<Box<Path>>> {
        match self.root.as_ref() {
            None => Ok(vec![]),
            Some(root) => {
                let read_dir = std::fs::read_dir(root)?;
                let group = self.group.lock().unwrap();
                let to_delete = read_dir
                    .filter_map(|entry| match entry {
                        Err(_) => None,
                        Ok(entry) => {
                            let path = entry.path().into_boxed_path();
                            if group.dirs.contains_key(&path) {
                                None
                            } else {
                                Some(path)
                            }
                        }
                    })
                    .take(limit)
                    .collect::<Vec<_>>();
                drop(group);
                for path in &to_delete {
                    remove_dir_all(path)?;
                }
                Ok(to_delete)
            }
        }
    }

    pub fn block_alignment(&self) -> Alignment {
        self.alignment
    }

    fn insufficient_disk(&self, size: u64) -> Result<bool> {
        let stat = statvfs(self.root.as_ref().unwrap().as_ref())
            .map_err(|e| ErrorCode::Internal(e.to_string()))?;
        Ok(stat.f_bavail < self.reserved + (size + stat.f_frsize - 1) / stat.f_frsize)
    }
}

struct Group {
    dirs: HashMap<Box<Path>, Arc<DirInfo>>,
}

impl Group {
    fn size(&self) -> usize {
        self.dirs.values().map(|v| *v.size.lock().unwrap()).sum()
    }
}

#[derive(Clone)]
pub struct TempDir {
    path: Box<Path>,
    dir_info: Arc<DirInfo>,
    manager: Arc<TempDirManager>,
}

impl TempDir {
    // It should be ensured that the actual size is less than or equal to
    // the reserved size as much as possible, otherwise the limit may be exceeded.
    pub fn new_file_with_size(&self, size: usize) -> Result<Option<TempPath>> {
        let path = self.path.join(GlobalUniqName::unique()).into_boxed_path();

        if self.dir_info.limit < *self.dir_info.size.lock().unwrap() + size
            || self.manager.global_limit < self.manager.group.lock().unwrap().size() + size
            || self.manager.insufficient_disk(size as u64)?
        {
            return Ok(None);
        }

        let mut dir_size = self.dir_info.size.lock().unwrap();
        if self.dir_info.limit < *dir_size + size {
            return Ok(None);
        }

        *dir_size += size;
        drop(dir_size);

        self.init_dir()?;

        let dir_info = self.dir_info.clone();
        dir_info.count.fetch_add(1, Ordering::SeqCst);

        Ok(Some(TempPath(Arc::new(InnerPath {
            path,
            size,
            dir_info,
        }))))
    }

    fn init_dir(&self) -> Result<()> {
        let mut rt = Ok(());
        self.dir_info.inited.call_once(|| {
            if let Err(e) = create_dir(&self.path) {
                if !matches!(e.kind(), ErrorKind::AlreadyExists) {
                    rt = Err(e);
                }
            }
        });
        Ok(rt?)
    }

    pub fn block_alignment(&self) -> Alignment {
        self.manager.alignment
    }
}

struct DirInfo {
    limit: usize,
    count: AtomicUsize,
    size: Mutex<usize>,
    inited: Once,
}

impl Debug for DirInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirInfo")
            .field("limit", &self.limit)
            .field("count", &self.count)
            .field("size", &self.size)
            .field("inited", &self.inited.is_completed())
            .finish()
    }
}

#[derive(Clone)]
pub struct TempPath(Arc<InnerPath>);

impl Debug for TempPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempPath")
            .field("path", &self.0.path)
            .field("size", &self.0.size)
            .field("dir_info", &self.0.dir_info)
            .finish()
    }
}

impl Hash for TempPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.path.hash(state);
    }
}

impl PartialEq for TempPath {
    fn eq(&self, other: &Self) -> bool {
        self.0.path == other.0.path
    }
}

impl Eq for TempPath {}

impl AsRef<Path> for TempPath {
    fn as_ref(&self) -> &Path {
        self.0.path.as_ref()
    }
}

impl Deref for TempPath {
    type Target = Path;

    fn deref(&self) -> &Path {
        self.as_ref()
    }
}

impl TempPath {
    pub fn size(&self) -> usize {
        self.0.size
    }

    pub fn set_size(&mut self, size: usize) -> std::result::Result<(), &'static str> {
        use std::cmp::Ordering;

        let Some(path) = Arc::get_mut(&mut self.0) else {
            return Err("can't set size after share");
        };
        match size.cmp(&path.size) {
            Ordering::Equal => {}
            Ordering::Greater => {
                let mut dir = path.dir_info.size.lock().unwrap();
                *dir += size - path.size;
            }
            Ordering::Less => {
                let mut dir = path.dir_info.size.lock().unwrap();
                *dir -= path.size - size;
            }
        }
        path.size = size;
        Ok(())
    }
}

struct InnerPath {
    path: Box<Path>,
    size: usize,
    dir_info: Arc<DirInfo>,
}

impl Drop for InnerPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);

        self.dir_info.count.fetch_sub(1, Ordering::SeqCst);
        let mut guard = self.dir_info.size.lock().unwrap();
        *guard -= self.size;
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::ffi::OsString;
    use std::fs;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_temp_dir() -> Result<()> {
        let thread = std::thread::current();
        GlobalInstance::init_testing(thread.name().unwrap());

        let config = SpillConfig {
            path: OsString::from("test_data"),
            reserved_disk_ratio: 0.01.into(),
            global_bytes_limit: 1 << 30,
        };

        TempDirManager::init(&config, "test_tenant")?;

        let mgr = TempDirManager::instance();
        let dir = mgr.get_disk_spill_dir(1 << 30, "some_query").unwrap();
        let mut path = dir.new_file_with_size(110)?.unwrap();

        println!("{:?}", &path);

        fs::write(&path, vec![b'a'; 100])?;
        path.set_size(100).unwrap();

        assert_eq!(1, dir.dir_info.count.load(Ordering::Relaxed));
        assert_eq!(100, *dir.dir_info.size.lock().unwrap());

        let path_str = path.as_ref().to_str().unwrap().to_string();
        drop(path);

        assert_eq!(0, dir.dir_info.count.load(Ordering::Relaxed));
        assert_eq!(0, *dir.dir_info.size.lock().unwrap());

        assert_matches!(fs::read_to_string(path_str), Err(_));

        mgr.drop_disk_spill_dir("some_query")?;

        remove_dir_all("test_data")?;

        Ok(())
    }

    #[test]
    fn test_drop_disk_spill_dir_unknown() -> Result<()> {
        let thread = std::thread::current();
        GlobalInstance::init_testing(thread.name().unwrap());

        let config = SpillConfig {
            path: OsString::from("test_data2"),
            reserved_disk_ratio: 0.99.into(),
            global_bytes_limit: 1 << 30,
        };

        TempDirManager::init(&config, "test_tenant")?;

        let mgr = TempDirManager::instance();
        mgr.get_disk_spill_dir(1 << 30, "some_query").unwrap();

        create_dir("test_data2/test_tenant/unknown_query1")?;
        create_dir("test_data2/test_tenant/unknown_query2")?;

        let mut deleted = mgr.drop_disk_spill_dir_unknown(10)?;

        deleted.sort();

        assert_eq!(
            vec![
                PathBuf::from("test_data2/test_tenant/unknown_query1").into_boxed_path(),
                PathBuf::from("test_data2/test_tenant/unknown_query2").into_boxed_path(),
            ],
            deleted
        );

        remove_dir_all("test_data2")?;

        Ok(())
    }
}
