use std::path::{Path, PathBuf};
use std::path::Component;
use std::collections::HashMap;
use anyhow::Context;
use log::debug;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::blob_storage;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct EntryId {
    id: usize
}

impl EntryId {
    pub fn to_usize(&self) -> usize {
        self.id
    }

    pub fn from_usize(val: usize) -> Self {
        Self { id: val }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Directory {
    name: String,
    entries: HashMap<String, EntryId>
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct BlobKey {
    key: blake3::Hash
}

impl TryFrom<&str> for BlobKey {
    type Error = anyhow::Error;

    fn try_from(hex: &str) -> Result<Self, Self::Error> {
        let key = blake3::Hash::from_hex(hex).context("Could not convert hex str to hash/blobkey")?;
        Ok(Self {key})
    }
}

impl BlobKey {
    fn to_string(&self) -> String {
        self.key.to_hex().to_string()
    }
}

impl fmt::Debug for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hash_hex = self.key.to_hex();
        write!(f, "{}", hash_hex.as_str())
    }
}

impl Default for BlobKey {
    fn default() -> Self {
        let all_zero = [0; blake3::OUT_LEN];
        Self {
            key: blake3::Hash::from_bytes(all_zero)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct File {
    name: String,
    blob_key: BlobKey,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Entry {
    Directory(Directory),
    File(File)
}

impl Entry {

    fn try_file_ref(&self) -> anyhow::Result<&File> {
        if let Entry::File(x) = self { Ok(x) } else { anyhow::bail!("Tried to force enum type but it's the wrong one") }
    }

    fn try_directory_ref(&self) -> anyhow::Result<&Directory> {
        if let Entry::Directory(x) = self { Ok(x) } else { anyhow::bail!("Tried to force enum type but it's the wrong one") }
    }

    fn try_directory_ref_mut(&mut self) -> anyhow::Result<&mut Directory> {
        if let Entry::Directory(x) = self { Ok(x) } else { anyhow::bail!("Tried to force enum type but it's the wrong one") }
    }

    fn name(&self) -> &str {
        match self {
            Entry::Directory(dir) => dir.name.as_str(),
            Entry::File(file) => file.name.as_str(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Manifest {
    root: EntryId,
    entries: Vec<Entry>
}

#[derive(Debug, Default)]
pub struct Stats {
    num_dirs: usize,
    num_files: usize
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

impl Manifest {
    pub fn new() -> Self {
        let root_entry = Entry::Directory(Directory { name: "ROOT".to_string(), entries: HashMap::new() });
        Self {
            root: EntryId::from_usize(0),
            entries: vec![root_entry]
        }
    }

    fn get_entry(&self, id: EntryId) -> &Entry {
        &self.entries[id.to_usize()]
    }

    fn join_and_get_entry_id(&self, base: EntryId, path_add: &Path) -> anyhow::Result<EntryId> {
        let mut cd = self.entries[base.to_usize()].try_directory_ref()?;
        let mut last_entry_id = None;
        for component in path_add.components() {
            match component {
                Component::RootDir => anyhow::bail!("Should not have root component in path_add"),
                Component::Normal(component) => {
                    let component_str = component.to_str().expect("Why would component be None here");
                    let entry_id = cd.entries.get(component_str)
                        .with_context(|| format!("Entry {} not found in cd {}", component_str, cd.name))?;
                    let entry = &self.entries[entry_id.to_usize()];
                    last_entry_id = Some(*entry_id);
                    if let Entry::Directory(directory) = entry {
                        cd = &directory;
                    }
                },
                _ => anyhow::bail!("Cannot handle path components other than root/normal")
            };
        }
        if path_add.components().count() == 0 {
            return Ok(base);
        }
        last_entry_id.context("last_entry is none?")
    }

    fn add(&mut self, entry: Entry, parent_dir: EntryId) -> anyhow::Result<EntryId> {
        {
            let parent_dir = self.entries[parent_dir.to_usize()].try_directory_ref()?;
            let maybe_exists = parent_dir.entries.get(entry.name());
            if maybe_exists.is_some() {
                anyhow::bail!("Entry with same name exists")
            }
        }
        let entry_name = entry.name().to_string();
        let entry_id = EntryId::from_usize(self.entries.len());
        self.entries.push(entry);
        let parent_dir = self.entries[parent_dir.to_usize()].try_directory_ref_mut()?;
        parent_dir.entries.insert(entry_name, entry_id);
        Ok(entry_id)
    }

    fn add_file(&mut self, file: File, parent_dir: EntryId) -> anyhow::Result<EntryId> {
        self.add(Entry::File(file), parent_dir)
    }

    fn add_dir(&mut self, dir: Directory, parent_dir: EntryId) -> anyhow::Result<EntryId> {
        self.add(Entry::Directory(dir), parent_dir)
    }

    pub fn from_fs(fs_dir: &Path) -> anyhow::Result<Self> {
        let mut me = Self::new();
        me.add_dir_from_fs(me.root, fs_dir)?;
        Ok(me)
    }

    fn add_dir_from_fs(&mut self, dir: EntryId, fs_dir: &Path) -> anyhow::Result<()>  {
        let fs_dir_content = std::fs::read_dir(fs_dir).context("Reading fs_dir")?;
        for fs_dir_entry in fs_dir_content {
            let fs_dir_entry = fs_dir_entry.context("Reading fs_dir entry")?;
            let file_type = fs_dir_entry.file_type().context("Getting file type")?;
            let entry_name = fs_dir_entry.file_name().into_string().expect("Convert osstr to string");

            if file_type.is_dir() {
                let manifest_entry = Entry::Directory(Directory {name: entry_name, entries: HashMap::new()});
                let new_dir = self.add(manifest_entry, dir)?;
                self.add_dir_from_fs(new_dir, &fs_dir_entry.path())?;
            }
            else if file_type.is_file() {
                let size = fs_dir_entry.metadata().context("Getting file metadata")?.len();
                let manifest_entry = Entry::File(File {name: entry_name, blob_key: BlobKey::default(), size});
                self.add(manifest_entry, dir)?;
            }
        }
        Ok(())
    }

    pub fn get_stats(&self) -> Stats {
        let mut stats = Stats::default();
        for entry in &self.entries {
            match entry {
                Entry::Directory(_) => {
                    stats.num_dirs += 1;
                },
                Entry::File(_) => {
                    stats.num_files += 1;
                }
            }
        }
        stats
    }

    pub fn save_as_file(&self, path: &Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create(path).context("Create/open file for saving manifest")?;
        rmp_serde::encode::write(&mut file, &self).context("Serialize/write manifest into file")?;
        Ok(())
    }

    pub fn to_bytes(&self) -> anyhow::Result<bytes::Bytes> {
        let serialized = rmp_serde::encode::to_vec(&self).context("Serialize manifest into bytes")?;
        Ok(bytes::Bytes::from(serialized))
    }

    pub fn from_bytes(bytes: bytes::Bytes) -> anyhow::Result<Self> {
        let manifest: Self = rmp_serde::decode::from_slice(&bytes)?;
        Ok(manifest)
    }

    // map each entry to its parent
    fn get_map_parent(&self) -> HashMap<EntryId, EntryId> {

        let mut map = HashMap::new();
        let mut dirs_to_visit = vec![self.root];

        while let Some(dir_entry_id) = dirs_to_visit.pop() {
            let dir = self.get_entry(dir_entry_id).try_directory_ref().unwrap();

            for sub_entry_id in dir.entries.values().cloned() {
                let sub_entry = self.get_entry(sub_entry_id);
                map.insert(sub_entry_id, dir_entry_id);
                match sub_entry {
                    Entry::File(_) => {},
                    Entry::Directory(_) => {
                        dirs_to_visit.push(sub_entry_id)
                    }
                }
            }
        }

        map
    }

    fn get_full_path(&self, entry_id: EntryId, map_parent: &HashMap<EntryId, EntryId>) -> PathBuf {
        if entry_id == self.root {
            return PathBuf::from("");
        }

        let mut components = vec![self.get_entry(entry_id).name()];
        let mut parent_id = map_parent.get(&entry_id).unwrap();
        while parent_id != &self.root {
            components.push(self.get_entry(*parent_id).name());
            parent_id = map_parent.get(parent_id).unwrap();
        }
        PathBuf::from_iter(components.iter().rev())
    }

    // allows to use get_full_path above, without the user having to build/pass map_parent
    // the lifetime/borrow check should also prevent manifest from changing without map_parent changing
    pub fn get_full_path_getter<'a>(&'a self) -> impl Fn(EntryId) -> PathBuf + 'a {
        let map_parent = self.get_map_parent();
        move |entry_id: EntryId| -> PathBuf {
            self.get_full_path(entry_id, &map_parent)
        }
    }

    pub fn get_child_files_recurs(&self, entry_id: EntryId) -> Vec<EntryId> {
        let entry = self.get_entry(entry_id);
        if let Entry::File(_) = entry {
            return vec![entry_id];
        }

        let mut to_visit: Vec<EntryId> = entry.try_directory_ref().unwrap().entries.values().cloned().collect();
        let mut child_files = Vec::new();

        while let Some(entry_id) = to_visit.pop() {
            let entry = self.get_entry(entry_id);
            match entry {
                Entry::File(_) => child_files.push(entry_id),
                Entry::Directory(dir) => to_visit.extend(dir.entries.values().clone()),
            }
        }

        child_files
    }

    pub fn get_child_dirs_recurs(&self, entry_id: EntryId) -> Vec<EntryId> {
        let entry = self.get_entry(entry_id);
        if let Entry::File(_) = entry {
            return Vec::new();
        }

        let mut to_visit: Vec<EntryId> = entry.try_directory_ref().unwrap().entries.values().cloned().collect();
        let mut child_dirs = vec![entry_id];

        while let Some(entry_id) = to_visit.pop() {
            let entry = self.get_entry(entry_id);
            match entry {
                Entry::File(_) => (),
                Entry::Directory(dir) => {
                    child_dirs.push(entry_id);
                    to_visit.extend(dir.entries.values().clone())
                },
            }
        }

        child_dirs
    }

    // this method does not really make sense
    // but should we make entry, file, directory pub instead?
    pub fn get_file_key_and_size(&self, entry_id: EntryId) -> anyhow::Result<(String, u64)> {
        let entry = self.get_entry(entry_id);
        let file = entry.try_file_ref()?;
        Ok((file.blob_key.to_string(), file.size))
    }
}

fn print_entry(manifest: &Manifest, entry: &Entry, indent: usize) {
    match entry {
        Entry::File(file) => println!("{}{:?}", " ".repeat(indent), file),
        Entry::Directory(dir) => {
            println!("{}{}", " ".repeat(indent), dir.name);
            for &entry_id in dir.entries.values() {
                let entry = manifest.get_entry(entry_id);
                print_entry(manifest, entry, indent + 2);
            }
        }
    }
}

pub fn print_tree(manifest: &Manifest) {
    print_entry(manifest, manifest.get_entry(manifest.root), 0);
}

#[derive(Default)]
pub struct DiffManifests {
    // top means non recursive, in other words not total
    // if not mentioned, it is recursive/total
    pub top_extra_ids_in_a: Vec<EntryId>,
    pub paths_of_top_extra_in_a: Vec<PathBuf>,
    pub extra_files_in_a: usize,
    pub extra_dirs_in_a: usize,
    pub paths_of_different_files: Vec<PathBuf>,
    dirs_num_files_dirs: HashMap<EntryId, (usize, usize)>, // recursive number of (files, dirs) in a dir
    archive_root: PathBuf,
    bucket_name: String,
    hash_check: bool,
    already_called: bool,
}

impl fmt::Display for DiffManifests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        writeln!(f, "top_extra_ids_in_a:{:?}", self.top_extra_ids_in_a)?;
        writeln!(f, "paths_of_top_extra_in_a:{:?}", self.paths_of_top_extra_in_a)?;
        writeln!(f, "extra_files_in_a:{:?}", self.extra_files_in_a)?;
        writeln!(f, "extra_dirs_in_a:{:?}", self.extra_dirs_in_a)
    }
}

impl DiffManifests {
    pub fn with_hash_check(mut self, archive_root: PathBuf, bucket_name: String) -> Self {
        self.hash_check = true;
        self.archive_root = archive_root;
        self.bucket_name = bucket_name;
        self
    }

    pub fn diff_manifests(mut self, manifest_a: &Manifest, manifest_b: &Manifest) -> Self {

        assert!(!self.already_called);
        self.already_called = true;

        let root_dir_a = manifest_a.get_entry(manifest_a.root).try_directory_ref().unwrap();
        let root_dir_b = manifest_b.get_entry(manifest_b.root).try_directory_ref().unwrap();
        let path_getter = manifest_a.get_full_path_getter();

        let mut to_visit_dirs: Vec<(&Directory, &Directory)> = vec![(root_dir_a, root_dir_b)];

        while let Some((dir_a, dir_b)) = to_visit_dirs.pop() {

            for entry_id_a in dir_a.entries.values().cloned() {

                // exclude stuff
                // todo: move to from_fs()
                let full_path = path_getter(entry_id_a);
                if full_path == Path::new(".har") {
                    continue;
                }

                let entry_a = manifest_a.get_entry(entry_id_a);
                match entry_a {
                    Entry::File(file) => {
                        if dir_b.entries.contains_key(&file.name) {
                            if self.hash_check {
                                let file_path = self.archive_root.join(&full_path);
                                let file_bytes = std::fs::read(file_path).unwrap();
                                let hash_name = blob_storage::get_hash_name(self.bucket_name.as_str(), bytes::Bytes::from(file_bytes));

                                let remote_entry = manifest_b.get_entry(dir_b.entries[&file.name]);
                                let remote_entry_hash_name = remote_entry.try_file_ref().unwrap().blob_key.to_string();

                                if hash_name != remote_entry_hash_name {
                                    self.paths_of_different_files.push(full_path);
                                }
                            }
                        }
                        else {
                            self.extra_files_in_a += 1;
                            self.top_extra_ids_in_a.push(entry_id_a);
                        }
                    },
                    Entry::Directory(subdir_a) => {
                        if dir_b.entries.contains_key(&subdir_a.name) {
                            let entry_id_b = dir_b.entries.get(&subdir_a.name).unwrap();
                            let subdir_b = manifest_b.get_entry(*entry_id_b).try_directory_ref().unwrap(); // todo handle error of mismatch entry type
                            to_visit_dirs.push((subdir_a, subdir_b));
                        }
                        else {
                            self.extra_dirs_in_a += 1;
                            self.top_extra_ids_in_a.push(entry_id_a);
                            let num_children = get_num_child_in_dir_recurs(&mut self.dirs_num_files_dirs, manifest_a, entry_id_a);
                            self.extra_files_in_a += num_children.0;
                            self.extra_dirs_in_a += num_children.1;
                        }
                    },
                }
            }
        }

        for &entry_id in &self.top_extra_ids_in_a {
            let full_path = path_getter(entry_id);
            self.paths_of_top_extra_in_a.push(full_path);
        }

        self
    }
}

pub fn diff_manifests(manifest_a: &Manifest, manifest_b: &Manifest) -> DiffManifests {
    let diff = DiffManifests::default();
    diff.diff_manifests(manifest_a, manifest_b)
}

pub fn add_new_entries_to_manifest(
    src: &Manifest,
    dest: &mut Manifest,
    diff: &DiffManifests,
    blob_keys: &HashMap<PathBuf, String>
) -> anyhow::Result<()> {

    let map_parent_src = src.get_map_parent();

    let mut dirs_to_visit: Vec<(EntryId, EntryId)> = Vec::new();

    let add_entry_src_to_dest = |entry_id_src, entry_src: &Entry, dest_dir, dir_path: &Path, dest_manifest: &mut Manifest, dirs_to_visit: &mut Vec<(EntryId, EntryId)>|
        -> anyhow::Result<()> {
        match entry_src {
            Entry::File(file) => {
                let path = dir_path.join(file.name.clone());
                let blob_key_str = blob_keys.get(&path).with_context(|| format!("Did not find path-key entry in map path:{}", path.to_str().unwrap()))?;
                let blob_key = BlobKey::try_from(blob_key_str.as_str())?;
                dest_manifest.add_file(File { name: file.name.clone(), blob_key, size: file.size }, dest_dir).context("Add file from src/dest diff in dest")?;
            },
            Entry::Directory(dir) => {
                let new_dir_b = dest_manifest.add_dir(Directory { name: dir.name.clone(), entries: HashMap::new() }, dest_dir).context("Add dir from src/dest diff in dest")?;
                dirs_to_visit.push((entry_id_src, new_dir_b));
            }
        }
        Ok(())
    };

    debug!("add_new_entries_to_manifest step 1");

    for &entry_id_a in &diff.top_extra_ids_in_a {
        let entry_a = src.get_entry(entry_id_a);
        let parent_a = map_parent_src[&entry_id_a];
        let parent_path = src.get_full_path(parent_a, &map_parent_src);
        let parent_b = dest.join_and_get_entry_id(dest.root, &parent_path)?;

        add_entry_src_to_dest(entry_id_a, entry_a, parent_b, &parent_path, dest, &mut dirs_to_visit)?;
    }

    debug!("add_new_entries_to_manifest step 2");

    while let Some((dir_entry_id_a, dir_entry_id_b)) = dirs_to_visit.pop() {
        let dir_entry_a = src.get_entry(dir_entry_id_a);
        let dir_a = dir_entry_a.try_directory_ref().unwrap();
        let parent_path = src.get_full_path(dir_entry_id_a, &map_parent_src);
        for (_, &sub_entry_id) in &dir_a.entries {
            let sub_entry = src.get_entry(sub_entry_id);
            add_entry_src_to_dest(sub_entry_id, sub_entry, dir_entry_id_b, &parent_path, dest, &mut dirs_to_visit)?;
        }
    }

    Ok(())
}

fn add_tuples(t0: (usize, usize), t1: (usize, usize)) -> (usize, usize) {
    (t0.0 + t1.0, t0.1 + t1.1)
}

fn get_num_child_in_dir_recurs(dirs_num_child: &mut HashMap<EntryId, (usize, usize)>, manifest: &Manifest, entry_id: EntryId) -> (usize, usize) {

    let entry = manifest.get_entry(entry_id);
    if let Entry::File(_) = entry {
        return (1, 0);
    }

    let maybe_known_size = dirs_num_child.get(&entry_id);
    if let Some(&size) = maybe_known_size {
        return size;
    }

    let Entry::Directory(dir) = entry else {
        panic!("What? we already tested if it's a file, it can't be")
    };

    let mut size = (0, 0);
    for &entry_id in dir.entries.values() {
        let entry = manifest.get_entry(entry_id);
        let sub_size = match entry {
            Entry::File(_) => (1, 0),
            Entry::Directory(_) => add_tuples(get_num_child_in_dir_recurs(dirs_num_child, manifest, entry_id), (0, 1)),
        };
        size = add_tuples(size, sub_size);
    }

    dirs_num_child.insert(entry_id, size);

    size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn dummy_blob_key() -> BlobKey {
        let stuffing: [u8; 32] = [1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        BlobKey {
            key: blake3::Hash::from_bytes(stuffing)
        }
    }

    fn dummy_file() -> Entry {
        Entry::File(File {name: "imafile".to_string(), blob_key: dummy_blob_key(), size: 42})
    }

    fn dummy_file_with_name(name: &str) -> Entry {
        Entry::File(File {name: name.to_string(), blob_key: BlobKey::default(), size: 42})
    }

    fn dummy_dir() -> Entry {
        Entry::Directory(Directory {name: "imadir".to_string(), entries: HashMap::new()})
    }

    fn dummy_dir_with_name(name: &str) -> Entry {
        Entry::Directory(Directory {name: name.to_string(), entries: HashMap::new()})
    }

    fn dummy_manifest() -> Manifest {
        let mut manifest = Manifest::new();
        let file_entry = dummy_file();
        manifest.add(file_entry.clone(), manifest.root).expect("Add entry");
        manifest.join_and_get_entry_id(manifest.root, Path::new("imafile")).expect("join and get entry id");
        manifest
    }

    #[test]
    fn create_file() {
        let mut manifest = Manifest::new();
        let file_entry = dummy_file();
        manifest.add(file_entry.clone(), manifest.root).expect("Add entry");
        let entry_id = manifest.join_and_get_entry_id(manifest.root, Path::new("imafile")).expect("join and get entry id");
        let file_entry_b = manifest.get_entry(entry_id);
        assert_eq!(file_entry.try_file_ref().unwrap(), file_entry_b.try_file_ref().unwrap());
    }

    #[test]
    fn create_dir_and_file() {
        let mut manifest = Manifest::new();
        manifest.add(dummy_dir(), manifest.root).expect("Add dir");
        let dir = manifest.join_and_get_entry_id(manifest.root, Path::new("imadir")).expect("Get dir");
        manifest.add(dummy_file(), dir).expect("Add file in dir");

        let file_a = manifest.join_and_get_entry_id(manifest.root, Path::new("imadir/imafile")).expect("Get file");
        let file_b = manifest.join_and_get_entry_id(dir, Path::new("imafile")).expect("Get file");

        assert_eq!(file_a, file_b);
        assert_eq!(dummy_file().try_file_ref().unwrap(), manifest.get_entry(file_a).try_file_ref().unwrap());
        assert_eq!(manifest.entries.len(), 3);

        print_tree(&manifest);
    }

    #[test]
    fn seialize_deserialize() -> anyhow::Result<()> {
        let manifest = dummy_manifest();
        let bytes = manifest.to_bytes().context("serializing")?;
        let manifest_b = Manifest::from_bytes(bytes).context("deserializing")?;

        assert_eq!(manifest.get_stats().num_files, manifest_b.get_stats().num_files);

        Ok(())
    }

    struct ManifestBuilder {
        manifest: Manifest,
        cwd: EntryId,
        previous_cwd: EntryId
    }

    impl ManifestBuilder {
        fn new(manifest: Manifest) -> Self {
            let cwd = manifest.root;
            ManifestBuilder {
                manifest,
                cwd,
                previous_cwd: EntryId::from_usize(0),
            }
        }
        fn get_manifest(self) -> Manifest {
            self.manifest
        }
        fn file(mut self, name: &str) -> Self {
            self.manifest.add(dummy_file_with_name(name), self.cwd).unwrap();
            self
        }
        fn start_dir(mut self, name: &str) -> Self {
            self.previous_cwd = self.cwd;
            self.cwd = self.manifest.add(dummy_dir_with_name(name), self.cwd).unwrap();
            self
        }
        fn cd_dir(mut self, name: &str) -> Self {
            self.previous_cwd = self.cwd;
            let dir = self.manifest.get_entry(self.previous_cwd).try_directory_ref().unwrap();
            self.cwd = *dir.entries.get(name).unwrap();
            self
        }
        fn end_dir(mut self) -> Self {
            self.cwd = self.previous_cwd;
            self
        }
    }

    #[test]
    fn diff_0() -> anyhow::Result<()> {

        let manifest = ManifestBuilder::new(Manifest::new())
            .file("felt")
            .start_dir("dango")
                .file("fetch")
            .end_dir()
            .start_dir("dog")
                .file("fault")
                .start_dir("deal")
                .end_dir()
            .end_dir()
            .get_manifest();

        let other = ManifestBuilder::new(manifest.clone())
            .cd_dir("dango")
                .file("voice")
            .end_dir()
            .get_manifest();

        let diff = diff_manifests(&other, &manifest);

        print!("{}", diff);
        assert_eq!(diff.extra_dirs_in_a, 0);
        assert_eq!(diff.extra_files_in_a, 1);
        assert_eq!(diff.top_extra_ids_in_a.len(), 1);
        assert_eq!(diff.paths_of_top_extra_in_a, vec![PathBuf::from("dango/voice")]);

        Ok(())
    }

    #[test]
    fn diff_1() -> anyhow::Result<()> {

        let manifest = ManifestBuilder::new(Manifest::new())
            .file("felt")
            .start_dir("dango")
                .file("fetch")
            .end_dir()
            .start_dir("dog")
                .file("fault")
                .start_dir("deal")
                .end_dir()
            .end_dir()
            .get_manifest();

        let other = ManifestBuilder::new(manifest.clone())
            .cd_dir("dango")
                .start_dir("cab")
                    .start_dir("choco")
                        .file("vault")
                    .end_dir()
                .end_dir()
            .end_dir()
            .get_manifest();

        let diff = diff_manifests(&other, &manifest);

        print!("{}", diff);
        assert_eq!(diff.extra_dirs_in_a, 2);
        assert_eq!(diff.extra_files_in_a, 1);
        assert_eq!(diff.top_extra_ids_in_a.len(), 1);
        assert_eq!(diff.paths_of_top_extra_in_a, vec![PathBuf::from("dango/cab")]);

        Ok(())
    }

    #[test]
    fn get_child_recurs() -> anyhow::Result<()> {
        let manifest = ManifestBuilder::new(Manifest::new())
            .file("felt")
            .start_dir("dango")
                .file("fetch")
            .end_dir()
            .start_dir("dog")
                .file("fault")
                .start_dir("deal")
                .end_dir()
            .end_dir()
            .get_manifest();

        let dango = manifest.join_and_get_entry_id(manifest.root, Path::new("dango"))?;
        let fetch = manifest.join_and_get_entry_id(dango, Path::new("fetch"))?;
        assert_eq!(manifest.get_child_files_recurs(dango), vec![fetch]);
        assert_eq!(manifest.get_child_files_recurs(fetch), vec![fetch]);

        let felt = manifest.join_and_get_entry_id(manifest.root, Path::new("felt"))?;
        let fault = manifest.join_and_get_entry_id(manifest.root, Path::new("dog/fault"))?;
        let child_recurs = manifest.get_child_files_recurs(manifest.root);
        assert!(child_recurs.contains(&felt));
        assert!(child_recurs.contains(&fault));
        assert!(child_recurs.contains(&fetch));

        Ok(())
    }
}