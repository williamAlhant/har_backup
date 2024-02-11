use std::path::{Path, Component};
use std::collections::HashMap;
use anyhow::Context;

#[derive(Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, Clone)]
struct Directory {
    name: String,
    entries: HashMap<String, EntryId>
}

#[derive(Clone, PartialEq)]
struct BlobKey {
    key: blake3::Hash
}

impl std::fmt::Debug for BlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hash_hex = self.key.to_hex();
        write!(f, "{}", hash_hex.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct File {
    name: String,
    blob_key: BlobKey
}

#[derive(Debug, Clone)]
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

struct Manifest {
    root: EntryId,
    entries: Vec<Entry>
}

impl Manifest {
    fn new() -> Self {
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
                    last_entry_id = Some(entry_id.clone());
                    if let Entry::Directory(directory) = entry {
                        cd = &directory;
                    }
                },
                _ => anyhow::bail!("Cannot handle path components other than root/normal")
            };
        }
        last_entry_id.context("last_entry is none?")
    }

    fn add(&mut self, entry: Entry, parent_dir: EntryId) -> anyhow::Result<()> {
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
        Ok(())
    }


    fn from_fs(fs_dir: &Path) -> Self {
        todo!();
    }
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
        Entry::File(File {name: "imafile".to_string(), blob_key: dummy_blob_key()})
    }

    fn dummy_dir() -> Entry {
        Entry::Directory(Directory {name: "imadir".to_string(), entries: HashMap::new()})
    }

    fn print_entry(manifest: &Manifest, entry: &Entry, indent: usize) {
        match entry {
            Entry::File(file) => println!("{}{:?}", " ".repeat(indent), file),
            Entry::Directory(dir) => {
                println!("{}{}", " ".repeat(indent), dir.name);
                for (entry_name, entry_id) in &dir.entries {
                    let entry = manifest.get_entry(entry_id.clone());
                    print_entry(manifest, entry, indent + 2);
                }
            }
        }
    }

    fn print_tree(manifest: &Manifest) {
        print_entry(manifest, manifest.get_entry(manifest.root), 0);
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
}