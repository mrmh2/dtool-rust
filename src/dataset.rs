mod utils;

use std::path::{Path,PathBuf};
use std::fs;
use std::fs::File;
use std::result::Result;
use std::io::{BufReader,Write};

use std::collections::HashMap;

use serde::{Serialize,Deserialize};

use uuid::Uuid;

const DTOOLCORE_VERSION: &str = "3.17.0";

#[derive(Serialize, Deserialize)]
struct AdminMetadata {
    name: String,
    uuid: String,
    r#type: String,
    dtoolcore_version: String,
    creator_username: String,
    created_at: f64,
    frozen_at: f64
}

pub struct ProtoDataSet {
    name: String,
    base_path: PathBuf,
    data_root: PathBuf,
    dtool_dirpath: PathBuf,
    admin_metadata: AdminMetadata
}

pub struct DataSet {
    base_path: PathBuf,
    data_root: PathBuf,
    dtool_dirpath: PathBuf,
    admin_metadata: AdminMetadata,
    manifest: Manifest
}

#[derive(Serialize, Deserialize, Debug)]
struct ManifestItem {
    hash: String,
    relpath: String,
    size_in_bytes: u64,
    utc_timestamp: f64
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    dtoolcore_version: String,
    hash_function: String,
    items: HashMap<String, ManifestItem>
}


fn generate_identifier(handle: &[u8]) -> String {
    let mut hasher = sha1::Sha1::new();
    hasher.update(handle);

    hasher.digest().to_string()
}

fn create_admin_metadata(name: &String) -> AdminMetadata {
    let uuid = Uuid::new_v4();

    AdminMetadata {
        name: name.clone(),
        uuid: uuid.to_string(),
        r#type: String::from("protodataset"),
        dtoolcore_version: String::from(DTOOLCORE_VERSION),
        creator_username: whoami::username(),
        created_at: utils::current_time().unwrap(),
        frozen_at: 0.0
    }
}

impl DataSet {
    pub fn from_uri(uri: PathBuf) -> Result<DataSet, std::io::Error> {
        let data_root = uri.join("data");
        let dtool_dirpath = uri.join(".dtool");
        let dtool_fpath = dtool_dirpath.join("dtool");

        let fh = File::open(&dtool_fpath)?;
        let reader = BufReader::new(fh);
        let admin_metadata: AdminMetadata = serde_json::from_reader(reader)?;

        let manifest_abspath = dtool_dirpath.join("manifest.json");
        let fh = File::open(&manifest_abspath)?;
        let mut reader = BufReader::new(fh);
        let manifest: Manifest = serde_json::from_reader(reader)?; 

        Ok(DataSet {
            admin_metadata: admin_metadata,
            data_root: data_root,
            dtool_dirpath: dtool_dirpath,
            base_path: uri,
            manifest: manifest
        })
    }

    pub fn list(&self) {
        let mut by_relpath: HashMap<String, &String> = self.manifest.items
            .iter()
            .map(|(k, v)| (v.relpath.clone(), k))
            .collect();

        let mut sorted_relpaths: Vec<String> = by_relpath.keys().map(|r| r.clone()).collect();
        sorted_relpaths.sort();

        for relpath in sorted_relpaths {
            println!("{}\t{}", by_relpath[&relpath], relpath);
        }    
    }
}

impl ProtoDataSet {
    pub fn new(name: &String, base_uri: PathBuf) -> ProtoDataSet {
        let admin_metadata = create_admin_metadata(&name);
        let base_path = base_uri.join(name);
        let data_root = base_path.join("data");
        let dtool_dirpath = base_path.join(".dtool");

        ProtoDataSet {
            name: name.clone(),
            base_path: base_path,
            data_root: data_root,
            dtool_dirpath: dtool_dirpath,
            admin_metadata: admin_metadata
        }
    }

    pub fn from_uri(uri: PathBuf) -> Result<ProtoDataSet, std::io::Error> {
        let data_root = uri.join("data");
        let dtool_dirpath = uri.join(".dtool");
        let dtool_fpath = dtool_dirpath.join("dtool");
        let fh = File::open(&dtool_fpath)?;
        let reader = BufReader::new(fh);
        let admin_metadata: AdminMetadata = serde_json::from_reader(reader)?;

        Ok(ProtoDataSet {
            name: admin_metadata.name.clone(),
            admin_metadata: admin_metadata,
            data_root: data_root,
            dtool_dirpath: dtool_dirpath,
            base_path: uri
        })
    }

    fn put_admin_metadata(&self) -> std::io::Result<()> {
       let j = serde_json::to_string(&self.admin_metadata)?;
        let mut fh = File::create(self.dtool_dirpath.join("dtool"))?;
        fh.write_all(j.as_bytes())?;

        Ok(())
    }

    pub fn create_structure(&self) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.data_root)?;
        fs::create_dir_all(&self.dtool_dirpath)?;

        self.put_admin_metadata()?;
 
        Ok(())
    }

    pub fn put_item(&self, fpath: &Path, relpath: PathBuf) -> Result<(), std::io::Error> {
        let dest_fpath = self.data_root.join(relpath);
        std::fs::copy(fpath, dest_fpath)?;
        Ok(())
    }

    pub fn put_readme(&self, readme_content: &[u8]) -> std::io::Result<()> {
        let mut fh = File::create(self.base_path.join("README.yml"))?;
        fh.write_all(readme_content)?;
        Ok(())
    }

    fn properties_from_path(&self, path: &Path) -> Result<ManifestItem, std::io::Error> {
        let metadata = std::fs::metadata(&path)?;
        let relpath = path.strip_prefix(self.data_root.as_path()).unwrap();

        Ok(ManifestItem {
            hash: utils::hexdigest(&path).unwrap(),
            relpath: String::from(relpath.to_str().unwrap()),
            size_in_bytes: metadata.len(),
            utc_timestamp: utils::mtime_from_path(&path).unwrap()
        })
    }

    pub fn freeze(&mut self) -> Result<(), std::io::Error> {

        let mut manifest_items = HashMap::new();

        for item in std::fs::read_dir(&self.data_root)? {
            let p = item.unwrap().path();
            let item_properties = self.properties_from_path(&p)?;
            let idn = generate_identifier(item_properties.relpath.as_bytes());
            manifest_items.insert(idn, item_properties);
        }

        let manifest = Manifest{
            dtoolcore_version: String::from(DTOOLCORE_VERSION),
            hash_function: String::from("md5sum_hexdigest"),
            items: manifest_items
        };

        let j = serde_json::to_string_pretty(&manifest)?;
        let mut fh = File::create(self.dtool_dirpath.join("manifest.json"))?;
        fh.write_all(j.as_bytes())?;

        self.admin_metadata.r#type = String::from("dataset");
        self.admin_metadata.frozen_at = utils::current_time().unwrap();
        self.put_admin_metadata()?;

        Ok(())
    }
}