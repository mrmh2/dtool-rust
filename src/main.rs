use std::env;
use std::path::{Path,PathBuf};
use std::fs;
use std::fs::File;
use std::result::Result;
use std::io::{BufReader,Write,copy,Read};

use std::collections::HashMap;

use serde::{Serialize,Deserialize};
use serde_json::Value;

use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct AdminMetadata {
    name: String,
    uuid: String,
    r#type: String
}

#[derive(Deserialize)]
struct HTTPManifest {
    admin_metadata: AdminMetadata,
    item_urls: HashMap<String, String>,
    manifest_url: String
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

struct DataSet {
    admin_metadata: AdminMetadata,
    manifest: Manifest,
    item_urls: HashMap<String, String>
}

struct ProtoDataSet {
    name: String,
    base_path: PathBuf,
    data_root: PathBuf,
    dtool_dirpath: PathBuf,
    admin_metadata: AdminMetadata
}

fn hexdigest(path: &Path) -> std::io::Result<String> {
    let fh = File::open(path)?;
    let mut reader = BufReader::new(fh);

    let mut buffer = [0; 1024];
    let mut context = md5::Context::new();

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break
        }
        context.consume(&buffer[..count]);
    }

    Ok((format!("{:x}", context.compute())))
}

fn mtime_from_path(path: &Path) -> std::result::Result<(f64), std::io::Error> {
    let metadata = std::fs::metadata(&path)?;
    let since_epoch = metadata
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Oops");
    let t_mod_s = since_epoch.as_secs() as f64 + since_epoch.subsec_nanos() as f64 * 1e-9;

    Ok(t_mod_s)
}

fn generate_identifier(handle: &[u8]) -> String {
    let mut hasher = sha1::Sha1::new();
    hasher.update(handle);

    hasher.digest().to_string()
}

impl ProtoDataSet {
    fn new(name: &String, base_uri: PathBuf) -> ProtoDataSet {
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

    fn create_structure(&self) -> std::result::Result<(), std::io::Error> {
        fs::create_dir_all(&self.data_root)?;

        let inner_dir = self.base_path.join(".dtool");
        fs::create_dir_all(&inner_dir)?;

        let j = serde_json::to_string(&self.admin_metadata)?;
        let mut fh = File::create(inner_dir.join("dtool"))?;
        fh.write_all(j.as_bytes())?;

        Ok(())
    }

    fn put_item(&self, fpath: &Path, relpath: PathBuf) -> std::result::Result<(), std::io::Error> {
        let dest_fpath = self.data_root.join(relpath);
        std::fs::copy(fpath, dest_fpath);
        Ok(())
    }

    fn properties_from_path(&self, path: &Path) -> std::result::Result<ManifestItem, std::io::Error> {
        let metadata = std::fs::metadata(&path)?;
        let relpath = path.strip_prefix(self.data_root.as_path()).unwrap();

        Ok(ManifestItem {
            hash: hexdigest(&path).unwrap(),
            relpath: String::from(relpath.to_str().unwrap()),
            size_in_bytes: metadata.len(),
            utc_timestamp: mtime_from_path(&path).unwrap()
        })
    }

    fn freeze(&self) -> std::result::Result<(), std::io::Error> {

        let mut manifest_items = HashMap::new();

        for item in std::fs::read_dir(&self.data_root)? {
            let p = item.unwrap().path();
            let item_properties = self.properties_from_path(&p)?;
            let idn = generate_identifier(item_properties.relpath.as_bytes());
            manifest_items.insert(idn, item_properties);
        }

        let manifest = Manifest{
            dtoolcore_version: String::from("3.0.0"),
            hash_function: String::from("md5sum_hexdigest"),
            items: manifest_items
        };

        let j = serde_json::to_string_pretty(&manifest)?;
        let mut fh = File::create(self.dtool_dirpath.join("manifest.json"))?;
        fh.write_all(j.as_bytes())?;

        Ok(())
    }
}

fn list(manifest: &Manifest) {
    let mut by_relpath: HashMap<String, &String> = manifest.items
        .iter()
        .map(|(k, v)| (v.relpath.clone(), k))
        .collect();

    let mut sorted_relpaths: Vec<String> = by_relpath.keys().map(|r| r.clone()).collect();
    sorted_relpaths.sort();

    for relpath in sorted_relpaths {
        println!("{}\t{}", by_relpath[&relpath], relpath);
    }    
}

type DataSetResult = std::result::Result<DataSet, std::io::Error>;

impl DataSet {
    fn from_http_uri(uri: String) -> DataSetResult {
        let http_manifest_uri = format!("{}/http_manifest.json", uri);
        let body = reqwest::blocking::get(&http_manifest_uri).unwrap().text().unwrap();
        let http_manifest: HTTPManifest = serde_json::from_str(&body)?;
        let body = reqwest::blocking::get(&http_manifest.manifest_url).unwrap().text().unwrap();
        let manifest: Manifest = serde_json::from_str(&body)?;    

        Ok(DataSet {
            admin_metadata: http_manifest.admin_metadata,
            manifest: manifest,
            item_urls: http_manifest.item_urls
        })
    }

    fn item_content_abspath(&self, idn: &String) -> std::result::Result<PathBuf, std::io::Error> {
        let cache_dirpath = Path::new("cache").join(&self.admin_metadata.uuid);
        let ext = Path::new(&self.manifest.items[idn].relpath)
            .extension()
            .unwrap()
            .to_str()
            .unwrap_or("");
        
        let dest_path = cache_dirpath.join(idn).with_extension(ext);

        if dest_path.exists() {
            Ok(dest_path)
        } else {
            fs::create_dir_all(cache_dirpath)?;
            let content = reqwest::blocking::get(&self.item_urls[idn]).unwrap().bytes().unwrap();
            let mut fh = File::create(&dest_path)?;
            copy(&mut content.as_ref(), &mut fh)?;
            Ok(dest_path)
        }
    }

    // fn from_path_uri(path_uri: &Path) -> DataSetResult {
    //     let manifest_abspath = path_uri.join(".dtool").join("manifest.json");
    //     let fh = File::open(&manifest_abspath)?;
    //     let mut reader = BufReader::new(fh);
    //     let manifest: Manifest = serde_json::from_reader(reader)?;    

    //     Ok(DataSet {
    //         manifest: manifest
    //     })
    // }
}

fn get_all(ds: DataSet) -> std::result::Result<(), std::io::Error> {
    let root_dirpath = Path::new(&ds.admin_metadata.name);
    let data_root = root_dirpath.join("data");

    for idn in ds.manifest.items.keys() {
        println!("{}", ds.item_urls[idn]);
        let relpath = Path::new(&ds.manifest.items[idn].relpath);
        let fpath = data_root.join(relpath);
        println!("Attempting to create {:?}", fpath.parent().unwrap());
        fs::create_dir_all(fpath.parent().unwrap())?;
        let body = reqwest::blocking::get(&ds.item_urls[idn]).unwrap().bytes().unwrap();
        let mut fh = File::create(&fpath)?;
        copy(&mut body.as_ref(), &mut fh)?;
    }

    Ok(())
}

fn create_admin_metadata(name: &String) -> AdminMetadata {
    let uuid = Uuid::new_v4();

    AdminMetadata {
        name: name.clone(),
        uuid: uuid.to_string(),
        r#type: String::from("protodataset")
    }
}

fn main() -> std::result::Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();
    let name = &args[1];

    let base_uri = PathBuf::from("scratch");
    let proto_dataset = ProtoDataSet::new(name, base_uri);

    proto_dataset.create_structure()?;

    let fpath = Path::new("image0001.jpg");
    proto_dataset.put_item(fpath, PathBuf::from("myim.jpg"))?;

    proto_dataset.freeze()?;

    Ok(())
}

fn tumain() -> std::result::Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();

    let uri = &args[1];

    if uri.starts_with("http") {
        let ds = DataSet::from_http_uri(args[1].clone())?;
        // let idn = ds.manifest.items.keys().next().unwrap();
        // let idn = String::from("4ddbab7076711d7b8912a26924bae863fcec1e6d");
        for idn in ds.manifest.items.keys() {
            println!("{:?}", ds.item_content_abspath(&idn));
        }

    } else {
        
        // let path_uri = Path::new(&args[1]);
        // let ds = DataSet::from_path_uri(path_uri)?;
        // list(&ds.manifest);
    }

    // let pathuri = Path::new(&args[1]);

    // println!("Loading from {}", pathuri.display());

    // let admin_metadata_abspath = pathuri.join(".dtool").join("dtool");

    // println!("Load {}", admin_metadata_abspath.display());

    // let fh = File::open(&admin_metadata_abspath)?;
    // let v: Value = serde_json::from_reader(fh)?;
    // println!("UUID: {}", v["uuid"]);

    // let manifest_abspath = pathuri.join(".dtool").join("manifest.json");

    // let fh = File::open(&manifest_abspath)?;
    // let v: Value = serde_json::from_reader(fh)?;

    // for (key, value) in v["items"].as_object().unwrap() {
    //     println!("{}    {}", key, value["relpath"]);
    // }


    // let mut relpaths: Vec<String> = manifest.items.values().map(|item| item.relpath.clone()).collect();
    // relpaths.sort();
    // println!("{:?}", relpaths);

    // let mut by_relpath = HashMap::new();

    // for (k, v) in manifest.items {
    //     by_relpath.insert(v.relpath, k);
    // }

    // let uri = "https://dtoolaidata.blob.core.windows.net/839ae396-74a7-44f9-9b08-436be53b1090";


    Ok(())
}
