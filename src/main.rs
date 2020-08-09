mod dataset;

use dataset::ProtoDataSet;

use std::path::PathBuf;
use std::fs;
use std::fs::File;
use std::result::Result;
use std::io::{BufReader,Write,copy,Read};

use std::collections::HashMap;

use clap::Clap;

#[derive(Clap)]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand
}

#[derive(Clap)]
enum SubCommand {
    Create(Create),
    Freeze(Freeze)
}

#[derive(Clap)]
struct Create {
    name: String
}

#[derive(Clap)]
struct Freeze {
    uri: String
}

// #[derive(Deserialize)]
// struct HTTPManifest {
//     admin_metadata: AdminMetadata,
//     item_urls: HashMap<String, String>,
//     manifest_url: String
// }


// struct DataSet {
//     admin_metadata: AdminMetadata,
//     manifest: Manifest,
//     item_urls: HashMap<String, String>
// }


// fn list(manifest: &Manifest) {
//     let mut by_relpath: HashMap<String, &String> = manifest.items
//         .iter()
//         .map(|(k, v)| (v.relpath.clone(), k))
//         .collect();

//     let mut sorted_relpaths: Vec<String> = by_relpath.keys().map(|r| r.clone()).collect();
//     sorted_relpaths.sort();

//     for relpath in sorted_relpaths {
//         println!("{}\t{}", by_relpath[&relpath], relpath);
//     }    
// }

// type DataSetResult = std::result::Result<DataSet, std::io::Error>;

// impl DataSet {
//     fn from_http_uri(uri: String) -> DataSetResult {
//         let http_manifest_uri = format!("{}/http_manifest.json", uri);
//         let body = reqwest::blocking::get(&http_manifest_uri).unwrap().text().unwrap();
//         let http_manifest: HTTPManifest = serde_json::from_str(&body)?;
//         let body = reqwest::blocking::get(&http_manifest.manifest_url).unwrap().text().unwrap();
//         let manifest: Manifest = serde_json::from_str(&body)?;    

//         Ok(DataSet {
//             admin_metadata: http_manifest.admin_metadata,
//             manifest: manifest,
//             item_urls: http_manifest.item_urls
//         })
//     }

//     fn item_content_abspath(&self, idn: &String) -> std::result::Result<PathBuf, std::io::Error> {
//         let cache_dirpath = Path::new("cache").join(&self.admin_metadata.uuid);
//         let ext = Path::new(&self.manifest.items[idn].relpath)
//             .extension()
//             .unwrap()
//             .to_str()
//             .unwrap_or("");
        
//         let dest_path = cache_dirpath.join(idn).with_extension(ext);

//         if dest_path.exists() {
//             Ok(dest_path)
//         } else {
//             fs::create_dir_all(cache_dirpath)?;
//             let content = reqwest::blocking::get(&self.item_urls[idn]).unwrap().bytes().unwrap();
//             let mut fh = File::create(&dest_path)?;
//             copy(&mut content.as_ref(), &mut fh)?;
//             Ok(dest_path)
//         }
//     }

//     // fn from_path_uri(path_uri: &Path) -> DataSetResult {
//     //     let manifest_abspath = path_uri.join(".dtool").join("manifest.json");
//     //     let fh = File::open(&manifest_abspath)?;
//     //     let mut reader = BufReader::new(fh);
//     //     let manifest: Manifest = serde_json::from_reader(reader)?;    

//     //     Ok(DataSet {
//     //         manifest: manifest
//     //     })
//     // }
// }

// fn get_all(ds: DataSet) -> std::result::Result<(), std::io::Error> {
//     let root_dirpath = Path::new(&ds.admin_metadata.name);
//     let data_root = root_dirpath.join("data");

//     for idn in ds.manifest.items.keys() {
//         println!("{}", ds.item_urls[idn]);
//         let relpath = Path::new(&ds.manifest.items[idn].relpath);
//         let fpath = data_root.join(relpath);
//         println!("Attempting to create {:?}", fpath.parent().unwrap());
//         fs::create_dir_all(fpath.parent().unwrap())?;
//         let body = reqwest::blocking::get(&ds.item_urls[idn]).unwrap().bytes().unwrap();
//         let mut fh = File::create(&fpath)?;
//         copy(&mut body.as_ref(), &mut fh)?;
//     }

//     Ok(())
// }



fn main() -> std::result::Result<(), std::io::Error> {

    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::Create(create) => {
            let base_uri = PathBuf::from("scratch");
            let proto_dataset = ProtoDataSet::new(&create.name, base_uri);
            proto_dataset.create_structure()?;
            proto_dataset.put_readme(b"")?;
            // println!("Created {}", base_uri.join(&create.name).display());
        }
        SubCommand::Freeze(freeze) => {
            let uri = PathBuf::from(freeze.uri);
            let mut proto_dataset = ProtoDataSet::from_uri(uri)?;
            proto_dataset.freeze()?;
            // println!("Brr {}", freeze.uri);
        }
    }
    // let args: Vec<String> = env::args().collect();
    // let name = &args[1];

    // let base_uri = PathBuf::from("scratch");
    // let proto_dataset = ProtoDataSet::new(name, base_uri);

    // proto_dataset.create_structure()?;

    // let fpath = Path::new("image0001.jpg");
    // proto_dataset.put_item(fpath, PathBuf::from("myim.jpg"))?;

    // proto_dataset.freeze()?;

    Ok(())
}

// fn tumain() -> std::result::Result<(), std::io::Error> {
//     let args: Vec<String> = env::args().collect();

//     let uri = &args[1];

//     if uri.starts_with("http") {
//         let ds = DataSet::from_http_uri(args[1].clone())?;
//         // let idn = ds.manifest.items.keys().next().unwrap();
//         // let idn = String::from("4ddbab7076711d7b8912a26924bae863fcec1e6d");
//         for idn in ds.manifest.items.keys() {
//             println!("{:?}", ds.item_content_abspath(&idn));
//         }

//     } else {
        
//         // let path_uri = Path::new(&args[1]);
//         // let ds = DataSet::from_path_uri(path_uri)?;
//         // list(&ds.manifest);
//     }

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


//     Ok(())
// }
