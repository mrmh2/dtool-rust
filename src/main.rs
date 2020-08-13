mod dataset;

use dataset::{DiskDataSet,HTTPDataSet,ProtoDataSet,DSList};

use std::io::ErrorKind;
use std::error::Error;
use std::path::PathBuf;
use std::result::Result;

use clap::Clap;
use url::{Url, ParseError};

#[derive(Clap)]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand
}

#[derive(Clap)]
enum SubCommand {
    Create(Create),
    Freeze(Freeze),
    List(List),
    Test(Test)
}

#[derive(Clap)]
struct Create {
    name: String
}

#[derive(Clap)]
struct Freeze {
    uri: String
}

#[derive(Clap)]
struct List {
    uri: String
}

#[derive(Clap)]
struct Test {
    uri: String
}









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


fn uri_from_file_path(fpath: &String) -> Result<Url, std::io::Error> {
    let pathuri = PathBuf::from(&fpath);
    let canonical = std::fs::canonicalize(&pathuri)?;
    
    match Url::from_file_path(&canonical) {
        Ok(url) => return Ok(url),
        Err(e) => return Err(std::io::Error::new(ErrorKind::Other, "Can't parse URI")),
    }  
}


fn main() -> Result<(), Box<dyn Error>> {

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
        SubCommand::List(list) => {
            let uri = PathBuf::from(list.uri);
            // let dataset = DataSet::from_uri(uri)?;
            // dataset.list();
        }
        SubCommand::Test(test) => {
            let parse_result = Url::parse(&test.uri);


            match parse_result {
                Ok(uri) => {
                    println!("Normal URI {}", uri.scheme());
                    let dataset = HTTPDataSet::from_uri(test.uri);
                    // dataset.list();
                }
                Err(e) => {
                    let uri = uri_from_file_path(&test.uri)?;
                    println!("Path URI {}", uri.scheme());
                    let pathuri = PathBuf::from(test.uri);
                    let dataset = DiskDataSet::from_uri(pathuri)?;
                    dataset.list();
                }
            }



            // println!("{}", url.scheme());

            // let pathuri = PathBuf::from(test.uri);
            // let canonical = std::fs::canonicalize(&pathuri)?;
            // let url = Url::from_file_path(&canonical).unwrap();

            // println!("{:?} {:?} {}", pathuri, canonical, url.as_str());
        }
    }

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
