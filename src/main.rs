mod dataset;

use dataset::{DiskDataSet,HTTPDataSet,ProtoDataSet,DataSet};

use std::error::Error;
use std::path::PathBuf;
use std::result::Result;

use clap::Clap;
use url::Url;

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
    source_uri: String,
    dest_base_uri: String
}

// fn uri_from_file_path(fpath: &String) -> Result<Url, std::io::Error> {
//     let pathuri = PathBuf::from(&fpath);
//     let canonical = std::fs::canonicalize(&pathuri)?;
    
//     match Url::from_file_path(&canonical) {
//         Ok(url) => return Ok(url),
//         Err(e) => return Err(std::io::Error::new(ErrorKind::Other, "Can't parse URI")),
//     }  
// }

fn dataset_from_uri(uri: String) -> Result<Box<dyn DataSet>, std::io::Error> {
    let parse_result = Url::parse(&uri);
    match parse_result {
        Ok(_pr) => Ok(Box::new(HTTPDataSet::from_uri(uri)?)),
        Err(_e) => {
            let pathuri = PathBuf::from(uri);
            Ok(Box::new(DiskDataSet::from_uri(pathuri)?))     
        }
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
            let dataset = dataset_from_uri(list.uri)?;
            // let uri = PathBuf::from(list.uri);
            // let dataset = DataSet::from_uri(uri)?;
            dataset.list();
        }
        SubCommand::Test(test) => {
            println!("Copy {} to {}", test.source_uri, test.dest_base_uri);
            let src_dataset = dataset_from_uri(test.source_uri)?;

            let dest_base_uri = PathBuf::from(test.dest_base_uri);
            let mut proto_dataset = ProtoDataSet::new(&src_dataset.name(), dest_base_uri);
            proto_dataset.create_structure()?;

            let readme_content = src_dataset.get_readme_content();
            proto_dataset.put_readme(&readme_content.as_bytes())?;

            for idn in src_dataset.identifiers() {
                let abspath = src_dataset.item_content_abspath(idn)?;
                let relpath = &src_dataset.item_properties(idn).relpath;
                proto_dataset.put_item(&abspath, PathBuf::from(relpath))?;
            }

            proto_dataset.freeze()?;


        }
    }

    Ok(())
}