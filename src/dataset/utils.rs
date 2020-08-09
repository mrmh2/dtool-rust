use std::fs::File;
use std::path::Path;
use std::io::{BufReader,Read};

pub fn hexdigest(path: &Path) -> std::io::Result<String> {
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

    Ok(format!("{:x}", context.compute()))
}

pub fn mtime_from_path(path: &Path) -> Result<f64, std::io::Error> {
    let metadata = std::fs::metadata(&path)?;
    let since_epoch = metadata
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Oops");
    let t_mod_s = since_epoch.as_secs() as f64 + since_epoch.subsec_nanos() as f64 * 1e-9;

    Ok(t_mod_s)
}

pub fn current_time() -> Result<f64, std::io::Error> {
    let now = std::time::SystemTime::now();
    let since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    let t_s = since_epoch.as_secs() as f64 + since_epoch.subsec_nanos() as f64 * 1e-9;

    Ok(t_s)
}