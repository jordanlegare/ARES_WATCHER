use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use ssh2::Session;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use zip::write::FileOptions;
use std::thread;
use std::time::Duration;
use std::net::TcpStream;



fn main() -> notify::Result<()> {
    // Define the folders you want to watch
    let watch_folders = vec!["/tmp/project_notes", "/tmp/skill_notes"]; //tmp
    let backup_path = "/tmp/backups";

    fs::create_dir_all(backup_path).unwrap();
    for folder in &watch_folders {
        fs::create_dir_all(folder).unwrap();
    }

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = RecommendedWatcher::new(tx, Config::default())?;

    // Watch multiple folders
    for folder in watch_folders {
        watcher.watch(Path::new(folder), RecursiveMode::Recursive)?;
        // Sleep for 100 milliseconds
        thread::sleep(Duration::from_millis(100));
    }

    for res in rx {
        if let Ok(event) = res {
            if is_file_change(&event) {
                for path in event.paths {
                    if path.is_file() {
                        let path_buf = path.to_path_buf(); // Clone the path for the thread
                        let backup_dir = backup_path.to_string();

                        thread::spawn(move || {
                            let filename = path_buf.file_name().unwrap().to_str().unwrap();
                            let timestamp = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap().as_secs();
                            
                            let zip_name = format!("{}/{}_{}.zip", backup_dir, filename, timestamp);
                            let backup_zip_name = format!("{}_{}.zip", filename, timestamp);
                            
                            // 1. Zip the file
                            if let Err(_e) = zip_file(&path_buf, &zip_name) {
                                return;
                            }
                            
                            // 2. Upload the file
                            let remote_dest = format!("HOME_SERVER_DIR/BACKUPS/{}", backup_zip_name); // replace HOME_SERVER_DIR/BACKUPS with your incremental storage folder.
                            match upload_to_server(Path::new(&zip_name), &remote_dest) {
                                Ok(_) => {
                                    // 3. Cleanup only on success
                                    let _ = std::fs::remove_file(&zip_name);
                                }
                                Err(_e) => {
                                    record_failed_transfer(&zip_name);
                                    process_queue();
                                }
                            }
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn is_file_change(event: &Event) -> bool {
    event.kind.is_create() || event.kind.is_modify()
}

fn zip_file(source: &Path, destination: &str) -> zip::result::ZipResult<()> {
    let file = File::create(destination)?;
    let mut zip = zip::ZipWriter::new(file);
    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Stored);

    let mut buffer = Vec::new();
    let mut f = File::open(source)?;
    f.read_to_end(&mut buffer)?;

    zip.start_file(source.file_name().unwrap().to_str().unwrap(), options)?;
    zip.write_all(&buffer)?;
    zip.finish()?;
    Ok(())
}

fn upload_to_server(local_path: &Path, remote_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let tcp = TcpStream::connect("server_ip:22")?; // replace server_ip with your ssh ready server ip
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;
    
    // Authenticate (Use your SSH key or password)
    sess.userauth_password("USER", "PASSWORD")?; //replace USER by ssh user and PASSWORD by ssh password

    let mut remote_file = sess.scp_send(Path::new(remote_path), 0o644, 
                                        fs::metadata(local_path)?.len(), None)?;
    
    let mut file = File::open(local_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    remote_file.write_all(&buffer)?;
    
    remote_file.send_eof()?;
    remote_file.wait_eof()?;
    remote_file.close()?;
    remote_file.wait_close()?;
    Ok(())
}

fn record_failed_transfer(filename: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/backups/failed_transfers.txt")
        .unwrap();
    writeln!(file, "{}", filename).unwrap();
}

// 2. Read and process the queue
fn process_queue() {
    if !Path::new("/tmp/backups/failed_transfers.txt").exists() { return; }

    let file = fs::File::open("/tmp/backups/failed_transfers.txt").unwrap();
    let reader = BufReader::new(file);
    let mut still_failed = Vec::new();

    for line in reader.lines() {
        let filename = line.unwrap();
        // Try to upload the previously failed file
        match upload_to_server(Path::new(&format!("/tmp/backups/{}", filename)), &format!("HOME_SERVER_DIR/BACKUPS/{}", filename)) { // replace HOME_SERVER_DIR/BACKUPS with your incremental storage folder
            Ok(_) => {},
            Err(_) => still_failed.push(filename),
        }
    }

    // Rewrite the file with only the ones that failed again
    let mut file = fs::File::create("/tmp/backups/failed_transfers.txt").unwrap();
    for f in still_failed {
        writeln!(file, "{}", f).unwrap();
    }
}