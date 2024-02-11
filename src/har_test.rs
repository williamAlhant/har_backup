use clap::{Parser, Args, Subcommand};
use anyhow::{Result, Context};
use har_backup::blob_storage::{EventContent, BlobStorage};
use har_backup::blob_storage_local_directory::BlobStorageLocalDirectory;
use har_backup::manifest::{Manifest, print_tree};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    MakeManifestFromFs(MakeManifestFromFsCli),
    Upload(UploadCli),
    Download(DownloadCli)
}

#[derive(Args, Debug)]
struct MakeManifestFromFsCli {
    dir: PathBuf
}

#[derive(Args, Debug)]
struct UploadCli {
    #[command(flatten)]
    blob_storage: BlobStorageArgs,
    data: String
}

#[derive(Args, Debug)]
struct DownloadCli {
    #[command(flatten)]
    blob_storage: BlobStorageArgs,
    blob_key: String
}

#[derive(Args, Debug)]
struct BlobStorageArgs {
    #[arg(name="blob_storage_dir")]
    dir: PathBuf,
    #[arg(name="blob_storage_key")]
    key: PathBuf
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    match cli.command {
        Command::MakeManifestFromFs(sub_cli) => {
            println!("{:?}", sub_cli);
            let manifest = Manifest::from_fs(&sub_cli.dir).context("Making manifest from fs")?;
            print_tree(&manifest);
        },
        Command::Upload(sub_cli) => {
            println!("{:?}", sub_cli);
            let mut blob_storage = BlobStorageLocalDirectory::new(&sub_cli.blob_storage.dir, &sub_cli.blob_storage.key)?;
            println!("Blob storage object created");
            let events = blob_storage.events();
            blob_storage.upload(bytes::Bytes::from(sub_cli.data));
            let event = events.recv().expect("receive an event for upload");
            let blob_hash = match event.content {
                EventContent::UploadSuccess(blob_hash) => blob_hash,
                _ => anyhow::bail!("Expected UploadSuccess but got {:?}", event.content)
            };
            println!("Upload success. Blob name: {}", blob_hash);
        },
        Command::Download(sub_cli) => {
            println!("{:?}", sub_cli);
            let mut blob_storage = BlobStorageLocalDirectory::new(&sub_cli.blob_storage.dir, &sub_cli.blob_storage.key)?;
            println!("Blob storage object created");
            let events = blob_storage.events();
            blob_storage.download(&sub_cli.blob_key);
            let event = events.recv().expect("receive an event for download");
            let bytes = match event.content {
                EventContent::DownloadSuccess(bytes) => bytes,
                _ => anyhow::bail!("Expected DownloadSuccess but got {:?}", event.content)
            };
            println!("Download success. Data: {:?}", bytes);
        }
    }
    Ok(())
}
