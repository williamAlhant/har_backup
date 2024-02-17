use clap::{Parser, Args, Subcommand};
use anyhow::{Result, Context};
use std::path::{Path, PathBuf};
use log::debug;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(
        about="Create an encryption key",
        after_help="The key is used to encrypt/decrypt blobs. It is up to you to store it safely.",
    )]
    CreateKey(CreateKey),
    #[command(
        about="Initialize the local archive directory",
        after_help="It makes the current working directory the archive root.\n\
                    It creates a .har directory containing config/metadata",
    )]
    InitLocal,
    #[command(
        about="Fetch the remote manifest",
        after_help="It stores the manifest in .har",
    )]
    FetchManifest,
    #[command(
        about="Compare local tree with fetched manifest",
        after_help="Do not forget to fetch before.",
    )]
    Diff,
    #[command(
        about="Push changes from local to remote",
        after_help="It diffs local tree with fetched remote manifest.\n\
                    It uploads new files, directories and uploads the updated manifest.",
    )]
    Push
}

#[derive(Args, Debug)]
struct CreateKey {
    path: PathBuf,
}

fn main() -> Result<()> {

    use har_backup::cmd_impl::{WithLocal, WithRemoteAndLocal};

    env_logger::init();
    let cli = Cli::parse();
    match cli.command {
        Command::CreateKey(sub_cli) => create_key(&sub_cli.path),
        Command::InitLocal => init_local(),
        Command::FetchManifest => WithRemoteAndLocal::new()?.fetch_manifest(),
        Command::Diff => WithLocal::new()?.diff(),
        Command::Push => WithRemoteAndLocal::new()?.push(),
    }
}

fn write_file_without_overwrite(path: &Path, content: &[u8]) -> Result<()> {
    if path.exists() {
        anyhow::bail!("{} already exists", path.to_str().unwrap());
    }
    std::fs::write(path, content)?;
    Ok(())
}

fn create_key(path: &Path) -> Result<()> {
    let path_str = path.to_str().context("Convert path to str")?;
    println!("Creating key");
    let key = har_backup::blob_encryption::create_key();
    write_file_without_overwrite(path, key.as_slice()).context("Writing key to file")?;
    println!("key stored at {}", path_str);
    Ok(())
}

fn init_local() -> Result<()> {
    use har_backup::dot_har::DOT_HAR_NAME;
    if Path::new(DOT_HAR_NAME).exists() {
        anyhow::bail!("It looks like this has been initialized already!")
    }
    std::fs::create_dir(DOT_HAR_NAME)?;
    println!("Archive initialized.");
    Ok(())
}
