use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::collections::{BTreeMap};

use regex::Regex;
use errors::{Result, ResultExt};

/// A migration direction, can be Up or Down
#[derive(Debug, PartialEq)]
pub enum Direction {
    /// Self-explanatory
    Up,
    /// Self-explanatory
    Down
}

impl ToString for Direction {
    fn to_string(&self) -> String {
        match *self {
            Direction::Up => "up".to_owned(),
            Direction::Down => "down".to_owned()
        }
    }
}

/// A single direction migration file
#[derive(Debug)]
pub struct MigrationFile {
    /// Content of the file
    pub content: Option<String>,
    /// Direction
    pub direction: Direction,
    /// Number
    pub number: i32,
    /// Filename
    pub filename: String,
    /// Actual migration name (filename with number removed)
    pub name: String
}

pub struct MigrationFileName {
    pub number: u16,
    pub name: String,
    pub direction: Direction,
}

/// A migration has 2 files: one up and one down
#[derive(Debug)]
pub struct Migration {
    /// The Up file
    pub up: Option<MigrationFile>,
    /// The Down file
    pub down: Option<MigrationFile>
}

/// Simple way to hold migrations indexed by their number
pub type Migrations = BTreeMap<i32, Migration>;

impl MigrationFile {
    /// Used when getting the info, therefore setting content to None at that point
    fn new(filename: &str, name: &str, number: i32, direction: Direction) -> MigrationFile {
        MigrationFile {
            content: None,
            filename: filename.to_owned(),
            number: number,
            name: name.to_owned(),
            direction: direction
        }
    }
}

/// Creates 2 migration file: one up and one down
pub fn create_migration(path: &Path, slug: &str, number: i32) -> Result<()> {
    let fixed_slug = slug.replace(" ", "_");

    let migration_filename_up = MigrationFileName {
        number: number as u16,
        name: fixed_slug,
        direction: Direction::Up,
    };
    let filename_up = migration_filename_up.to_string();
    MigrationFileName::parse(&filename_up)?;

    let migration_filename_down = MigrationFileName {
        direction: Direction::Down,
        ..migration_filename_up
    };
    let filename_down = migration_filename_down.to_string();
    MigrationFileName::parse(&filename_down)?;

    println!("Creating {}", filename_up);
    File::create(path.join(filename_up.clone())).chain_err(|| format!("Failed to create {}", filename_up))?;
    println!("Creating {}", filename_down);
    File::create(path.join(filename_down.clone())).chain_err(|| format!("Failed to create {}", filename_down))?;

    Ok(())
}

/// Read the path given and read all the migration files, pairing them by migration
/// number and checking for errors along the way
pub fn read_migration_files(path: &Path) -> Result<Migrations> {
    let mut btreemap: Migrations = BTreeMap::new();

    for entry in fs::read_dir(path).chain_err(|| format!("Failed to open {:?}", path))? {
        let entry = entry.unwrap();
        // Will panic on invalid unicode in filename, unlikely (heh)
        let filename_bytes = entry.file_name();
        let filename = filename_bytes.to_str().unwrap();
        let info = match MigrationFileName::parse(filename) {
            Ok(info) => info,
            Err(_) => continue,
        };
        let mut file = File::open(entry.path())
            .chain_err(|| format!("Failed to open {:?}", entry.path()))?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        let migration_file = MigrationFile {
            content: Some(content),
            direction: info.direction,
            number: info.number as i32,
            filename: filename.into(),
            name: info.name,
        };
        let migration_number = migration_file.number;
        let mut migration = match btreemap.remove(&migration_number) {
            None => Migration { up: None, down: None },
            Some(m) => m
        };
        if migration_file.direction == Direction::Up {
            migration.up = Some(migration_file);
        } else {
            migration.down = Some(migration_file);
        }
        btreemap.insert(migration_number, migration);
    }

    // Let's check the all the files we need now
    let mut index = 1;
    for (number, migration) in &btreemap {
        if index != *number {
            bail!("Files for migration {} are missing", index);
        }
        if migration.up.is_none() || migration.down.is_none() {
            bail!("Migration {} is missing its up or down file", index);
        }
        index += 1;
    }
    Ok(btreemap)
}

impl MigrationFileName {
    /// Gets a filename and check whether it's a valid format.
    /// If it is, grabs all the info from it
    pub fn parse(filename: &str) -> Result<MigrationFileName> {
        let re = Regex::new(
            r"^(?P<number>[0-9]{4})\.(?P<name>[_0-9a-zA-Z]*)\.(?P<direction>up|down)\.sql$"
        ).unwrap();

        let caps = match re.captures(filename) {
            None => bail!("File {} has an invalid filename", filename),
            Some(c) => c
        };

        // Unwrapping below should be safe (in theory)
        let number = caps.name("number").unwrap().as_str().parse::<u16>().unwrap();
        let name = caps.name("name").unwrap().as_str().to_string();
        let direction = if caps.name("direction").unwrap().as_str() == "up" {
            Direction::Up
        } else {
            Direction::Down
        };

        Ok(MigrationFileName {
            number,
            direction,
            name,
        })
    }
}

impl ToString for MigrationFileName {
    fn to_string(&self) -> String {
        format!("{:04}.{}.{}.sql", self.number, self.name, self.direction.to_string())
    }
}


#[cfg(test)]
mod tests {
    use super::{MigrationFileName, read_migration_files, Direction};
    use tempdir::TempDir;
    use std::path::{PathBuf};
    use std::io::prelude::*;
    use std::fs::File;

    fn create_file(path: &PathBuf, filename: &str) {
        let mut new_path = path.clone();
        new_path.push(filename);
        let mut f = File::create(new_path.to_str().unwrap()).unwrap();
        f.write_all(b"Hello, world!").unwrap();
    }

    #[test]
    fn test_parse_good_filename() {
        let result = MigrationFileName::parse("0001.tests.up.sql").unwrap();
        assert_eq!(result.number, 1);
        assert_eq!(result.name, "tests");
        assert_eq!(result.direction, Direction::Up);
    }

    #[test]
    fn test_parse_bad_filename_format() {
        // Has _ instead of . between number and name
        let result = MigrationFileName::parse("0001_tests.up.sql");
        assert_eq!(result.is_ok(), false);
    }

    #[test]
    fn test_migration_filename_to_string() {
        let migration_file_name = MigrationFileName {
            number: 1,
            name: "initial".to_string(),
            direction: Direction::Up
        };
        let result = migration_file_name.to_string();
        assert_eq!(result, "0001.initial.up.sql");
    }

    #[test]
    fn test_parse_good_migrations_directory() {
        let pathbuf = TempDir::new("migrations").unwrap().into_path();
        create_file(&pathbuf, "0001.tests.up.sql");
        create_file(&pathbuf, "0001.tests.down.sql");
        create_file(&pathbuf, "0002.tests_second.up.sql");
        create_file(&pathbuf, "0002.tests_second.down.sql");
        let migrations = read_migration_files(pathbuf.as_path());

        assert_eq!(migrations.is_ok(), true);
    }

    #[test]
    fn test_parse_missing_migrations_directory() {
        let pathbuf = TempDir::new("migrations").unwrap().into_path();
        create_file(&pathbuf, "0001.tests.up.sql");
        create_file(&pathbuf, "0001.tests.down.sql");
        create_file(&pathbuf, "0002.tests_second.up.sql");
        let migrations = read_migration_files(pathbuf.as_path());

        assert_eq!(migrations.is_err(), true);
    }

    #[test]
    fn test_parse_skipping_migrations_directory() {
        let pathbuf = TempDir::new("migrations").unwrap().into_path();
        create_file(&pathbuf, "0001.tests.up.sql");
        create_file(&pathbuf, "0001.tests.down.sql");
        create_file(&pathbuf, "0003.tests_second.up.sql");
        create_file(&pathbuf, "0003.tests_second.down.sql");
        let migrations = read_migration_files(pathbuf.as_path());

        assert_eq!(migrations.is_err(), true);
    }
}
