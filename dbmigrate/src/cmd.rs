use std::path::Path;
use std::time::Instant;

use dbmigrate_lib::{Driver, create_migration, Migrations, MigrationFile, Direction};
use print;
use errors::{Result};


// Does the whole migration thingy, along with timing and handling errors
fn migrate(driver: &dyn Driver, migration_file: &MigrationFile) -> Result<()> {
    let direction = &migration_file.direction;
    println!("Running {} migration #{}: {}", direction.to_string(), migration_file.number, migration_file.name);

    let start = Instant::now();

    let number = match direction {
        Direction::Up => migration_file.number,
        Direction::Down => migration_file.number - 1,
    };
    let content = migration_file.content.as_ref().unwrap().to_owned();

    driver.migrate(content, number)?;

    let duration = start.elapsed();
    print::success(&format!("> Done in {} second(s)", duration.as_secs()));
    Ok(())
}

pub fn create(migration_files: &Migrations, path: &Path, slug: &str) -> Result<()> {
    let current_number = migration_files.keys().cloned().max().unwrap_or(0i32);
    let number = current_number + 1;
    match create_migration(path, slug, number) {
        Err(e) => Err(e.into()),
        Ok(_) => {
            print::success("Migration files successfully created!");
            Ok(())
        }
    }
}


pub fn status(driver: Box<Driver>, migration_files: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No migration has been ran");
    }
    for (number, migration) in migration_files.iter() {
        let mig_file = migration.up.as_ref().unwrap();
        if number == &current {
            print::success(&format!("{} - {} (current)", mig_file.number, mig_file.name));
        } else {
            println!("{} - {}", mig_file.number, mig_file.name);
        }
    }
    Ok(())
}


pub fn up(driver: Box<Driver>, migration_files: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    let max = migration_files.keys().max().unwrap();
    if current == *max {
        print::success("Migrations are up-to-date");
        return Ok(());
    }

    for (number, migration) in migration_files.iter() {
        if number > &current {
            let mig_file = migration.up.as_ref().unwrap();
            migrate(driver.as_ref(), mig_file)?;
        }
    }
    Ok(())
}

pub fn down(driver: Box<Driver>, migration_files: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No down migrations to run");
        return Ok(());
    }

    let mut numbers: Vec<i32> = migration_files.keys().cloned().filter(|i| i <= &current).collect();
    numbers.sort_by(|a, b| b.cmp(a));

    for number in numbers {
        let migration = migration_files.get(&number).unwrap();
        let mig_file = migration.down.as_ref().unwrap();
        migrate(driver.as_ref(), mig_file)?;
    }
    Ok(())
}

pub fn redo(driver: Box<Driver>, migration_files: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No migration to redo");
        return Ok(());
    }
    let migration = migration_files.get(&current).unwrap();

    let down_file = migration.down.as_ref().unwrap();
    let up_file = migration.up.as_ref().unwrap();

    migrate(driver.as_ref(), down_file)?;
    migrate(driver.as_ref(), up_file)?;
    Ok(())
}


pub fn revert(driver: Box<Driver>, migration_files: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No migration to revert");
        return Ok(());
    }
    let migration = migration_files.get(&current).unwrap();
    let down_file = migration.down.as_ref().unwrap();

    migrate(driver.as_ref(), down_file)?;
    Ok(())
}
