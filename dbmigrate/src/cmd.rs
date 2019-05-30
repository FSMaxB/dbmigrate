use std::path::Path;
use std::time::Instant;

use dbmigrate_lib::{Driver, create_migration, Migrations, Migration, Direction};
use print;
use errors::{Result};


// Does the whole migration thingy, along with timing and handling errors
fn migrate(driver: &dyn Driver, migration: &Migration, number: u16, direction: Direction) -> Result<()> {
    println!("Running {} migration #{}: {}", direction.to_string(), number, migration.name);

    let start = Instant::now();

    let (number, content) = match direction {
        Direction::Up => (number, migration.up.to_owned()),
        Direction::Down => (number - 1, migration.down.to_owned()),
    };

    driver.migrate(content, number)?;

    let duration = start.elapsed();
    print::success(&format!("> Done in {} second(s)", duration.as_secs()));
    Ok(())
}

pub fn create(migration_files: &Migrations, path: &Path, slug: &str) -> Result<()> {
    let current_number = migration_files.keys().cloned().max().unwrap_or(0u16);
    let number = current_number + 1;
    match create_migration(path, slug, number) {
        Err(e) => Err(e.into()),
        Ok(_) => {
            print::success("Migration files successfully created!");
            Ok(())
        }
    }
}


pub fn status(driver: Box<Driver>, migrations: &Migrations) -> Result<()> {
    let current = driver.get_current_number();

    if current == 0 {
        print::success("No migration has been ran");
    }
    for (number, migration) in migrations.iter() {
        let name = &migration.name;
        if number == &current {
            print::success(&format!("{} - {} (current)", number, name));
        } else {
            println!("{} - {}", number, name);
        }
    }
    Ok(())
}


pub fn up(driver: Box<Driver>, migrations: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    let max = migrations.keys().max().unwrap();
    if current == *max {
        print::success("Migrations are up-to-date");
        return Ok(());
    }

    for (number, migration) in migrations.iter() {
        if number > &current {
            migrate(driver.as_ref(), migration, *number, Direction::Up)?;
        }
    }
    Ok(())
}

pub fn down(driver: Box<Driver>, migrations: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No down migrations to run");
        return Ok(());
    }

    let numbers = migrations.keys()
        .rev()
        .cloned()
        .filter(|i| i <= &current);

    for number in numbers {
        let migration = migrations.get(&number).unwrap();
        migrate(driver.as_ref(), migration, number, Direction::Down)?;
    }
    Ok(())
}

pub fn redo(driver: Box<Driver>, migrations: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No migration to redo");
        return Ok(());
    }
    let migration = migrations.get(&current).unwrap();


    migrate(driver.as_ref(), migration, current, Direction::Down)?;
    migrate(driver.as_ref(), migration, current, Direction::Up)?;
    Ok(())
}


pub fn revert(driver: Box<Driver>, migrations: &Migrations) -> Result<()> {
    let current = driver.get_current_number();
    if current == 0 {
        print::success("No migration to revert");
        return Ok(());
    }
    let migration = migrations.get(&current).unwrap();

    migrate(driver.as_ref(), migration, current, Direction::Down)?;
    Ok(())
}
