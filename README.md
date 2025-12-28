The entry point for your SQLite implementation is in `src/main/java/Main.java`.

1. Ensure you have `mvn` installed locally
2. Run `./your_program.sh` to run your program, which is implemented in
   `src/main/java/Main.java`.

# Functionality Supported

1. Basic Functionality on ".dbinfo" and ".tables"
2. Read data from a single column
3. Read data from multiple columns
4. Filter data with a WHERE clause (Single Page)
5. Retrieve data using a full-table scan ( Multi-Page)
6. Retrieve data using an index 
   ( Use Index if exists by Default, with Full Table Scan as Fallback)


# Sample Databases

To make it easy to test queries locally, we've added a sample database in the
root of this repository: `sample.db`.

This contains two tables: `apples` & `oranges`. You can use this to test your
implementation for the first 6 stages.

You can explore this database by running queries against it like this:

```sh
$ sqlite3 sample.db "select id, name from apples"
1|Granny Smith
2|Fuji
3|Honeycrisp
4|Golden Delicious
```

There are two other databases that you can use:

1. `superheroes.db`:
   - This is a small version of the test database used in the table-scan stage.
   - It contains one table: `superheroes`.
   - It is ~1MB in size.
1. `companies.db`:
   - This is a small version of the test database used in the index-scan stage.
   - It contains one table: `companies`, and one index: `idx_companies_country`
   - It is ~7MB in size.

These aren't included in the repository because they're large in size. You can
download them by running this script:

```sh
./download_sample_databases.sh
```

If the script doesn't work for some reason, you can download the databases
directly from [here](https://github.com/codecrafters-io/sample-sqlite-databases).


## Sample Commands

```sh
   ./your_sqlite3.sh sample.db .dbinfo
   ./your_sqlite3.sh sample.db "SELECT count(*) FROM oranges"
   ./your_sqlite3.sh sample.db "SELECT name FROM apples"
   ./your_sqlite3.sh superheroes.db "select name, first_appearance from superheroes where hair_color = 'Brown Hair'"
   ./your_sqlite3.sh superheroes.db "select count(*) from superheroes where eye_color = 'Blue Eyes'"
   ./your_sqlite3.sh companies.db "SELECT id, name FROM companies WHERE country = 'republic of the congo'"

   # this is a  good one! 
   ./your_sqlite3.sh test.db "SELECT id, name FROM companies WHERE country = 'republic of the congo'"

   # Reroder column Names 
    ./your_sqlite3.sh superheroes.db "select appearance, name from superheroes"

```