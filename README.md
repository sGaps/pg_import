
# `pg_import`, import records into Postgres with python

## Table of Contents

- [TLDR](#tldr)
- [How to use](#usage)
- [Configuration](#configuration)
- [Approach](#approach)
- [Limitations](#limitations)
- [Benchmarks](#benchmarks)
- [Examples](#examples)

## TLDR;

TODO: Project description


## Quick Setup

Clone this repo.
```bash
git clone https://github.com/sGaps/pg_import.git
```

Open the project folder

```bash
cd pg_import
```

If you have an existing database to test, configure the credentials files `.postgres.*` that are inside the `credentials/dev` folder, for example, you can edit the password used to connect
to the database with the following command:
```bash
echo 'my-password' > credentials/dev/.postgres.psw
```

If you don't have a running or existing database, you can create one by executing
the `docker-compose.yml` file also included in this project.
```bash
docker compose up -d
```

## How to use

After completing [Quick setup](#quick-setup), you can import some records with the module notation:
```bash
python3 -m py_import -i <path/to/file.csv>
```

or using the script notation:
```bash
python3 run_import.py -i <path/to/file.csv>
```

You can cancel the process by pressing `Ctrl+C` in the terminal. Before exiting, the program will delete
all the records it inserted. If you press `Ctrl+C` before the rollback finish, you will be able to preserve the
changes made in the db.

Also, you may be interested in seeing some addditional [examples](#examples).

## Configuration

The connection settings must be put in a directory named `credentials/`,
there you should create a directory called `dev` which will contain the
configuration files of your current staging.

This project was created with multiple stagings in mind, so you can have
configurations of different stagings to manage credentials, connection
settings, and misc. parameters as well.

The directory structure that needs this program to work is the following one:
```
{credentials}/
    {staging}/
        secret01
        secret02
```

Where the fragment `{credentials}` is a path that can be set by the
environment variable `$PG_IMPORT_CONFIG_PATH`. It's default value is
`credentials/`. This path must contain a folder called `{staging}`
which can be set by the environment variable `$PG_IMPORT_STAGING`
or by a file contained in `$PWD/.staging`.

When `{stagging}` is not set, the value `dev` is assumed. The
files that are currently loaded from the configuration
`{credentials}/{stagging}/` are:
    - .postgres.db
    - .postgres.psw
    - .postgres.user
    - .postgres.port
    - .postgres.host

for example:
```
./
    credentials/
        dev/
            .postgres.db
            .postgres.psw
            .postgres.user
            .postgres.port
            .postgres.host
        qa/
            .postgres.db
            .postgres.psw
            .postgres.user
            .postgres.port
            .postgres.host
        ...
    pg_import/
        ...
    ...
    .staging
```


## Approach

The first thing I had to consider was to create a testing database.  In this case, I used docker compose to configure a PostgreSQL service easily, which can run with the command:
```bash
docker compose up -d
```
During that process, I created some docker secrets to manage the credentials of the database
and created the configuration based on the directory `./credentials./` (see also [config](#configuration)).


Secondly, I needed to devise a way to connect to the database. So I decided to use `psycopg` because I already have experience with its older version `psycopg2` and also, because it's widely used for projects that involves PostgreSQL databases. After that, I had to create a simple module to connect to the testing database, and a way to create tables easily. So I chose `SqlAlchemy` to help me to define models and perform ORM operations quickly.

Then, I started to create a simple brute force solution to upload all the records from
the input sample, which had +17M records in it. After almost running out of memory, I canceled the process and saw that I only could upload 6.9M of records from python by using 7GiB of RAM.

Knowning the limitations that the brute force approach had, I realized that I had to improve
the solution with several techniques that will be discussed below.

The original solution consisted in using the ORM provided by sqlalchemy and add each record in the session pool, which will increase the memory usage of the program if we do not flush nor commit the results frequently. So I knew that I had to split up the sample into several chunks.

But before diving into the hard problem, I decided to move forward to give the program a better interface so that I could run the tests easier. In that sense, I used argparse to create a simple CLI and so, manage the input and delimiter in a much better way (I also could compose my program with some shell utilities thanks to the CLI interface).

Coming back to the harder problem, I had to see how to split up the data, and also, started to perform some tests over the sizes of the built-in python objects and see how much memory they take when we are using a massive amounts of them.

After doing some refactors, and splitting the massive insertions into smaller ones, I obtained an script that took 18 minutes to import all of the data, which in my opinion was too slow. However, I had no idea of how fast postgres is able to insert/import the 17M records, so I decided to test it by using the command `\copy` that `psql` has.

This command took around 1 minute to insert all of the records in the database, meaning that I had to improve the import process (without using any `COPY` statemen).

I decided to drop the fragment of code that relied on the SQLAlchemy ORM and decided to use a db cursor directly to gain the ability to write raw SQL queries, which improved the times dramatically. It took around +12 minutes to complete the process.

That execution time meant that there was a room to keep improving the code. After some research, I came accross a fragment of the documentation of psycopg which tells that since the third major version of the library, the default cursors were Server-sided cursors instead of the ones used in the version two, that can be considered as Client-sided cursors.

It seems that Client-sided cursors let us create more efficient queries at the cost of compromising the security abstraction layer that offers the psycopg. As our goal is reduce the execution time of the program, I decided to use these cursors in the pieces of code that needed a huge improvement. (An additional comment about this matter is in this commit 897960a8e34ad3be5fb7c3f9faeca43e7f3537f3).

After improving the insertions, I had to work on deletions. I implemented a table with name ImportRegistry that holds information about the records that has been imported into the model SaleOrder, the one that will contain the data that we are trying to import. I decided to write some critical deletions by using client cursors as well.

Later, having a working example, I decided to benchmark the program again and the results were impressive: The script took +8 minutes to complete the task!

Finally, I started to clean up the code, add documentation, test the code to check that it's working properly, and finally, adding some features to improve the CLI interface.

## Limitations

This project was made only for educative purposes, so the program assumes that the
csv always have the same csv format like the one described in the following table:

|PointOfSale|Product     |Date      |Stock|
|-----------|------------|----------|-----|
|POS-TEST   |PRODUCT-TEST|2019-08-17|10   |
|...        |...         |...       |...  |

which can be translated to a plain csv file similar to:
```csv
PointOfSale;Product;Date;Stock
POS-TEST;PRODUCT-TEST;2019-08-17;10
...
```

## Benchmarks

Environment used for the test was:
- Hardware
    - CPU: i5-10400
    - RAM: 2x8 GiB @ 2400 MT/s
    - SWAP: 16GiB (HDD)
- Software:
    - OS: Manjaro 23.0
    - Kernel: 5.15.131-1

The parameter that is being tested is `CHUNK_SIZE`. This indicates the size of record frames that we send
to the database to perform an incremental create/update/delete. After experimenting with several settings for
this parameter, we could obtain the following results:

| CHUNK_SIZE | Exec. Time (min) | Exec. Time (max) | RAM (min) | RAM (max) | CPU% Sinle thread | Optimal |
| ---        | ---              | ---              | ---       | ---       | ---               | ---     |
| 1_000_000 | 08m 53s | ??????? | 583 MiB | 002 GiB | 100% | |
| 100_000   | 08m 33s | 08m 55s | 410 MiB | 680 MiB | 100% | |
| 10_000    | 08m 30s | 08m 53s | 093 MiB | 150 MiB | 079% | Yes |
| 1_000     | 10m 55s | 11m 03s | 053 MiB | 069 MiB | 070% | |


## Examples

Show help
```bash
python3 -m pg_import
```

Read csv from stdin

```bash
python3 -m pg_import
```

Read csv without header fom stdin

```bash
python3 -m pg_import --no-header
```

Reading from file
```bash
python3 -m pg_import -i values.csv
```

Alternatively
```bash
python3 -m pg_import --input values.csv
```

Specifiying separator or delimiter (it has the value `';'` by default).
```bash
python3 -m pg_import -d , values_with_comma.csv
```

Or also works with
```bash
python3 -m pg_import --delimiter=, values_with_comma.csv
```

Combining with other comands. In this example, we remove the header of the file, with `tail`, take the first 100 records, sort them and finally, importing them into the database.
```bash
tail -n +2 values.csv | head -n 100 | sort | python3 -m pg_import --no-header 
```

Using the `run_import.py` script instead of the module `pg_import`
```bash
python3 run_import.py -i values.csv
```

Using connection settings from staging an specific staging:
```bash
env PG_IMPORT_STAGING=QA python3 -m pg_import -i values.csv
```

Using custom connection settings path and staging:
```bash
env PG_IMPORT_STAGING=QA PG_IMPORT_CONFIG_PATH=../different/location/credentials python3 -m pg_import -i values.csv
```




