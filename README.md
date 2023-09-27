
# pg_import: import csv rows to a db with python

## Table of Contents

  - [About](#about)
  - [Quick Setup](#quick-setup)
  - [How to use](#how-to-use)
  - [Configuration](#configuration)
  - [Approach](#approach)
  - [Limitations](#limitations)
  - [Benchmarks](#benchmarks)
  - [Examples](#examples)

## About

Import records from a csv input to a existing postgres database without using the instruction `COPY`.

Execute the command and wait until the import process is complete:
```bash
python3 -m py_import -i path/to/your/file.csv
```

## Quick Setup

Clone this repo.
```bash
git clone https://github.com/sGaps/pg_import.git
```

Open the project folder
```bash
cd pg_import
```

If you have a testing database already, you may be interested in configuring the credentials files `.postgres.*` that are inside the `credentials/dev` folder. For example, if you want to edit the password used to connect to the database, then you should edit the file `credentials/dev/.postgres.psw`.
```bash
echo 'my-password' > credentials/dev/.postgres.psw
```

If you don't have any existing database, you can create a new one by executing
```bash
docker compose up -d
```

And you won't need to create the tables manually, `pg_import` will create the tables it needs to work.

## How to use

After completing [Quick setup](#quick-setup), you can import some records using the module notation:
```bash
python3 -m py_import -i <path/to/file.csv>
```

or using the script utility:
```bash
python3 run_import.py -i <path/to/file.csv>
```

Note that you can cancel the process by pressing `Ctrl+C` in the terminal or sending a `SIGTERM` to the process. Before exiting, the program will delete all records it has inserted. If you press `Ctrl+C` before the rollback process ends, you will be able to preserve the changes made in the db. (but sometimes, the rollback will be so fast that you won't be able to cancel it).

Also, you may be interested in seeing some addditional [examples](#examples).

## Configuration

This project loads its configuration from a directory named `credentials/`,
which should contain a subfolder named `dev`, which can have several files
that define the values used to create PostgreSQL Connections.

```
./
    credentials/
        dev/
            .postgres.db
            .postgres.user
            ...
```

For now, it seems that it's a little weird to have a folder with a subfolder to
contain connection arguments. Well, it actually is!, but there's a reason for that.

`pg_import` accepts a parameter named `staging`, which instructs the program to load
a bunch of parameters from another subfolder so that you can keep all those 
configurations in your workflow. 

By default, `staging` has the value `dev`, so it can load the files inside `credentials/dev`.
But you can specify the staging by setting the environment variable `$PG_IMPORT_STAGING` or
also by adding a file named `.staging` in your current folder. (note that the env variable has
precedence over the `.staging` file).

```bash
env PG_IMPORT_STAGING=qa python3 -m pg_import -i ...
# > will load the files inside: `credentials/qa`
```

Also, you can specify the directory that holds the `staging` folders by setting the
environment variable `PG_IMPORT_CONFIG_PATH`, which has the value `credentials` as
default.

Currently, the files that are loaded from the path `{credentials}/{stagging}` are:
- `.postgres.db`
- `.postgres.psw`
- `.postgres.user`
- `.postgres.port`
- `.postgres.host`



## Approach

The goal was to insert more than 17M records without using the `COPY` directive that `PostgreSQL` implements, and using a program written in python to achieve it.

So, the first thing I needed was a way to create a database quickly. In this case, I used
docker compose to configure a PostgreSQL service quickly, which can be run with the command:
```bash
docker compose up -d
```

Then, I added some docker secrets to manage the credentials and organize it into a basic
configuration structure like the one seen in the [configuration](#configuration) section.

Already having a database, the next step was to create a simple connection to the database using
python so I considered to use the package `psycopg` because it's widely used for projects that
involve Postgres databases. Also, I used `psycopg2` before, so I thought it was
good to experiment with a newer version of that package.

After that, I implemented a simple connection script to perform some queries and check whether
the connection works fine. Later, I saw that I needed to create an initial schema, so I chose
the package `sqlalchemy` to help me define the tables/models the program requires by using
the declarative interface and ORM that this component provides.

Later, I designed the schema with two models: `SaleOrder` (indexed) which holds the data that we are trying to import and `ImportRegistry`, that keeps track of the records that were imported in `SaleOrder`. But why not to just add a simple `Boolean` to mark the records that were imported?

> Well, It could have worked for this simple example

But if we tried to move this solution to a general case, we could have some problems with the existing database tables. To implement the `Boolean` solution, we need to add this field to every table that will implement an import scheme. This implies that we have to choose a name that doesn't cause any conflict with the existing columns of all tables, which is hard.

In contrast, having a separate table that keeps track every import is more suitable for a growing application. If we want to track imports on other tables, 
we could just add a new attribute in this model that holds the name of a table where an import process has been performed. And as we know that we only import massive amounts of data, I decided to have the records `start_id` and
`end_id` to represent import ranges.

Having the schema and both packages included in the project, I made some simple scritps that implemented a brute force solution, which consisted on uploading all records at once. The purpose of this approach was exploring how many resources the program needs to process the whole sample.

After almost running out of memory, I canceled the process and saw that It could upload 6.9M of records by using 7GiB of RAM. Knowning the limitations the brute force approach had, I realized that a huge improvement was needed in order to come up with a real solution, so I started analyzig what went wrong.

The first thing I detected was that the original scripts were creating ORM records in memory and saving them in the session pool. Each time we created a new record, the memory usage of the session was increasing as well until reaching the point where the program couldn't allocate more memory. With that in mind, we could just divide the sample into chunks and send them to the database.

To support this solution proposal, I designed an incremental import processing, where the program loads chunks of 100k records and commits the changes after that. But as I was still using the ORM, it took around 18 minutes and 4GiB of RAM to complete the process.

Having a minimal working program, I decided to implement a simple but useful CLI interface to make easier performing the tests. During this step, I thought that `pg_import` needed to be composable, so it should be able to read from stdin, and as it was accepting csv files, it also needed a way to specify the column delimiters used in the input. The right tool to create a simple CLI was the module `argparse`, which is much simpler than the `getopts` alternative.

Coming back to the harder problem, I performed an import test with the command `psql` and its built-in instruction `\copy` to see how fast it can load the sample of 17M records. The test took 1 minute and few seconds. It seems that `COPY` uses a large set of optimizations to ensure that the data will be available as soon as possible.

Knowing that the challenge is to avoid using this bulk-insert operation, I needed to follow some additional strategies to provide an efficient and scalable solution.

I decided to drop the fragment of code that relied on the SQLAlchemy's ORM for the insert operations and decided to use a db cursor directly. This is possible because we are only interested in submit data in a PostgreSQL instance, so we don't worry about the compatibility layer among database engines that the ORM provides.

After rewriting the ORM queries as raw SQL queries, the execution time improved dramatically. It took around 12 minutes to complete the task, although, the program was not fast enough.

I knew that there was room to keep improving the code, so I started a research on the `psycopg` documentation, looking for parameters, configurations and utilites that could improve the performance even more. After a while, I came accross a fragment of the documentation which said that since the third major version of the package, every conection created by the database engine used *Server-sided* cursors by default. This kind of cursor offers more reliability and let library users to write pythonic code with results that come from a `cursor.execute` call.

The Server-sided cursors also split the queries that have more than a single record in it, which causes an overhead and make harder to define efficient queries with raw sql. So I decided to switch to `Client-sided` cursors. These, are the same that `psycopg2` uses, and they provide an interface to write queries without limiting the amount of records sent and returned as the other cursor type does. The previous benefits come at the cost of compromsing the security abstraction layer that offered by the new cursor type. (An additional comment about this matter can be found in the commit 897960a8e34ad3be5fb7c3f9faeca43e7f3537f3).

After inserting the `Client-sided` cursors on the bulk insertions and deletions, the process
took much less time to complete the task: 8 minutes and 53 seconds. Unfortunately, deleting the
records from the previous import process took too much time in the next program execution (after 10 whole minutes, I
canceled the delete operation), so I noticed that I needed to change the query in order
to optimize the execution time.

Later, I tested the program and it took 8 minutes and 45s to finish, including the
deletions that must be performed before inserting the new records.

Now, having the program that solves the problem, I started to tune it by changing the value
of a constant named `CHUNK_SIZE` until I found the optimal size that the chunk must have
in order to consume the minimal amount of resources.

Finally, I started to clean up the code, add documentation, test the code to check that it's working properly, and finally, adding some features to improve the CLI interface.

> SIDE NOTE: We could have used temporal unlogged tables among the one used to store the actual data, but this kind of table would be truncated if the database fails in the middle of the process (for example, if there's a power outage).

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

Environment used for the test:
- Hardware
    - CPU: i5-10400
    - RAM: 2x8 GiB @ 2400 MT/s
    - SWAP: 16GiB (HDD)
- Software:
    - OS: Manjaro 23.0
    - Kernel: 5.15.131-1

The parameter that is being tested is `CHUNK_SIZE`. This indicates the size of record frames that we send
to the database to perform an incremental create/update/delete. After experimenting with several settings for
this parameter, we obtained the following results:

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

Combining `pg_import` with other shell utilities. In this example, we remove the header of the file, with `tail`, take the first 100 records, sort them and finally, importing them into the database.
```bash
tail -n +2 values.csv | head -n 100 | sort | python3 -m pg_import --no-header 
```

Using the `run_import.py` script instead of the module `pg_import`
```bash
python3 run_import.py -i values.csv
```

Using connection settings from a specific staging:
```bash
env PG_IMPORT_STAGING=QA python3 -m pg_import -i values.csv
```

Using custom path and staging for the connection settings:
```bash
env PG_IMPORT_STAGING=QA PG_IMPORT_CONFIG_PATH=../different/location/credentials python3 -m pg_import -i values.csv
```
