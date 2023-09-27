
# pg_import: import csv rows to a db with python

## Table of Contents

- [About](#about)
- [How to use](#usage)
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

If you have a testing database already, you may be interested in configure the credentials files `.postgres.*` that are inside the `credentials/dev` folder. For example, if you want to edit the password used to connect to the database, then you should edit the file `credentials/dev/.postgres.psw`.
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

Note that you can cancel the process by pressing `Ctrl+C` in the terminal o sending a `SIGTERM` to the process. Before exiting, the program will delete all records it has inserted. If you press `Ctrl+C` before the rollback process ends, you will be able to preserve the changes made in the db. (but sometimes, the rollback will be so fast that you won't be able to cancel it).

Also, you may be interested in seeing some addditional [examples](#examples).

## Configuration

This project loads its configuration from a directory named `credentials/`,
which should contain a subfolder named `dev` which can have several files
that defines the values used to create PostgreSQL Connections.

```
./
    credentials/
        dev/
            .postgres.db
            .postgres.user
            ...
```

For now, it seems that it's a little weird to have a folder wth a subfolder to
contain connection arguments. Well, it actually is!, but there's a reason for that.

`pg_import` accepts a parameter named `staging`, which instructs the program to load
a bunch of parameters from another subfolder so that you can keep all those the
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

Currently, the files that are currently loaded from the path `{credentials}/{stagging}` are:
- `.postgres.db`
- `.postgres.psw`
- `.postgres.user`
- `.postgres.port`
- `.postgres.host`



## Approach

The goal is to insert more than 17M without using the `COPY` directive that `PostgreSQL` implements, and using a program written in python to achieve it.

So, the first thing I needed was a way to create a database quickly. In this case, I used
docker compose to configure a PostgreSQL service quickly, which can be run with the command:
```bash
docker compose up -d
```

Then, I added some docker secrets to manage the credentiasl and organize it into a base
configuration structure like the one seen in the [configuration](#configuration) section.

Already having a database, the next step was to create a simple connection to the database using
pythonm so I considered to use the package `psycopg` because it's widely used for projects that
involves Postgres databases and python. Also, I used `psycopg2` before, so I thought it was
good to explore a newer version of the package.

After that, implemented a simple connection script to perform some queries and check whether
the connection works fine. Later, I saw that I needed to define an initial schema so, I chose
the package `sqlalchemy` to help me define the tables/models the program requires by using
the declarative interface and ORM that this component provides.

With both packages included in the project, I made some simple scritps that implenented a brute force solution, which consisted on uploading all records at once. The purpose of this approach was exploring how many resources the program needs to process the whole sample.

After almost running out of memory, I canceled the process and saw that It could upload 6.9M of records by using 7GiB of RAM. Knowning the limitations the brute force approach had, I realized that a huge improvement was needed in order to come up with a real solution, so I started analyzig what's went wrong.

The first thing I detected was that the original scripts were creating ORM records in memory and saving them in the session pool. Each time we created a new record, the memory usage of the session were increasing as well until reaching the point that the program couldn't allocate more memory. With that in mind, we could just divide the sample into chunks and send them to the database.

To support this solution proposal, I designed an incremental import processing, where The programs loads chunks of 100k records and commiting the changes after that. But as I was still using the ORM, it took around 18 minutes and 4GiB of RAM to complete the process.

Having a minimal working program, I decided to implement a simple but useful CLI interface to make easier performing the tests. During this step, I thought that `pg_import` needed to be composable, so it should be able to read from stdin, and as it was accepting csv files, it also needed a way to specify the column delimiters used in the input. The right tool to create a simple cli was the module `argparse`, which is much simpler than the `getopts` alternative.

This new version
[TODO CONTINUE]

comming back to the harder problem, I had to see how to split up the data, and also, started to perform some tests over the sizes of the built-in python objects and see how much memory they take when we are using a massive amounts of them.

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




