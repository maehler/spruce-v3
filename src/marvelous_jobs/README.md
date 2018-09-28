# Marvelous jobs

This is a simple SLURM job manager for [MARVEL](https://github.com/schloi/MARVEL).
At the moment, this tool only supports slurm, and it is most likely dependent on the setup of [UPPMAX](https://uppmax.uu.se).

This tool was developed to help manage the assembly of version 3 of the spruce (*Picea abies*) genome, and at the moment the tool is limited to manage the first alignment steps of the MARVEL workflow.

## Requirements

- An installation of [MARVEL](https://github.com/schloi/MARVEL), including the python library
- [PySlurm](https://github.com/PySlurm/pyslurm)

## Installation

It is preferable to install the package in a virtual environment, such as conda, together with MARVEL.
You can either install it directly with `pip` as such:

```sh
pip install 'git+https://github.com/bschiffthaler/spruce-v3.git#subdirectory=src/marvelous_jobs'
```

Or you can clone the repository and install it that way:

```sh
git clone https://github.com/bschiffthaler/spruce-v3
pip install spruce-v3/src/marvelous_jobs
```

If you want to play around with the code and see the change, just add the `-e` flag to the `pip` command.

## Usage

As an example use case, I will use the [*E. coli* example data](https://github.com/schloi/MARVEL/tree/master/examples/e.coli_pacbio) that is included in the MARVEL repository. The sequence data is compressed, so we will need to uncompress that before we continue.

```sh
gunzip p6.25x.fasta.gz
```

First we need to initialise the project in the directory where all files will be saved.
This can take the expected coverage as a parameter, as well as a SLURM account number that will be used for all job submissions in the project.
The last argument is simply the name of the project and will be used as an identifier in file names.

```sh
marvelous_jobs init --account <slurm account> --coverage 25 ecoli
```

This will create the config file `config.ini` as well as the SQLite database `marveldb`.
The config file will contain parameters that are common for all jobs of the same type.
It can be edited manually, but at the moment it cannot be guaranteed that this work as expected.
In most cases this file gets updated automatically, so there should be no need to edit it manually.
Some edits might break the whole workflow, so proceed with caution.

The SQLite database contains information about all jobs that planned.
Job IDs and statuses are kept here in order to enable quick and easy restart of failed or otherwise cancelled jobs.

The next step is to prepare the sequence data for MARVEL:

```sh
marvelous_jobs prepare --blocksize 20 p6.25x.fasta
```

This will submit a batch script that will convert the data into the format that MARVEL requires, as well as splitting the data into blocks.
Depending on the size of the data, this can take everything from a couple of seconds to several hours.
For this *E. coli* data, it takes about 10 seconds.
For 30X PacBio data from spruce, it takes almost 8 hours.

Now everything is ready for the first alignment step, and you have to choose whether you want to use the masking server or not.
The masking server can be used to speed up the alignments by ignoring repeat regions.
If the genome that you want to assemble is big (such as the axolotl or the spruce genome), then this can be advisable.

The masking server is started as such:

```sh
marvelous_jobs mask start --node <slurm node name>
```

The `--node` argument is needed since we need to tell the alignment jobs where to access the masking server.
Since the job is not likely to start right away in most cases, we explicitly request a node where it should run and thus know the IP of the masking server right away.

Starting the alignment jobs is then done by running the command:

```sh
marvelous_jobs daligner
```

This adds all the alignment jobs to the database and submits them to the queueing system.
By default, these jobs require the masking server, and therfore they have the dependency that they won't start until the after masking server has started.
If you don't want to use the masking server, then simply run the above command with the `--no-masking` flag.

If some jobs should fail along the way, then this command is useful:

```sh
marvelous_jobs fix
```

It will check the status of the masking server and all alignment jobs.
If the masking server is down, all currently running alignment jobs will be cancelled, and the masking server will be restarted.
If the IP of the masking server changes, then all queued alignemnt jobs will be cancelled as well.
Finally all cancelled or failed alignment jobs will be restarted.

The current status can be checked using the command:

```sh
marvelous_jobs info
```

## Command line documentation

### `marvelous_jobs`

```
usage: marvelous_jobs mask start [-h] -w NODE [-t THREADS] [-p PORT]

optional arguments:
  -h, --help            show this help message and exit
  -w NODE, --node NODE  node where to run the process
  -t THREADS, --threads THREADS
                        number of worker threads (default: 4)
  -p PORT, --port PORT  port to listen to (default: 12345)
(marvel) [niklasm@rackham1 marvel_test]$ marvelous_jobs -h
usage: marvelous_jobs [-h] {init,info,prepare,mask,daligner,fix} ...

Job manager for MARVEL

optional arguments:
  -h, --help            show this help message and exit

subcommands:
  {init,info,prepare,mask,daligner,fix}
    init                Initialise a new project
    info                Show project info
    prepare             Prepare data files
    mask                Masking server
    daligner            Run daligner
    fix                 Update and restart jobs
```

### `marvelous_jobs init`

```
usage: marvelous_jobs [-h] {init,info,prepare,mask,daligner,fix} ...

Job manager for MARVEL

optional arguments:
  -h, --help            show this help message and exit

subcommands:
  {init,info,prepare,mask,daligner,fix}
    init                Initialise a new project
    info                Show project info
    prepare             Prepare data files
    mask                Masking server
    daligner            Run daligner
    fix                 Update and restart jobs
(marvel) [niklasm@rackham1 marvel_test]$ marvelous_jobs init -h
usage: marvelous_jobs init [-h] [-A ACCOUNT] [-x COVERAGE] [-f]
                           name [directory]

positional arguments:
  name                  name of the project
  directory             directory where to initialise the run (default: .)

optional arguments:
  -h, --help            show this help message and exit
  -A ACCOUNT, --account ACCOUNT
                        SLURM account where jobs should be run
  -x COVERAGE, --coverage COVERAGE
                        sequencing coverage of the project (default: 30)
  -f, --force           force overwrite of existing database
```

### `marvelous_jobs info`

```
usage: marvelous_jobs info [-h]

optional arguments:
  -h, --help  show this help message and exit
```

### `marvelous_jobs prepare`

```
usage: marvelous_jobs prepare [-h] [-s N] [-f] [-d SCRIPT_DIRECTORY]
                              [-l LOG_DIRECTORY]
                              fasta

positional arguments:
  fasta                 input reads in FASTA format

optional arguments:
  -h, --help            show this help message and exit
  -s N, --blocksize N   database block size in megabases (default: 200)
  -f, --force           force prepare
  -d SCRIPT_DIRECTORY, --script-directory SCRIPT_DIRECTORY
                        directory where to store scripts (default: scripts)
  -l LOG_DIRECTORY, --log-directory LOG_DIRECTORY
                        directory where to store log files (default: logs)
```

### `marvelous_jobs mask`

```
usage: marvelous_jobs mask [-h] {status,start,stop} ...

optional arguments:
  -h, --help           show this help message and exit

masking command:
  {status,start,stop}
    status             masking server status
    start              start masking server
    stop               stop masking server
```

```
usage: marvelous_jobs mask start [-h] -w NODE [-t THREADS] [-p PORT]

optional arguments:
  -h, --help            show this help message and exit
  -w NODE, --node NODE  node where to run the process
  -t THREADS, --threads THREADS
                        number of worker threads (default: 4)
  -p PORT, --port PORT  port to listen to (default: 12345)
```

```
usage: marvelous_jobs mask stop [-h]

optional arguments:
  -h, --help  show this help message and exit
```

```
usage: marvelous_jobs mask status [-h]

optional arguments:
  -h, --help  show this help message and exit
```
