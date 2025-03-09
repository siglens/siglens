Infrastructure:
- Local single-node install of SigLens 1.0.25 on AWS EC2 instance c6a.4xlarge
- Needs 700GB of free disk space to store the test data files, intermediate files and SigLens data

Data loading process: 
- Add an index string to the dataset JSON file 
- Split JSON file into smaller files and load them in prallel via SigLens Bulk API

Running the benchmark: 
- Benchmark assumes that python 3 is already available in the environment
- It installs git, go and downloads and compiles SigLens release 1.0.25 
- Benchmark is run by running the `benchmark.sh` in bash, that in turn executes `run.sh` to run the queries and produce results automatically
- SigLens supports Splunk Query Language. The SQL queries used by the benchmark have been translated into the splunk query languages. 
- Three of the original queries are not supported and benchmark does not run those. These 3 queries and corresponding results have been recorded as null in the `queries.spl` and `results.csv` respectively.

Time to run the benchmark:
- Install of dependencies, compilation of SigLens and preprocessing of data takes about three hours.
- Data loading process takes about a couple of hours. 