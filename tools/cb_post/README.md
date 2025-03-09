# SigLens Benchmark - Data Loading and Query Performance

This document outlines the process for running a benchmark on SigLens 1.0.25, a log analysis platform.

**Infrastructure:**
*   **SigLens Version:** 1.0.25
*   **Deployment:** Local single-node instance.
*   **AWS EC2 Instance:** c6a.4xlarge 
*   **Disk Space:** Requires at least 700GB of free disk space on the main partition for the dataset, intermediate files, and SigLens data.
* **Package manager**: `yum`.
* **Current user**: needs `sudo` privileges.

**Dependencies:**

*   **Operating System:** Amazon Linux 2023
*   **Python 3:** A recent version of Python 3 is required
*   **Git:** For cloning the SigLens source repository [Installed by the benchmark script].
*   **Go:** For building SigLens from source [Installed by the benchmark script].

**Data Loading Process:**
*   The `benchmark.sh` script downloads and decompresses the dataset.
*   The script `fix_hits.py` adds an index string to the dataset JSON file and preprocesses the dataset for loading stage. 
*   The `benchmark.sh` and the `send_data.py` script loads the split files into SigLens using the SigLens Bulk API. 

**Running the benchmark:** 
*   The `benchmark.sh` script is run by running the `benchmark.sh` in bash shell.
*   The `benchmark.sh` script assumes that python 3 is already available in the environment.
*   The `benchmark.sh` 
    *   installs git and go.
    *   downloads and compiles SigLens release 1.0.25.
    *   starts SigLens in the background.
    *   downloads and preprocesses dataset
    *   loads data into SigLens
    *   executes `run.sh` to run the queries and produce results automatically

**Note about queries:**
*   SigLens supports Splunk Query Language. 
*   The SQL queries used by the benchmark have been translated into the splunk query languages. 
*   Three of the original queries are not supported and benchmark does not run those. These 3 queries and corresponding results have been recorded as null in the `queries.spl` and `results.csv` respectively.

**Time to run the benchmark:**
- Install of dependencies, compilation of SigLens and preprocessing of data takes about three hours.
- Data loading process takes about a couple of hours. 