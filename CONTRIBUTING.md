# Siglens Contributor Guide

- [Siglens Contributor Guide](#siglens-contributor-guide)
  - [Ways to Contribute](#ways-to-contribute)
  - [Find an Issue](#find-an-issue)
  - [Ask for Help](#ask-for-help)
  - [Pull Request Lifecycle](#pull-request-lifecycle)
  - [Development Environment Setup](#development-environment-setup)
    - [Start up Siglens](#start-up-siglens)
    - [Send Data to SigLens](#send-data-to-siglens)
    - [Send Queries on Siglens](#send-queries-on-siglens)
  - [Pull Request Checklist](#pull-request-checklist)

Hello there! We are glad that you want to contribute to our project! 💖

As you get started, you are in the best position to give us feedback on areas of
our project that we need help with including:

* Problems found during setting up a new developer environment
* Gaps in our documentation

If anything doesn't make sense, or doesn't work when you run it, please open a
bug report and let us know!

## Ways to Contribute

We welcome many different types of contributions including:

* New features
* Bug fixes
* Documentation
* Issue Triage

Not everything happens through a GitHub pull request. Please join our Slack: [SigLens Community](https://www.siglens.com/slack) and let's discuss how we can work together.


## Find an Issue

We have good first issues for new contributors and help wanted issues suitable
for any contributor. [good first issue](https://github.com/siglens/siglens/labels/good%20first%20issue) has extra information to help you make your first contribution.
[help wanted](https://github.com/siglens/siglens/labels/help%20wanted) are issues suitable for someone who isn't a core maintainer and is good to move onto after your first pull request.

Sometimes there won’t be any issues with these labels. That’s ok! There is
likely still something for you to work on. If you want to contribute but you
don’t know where to start or can't find a suitable issue, you can
join our slack [SigLens Community](https://www.siglens.com/slack)

Once you see an issue that you'd like to work on, please post a comment saying
that you want to work on it. Something like `"I want to work on this"` is fine.

## Open an Issue

### Check for existing Issues

* Before you create a new issue, please search on the [open issues](https://github.com/siglens/siglens/issues) page to see if the issue or feature request has already been filed.
* If you can not find your issue exists, [choose a specific issue type](https://github.com/siglens/siglens/issues/new/choose) and open a new issue.

### Issue Types

* **Bug report**: You’ve found a bug with the code, and want to report or track the bug. Show more details, and let us know about an unexpected error, a crash, or an incorrect behavior.
* **New feature request**: Suggest a new feature. This allows feedback from others before the code is written.
* **Story**: Clearly describe the task. Break it down as much as you can so it's easier to implement. Link to existing issues if appropriate using #issue-number.
* **Report a security vulnerability**: Review our security policy first and then report a vulnerability.

## Ask for Help

The best way to reach us with a question when contributing is to ask on:

* The original github issue
* Our Slack channel : [SigLens Community](https://www.siglens.com/slack)

## Pull Request Lifecycle

Once you have found the issue to be fixed or feature to be added, you can comment on the issue and put the approach you want to follow to solve the issue. Once we agree upon the approach, you can open a PR.

If its your first time on github, please read [FIRST_TIME_GIT_USERS_GUIDE.md](FIRST_TIME_GIT_USERS_GUIDE.md) to understand terms like `fork`, `clone`, `create a repo`, and others.

Steps to open a PR:

1. Fork Siglens repo and clone it on your local machine.  
2. Make your desired code changes
3. Make sure local tests work. (`make all`)
4. Make sure go code is formatted correctly. (`make pr`)
5. Commit your changes to your fork.
6. Create a pull request. (Automated CI tests will run)
7. Once the change has been approved and merged, we will inform you in a comment.


## Development Environment Setup

Download golang version that is defined in `go.mod` and make sure git is installed.

VS Code is the recomended IDE and offers good extensions and tools that will help developers write code.

To run the Go linter locally, install it from [here](https://golangci-lint.run/welcome/install/#local-installation).

Siglens currently support `Linux` and `macOS` for development purposes.

- To setup project locally on macOS please jump to [Start up Siglens](#start-up-siglens) section.
- For Linux systems, the `build-essential` package is necessary. If it's already installed, no action is needed. Otherwise, please execute the following commands:

    - For Debian/Ubuntu:

    ```bash
    sudo apt update
    sudo apt install build-essential
    ```
    
    - For Fedora/CentOS/RHEL:
   ```bash
   sudo yum groupinstall "Development Tools"
   sudo yum install gcc g++ make
   ```

## How to work with PEG

To contribute to PEG related stories, it's important to first understand PEG and pigeon. 

Pigeon is a PEG parser generator for Go. It takes a PEG grammar file and generates a parser written in Go.

For information on the PEG syntax, you can refer to the [official PEG syntax documentation](https://pkg.go.dev/github.com/mna/pigeon#hdr-PEG_syntax).

For a deeper understanding of PEG parsing, you can read this [series of articles](https://medium.com/@gvanrossum_83706/peg-parsing-series-de5d41b2ed60).

Once you have an understanding of PEG and pigeon, you can then update the `.peg` files as needed.

After updating the `.peg` files, you need to convert these PEG files to Go files. 

First, install pigeon using the following command:

```bash
go get -u github.com/mna/pigeon
```

Then, you can convert a PEG file to a Go file using the following command:

```bash
~/go/bin/pigeon -o <path_to_your_go_file> <path_to_your_peg_file>
```
Please replace `<path_to_your_go_file>` and `<path_to_your_peg_file>` with the actual paths to your Go and PEG files.

For example, if you are working with the files `./pkg/ast/spl/spl.peg` and `./pkg/ast/spl/spl.go`, the command would be:

```bash
~/go/bin/pigeon -o ./pkg/ast/spl/spl.go ./pkg/ast/spl/spl.peg
```

Once a PEG file is converted into a Go file, you can then utilize the parsed variables in your subsequent code implementation.

### Start up Siglens

Once golang is installed, start up SigLens by running the following command at the root of the repo:
```
go run cmd/siglens/main.go --config server.yaml
```

By default, the UI server will start up on port `5122` and the backend will start on port `8081`.

You should be able to access `http://localhost:5122` and see the SigLens UI. If you are not able to, check `siglens.log` for any error messages.


### Send Data to SigLens

In another terminal, go to the location of sigclient:
```
cd tools/sigclient
```
 start the ingestion via `sigscalr-client` by running:
```
go run main.go ingest esbulk -t 10_000 -d http://localhost:8081/elastic --processCount 1 -n 1 -b 500 -g dynamic-user
```

Look through the [sigscalr-client ReadMe](tools/sigclient/README.md) to see all command arguments.


### Send Queries on Siglens

Using the UI, you should be able to send queries using our pipe search query langauge. Look through the dropdown highlighting different query syntax and try running some test queries on the UI.

The sigscalr-client also supports sending queries using:
```
go run main.go query -d http://localhost:5122/elastic -n 10 -v
```


## Pull Request Checklist

When you submit your pull request, or you push new commits to it, our automated
systems will run some checks on your new code. We require that your pull request
passes these checks, but we also have more criteria than just that before we can
accept and merge it. We recommend that you check the following things locally
before you submit your code:

lint, UTs, gofmt :
```
    make pr
```
