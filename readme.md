## Reddstack Social Microservice

This package contains the microservice for Reddstack to stage social media verification and validation. It talks to the Reddstack server and provides an interface for retrieving and managing names in decentralized namespaces and database tables on the blockchain

## Installation
Installing this package and required dependencies

### Debian + Ubuntu

Download the source from github

```
$ git clone https://github.com/reddcoin-project/reddstack-social
```

Ideally you will install reddstack-social into a virtual environment


```
$ [sudo] pip install virtualenv
$ cd reddstack-social
$ virtualenv venv
$ source venv/bin/activate
```

Install dependencies (via pip:)

```
see requirements.txt
```

This will download the latest internal dependencies for the module to the virtual environment

## Usage

Start microservice

```
cd bin
./reddstacksociald.py [start, stop, version]
```


