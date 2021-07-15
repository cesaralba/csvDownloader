# Building a test development environment

This document explains how to create a development environment that can be useful to reproduce problems or evolve 
the software without using the original/production data sources or repositories.  

The main idea for this environment is to mimic the general scenario (a remote dataset that changes) 
so there is no need to poll the original source. So we need a local replica of the data that we can 
change at will and -optionally- a remote git server to mimic GitHub.  

The for the web server part [NGINX](https://www.nginx.com/) can do the trick as we only need it 
to serve a single 'static' file.

[GOGS](https://gogs.io/) can do the Git Server part. Notice it takes some extra manual steps to 
complete the GOGS configuration: complete configuration, create a user, authorize a SSH key for the user, and 
create the remote repo with the target branch. 

## Automating the development environment

To launch everything at once you can use the following docker-compose.yml
~~~yaml
---
version: '3'

services:
  datawebserver:
    image: nginx:stable-alpine
    expose:
      - 10082
    ports:
      - "10082:80"
    environment: {}
    volumes:
      - DATA/wsDataHome:/usr/share/nginx/html:ro
    restart: always

  gogs:
    image: gogs/gogs:latest
    expose:
      - 10022
      - 10081
    ports:
      - "10022:22"
      - "10081:3000"
    environment: {}
    volumes:
      - DATA/gogsHome:/data
    restart: always
~~~

With the companion _Makefile_
~~~makefile
DOCKER_COMPOSE := COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 $(shell which docker-compose)
DOCKER := $(shell which docker)

COMPOSE_FILE := "docker-compose.yml"

# Development stack
.PHONY: up
up:
	$(DOCKER_COMPOSE)  -f $(COMPOSE_FILE)  up -d

.PHONY: down
down:
	$(DOCKER_COMPOSE)  -f $(COMPOSE_FILE)  down

.PHONY: logs
logs:
	$(DOCKER_COMPOSE)  -f $(COMPOSE_FILE)  logs -ft

.PHONY: status
status:
	$(DOCKER_COMPOSE)  -f $(COMPOSE_FILE)  ps
~~~

In the _docker-compose_ file there are two references to volumes to provide persistence:
* **DATA/gogsHome** is where _GOGS_ will store all its data and configuration
* **DATA/wsDataHome** contains the mock data our system will feed from. It is explained with more detail [here](#mock-data)

## Configuring remote repository access
A remote git repository also requires to set the local part to access the remote repo non-interactively. Choices here
is either adding an entry to the SSH configuration specifying all non-standard, or using the _GIT_SSH_COMMAND_
environment variable containing the full [_SSH_](https://linux.die.net/man/1/ssh). Examples:

* SSH config entry
~~~text
Host gogs
    Port 10022
    HostName localhost
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    IdentityFile MySSHkeyfile
~~~
* Environment variable
~~~shell
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i /run/secrets/user_ssh_key
~~~


## Mock data
The key par of the development environment is the mock data source that behaves as the original source.

A clever way to do this is to **clone** the repository where we store the versioned dataset and select 
a version (i.e. checkout a certain commit). From then on, to get a new version just move (i.e. _checkout_)
to a **later** commit.


Interesting commands:
* Clone the repo of versioned dataset to local disk (replace with proper values). _SOMEPATHINMYDISK_
will be the location that appears in _docker-compose_ file shown above as _DATA/wsDataHome_
~~~shell
git clone git@github.com:MYVERSIONEDDATASETREPO.git SOMEPATHINMYDISK 
~~~ 
* List all commits available (from the location of the cloned repository we assume the default branch is _master_) 
~~~shell
git --no-pager log master --oneline
~~~
* Move to a new version (replace _COMMIT_ with a proper value from the list above)
~~~shell
git checkout COMMIT
~~~
