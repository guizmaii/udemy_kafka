# Setup Kafka & KSQL for Windows


## Windows Subsystem for Linux (WSL) 

Follow https://docs.microsoft.com/en-us/windows/wsl/install-win10

- Ensure that the "Windows Subsystem for Linux" optional feature is enabled
- Install your Linux distribution choice; Ubuntu is preferred 



## Install docker on WSL

Notes from https://gist.github.com/kekru/0d14eb363260df78d012c901b94a19be

```
export DOCKERVERSION=docker-18.03.1-ce
export DIR=/tmp
curl https://download.docker.com/linux/static/edge/x86_64/$DOCKERVERSION.tgz | tar xvz --directory $DIR
sudo mv -v $DIR/docker/docker /usr/local/bin/docker
chmod +x /usr/local/bin/docker
```

## Install docker compose on WSL
```
export COMPOSEVERSION=1.21.1
curl -L https://github.com/docker/compose/releases/download/$COMPOSEVERSION/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
```

