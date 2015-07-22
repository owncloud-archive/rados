# rados
This app uses the php bindings for librados to directly interface with a Ceph cluster and use it as the primary storage for ownCloud.

## Installation

This app requres you to [install](https://github.com/ceph/phprados/blob/master/INSTALL) the [php bindings for librados](https://github.com/ceph/phprados).

To start a demo ceph docker instance 
```sh
# wipe any config, it will be created by docker. use move if you need the existing config
sudo rm -rf /etc/ceph/* 

# start the ceph/demo with docker. adjust MON_IP to your IP and CEPH_NETWORK to your subnet
sudo docker run -d --net=host -e MON_IP=192.168.1.105 -e CEPH_NETWORK=192.168.1.0/24 -v /etc/ceph:/etc/ceph -P ceph/demo 

# a quick hack to allow the webserver access to the cluster, you really shold not do this in your production environment ...
sudo chmod go+r /etc/ceph/ceph.client.admin.keyring
```

## Set Up
Object storage can not yet be set up with the installation dialog. For now just leave the default data folder path and manually configure the objectstore after installation.

## Configuration
To activate object store mode add a `objectstore` entry to the config.php like this:

```php
  'objectstore' => array(
    'class' => 'OCA\Rados\RadosStore',
        'arguments' => array(
        ),
  ),
```

The objectstore kind of replaces the data directory. By default, the ownCloud log file and the sqlite db will be saved in the data directory. Even when objectstore is configured. However, sqlite is highly unlikely to be used in conjunction with objectstore and the log file path can be changed in config.php. So, in theory the data folder can be empty. However old apps might not use our stream wrappers to access their data and as a result a writable data folder might be needed to achieve backward compatibility.

# Todo

- [ ] unit test this on travis ... needs investigation on how to install docker and then start ceph/demo 
