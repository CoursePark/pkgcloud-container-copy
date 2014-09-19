pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
remoteConfig = require('./remote-config.json');
path = require('path');

source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.a);
destination = path.resolve('temp/a');

pkgcloudContainerCopy.copyContainer(source, destination);
