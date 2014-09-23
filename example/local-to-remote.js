pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
remoteConfig = require('./remote-config.json');
path = require('path');

source = path.resolve(__dirname, 'temp/b');
destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.b);

pkgcloudContainerCopy.copyContainer(source, destination);
