pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
remoteConfig = require('./remote-config.json');
path = require('path');

source = path.resolve(__dirname, 'temp/a');
destination = path.resolve(__dirname, 'temp/b');

pkgcloudContainerCopy.copyContainer(source, destination);
