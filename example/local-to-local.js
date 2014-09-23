var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');
var path = require('path');

var source = path.resolve(__dirname, 'temp/a');
var destination = path.resolve(__dirname, 'temp/b');

pkgcloudContainerCopy.copyContainer(source, destination);
