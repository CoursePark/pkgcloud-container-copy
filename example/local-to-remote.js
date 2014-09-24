var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');
var path = require('path');

var source = path.resolve(__dirname, 'temp/b');
var destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.b);

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		console.log(result);
	})
;
