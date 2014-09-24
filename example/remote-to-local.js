var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');
var path = require('path');

var source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.a);
var destination = path.resolve(__dirname, 'temp/a');

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		console.log(result);
	})
;
