var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');

var destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.b);
var source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.a);

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		console.log(result);
	})
;
