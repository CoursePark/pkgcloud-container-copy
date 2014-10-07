var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');

var source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.a);
var destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.b);

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		// console.log(result.map(function (x) {return x.name;}).join('\n'));
	})
;