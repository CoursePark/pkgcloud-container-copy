var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');
var path = require('path');

var source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.e);
var destination = path.resolve(__dirname, 'temp/a');

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		// console.log(result.map(function (x) {return x.name;}).join('\n'));
	})
;
