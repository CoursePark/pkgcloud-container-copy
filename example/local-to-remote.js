var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');
var path = require('path');

var source = path.resolve(__dirname, 'temp/a');
var destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.d);

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		// console.log(result.map(function (x) {return x.name;}).join('\n'));
	})
;
