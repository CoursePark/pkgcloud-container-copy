var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var path = require('path');

var source = path.resolve(__dirname, 'temp/a');
var destination = path.resolve(__dirname, 'temp/b');

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		console.log(result);
	})
;
