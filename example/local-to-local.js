var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var path = require('path');

var source = path.resolve(__dirname, 'temp/b');
var destination = path.resolve(__dirname, 'temp/a');

pkgcloudContainerCopy.copyContainer(source, destination)
	.done(function (result) {
		// console.log(result.map(function (x) {return x.name;}).join('\n'));
	})
;
