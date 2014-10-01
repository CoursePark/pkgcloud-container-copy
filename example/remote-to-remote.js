var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');

var source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.d);
var destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.e);

var attempt = 0;
var attemptCopy = function () {
	pkgcloudContainerCopy.copyContainer(source, destination)
		.then(function (result) {
			console.log('copy attempt:', ++attempt);
			if (result.length) {
				attemptCopy();
			}
		})
		.catch(function (err) {
			console.log(err);
		})
	;
};

attemptCopy();

/* note that the rackspace API doesn return 201 indicating it has created an
 * item even when it hasn't. This is distressing. One way around it is to just
 * keep trying until it works.
 */