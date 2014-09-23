// not sure why this isn't working, tried to debug it but the file don't end up being created. The
// source and destination are successfully queried for their file lists and then the stream download
// from the source is started for several files but the writing of those files on the destination
// doesn't seem to work. It however does work just fine in the remote-to-local and local-to-remote
// examples.
var pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
var remoteConfig = require('./remote-config.json');

var destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.b);
var source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.a);

pkgcloudContainerCopy.copyContainer(source, destination);
