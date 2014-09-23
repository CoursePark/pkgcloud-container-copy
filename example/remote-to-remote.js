pkgcloudContainerCopy = require('../pkgcloudContainerCopy.js');
remoteConfig = require('./remote-config.json');

destination = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.b);
source = pkgcloudContainerCopy.createCloudContainerSpecifer(remoteConfig.a);

pkgcloudContainerCopy.copyContainer(source, destination);
