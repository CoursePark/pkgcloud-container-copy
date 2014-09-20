var pkgcloud = require('pkgcloud');
var path = require('path');
var fs = require('fs');
var when = require('when');
var nodefn = require('when/node');
var md5 = require('MD5');

var pkgcloudContainerCopy = {};

pkgcloudContainerCopy.copyContainer = function (source, destination) {
	when.all([
		pkgcloudContainerCopy.getFileList(source),
		pkgcloudContainerCopy.getFileList(destination)
	])
	.then(function (valueList) {
		var sourceFileList = valueList[0];
		var destinationFileList = valueList[1];
		
		var plan = pkgcloudContainerCopy.transferPlan(sourceFileList, destinationFileList);
		console.log(plan);
		
		// created
		for (var i = 0; i < plan.created.length; i++) {
			file = plan.created[i];
			
			var destinationStream = pkgcloudContainerCopy.getDestinationStream(destination, file);
			var sourceStream = pkgcloudContainerCopy.getSourceStream(source, file);
			sourceStream.pipe(destinationStream);
			destinationStream.on('finish', function () {
				console.log('created: ' + file);
			});
		}
		
		// modified
		for (var i = 0; i < plan.modified.length; i++) {
			file = plan.modified[i];
			
			var destinationStream = pkgcloudContainerCopy.getDestinationStream(destination, file);
			var sourceStream = pkgcloudContainerCopy.getSourceStream(source, file);
			sourceStream.pipe(destinationStream);
			destinationStream.on('finish', function () {
				console.log('modified: ' + file);
			});
		}
		
		// touched
		for (var i = 0; i < plan.touched.length; i++) {
			file = plan.touched[i];
			// can't yet set dates on files in pkgcloud storage
		}
		
		// deleted
		plan.deleted.forEach(function (file) {
			pkgcloudContainerCopy.deleteFile(destination, file)
				.then(function () {
					console.log('deleted: ' + file);
				})
			;
		});
	})
	.catch(function (err) {
		console.log(err);
	});
};

pkgcloudContainerCopy.createCloudContainerSpecifer = function (clientOption, container) {
	if (container === undefined) {
		container = clientOption.container;
		clientOption = clientOption.client;
	}
	
	var client = pkgcloud.storage.createClient(clientOption);
	
	return {
		client: client,
		container: container
	};
};

pkgcloudContainerCopy.getSourceStream = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return fs.createReadStream(path.resolve(file));
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return client.download({container: container, remote: file}, function (err, file) {
			if (err) {
				console.log('source stream err');
				console.log(err);
			}
			console.log('source stream file');
			console.log(file);
		});
	}
};

pkgcloudContainerCopy.getDestinationStream = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return fs.createWriteStream(path.resolve(containerSpecifer, file));
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return client.upload({container: container, remote: file}, function (err, file) {
			if (err) {
				console.log('source stream err');
				console.log(err);
			}
			console.log('source stream file');
			console.log(file);
		});
	}
};

pkgcloudContainerCopy.deleteFile = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return nodefn.lift(fs.unlink).bind(fs)(path.resolve(containerSpecifer, file));
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return nodefn.lift(client.removeFile).bind(client)(container, file);
	}
};

pkgcloudContainerCopy.transferPlan = function (sourceFileList, destinationFileList) {
	var plan = {
		created: [],
		modifed: [],
		touched: [],
		deleted: [],
		unchanged: []
	};
	for (var i = 0; i < sourceFileList.length; i++) {
		var s = sourceFileList[i];
		
		for (var j = 0; j < destinationFileList.length; j++) {
			var d = destinationFileList[j];
			
			if (s.name !== d.name) {
				// not the same file name
				continue;
			}
			
			// same file name
			
			if (s.size !== d.size || pkgcloudContainerCopy.contentHash(s) !== pkgcloudContainerCopy.contentHash(d)) {
				// modified file
				plan.modifed.push(s.name);
			// } else if (s.lastModified !== d.lastModified) { // date isn't settable. So destination date will never match source
			// 	// date changed
			// 	plan.touched.push(s.name);
			} else {
				// unchanged
				plan.unchanged.push(s.name);
			}
			
			sourceFileList.splice(i, 1);
			destinationFileList.splice(j, 1);
			i = j = 0;
		}
		
		// created
		plan.created.push(s.name);
		sourceFileList.splice(i, 1);
		i = 0;
	}
	
	// deleted
	for (var i = 0; i < destinationFileList.length; i++) {
		plan.deleted.push(destinationFileList[i].name);
	}
	
	return plan;
};

pkgcloudContainerCopy.contentHash = function (fileModel) {
	if (fileModel.etag) {
		return fileModel.etag;
	}
	
	return md5(fs.readFileSync(path.resolve(fileModel.dir, fileModel.name)));
};

pkgcloudContainerCopy.readdirRecurse = function (dir) {
	var fileList = [];
	var baseDir = path.resolve(dir);
	
	var _readdirRecurse = function (dir) {
		var p = nodefn.lift(fs.readdir).bind(fs)(dir);
		p = when.map(p, function (filename) {
			var file = dir + path.sep + filename;
			return nodefn.lift(fs.stat).bind(fs)(file).then(function (stat) {
				if (stat.isDirectory()) {
					return _readdirRecurse(file);
				}
				
				// a file
				fileList.push({
					name: file,
					lastModified: stat.mtime,
					size: stat.size,
					baseDir: baseDir
				});
			});
		});
		
		return when.all(p);
	};
	
	return _readdirRecurse(dir).then(function () {
		return fileList;
	});
};

pkgcloudContainerCopy.getFileList = function (containerSpecifer) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return pkgcloudContainerCopy.readdirRecurse(containerSpecifer);
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		
		var p = nodefn.lift(client.getFiles).bind(client)(container);
		
		p = when.map(p, function (fileModel) {
			return {
				name: fileModel.name,
				lastModified: fileModel.lastModified,
				size: fileModel.size
			}
		});
		
		return p;
	}
};

module.exports = pkgcloudContainerCopy;
