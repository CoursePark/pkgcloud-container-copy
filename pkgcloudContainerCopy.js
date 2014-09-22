var pkgcloud = require('pkgcloud');
var path = require('path');
var fs = require('fs');
var when = require('when');
var nodefn = require('when/node');
var md5 = require('MD5');
var Stream = require('stream');
var mkdirp = require('mkdirp');

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
		
		var i, file, sourceStream, destinationStream;
		
		// created
		plan.created.forEach(function (file) {
			destinationStream = pkgcloudContainerCopy.getDestinationStream(destination, file);
			sourceStream = pkgcloudContainerCopy.getSourceStream(source, file);
			sourceStream.pipe(destinationStream);
			destinationStream.on('finish', function () {
				console.log('created:', file);
			});
		});
		
		// modified
		plan.modified.forEach(function (file) {
			destinationStream = pkgcloudContainerCopy.getDestinationStream(destination, file);
			sourceStream = pkgcloudContainerCopy.getSourceStream(source, file);
			sourceStream.pipe(destinationStream);
			destinationStream.on('finish', function () {
				console.log('modified: ' + file);
			});
		});
		
		// touched
		plan.touched.forEach(function (file) {
			// can't yet set dates on files in pkgcloud storage
		});
		
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
		var stream = client.download({container: container, remote: file}, function (err, fileModel) {
			if (err) {
				console.log('source callback err');
				console.log(err);
			}
		});
		
		return stream;
	}
};

pkgcloudContainerCopy.getDestinationStream = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// need a placeholder stream because the writer stream is going to be
		// available until the directory is created
		var stream = new Stream.PassThrough();
		// path on local file system
		var filePath = path.resolve(containerSpecifer, file);
		nodefn.lift(mkdirp)(path.dirname(filePath))
			.then(function () {
				stream.pipe(fs.createWriteStream(filePath));
			})
		;
		return stream;
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return client.upload({container: container, remote: file}, function (err, fileModel) {
			if (err) {
				console.log('destination callback err');
				console.log(err);
			}
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

pkgcloudContainerCopy.sortByName = function (x, y) {
	if (x.name == y.name) {
		return 0;
	}
	return x.name > y.name ? 1 : -1;
};

pkgcloudContainerCopy.transferPlan = function (sourceFileList, destinationFileList) {
	var plan = {
		created: [],
		modified: [],
		touched: [],
		deleted: [],
		unchanged: []
	};
	
	sourceFileList = sourceFileList.sort(pkgcloudContainerCopy.sortByName);
	destinationFileList = destinationFileList.sort(pkgcloudContainerCopy.sortByName);
	
	for (var sPos = 0, dPos = 0; sPos < sourceFileList.length || dPos < destinationFileList;) {
		if (dPos === destinationFileList.length) {
			plan.created.push(sourceFileList[sPos].name);
			sPos++;
			continue;
		}
		
		if (sPos === sourceFileList.length) {
			plan.deleted.push(destinationFileList[dPos].name);
			dPos++;
			continue;
		}
		
		var s = sourceFileList[sPos];
		var d = destinationFileList[dPos];
		
		if (s.name < d.name) {
			plan.created.push(s.name);
			sPos++;
			continue;
		}
		
		if (s.name > d.name) {
			plan.deleted.push(d.name);
			dPos++;
			continue;
		}
		
		if (s.size !== d.size || pkgcloudContainerCopy.contentHash(s) !== pkgcloudContainerCopy.contentHash(d)) {
			plan.modified.push(s.name);
		} else if (s.lastModified !== d.lastModified && false) { // date isn't settable. So destination date will never match source
			// date changed
			plan.touched.push(s.name);
		} else {
			plan.unchanged.push(s.name);
		}
		
		sPos++;
		dPos++;
	}
	
	return plan;
};

pkgcloudContainerCopy.contentHash = function (fileModel) {
	if (fileModel.etag) {
		return fileModel.etag;
	}
	
	return md5(fs.readFileSync(path.resolve(fileModel.baseDir, fileModel.name)));
};

pkgcloudContainerCopy.readdirRecurse = function (dir) {
	var fileList = [];
	var baseDir = path.resolve(dir) + path.sep;
	
	var _readdirRecurse = function (dir) {
		var p = nodefn.lift(fs.readdir).bind(fs)(dir);
		p = when.map(p, function (filename) {
			var file = path.resolve(dir, filename)
			return nodefn.lift(fs.stat).bind(fs)(file).then(function (stat) {
				if (stat.isDirectory()) {
					return _readdirRecurse(file);
				}
				
				// a file
				fileList.push({
					name: file.substring(baseDir.length),
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
			return pkgcloudContainerCopy.cloudFileModel(fileModel);
		});
		
		return p;
	}
};

pkgcloudContainerCopy.cloudFileModel = function (fullModel) {
	return {
		name: fullModel.name,
		lastModified: fullModel.lastModified,
		size: fullModel.size,
		etag: fullModel.etag
	};
};

module.exports = pkgcloudContainerCopy;
