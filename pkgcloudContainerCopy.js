var pkgcloud = require('pkgcloud');
var path = require('path');
var fs = require('fs');
var when = require('when');
var nodefn = require('when/node');
var md5 = require('MD5');
var Stream = require('stream');
var mkdirp = require('mkdirp');

var pcc = {};

pcc.transferFile = function (source, destination, file) {
	return when.promise(function (resolve, reject) {
		destinationStream = pcc.getDestinationStream(destination, file);
		sourceStream = pcc.getSourceStream(source, file);
		sourceStream.pipe(destinationStream);
		
		destinationStream.on('finish', function () {
			resolve(file);
		});
		destinationStream.on('error', function (err) {
			process.stdout.write('destination error: ' + file + '\n');
			reject(err);
		});
		sourceStream.on('error', function (err) {
			process.stdout.write('source error: ' + file + '\n');
			reject(err);
		});
	});
};

pcc.copyContainer = function (source, destination) {
	return when.join(pcc.getFileList(source), pcc.getFileList(destination))
		.then(function (valueList) {
			var sourceFileList = valueList[0];
			var destinationFileList = valueList[1];
			
			var plan = pcc.getTransferPlan(sourceFileList, destinationFileList);
			
			var i, file, sourceStream, destinationStream;
			
			var promiseList = [];
			var p;
			
			// created
			plan.created.forEach(function (file) {
				p = pcc.transferFile(source, destination, file).then(function (file) {
					process.stdout.write('created: ' + file + '\n');
					return file;
				});
				promiseList.push(p);
			});
			
			// modified
			plan.modified.forEach(function (file) {
				p = pcc.transferFile(source, destination, file).then(function (file) {
					process.stdout.write('modified: ' + file + '\n');
					return file;
				});
				promiseList.push(p);
			});
			
			// touched
			plan.touched.forEach(function (file) {
				// can't yet set dates on files in pkgcloud storage
			});
			
			// deleted
			plan.deleted.forEach(function (file) {
				p = pcc.deleteFile(destination, file).then(function () {
					process.stdout.write('deleted: ' + file + '\n');
				});
				promiseList.push(p);
			});
			
			return when.all(promiseList);
		})
		.catch(function (err) {
			console.log('error:', err);
		})
	;
};

pcc.createCloudContainerSpecifer = function (clientOption, container) {
	if (container === undefined) {
		container = clientOption.container;
		clientOption = clientOption.client;
	}
	
	var client;
	
	if (clientOption.download === undefined) {
		client = pkgcloud.storage.createClient(clientOption);
	} else {
		// looks like it might be an existing client that is be suggested, use that
		client = clientOption;
	}
	
	return {
		client: client,
		container: container
	};
};

pcc.getSourceStream = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return fs.createReadStream(path.resolve(containerSpecifer, file));
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return client.download({container: container, remote: file}, function (err) {
			if (err) {
				process.stdout.write('source callback err' + '\n');
				console.log(err);
				return;
			}
		});
	}
};

pcc.getDestinationStream = function (containerSpecifer, file) {
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
		return client.upload({container: container, remote: file}, function (err, result) {
			if (err) {
				process.stdout.write('destination callback err' + '\n');
				console.log(err);
				return;
			}
		});
	}
};

pcc.deleteFile = function (containerSpecifer, file) {
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

pcc.sortByName = function (x, y) {
	if (x.name == y.name) {
		return 0;
	}
	return x.name > y.name ? 1 : -1;
};

pcc.getTransferPlan = function (sourceFileList, destinationFileList) {
	var plan = {
		created: [],
		modified: [],
		touched: [],
		deleted: [],
		unchanged: []
	};
	
	sourceFileList = sourceFileList.sort(pcc.sortByName);
	destinationFileList = destinationFileList.sort(pcc.sortByName);
	
	for (var sPos = 0, dPos = 0; sPos < sourceFileList.length || dPos < destinationFileList.length;) {
		
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
		
		if (s.size !== d.size || pcc.contentHash(s) !== pcc.contentHash(d)) {
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

pcc.contentHash = function (fileModel) {
	if (fileModel.etag) {
		return fileModel.etag;
	}
	
	return md5(fs.readFileSync(path.resolve(fileModel.baseDir, fileModel.name)));
};

pcc.readdirRecurse = function (dir) {
	var fileList = [];
	var baseDir = path.resolve(dir) + path.sep;
	
	var _readdirRecurse = function (dir) {
		var p = nodefn.lift(fs.readdir).bind(fs)(dir);
		p = when.map(p, function (filename) {
			var file = path.resolve(dir, filename);
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

pcc.getFileList = function (containerSpecifer) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return pcc.readdirRecurse(containerSpecifer);
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		
		var p = nodefn.lift(client.getFiles).bind(client)(container);
		
		p = when.map(p, function (fileModel) {
			return pcc.cloudFileModel(fileModel);
		});
		
		return p;
	}
};

pcc.cloudFileModel = function (fullModel) {
	return {
		name: fullModel.name,
		lastModified: fullModel.lastModified,
		size: fullModel.size,
		etag: fullModel.etag
	};
};

module.exports = pcc;
