var pkgcloud = require('pkgcloud');
var AWS = require('aws-sdk');
var path = require('path');
var fs = require('fs');
var when = require('when');
var nodefn = require('when/node');
var md5 = require('MD5');
var stream = require('stream');
var mkdirp = require('mkdirp');
var url = require('url');
var mime = require('mime-types');

var pcc = {};

pcc.copyContainer = function (source, destination) {
	var maxAttempt = 5;
	var attempt = 0;
	
	var cummulation = [];
	
	var unspool = function () {
		if (attempt >= maxAttempt) {
			return when.reject('transfer not completed, files on destination do not match the source');
		}
		return pcc._copyContainer(source, destination, attempt++)
			.then(function (result) {
				return when.filter(function (x) {return x !== null;}, result);
			})
			.then(function (result) {
				cummulation = cummulation.concat(result);
				return when.resolve([null, result.length === 0]);
			})
		;
	};
	var predicate = function (seed) {
		return seed;
	};
	return when.unfold(unspool, predicate, function () {})
		.then(function () {
			var filenameIndex = cummulation.map(function (x) {return x.name;});
			return cummulation.filter(function (x, i) {
				return filenameIndex.lastIndexOf(x.name) === i;
			});
		})
	;
};

pcc._copyContainer = function (source, destination, attempt) {
	var sourceFileListP = pcc.getFileList(source)
		.catch(function (err) {
			console.log('source error:', err);
		})
	;
	var destinationFileListP = pcc.getFileList(destination)
		.catch(function (err) {
			console.log('destination error:', err);
		})
	;
	
	return when.join(sourceFileListP, destinationFileListP)
		.then(function (valueList) {
			var plan = pcc.getTransferPlan(valueList[0], valueList[1]);
			
			var taskList = [];
			
			// created
			plan.created.forEach(function (file) {
				if (file.isDir) {
					// directory
					taskList.push({file: file, action: function (file) {
						return pcc.createDir(destination, file).then(function (file) {
							if (file) {
								process.stdout.write('created: ' + file.name + path.sep + (attempt > 0 ? ' retry ' + attempt : '') + '\n');
							}
							return file;
						});
					}});
				} else {
					// file
					taskList.push({file: file, action: function (file) {
						return pcc.transferFile(source, destination, file).then(function (file) {
							if (file) {
								process.stdout.write('created: ' + file.name + ' ' + file.size + (attempt > 0 ? ' retry ' + attempt : '') + '\n');
							}
							return file;
						});
					}});
				}
			});
			
			// modified
			plan.modified.forEach(function (file) {
				taskList.push({file: file, action: function (file) {
					return pcc.transferFile(source, destination, file).then(function (file) {
						if (file) {
							process.stdout.write('modified: ' + file.name + ' ' + file.size + (attempt > 0 ? ' retry ' + attempt : '') + '\n');
						}
						return file;
					});
				}});
			});
			
			// touched
			plan.touched.forEach(function (file) {
				// can't yet set dates on files in pkgcloud storage
			});
			
			// deleted
			plan.deleted.reverse().forEach(function (file) {
				if (file.isDir) {
					// directory
					taskList.push({file: file, action: function (file) {
						return pcc.deleteDir(destination, file).then(function (file) {
							if (file) {
								process.stdout.write('deleted: ' + file.name + path.sep + (attempt > 0 ? ' retry ' + attempt : '') + '\n');
							}
							return file;
						});
					}});
				} else {
					// file
					taskList.push({file: file, action: function (file) {
						return pcc.deleteFile(destination, file).then(function (file) {
							if (file) {
								process.stdout.write('deleted: ' + file.name + (attempt > 0 ? ' retry ' + attempt : '') + '\n');
							}
							return file;
						});
					}});
				}
			});
			
			var sourceHostname = pcc.getHostname(source);
			var destinationHostname = pcc.getHostname(destination);
			if (sourceHostname !== null && sourceHostname === destinationHostname) {
				// both source and destination are to the same host, probably
				// going to have problems with max sockets to servers. So
				// instead do each file sequentially
				return when.reduce(taskList, function (completedFiles, task) {
					return task.action(task.file).then(function (file) {
						completedFiles.push(file);
						return completedFiles;
					});
				}, []);
			}
			
			return when.all(when.map(taskList, function (task) {
				return task.action(task.file);
			}));
		})
		.catch(function (err) {
			console.log('error:', err);
		})
	;
};

pcc.transferFile = function (source, destination, file) {
	return when.promise(function (resolve, reject) {
		var destinationStream = pcc.getDestinationStream(destination, file);
		var sourceStream = pcc.getSourceStream(source, file);
		sourceStream.pipe(destinationStream);
		
		destinationStream.on('finish', function () {
			resolve(file);
		});
		// 'end' event because of old streams used in request package, which is used by pkgcloud upload
		destinationStream.on('end', function () {
			resolve(file);
		});
		destinationStream.on('error', function (err) {
			console.log('destination error:', file.name);
			reject(err);
		});
		sourceStream.on('error', function (err) {
			console.log('source error:', file.name);
			reject(err);
		});
	});
};

pcc.createCloudContainerSpecifer = function (clientOption, container, namePrefix) {
	if (container === undefined) {
		container = clientOption.container;
		namePrefix = clientOption.namePrefix;
		clientOption = clientOption.client;
	}
	
	if (namePrefix === undefined) {
		namePrefix = '';
	}
	
	var client;
	if (clientOption.provider === 'aws-sdk') {
		client = new AWS.S3({
			region: clientOption.region,
			params: {
				Bucket: container,
				EncodingType: 'url',
				Prefix: namePrefix
			}
		});
	} else if (clientOption.provider === 'rackspace' ||
		clientOption.provider === 'amazon' ||
		clientOption.download === undefined
	) {
		client = pkgcloud.storage.createClient(clientOption);
	} else {
		// looks like it might be an existing client that is be suggested, use that
		client = clientOption;
	}
	
	var containerSpecifer = {
		client: client,
		container: container,
		namePrefix: namePrefix,
		delete: {
			// default to true if not delete.enabled is not specified
			enabled: !clientOption.delete || clientOption.delete.enabled === undefined || clientOption.delete.enabled
		}
	};
	
	if (clientOption.meta) {
		containerSpecifer.meta = clientOption.meta;
	}
	return containerSpecifer;
};

pcc.getSourceStream = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return fs.createReadStream(path.resolve(containerSpecifer, file.name));
	} else if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		return containerSpecifer.client.getObject({Key: containerSpecifer.namePrefix + file.name}).createReadStream();
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return client.download({container: container, remote: file.name}, function (err) {
			if (err) {
				console.log('source callback error:', err);
			}
		});
	}
};

pcc.getDestinationStream = function (containerSpecifer, file) {
	// need a placeholder stream because the writer stream is going to be
	// available until the directory is created
	var passThrough = new stream.PassThrough();
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		var filePath = path.resolve(containerSpecifer, file.name);
		nodefn.lift(mkdirp)(path.dirname(filePath))
			.then(function () {
				passThrough.pipe(fs.createWriteStream(filePath));
			})
		;
		return passThrough;
	} else if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		var putParam = {};
		
		// use the Cache-Control meta if specified
		if (containerSpecifer.meta) {
			var awsMetaMap = {
				'Cache-Control': 'CacheControl',
				'Content-Encoding': 'ContentEncoding'
			};
			
			var meta = containerSpecifer.meta;
			
			if (meta.path && meta.default) {
				for (var prop in meta.default) {
					if (meta.default.hasOwnProperty(prop)) {
						if (awsMetaMap[prop]) {
							putParam[awsMetaMap[prop]] = meta.default[prop];
						} else {
							putParam[prop] = meta.default[prop];
						}
					}
				}
			}
			
			if (meta.path && meta.path[file.name]) {
				for (var prop in meta.path[file.name]) {
					if (meta.path[file.name].hasOwnProperty(prop)) {
						if (awsMetaMap[prop]) {
							putParam[awsMetaMap[prop]] = meta.path[file.name][prop];
						} else {
							putParam[prop] = meta.path[file.name][prop];
						}
					}
				}
			}
		}
		
		putParam.Key = containerSpecifer.namePrefix + file.name;
		putParam.Body = passThrough;
		putParam.ContentLength = file.size;
		
		putParam.ContentType = mime.lookup(file.name) || 'application/octet-stream';
		
		containerSpecifer.client.putObject(putParam, function (err) {
			if (err) {
				console.log('destination callback error:', err);
			}
		});
		
		return passThrough;
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		var clientStream = client.upload({container: container, remote: file.name}, function (err) {
			if (err) {
				console.log('destination callback error:', err);
			}
		});
		// force to node stream v2, solves pkgcloud incompatibility with itself
		passThrough.pipe(clientStream);
		return passThrough;
	}
};

pcc.createDir = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return nodefn.lift(mkdirp)(path.resolve(containerSpecifer, file.name))
			.then(function () {
				return file;
			})
		;
	} else if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		return when.resolve(file); // not applicable
	} else {
		// pkgcloud storage container
		return when.resolve(file); // not applicable
	}
};

pcc.deleteDir = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		// remove empty directory
		
		// directory may not yet be empty (because async) so.. hack, looped waiting
		
		var maxTry = 20;
		var attempt = 0;
		
		var unspool = function () {
			attempt++;
			if (attempt > maxTry) {
				return when.reject('directory not empty ' + file.name);
			}
			return nodefn.lift(fs.readdir).bind(fs)(path.resolve(containerSpecifer, file.name))
				.then(function (dirContent) {
					if (dirContent.length) {
						// must wait until dir is empty
						return when.resolve([null, false]).delay(100);
					}
					return nodefn.lift(fs.rmdir).bind(fs)(path.resolve(containerSpecifer, file.name));
				})
				.catch(function (err) {
					if (err.code !== 'ENOENT') {
						return when.reject(err);
					}
					// directory no longer exists, good enough
					return [null, true];
				})
				.then(function () {
					return [null, true];
				})
			;
		};
		
		var predicate = function (seed) {
			return seed;
		};
		return when.unfold(unspool, predicate, function () {})
			.then(function() {
				return file;
			})
		;
	}
	
	if (!containerSpecifer.delete.enabled) {
		return when.resolve(null);
	}
	
	if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		return when.resolve(file); // not applicable
	} else {
		// pkgcloud storage container
		return when.resolve(file); // not applicable
	}
};

pcc.deleteFile = function (containerSpecifer, file) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return nodefn.lift(fs.unlink).bind(fs)(path.resolve(containerSpecifer, file.name))
			.then(function () {
				return file;
			})
		;
	}
	
	if (!containerSpecifer.delete.enabled) {
		return when.resolve(null);
	}
	
	if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		return nodefn.lift(containerSpecifer.client.deleteObject).bind(containerSpecifer.client)({Key: containerSpecifer.namePrefix + file.name})
			.then(function () {
				return file;
			})
		;
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		return nodefn.lift(client.removeFile).bind(client)(container, file.name)
			.then(function () {
				return file;
			})
		;
	}
};

pcc.getHostname = function (containerSpecifer) {
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return null;
	} else if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		return containerSpecifer.client.endpoint.hostname;
	} else {
		// pkgcloud storage container
		return containerSpecifer.client && containerSpecifer.client._serviceUrl ?
			url.parse(containerSpecifer.client._serviceUrl).hostname : null
		;
	}
};

pcc.sort = function (x, y) {
	if (x.name === y.name) {
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
	
	sourceFileList = sourceFileList.sort(pcc.sort);
	destinationFileList = destinationFileList.sort(pcc.sort);
	
	for (var sPos = 0, dPos = 0; sPos < sourceFileList.length || dPos < destinationFileList.length;) {
		
		if (dPos === destinationFileList.length) {
			plan.created.push(sourceFileList[sPos]);
			sPos++;
			continue;
		}
		
		if (sPos === sourceFileList.length) {
			plan.deleted.push(destinationFileList[dPos]);
			dPos++;
			continue;
		}
		
		var s = sourceFileList[sPos];
		var d = destinationFileList[dPos];
		
		if (pcc.sort(s, d) === -1) {
			if (s.isDir && d.name.substring(0, s.name.length + 1) === s.name + path.sep) {
				// source directory is contained in destination file
				sPos++;
				continue;
			}
			// source file doesn't exist on destination
			plan.created.push(s);
			sPos++;
			continue;
		}
		
		if (pcc.sort(s, d) === 1) {
			if (d.isDir && s.name.substring(0, d.name.length + 1) === d.name + path.sep) {
				// destination directory is contained in source file
				dPos++;
				continue;
			}
			plan.deleted.push(d);
			dPos++;
			continue;
		}
		
		if (s.name === d.name && s.isDir ^ d.isDir) {
			plan.created.push(s);
			plan.deleted.push(d);
		}
		
		if (!s.isDir && !d.isDir && (s.size !== d.size || s.etag === null || d.etag === null || s.etag !== d.etag)) {
			plan.modified.push(s);
		} else if (!s.isDir && !d.isDir && (s.lastModified !== d.lastModified && false)) { // date isn't settable. So destination date will never match source
			// date changed
			plan.touched.push(s);
		} else {
			plan.unchanged.push(s);
		}
		
		sPos++;
		dPos++;
	}
	
	return plan;
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
					// a directory
					fileList.push({
						name: file.substring(baseDir.length),
						isDir: true,
						location: 'local'
					});
					return _readdirRecurse(file);
				}
				
				// a file
				fileList.push({
					name: file.substring(baseDir.length),
					lastModified: stat.mtime,
					size: stat.size,
					etag: md5(fs.readFileSync(file)),
					isDir: false,
					location: 'local'
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
	var p;
	
	if (typeof containerSpecifer === 'string') {
		// path on local file system
		return pcc.readdirRecurse(containerSpecifer);
	} else if (containerSpecifer.client instanceof AWS.S3) {
		// AWS S3
		p = nodefn.lift(containerSpecifer.client.listObjects).bind(containerSpecifer.client)({
			Prefix: containerSpecifer.namePrefix
		});
		
		p = p.then(function (data) {
			return data.Contents;
		});
		
		p = when.filter(p, function (fileModel) {
			// AWS can have zero byte files that are used to represent of a
			// directory. Ignore them for now. Might be a future project.
			return fileModel.Key.substr(-1) !== path.sep;
		});
		
		p = when.map(p, function (fileModel) {
			return pcc.cloudFileModel(fileModel, containerSpecifer.namePrefix);
		});
		
		return p;
	} else {
		// pkgcloud storage container
		var client = containerSpecifer.client;
		var container = containerSpecifer.container;
		
		p = nodefn.lift(client.getFiles).bind(client)(container)
			.catch(function (err) {
				console.log('get file list error:', err);
			})
		;
		
		p = when.map(p, function (fileModel) {
			return pcc.cloudFileModel(fileModel, '');
		});
		
		return p;
	}
};

pcc.cloudFileModel = function (fullModel, namePrefix) {
	var etag = (fullModel.etag || fullModel.ETag) ?
		(fullModel.etag || fullModel.ETag.replace('"', '').replace('"', '')) : null
	;
	return {
		name: fullModel.name || fullModel.Key.substring(namePrefix.length),
		lastModified: fullModel.lastModified || fullModel.LastModified,
		size: fullModel.size || fullModel.Size,
		etag: etag,
		isDir: false,
		location: 'remote'
	};
};

module.exports = pcc;
