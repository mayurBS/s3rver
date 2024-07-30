'use strict';

const crypto = require('crypto');
const fs = require('fs-extra');
const { Transform } = require('stream');
const { pick, pickBy, sortBy, zip } = require('lodash');
const path = require('path');
const { format } = require('util');

const axios = require('axios');
const { getConfigModel } = require('../models/config');
const S3Bucket = require('../models/bucket');
const S3Object = require('../models/object');
const { concatStreams, walk } = require('../utils');
const { exec } = require('child_process');
const S3RVER_SUFFIX = '%s._SDNA_%s';
const basePath = "/sdna_fs/PROXIES/ARCHIVES";
// dataFetcher.js

function formatToRFC1123(dateString){
  const date = new Date(dateString);
  return date.toUTCString();
}

const { connectDB } = require('../middleware/database');

async function fetchDataFromDatabase(bucket, options) {
  console.log("🚀 :===== bucket:", bucket)
  try {
    const db = await connectDB();
    const s3Collection = db.collection('s3_catalogs');
    const { delimiter = '', startAfter = '', prefix = '', maxKeys = 100 } = options;
    console.log("🚀 :===== delimiter:", JSON.stringify(delimiter,null,2))
    console.log("🚀 :===== prefix:", JSON.stringify(prefix,null,2))

    const query = {
      repoGuid: bucket,
      isDeleted: false
    };

    let result = [];
    
    if(delimiter || prefix){
      if(delimiter === "/" && !prefix){
        result = await s3Collection.find({ ...query, parent: null }).toArray();
      } else if (delimiter && prefix){
          const prefixPath = prefix.endsWith('/') ? prefix.slice(0, -1) : prefix;
          const directoryData = await s3Collection.findOne({ ...query, fullPath: `/${prefixPath}` });
          result = await s3Collection.find({ ...query, parent: directoryData._id }).toArray();
      } else if (prefix) {
        const directoryKey = prefix.endsWith('/') ? prefix.slice(0, -1) : prefix;
        result = await s3Collection.find({ ...query, fullPath: new RegExp(`^/${directoryKey}`), isDir: false}).toArray();
      }
    } else {
      result = await s3Collection.find({ ...query, isDir: false}).toArray();
    }
    
    return result;
  } catch (error) {
    console.error('Error fetching data from MongoDB:', error);
    throw error;
  }
}

class FilesystemStore {
  static decodeKeyPath(keyPath) {
    return process.platform === 'win32'
      ? keyPath.replace(/&../g, ent =>
          Buffer.from(ent.slice(1), 'hex').toString(),
        )
      : keyPath;
  }

  static encodeKeyPath(key) {
    return process.platform === 'win32'
      ? key.replace(
          /[<>:"\\|?*]/g,
          ch => '&' + Buffer.from(ch, 'utf8').toString('hex'),
        )
      : key;
  }

  constructor(rootDirectory) {
    this.rootDirectory = rootDirectory;
  }

  // helpers
  getBucketPath(bucketName) {
    return path.join(this.rootDirectory, bucketName);
  }

  getResourcePath(bucket, key = '', resource = '') {
    const parts = FilesystemStore.encodeKeyPath(key).split('/');
    const suffix = resource ? format(S3RVER_SUFFIX, parts.pop(), resource) : format(parts.pop(), resource).trim() ;
    return path.join(this.rootDirectory, bucket, ...parts, suffix);
  }

  async getMetadata(bucket, key) {
    const objectPath = this.getResourcePath(bucket, key);
    const metadataPath = this.getResourcePath(bucket, key, 'metadata.json');

    // this is expected to throw if the object doesn't exist
    const stat = await fs.stat(objectPath);
    const [storedMetadata, md5] = await Promise.all([
      fs
        .readFile(metadataPath)
        .then(JSON.parse)
        .catch(err => {
          if (err.code === 'ENOENT') return undefined;
          throw err;
        }),
      fs
      .readFile(`${objectPath}._SDNA_object.md5`)
        .then(md5 => md5.toString())
        .catch(async err => {
          if (err.code !== 'ENOENT') throw err;
          // create the md5 file if it doesn't already exist
          const md5 = await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(objectPath);
            const md5Context = crypto.createHash('md5');
            stream.on('error', reject);
            stream.on('data', chunk => md5Context.update(chunk, 'utf8'));
            stream.on('end', () => resolve(md5Context.digest('hex')));
          });
          await fs.writeFile(`${objectPath}._SDNA_object.md5`, md5);
          return md5;
        }),
    ]);

    return {
      ...storedMetadata,
      etag: JSON.stringify(md5),
      'last-modified': stat.mtime.toUTCString(),
      'content-length': stat.size,
    };
  }

  async putMetadata(bucket, key, metadata, md5) {
    const metadataPath = this.getResourcePath(bucket, key, 'metadata.json');
    const md5Path = this.getResourcePath(bucket, key, 'object.md5');

    const json = {
      ...pick(metadata, S3Object.ALLOWED_METADATA),
      ...pickBy(metadata, (value, key) => key.startsWith('x-amz-meta-')),
    };

    if (md5) await fs.writeFile(md5Path, md5);
    await fs.writeFile(metadataPath, JSON.stringify(json, null, 2));
    return json;
  }

  // store implementation

  reset() {
    const list = fs.readdirSync(this.rootDirectory);
    for (const file of list) {
      fs.removeSync(path.join(this.rootDirectory, file));
    }
  }

  async listBuckets() {
    const list = await fs.readdir(this.rootDirectory);
    const buckets = await Promise.all(
      list.map(filename => this.getBucket(filename)),
    );
    return buckets.filter(Boolean);
  }

  async getBucket(bucket) {
    const bucketPath = this.getBucketPath(bucket);
    try {
      const stat = await fs.stat(bucketPath);
      if (!stat.isDirectory()) return null;
      return new S3Bucket(bucket, stat.birthtime);
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  async putBucket(bucket) {
    const bucketPath = this.getBucketPath(bucket);
    await fs.mkdirp(bucketPath, 0o0755);
    return this.getBucket(bucket);
  }

  async deleteBucket(bucket) {
    return fs.remove(this.getBucketPath(bucket));
  }

  async listObjects(bucket, options) {
    const {
      delimiter = '',
      startAfter = '',
      prefix = '',
      maxKeys = Infinity,
    } = options;
    // const bucketPath = this.getBucketPath(bucket);

    // const delimiterEnc = FilesystemStore.encodeKeyPath(delimiter);
    // const startAfterPath = [
    //   bucketPath,
    //   FilesystemStore.encodeKeyPath(startAfter),
    // ].join('/');
    // const prefixPath = [bucketPath, FilesystemStore.encodeKeyPath(prefix)].join(
    //   '/',
    // );

    // const it = walk(bucketPath, dirPath => {
    //   // avoid directories occurring before the startAfter parameter
    //   if (dirPath < startAfterPath && startAfterPath.indexOf(dirPath) === -1) {
    //     return false;
    //   }
    //   if (dirPath.startsWith(prefixPath)) {
    //     if (
    //       delimiterEnc &&
    //       dirPath.slice(prefixPath.length).indexOf(delimiterEnc) !== -1
    //     ) {
    //       // avoid directories occurring beneath a common prefix
    //       return false;
    //     }
    //   } else if (!prefixPath.startsWith(dirPath)) {
    //     // avoid directories that do not intersect with any part of the prefix
    //     return false;
    //   }
    //   return true;
    // });

    const catalogs = await fetchDataFromDatabase(bucket, options);
    console.log("🚀 :===== total catalogs:", catalogs.length)
    
    const commonPrefixes = new Set();
    const objectSuffix = format(S3RVER_SUFFIX, '', 'object');
    const keys = [];
    
    let isTruncated = false;
    for (const keyPath of catalogs) {
      // if (keyPath.endsWith("_SDNA_object.md5") || keyPath.endsWith("_SDNA_metadata.json")) {
      //   continue;
      // }

      // const key = FilesystemStore.decodeKeyPath(
      //   keyPath.slice(bucketPath.length + 1),
      // );
      const key = keyPath.fullPath.startsWith("/") ? keyPath.fullPath.slice(1) : keyPath.fullPath;
      // console.log("🚀 :===== key:", key)

      if (key <= startAfter || !key.startsWith(prefix)) {
        continue;
      }

      if (delimiter && keyPath.isDir) {
          // add to common prefixes before filtering this key out
          commonPrefixes.add(key);
          continue;
      }
      keyPath.fullPath = key
      if (keys.length < maxKeys) {
        keys.push(keyPath);
      } else {
        isTruncated = true;
        break;
      }
    }
    // const metadataArr = await Promise.all(
    //   keys.map(key =>
    //     this.getMetadata(bucket, key).catch(err => {
    //       if (err.code === 'ENOENT') return undefined;
    //       throw err;
    //     }),
    //   ),
    // );
    return {
      objects: keys,
      commonPrefixes: [...commonPrefixes].sort(),
      isTruncated,
    };
  }

  async existsObject(bucket, key) {
    const objectPath = this.getResourcePath(bucket, key);
    try {
      await fs.stat(objectPath);
      return true;
    } catch (err) {
      if (err.code === 'ENOENT') return false;
      throw err;
    }
  }

  async getObject(bucket, key, options) {
    try {
      const db = await connectDB();
      const s3Collection = db.collection('s3_catalogs');
      const catalog = await s3Collection.findOne({ repoGuid: bucket, fullPath: `/${key}`});
      
      const catalogMetadata = {
        'last-modified': formatToRFC1123(catalog.modTime),
        etag: catalog.checksum,
        'content-length': catalog.size,
        'x-amz-meta-target-path': catalog.targetPath ?? "",
        ...(catalog.metadata.reduce((prev,curr) => {
          prev[curr.name] = curr.value;
          return prev;
        }, {}))
      };
      catalogMetadata['x-amz-meta-test-metadata'] = 'test metadata value';
      console.log("🚀 :===== catalogMetadata:", catalogMetadata)
      const metadata = catalogMetadata;
      // const metadata = await this.getMetadata(bucket, key);
      // console.log("🚀 :===== metadata:", metadata)
      const lastByte = Math.max(0, Number(metadata['content-length']) - 1);
      const range = {
        start: (options && options.start) || 0,
        end: Math.min((options && options.end) || Infinity, lastByte),
      };

      if (range.start < 0 || Math.min(range.end, lastByte) < range.start) {
        // the range is not satisfiable
        const object = new S3Object(bucket, key, null, metadata);
        if (options && (options.start !== undefined || options.end)) {
          object.range = range;
        }
        return object;
      }

      // const content = await new Promise((resolve, reject) => {
      //   const stream = fs
      //     .createReadStream(this.getResourcePath(bucket, key), range)
      //     .on('error', reject)
      //     .on('open', () => resolve(stream));
      // });
      
      const object = new S3Object(bucket, key, "", metadata);
      if (options && (options.start !== undefined || options.end)) {
        object.range = range;
      }
      return object;
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  async putObject(object) {
    const bucketName = object.bucket;
    console.log("🚀 :===== bucketName:", bucketName)
    const objectPath = this.getResourcePath(
      bucketName,
      object.key,
    );

    await fs.mkdirp(path.dirname(objectPath));

    const [size, md5] = await new Promise((resolve, reject) => {
      const writeStream = fs.createWriteStream(objectPath);
      const md5Context = crypto.createHash('md5');

      if (Buffer.isBuffer(object.content)) {
        writeStream.end(object.content);
        md5Context.update(object.content);
        resolve([object.content.length, md5Context.digest('hex')]);
      } else {
        let totalLength = 0;
        object.content
          .pipe(
            new Transform({
              transform(chunk, encoding, callback) {
                md5Context.update(chunk, encoding);
                totalLength += chunk.length;
                callback(null, chunk);
              },
            }),
          )
          .on('error', reject)
          .on('finish', () => resolve([totalLength, md5Context.digest('hex')]))
          .pipe(writeStream);
      }
    });
    const objectMetadata = await this.putMetadata(bucketName, object.key, object.metadata, md5);
    const actualObjectPath = path.normalize(objectPath).replaceAll('\\', '/');
    console.log("🚀 :===== actualObjectPath:", actualObjectPath)
    const catalogPath = actualObjectPath.replace(basePath + '/' + bucketName,'');
    const fileName = path.basename(catalogPath);
    const sourcePath = path.dirname(catalogPath);
    const repoGuid = bucketName;

    const fileDetails = await fs.stat(actualObjectPath)

    const catalogMetadata = Object.entries(objectMetadata).map(([key, value]) => ({ name: key, value }));

    /**
     * Getting data manager guid
     */
    // const command = 'sudo /opt/sdna/clientHash';

    // try {
    //     const { stdout, stderr } = await new Promise((resolve, reject) => {
    //         exec(command, (error, stdout, stderr) => {
    //             if (error) reject(error);
    //             else resolve({ stdout, stderr });
    //         });
    //     });

    //     if (stderr) {
    //         console.error(`Error output: ${stderr}`);
    //         return;
    //     }

    //     console.log(`Output: ${stdout}`);
    // } catch (error) {
    //     console.error(`Execution error: ${error}`);
    // }

    const catalogData = {
      "repoGuid": repoGuid,
      "projectName": repoGuid,
      "dataManager": "AAA05EB7-EB03-7894-1111-55A716245888",
      "catalogsData": [
          {
              // "snapshotCount": 2,
              "fileType": "file",
              "fileName": fileName,
              "relativePath": `${repoGuid}/1${sourcePath === "/" ? "" : sourcePath}`,
              "sourcePath": sourcePath,
              "projectName": repoGuid,
              "actions": [
                  {
                      "permissions": null,
                      "size": `${fileDetails.size}`,
                      "modTime": Math.floor(fileDetails.mtimeMs / 1000),
                      "checksum": md5,
                      "writeTime": Math.floor(fileDetails.birthtimeMs / 1000),
                      "writeIndex": 1,
                      "type": "s3-api",
                      "accessTime": Math.floor(fileDetails.atimeMs / 1000),
                      "targetId": "S3"
                  }
              ],
              "metadata": catalogMetadata
          }
      ]
    }
    console.log("🚀 :===== catalogData:", JSON.stringify(catalogData,null,2))
    const apiHeaders = {
      'Content-Type': 'application/json',
      apikey: "$2a$10$AEgyxdqJpFASNJr81wthi.eKObc3wqXh4vmLapMFomkcIFBLWEvsW"
    }
    try {
      const catalog = await axios.post("http://localhost:4080/s3/catalogs", JSON.stringify(catalogData), { headers: apiHeaders})
      console.log("🚀 :===== catalog:", catalog.data)
    } catch (error) {
      console.log("🚀 :===== error:", error.message)
      throw error
    }
    return { size, md5 };
  }

  async copyObject(
    srcBucket,
    srcKey,
    destBucket,
    destKey,
    replacementMetadata,
  ) {
    const srcObjectPath = this.getResourcePath(srcBucket, srcKey);
    const destObjectPath = this.getResourcePath(destBucket, destKey);

    if (srcObjectPath !== destObjectPath) {
      await fs.mkdirp(path.dirname(destObjectPath));
      await fs.copy(srcObjectPath, destObjectPath);
    }

    if (replacementMetadata) {
      await this.putMetadata(destBucket, destKey, replacementMetadata);
      return this.getMetadata(destBucket, destKey);
    } else {
      if (srcObjectPath !== destObjectPath) {
        await Promise.all([
          fs.copy(
            this.getResourcePath(srcBucket, srcKey, 'metadata.json'),
            this.getResourcePath(destBucket, destKey, 'metadata.json'),
          ),
          fs.copy(
            this.getResourcePath(srcBucket, srcKey, 'object.md5'),
            this.getResourcePath(destBucket, destKey, 'object.md5'),
          ),
        ]);
      }
      return this.getMetadata(destBucket, destKey);
    }
  }

  async deleteObject(bucket, key) {
    this.getResourcePath(bucket, key)
    console.log("delete Object path=========:",this.getResourcePath(bucket, key))
    console.log("delete Object path object.md5=========:",this.getResourcePath(bucket, key, 'object.md5'))
    console.log("delete Object path metadata.json=========:",this.getResourcePath(bucket, key, 'metadata.json'))
    await Promise.all(
      [
        this.getResourcePath(bucket, key),
        this.getResourcePath(bucket, key, 'object.md5'),
        this.getResourcePath(bucket, key, 'metadata.json'),
      ].map(filePath =>
        fs.unlink(filePath).catch(err => {
          if (err.code !== 'ENOENT') throw err;
        }),
      ),
    );
    // clean up empty directories
    const bucketPath = this.getBucketPath(bucket);
    const parts = key.split('/');
    // the last part isn't a directory (it's embedded into the file name)
    parts.pop();
    while (
      parts.length &&
      !fs.readdirSync(path.join(bucketPath, ...parts)).length
    ) {
      await fs.rmdir(path.join(bucketPath, ...parts));
      parts.pop();
    }
  }

  async initiateUpload(bucket, key, uploadId, metadata) {
    const uploadDir = path.join(
      this.getResourcePath(bucket, undefined, 'uploads'),
      uploadId,
    );

    await fs.mkdirp(uploadDir);

    await Promise.all([
      fs.writeFile(path.join(uploadDir, 'key'), key),
      fs.writeFile(path.join(uploadDir, 'metadata'), JSON.stringify(metadata)),
    ]);
  }

  async putPart(bucket, uploadId, partNumber, content) {
    const partPath = path.join(
      this.getResourcePath(bucket, undefined, 'uploads'),
      uploadId,
      partNumber.toString(),
    );

    await fs.mkdirp(path.dirname(partPath));

    const [size, md5] = await new Promise((resolve, reject) => {
      const writeStream = fs.createWriteStream(partPath);
      const md5Context = crypto.createHash('md5');
      let totalLength = 0;

      content
        .pipe(
          new Transform({
            transform(chunk, encoding, callback) {
              md5Context.update(chunk, 'binary');
              totalLength += chunk.length;
              return callback(null, chunk);
            },
          }),
        )
        .on('error', reject)
        .on('finish', () => {
          resolve([totalLength, md5Context.digest('hex')]);
        })
        .pipe(writeStream);
    });
    await fs.writeFile(`${partPath}.md5`, md5);
    return { size, md5 };
  }

  async putObjectMultipart(bucket, uploadId, parts) {
    const uploadDir = path.join(
      this.getResourcePath(bucket, undefined, 'uploads'),
      uploadId,
    );
    const [key, metadata] = await Promise.all([
      fs.readFile(path.join(uploadDir, 'key')).then(data => data.toString()),
      fs.readFile(path.join(uploadDir, 'metadata')).then(JSON.parse),
    ]);
    const partStreams = sortBy(parts, part => part.number).map(part =>
      fs.createReadStream(path.join(uploadDir, part.number.toString())),
    );
    const object = new S3Object(
      bucket,
      key,
      concatStreams(partStreams),
      metadata,
    );
    const result = await this.putObject(object);
    await fs.remove(uploadDir);
    return result;
  }

  async getSubresource(bucket, key, resourceType) {
    const resourcePath = this.getResourcePath(
      bucket,
      key,
      `${resourceType}.xml`,
    );

    const Model = getConfigModel(resourceType);

    try {
      const data = await fs.readFile(resourcePath);
      return new Model(data.toString());
    } catch (err) {
      if (err.code === 'ENOENT') return null;
      throw err;
    }
  }

  async putSubresource(bucket, key, resource) {
    const resourcePath = this.getResourcePath(
      bucket,
      key,
      `${resource.type}.xml`,
    );
    await fs.writeFile(resourcePath, resource.toXML(2));
  }

  async deleteSubresource(bucket, key, resourceType) {
    const resourcePath = this.getResourcePath(
      bucket,
      key,
      `${resourceType}.xml`,
    );
    try {
      await fs.unlink(resourcePath);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }
  }
}

module.exports = FilesystemStore;