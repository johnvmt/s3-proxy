var AWS = require('aws-sdk');
var pick = require('lodash.pick');
var trim = require('lodash.trim');
var map = require('lodash.map');
var isEmpty = require('lodash.isempty');
var reject = require('lodash.reject');
var assign = require('lodash.assign');
var awsConfig = require('aws-config');
var mime = require('mime');
var base64 = require('base64-stream');
var debug = require('debug')('s3-proxy');

module.exports = function(options) {
  var s3 = new AWS.S3(assign(awsConfig(options), pick(options, 'endpoint', 's3ForcePathStyle')));

  // HTTP headers from the AWS request to forward along
  var awsForwardHeaders = ['content-type', 'last-modified', 'etag', 'cache-control'];

  return function(req, res, next) {
    if (req.method !== 'GET')
      next();
    else {
      // This will get everything in the path following the mountpath
      var prefixFormatted = options.prefix ? options.prefix.replace(/^\/|\/$/g, '') : ''; // strip leading and trailing slash
      var sanitizedBaseKey = sanitizeS3Path(decodeURIComponent(req.originalUrl.substr(req.baseUrl.length + 1))); // strip leading and trailing slashes
      var baseS3Key = (prefixFormatted.length) ? (prefixFormatted + '/' + sanitizedBaseKey) : sanitizedBaseKey;

      var s3PathsToTry = [];

      if(baseS3Key.length)
        s3PathsToTry.push(baseS3Key);

      if(options.hasOwnProperty('index') && Array.isArray(options.index)) {
        options.index.forEach(function(indexDoc) {
          s3PathsToTry.push(baseS3Key + '/' + indexDoc.replace(/^\/|\/$/g, ''));
        });
      }

      if(options.hasOwnProperty('list') && options.list)
        s3PathsToTry.push(baseS3Key + '/');

      tryNextS3Path();

      function tryNextS3Path() {
        if(s3PathsToTry.length) {
          var s3Path = s3PathsToTry.shift();

          debug('try path', s3Path);

          var s3Params = {
            Bucket: options.bucket
          };

          if(s3Path.slice(-1) === '/' || s3Path.length === 0) {
            s3Params.Prefix = s3Path;
            outputListing(s3Params, req, res, tryNextS3Path);
          }
          else {
            s3Params.Key = s3Path; // strip leading slash

            var base64Encode = req.acceptsEncodings(['base64']) === 'base64'; // add to options

            // The IfNoneMatch in S3 won't match if client is requesting base64 encoded response.
            if (req.headers['if-none-match'] && !base64Encode)
              s3Params.IfNoneMatch = req.headers['if-none-match'];

            outputObjectIfExists(s3Params, base64Encode, req, res, tryNextS3Path);
          }
        }
        else // out of options here
          next();
      }
    }
  };

  function sanitizeS3Path(s3Key) {
    // Chop off the querystring, it causes problems with SDK.
    var queryIndex = s3Key.indexOf('?');
    if (queryIndex !== -1) {
      s3Key = s3Key.substr(0, queryIndex);
    }

    // Strip out any path segments that start with a double dash '--'. This is just used
    // to force a cache invalidation.
    s3Key = reject(s3Key.split('/'), function (segment) {
      return segment.slice(0, 2) === '--';
    }).join('/');

    return s3Key.replace(/^\/|\/$/g, ''); // strip leading and trailing slashes
  }

  function outputObjectIfExists(s3Params, base64Encode, req, res, next) {
    getObject(s3Params, function (errorCode, response) {
      if (errorCode) {
        if(errorCode === 304)
          res.status(304).end();
        else if(errorCode === 404)
          next(404); //Error.http(404, 'Missing S3 key', {code: 'missingS3Key', key: s3Params.Key}));
      }
      else {
        awsForwardHeaders.forEach(function (headerKey) {
          var headerValue = response.headers[headerKey];

          if (headerKey === 'content-type') {
            if (headerValue === 'application/octet-stream') {
              // If the content-type from S3 is the default "application/octet-stream",
              // try and get a more accurate type based on the extension.
              headerValue = mime.lookup(req.path);
            }
          } else if (headerKey === 'cache-control') {
            if (options.overrideCacheControl) {
              debug('override cache-control to', options.overrideCacheControl);
              headerValue = options.overrideCacheControl;
            } else if (!headerValue && options.defaultCacheControl) {
              debug('default cache-control to', options.defaultCacheControl);
              headerValue = options.defaultCacheControl;
            }
          } else if (headerKey === 'etag' && base64Encode) {
            headerValue = '"' + trim(headerValue, '"') + '_base64' + '"';
          } else if (headerKey === 'content-length' && base64Encode) {
            // Clear out the content-length if we are going to base64 encode the response
            headerValue = null;
          }

          if (headerValue) {
            debug('set header %s=%s', headerKey, headerValue);
            res.setHeader(headerKey, headerValue);
          }
        });

        // Write a custom http header with the path to the S3 object being proxied
        var headerPrefix = req.app.settings.customHttpHeaderPrefix || 'x-4front-';
        res.setHeader(headerPrefix + 's3-proxy-key', s3Params.Key);

        if (base64Encode) {
          debug('base64 encode response');
          res.setHeader('Content-Encoding', 'base64');
          response.stream = response.stream.pipe(base64.encode());
        }

        //ON success, return readStream and headers

        response.stream.pipe(res);
      }
    });

  }

  function getObject(s3Params, callback) {
    debug('get s3 object with key %s', s3Params.Key);

    var s3Request = s3.getObject(s3Params);

    s3Request.on('httpHeaders', function(statusCode, s3Headers) {
      if(statusCode === 200) {
        callback(null, {
          headers: s3Headers,
          stream: s3Request.createReadStream()
        });
      }
      else
        callback(statusCode, null);
    });

    s3Request.send();
  }

  // Will list everything underneath this path, including in subfolders
  function outputListing(s3Params, req, res, next) {
    debug('list s3 keys at', s3Params.Prefix);
    s3.listObjects(s3Params, function(error, data) {
      if(error)
        res.status(500).end();
      else {
        var keys = [];
        map(data.Contents, 'Key').forEach(function(key) {
          // Chop off the prefix path
          if (key !== s3Params.Prefix) {
            if (isEmpty(s3Params.Prefix)) {
              keys.push(key);
            } else {
              keys.push(key.substr(s3Params.Prefix.length));
            }
          }
        });

        if(keys.length)
          res.json(keys);
        else
          next();
      }
    });
  }
};
