"use strict";

var fileUtils = require("./file-utils"),
  ACCESS_CONTROL_ALLOW_ORIGIN,
  fs = require("fs"),
  utils = require("itsa-utils"),
  multiparty = require("multiparty"),
  idGenerator = utils.idGenerator,
  PM2_INSTANCE = process.env.NODE_APP_INSTANCE || "0", // see https://github.com/Unitech/PM2/issues/1510
  DEF_NS_CLIENT_ID = "ITSA_CL_ID_" + PM2_INSTANCE,
  DEF_MAX_FILESIZE = 100 * 1024 * 1024, // 100Mb
  REVIVER = function (key, value) {
    return (typeof value === "string" && value.itsa_toDate()) || value;
  },
  getFns;

require("itsa-jsext");
require("fs-extra");
require("itsa-writestream-promise"); // extends fs.WriteStream with .endPromise

/**
 * The modules.export-function, which returns an object with 2 properties: `generateClientId` and `recieveFile`,
 * which are both functions.
 *
 * `generateClientId` generates an unique clientId, which clients should use to identify themselves during fileuploads.
 *
 * `recieveFile` should be invoked for every filechunk that is send to the server.
 *
 * Both methods expect the client to follow specific rules, as specified by http://itsa.io/docs/io/index.html#io-filetransfer
 * Therefore, this module is best used together with the ITSA-framework (http://itsa.io)
 *
 * IMPORTANT NOTE: this method is build for usage with hapijs.
 *
 * @method getFns
 * @param [tempdir] {String}
 * @param [maxFileSize] {Number}
 * @param [nsClientId] {String}
 * @return {Object}
 * @since 0.0.1
 */
getFns = function (tempdir, maxFileSize, accessControlAllowOrigin, nsClientId) {
  var TMP_DIR = tempdir || process.env.TMP || process.env.TEMP || "/tmp",
    NS_CLIENT_ID = nsClientId || DEF_NS_CLIENT_ID,
    FILE_TRANSMISSIONS = {},
    globalMaxFileSize = maxFileSize,
    tmpDirCreated;

  TMP_DIR.itsa_endsWith("/") || (TMP_DIR = TMP_DIR + "/");

  tmpDirCreated = fileUtils.createDir(TMP_DIR);

  ACCESS_CONTROL_ALLOW_ORIGIN =
    accessControlAllowOrigin === true ? "*" : accessControlAllowOrigin || "";

  return {
    generateClientId: function (request, reply) {
      var clientId = idGenerator(NS_CLIENT_ID);
      if (ACCESS_CONTROL_ALLOW_ORIGIN) {
        reply(clientId)
          .header("access-control-allow-origin", ACCESS_CONTROL_ALLOW_ORIGIN)
          .header("Content-Type", "text/plain");
      } else {
        reply(clientId).header("Content-Type", "text/plain");
      }
    },

    responseOptions: function (request, reply) {
      var requestHeaders = request.headers["access-control-request-headers"];
      reply()
        .header("access-control-allow-origin", ACCESS_CONTROL_ALLOW_ORIGIN)
        .header("access-control-allow-methods", "PUT,GET,POST")
        .header("access-control-allow-headers", requestHeaders)
        .header("access-control-max-age", "1728000")
        .header("content-length", "0");
    },

    recieveFormFiles: function (
      request,
      reply,
      maxFileSize,
      callback,
      waitForCb
    ) {
      var form;
      if (typeof maxFileSize === "function") {
        callback = maxFileSize;
        maxFileSize = null;
      }
      if (!maxFileSize) {
        maxFileSize = maxFileSize || globalMaxFileSize || DEF_MAX_FILESIZE;
      }

      form = new multiparty.Form({
        autoFiles: true,
        uploadDir: TMP_DIR,
        maxFilesSize: maxFileSize,
      });
      return new Promise(function (fulfill, reject) {
        form.parse(request.payload, function (err, fields, payload) {
          var files, replyInstance, wrapper;
          if (err) {
            reply({ status: "ERROR", message: "max filesize exceeded" });
            fulfill();
          } else {
            request.params || (request.params = {});
            Object.itsa_isObject(fields) &&
              fields.itsa_each(function (value, key) {
                request.params[key] = value[0];
              });
            files = payload.uploadfiles.map(function (item) {
              return {
                fullFilename: item.path,
                originalFilename: item.originalFilename,
              };
            });
            typeof callback === "function" &&
              (wrapper = callback.call(request, files));
            if (waitForCb === false) {
              replyInstance = reply({ status: "OK" });
              ACCESS_CONTROL_ALLOW_ORIGIN &&
                replyInstance.header(
                  "access-control-allow-origin",
                  ACCESS_CONTROL_ALLOW_ORIGIN
                );
              Promise.resolve(wrapper)
                .then(function () {
                  files.forEach(function (item) {
                    return fileUtils.removeFile(item.fullFilename);
                  });
                })
                .catch(function (err) {
                  console.error(err);
                });
              fulfill();
            } else {
              return Promise.resolve(wrapper)
                .then(function (data) {
                  if (!reply._replied) {
                    replyInstance = reply({ status: "OK", data });
                    ACCESS_CONTROL_ALLOW_ORIGIN &&
                      replyInstance.header(
                        "access-control-allow-origin",
                        ACCESS_CONTROL_ALLOW_ORIGIN
                      );
                  }
                  files.forEach(function (item) {
                    return fileUtils.removeFile(item.fullFilename);
                  });
                })
                .then(fulfill, reject);
            }
          }
        });
      });
    },

    /**
     * Recieves and processes filechunks from a client's fileupload.
     */
    recieveFile: function (request, reply, maxFileSize, callback, waitForCb) {
      var filedata = request.payload,
        isStream = !!(filedata && typeof filedata.pipe === "function"),
        // try to get the size for accounting:
        filedataSize = Buffer.isBuffer(filedata)
          ? filedata.length
          : isStream
          ? parseInt(request.headers["content-length"] || "0", 10)
          : typeof filedata === "string"
          ? Buffer.byteLength(filedata)
          : 0,
        originalFilename,
        transId,
        clientId,
        partialId,
        promise,
        data,
        totalSize,
        replyInstance,
        errorMsg;

      originalFilename = request.headers["x-filename"];
      transId = request.headers["x-transid"];
      clientId = request.headers["x-clientid"];
      partialId = request.headers["x-partial"];
      totalSize = parseInt(request.headers["x-total-size"] || "0", 10);

      // create clientid if not defined:
      FILE_TRANSMISSIONS[clientId] || (FILE_TRANSMISSIONS[clientId] = {});
      // create transid if not defined, and fill the property `cummulatedSize`:
      if (!FILE_TRANSMISSIONS[clientId][transId]) {
        FILE_TRANSMISSIONS[clientId][transId] = {
          cummulatedSize: filedataSize,
        };
      } else {
        FILE_TRANSMISSIONS[clientId][transId].cummulatedSize += filedataSize;
      }

      if (typeof maxFileSize === "function") {
        callback = maxFileSize;
        maxFileSize = null;
      }
      if (!maxFileSize) {
        maxFileSize = maxFileSize || globalMaxFileSize || DEF_MAX_FILESIZE;
      }

      // Abort upfront using declared total size (stream chunk sizes may not be known yet).
      if (
        totalSize > maxFileSize ||
        FILE_TRANSMISSIONS[clientId][transId].cummulatedSize > maxFileSize
      ) {
        delete FILE_TRANSMISSIONS[clientId][transId];
        if (FILE_TRANSMISSIONS[clientId].itsa_size() === 0) {
          delete FILE_TRANSMISSIONS[clientId];
        }
        errorMsg = "Error: max filesize exceeded";
        replyInstance = reply({ status: errorMsg }).code(403);
        ACCESS_CONTROL_ALLOW_ORIGIN &&
          replyInstance.header(
            "access-control-allow-origin",
            ACCESS_CONTROL_ALLOW_ORIGIN
          );
        return Promise.reject(errorMsg);
      }

      return tmpDirCreated
        .then(fileUtils.getUniqueFilename)
        .then(function (fullFilename) {
          var partCount,
            wstream = fs.createWriteStream(fullFilename);

          wstream.on("error", function (e) {
            console.error("WriteStream error while writing chunk:", e);
          });

          // ==== FIX: handle both Buffer/string and Stream payloads ====
          var writePromise;
          if (isStream) {
            // Pipe the incoming stream into the file stream
            writePromise = new Promise(function (resolve, reject) {
              var streamed = 0;
              filedata.on("data", function (c) {
                if (c && c.length) streamed += c.length;
              });
              filedata.on("error", reject);
              wstream.on("error", reject);
              wstream.on("finish", function () {
                // If we didn't know size earlier, account it now:
                if (!filedataSize && streamed) {
                  FILE_TRANSMISSIONS[clientId][transId].cummulatedSize +=
                    streamed;
                }
                resolve();
              });
              filedata.pipe(wstream); // end() is called automatically on wstream when source ends
            });
          } else {
            // Original path: write Buffer/string directly
            wstream.write(filedata);
            writePromise = wstream.itsa_endPromise();
          }
          // ============================================================

          return writePromise.then(function () {
            // now save the chunk's filename:
            FILE_TRANSMISSIONS[clientId][transId][partialId] = fullFilename;

            if (originalFilename) {
              FILE_TRANSMISSIONS[clientId][transId].count = parseInt(
                partialId,
                10
              );
              FILE_TRANSMISSIONS[clientId][transId].filename = originalFilename;
              // store any params that might have been sent with the request:
              data = request.headers["x-data"];
              if (data) {
                try {
                  FILE_TRANSMISSIONS[clientId][transId].data = JSON.parse(
                    data,
                    REVIVER
                  );
                } catch (err) {
                  console.error(err);
                  FILE_TRANSMISSIONS[clientId][transId].data = {};
                }
              } else {
                FILE_TRANSMISSIONS[clientId][transId].data = {};
              }
            }

            // if all parts are processed, we can build the final file:
            partCount = FILE_TRANSMISSIONS[clientId][transId].count;
            if (
              partCount &&
              FILE_TRANSMISSIONS[clientId][transId].itsa_size() ===
                partCount + 4
            ) {
              request.params || (request.params = {});
              request.params.itsa_merge(
                FILE_TRANSMISSIONS[clientId][transId].data
              );
              promise = fileUtils.getFinalFile(
                TMP_DIR,
                FILE_TRANSMISSIONS[clientId][transId]
              );
            } else {
              promise = Promise.resolve();
            }

            return promise.then(function (filedata) {
              var wrapper =
                filedata && typeof callback === "function"
                  ? callback.call(
                      request,
                      filedata.tmpBuildFilename,
                      filedata.originalFilename
                    )
                  : null;

              if (waitForCb === false) {
                var replyInstance = reply({ status: filedata ? "OK" : "BUSY" });
                ACCESS_CONTROL_ALLOW_ORIGIN &&
                  replyInstance.header(
                    "access-control-allow-origin",
                    ACCESS_CONTROL_ALLOW_ORIGIN
                  );
                if (filedata) {
                  Promise.resolve(wrapper)
                    .then(function () {
                      delete FILE_TRANSMISSIONS[clientId][transId];
                      if (FILE_TRANSMISSIONS[clientId].itsa_size() === 0) {
                        delete FILE_TRANSMISSIONS[clientId];
                      }
                      fileUtils.removeFile(filedata.tmpBuildFilename);
                    })
                    .catch(function (err) {
                      console.error("****** ERROR 1 ********", err);
                    });
                }
              } else {
                return Promise.resolve(wrapper).then(function (data) {
                  if (!reply._replied) {
                    var replyInstance = reply(
                      filedata
                        ? { status: "OK", data: data }
                        : { status: "BUSY" }
                    );
                    ACCESS_CONTROL_ALLOW_ORIGIN &&
                      replyInstance.header(
                        "access-control-allow-origin",
                        ACCESS_CONTROL_ALLOW_ORIGIN
                      );
                  }
                  if (filedata) {
                    delete FILE_TRANSMISSIONS[clientId][transId];
                    if (FILE_TRANSMISSIONS[clientId].itsa_size() === 0) {
                      delete FILE_TRANSMISSIONS[clientId];
                    }
                    return fileUtils.removeFile(filedata.tmpBuildFilename);
                  }
                });
              }
            });
          });
        })
        .catch(function (err) {
          console.error(err);
          throw new Error(err);
        });
    },
  };
};

module.exports = getFns;
