# file-upload-handler
Handles fileuploads and streams the progress to the client

## Upgrading to 2.0.0

Version 2 is built for **hapi 17 and later** (`@hapi/hapi`, tested with hapi 21 on Node.js 24). hapi 17
replaced the `reply` interface with the response toolkit `h`, and every route handler must return its
response. Version 1.x only works with hapi 16.

Requirements: Node.js 14 or later, `@hapi/hapi` 17 or later.

**The functions take `h` and return the response.** `getHapiFns(tempdir, maxFileSize, accessControlAllowOrigin, nsClientId)`
is unchanged. Its functions take `h` where they took `reply`, and return the response to send (or a Promise of it).
Return that from your route handler:

Before (1.x, hapi 16):

```js
handler: function(request, reply) {
    fns.recieveFile(request, reply, null, processFile, false);
}
```

After (2.x, hapi 17+):

```js
handler: (request, h) => fns.recieveFile(request, h, null, processFile, false)
```

| Function | Returns |
|---|---|
| `generateClientId(request, h)` | the client id as `text/plain` |
| `responseOptions(request, h)` | the empty CORS preflight answer, with status 200 |
| `recieveFile(request, h, maxFileSize, callback, waitForCb)` | a Promise of the response: `{status: 'BUSY'}` for a chunk, `{status: 'OK'}` for the last one |
| `recieveFormFiles(request, h, maxFileSize, callback, waitForCb)` | a Promise of the response: `{status: 'OK'}` |

**What changed besides the signature**

1. With `waitForCb` left out (or `true`), the answer to the last chunk is `{status: 'OK', data}`, where `data` is
   what your `callback` returned (or resolved with). A callback can no longer send its own reply: return the data
   instead.
2. With `waitForCb === false` the answer is sent right away; your callback and the cleanup of the temporary files
   run in the background, as before.
3. An upload whose size exceeds `maxFileSize` answers 403 with `{status: 'Error: max filesize exceeded'}`, as
   before, but the Promise now **resolves** with that response instead of rejecting, so the route handler can
   return it. A form upload that is too large still answers `{status: 'ERROR', message: 'max filesize exceeded'}`.
4. `responseOptions` answers 200 as before: it sets the status explicitly, because hapi 17+ answers an empty
   response with 204.
5. Errors while writing or rebuilding a file still reject the Promise.

Upload routes need `payload: {output: 'stream', parse: false}`, the same as with 1.x.
