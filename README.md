
### Desired functionality for 0.1.0

```bash
$ git clone https://github.com/snowplow/snowplow-website-event-dictionary.git
$ ./iglu-utils snowplow-website-event-dictionary/schemas --dest ./
$ diff -r ./sql snowplow-website-event-dictionary/sql
$ diff -r ./jsonpaths snowplow-website-event-dictionary/jsonpaths
```

Note: any differences in the diffs should expose human error in the manual creation of the `snowplow-website-event-dictionary`, rather than bugs in iglu-utils.

### Packaging

```bash
VERSION=0.1.0
cat bin/jarx-stub.sh target/scala-2.10/iglu-utils.jar > target/scala-2.10/iglu-utils
chmod +x target/scala-2.10/iglu-utils
zip -j "package/snowplow_iglu_utils_${VERSION}_linux.zip" target/scala-2.10/iglu-utils
```

Then upload `package/snowplow_iglu_utils_${VERSION}_linux.zip` to Snowplow's open source Bintray.
