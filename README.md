### Packaging

```bash
VERSION=0.1.0
cat bin/jarx-stub.sh target/scala-2.10/iglu-utils.jar > target/scala-2.10/iglu-utils
chmod +x target/scala-2.10/iglu-utils
zip -j "package/snowplow_iglu_utils_${VERSION}_linux.zip" target/scala-2.10/iglu-utils
```

Then upload `package/snowplow_iglu_utils_${VERSION}_linux.zip` to Snowplow's open source Bintray.
