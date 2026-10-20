`gremlin.h` and `normfs.pb.h` come from [gremlin.c](https://github.com/norma-core/gremlin.c),
commit d0fddb9 (norma-core/gremlin.c#4). The generated code normfs calls is
proved here; the `gremlin.h` runtime under it is proved by gremlin.c's
`verify-runtime`. Regenerate after a change to `proto/normfs.proto`:

```sh
gremlinc-gen -R proto -o normfs/c/gremlin normfs.proto
```
