# VARIANT Type

Status: Draft. Follows the discussion in
[#17033](https://github.com/facebookincubator/velox/discussions/17033).

## Goal

Support the Parquet / Iceberg / Delta `VARIANT` type for semi-structured data.
Spark 4.1 is the first target (through Gluten); Presto and Nimble are expected
to follow.

A Variant value is two byte strings defined by the
[Variant encoding spec](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md):

- `metadata`: version, flags, and a dictionary of field names.
- `value`: a recursively encoded value. Objects refer to field names by
  dictionary id, keep their fields ordered by name, and have offset arrays, so
  a reader can find a path by binary search without parsing the whole value.

Writers can also *shred* frequently used paths into separate typed Parquet
columns ([shredding spec](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)).
Shredding is decided at write time, independently per file, so files in one
table can shred different paths.

## Decision: shredding stays inside the reader

The original proposal added `TypeKind::VARIANT` and a `VariantVector` that
carries shredded columns through the pipeline. The discussion rejected that:
the scan should produce a stable schema, the same way subfield pruning works
for `ROW`, `MAP` and `ARRAY`. Shredded columns are a reader optimization that
the rest of the pipeline never sees.

This matches Spark's `PushVariantIntoScan`. For

```sql
SELECT variant_get(data, '$.user.id', 'bigint'),
       variant_get(data, '$.event.type', 'string'),
       to_json(data)
FROM t
```

Spark asks the scan for `data` as `struct<0: bigint, 1: string, 2: variant>`
and rewrites the expressions into field accesses. The reader fills each field
from a shredded column when the file has one and from the binary value
otherwise. Only field `2` is still a full Variant, and the reader reassembles it
from the shredded columns and the remainder `value`.

## Type

`VARIANT` is a custom type over `ROW(metadata VARBINARY, value VARBINARY)`.

- No new `TypeKind` or `VectorEncoding`. Vectors are ordinary `RowVector`s.
- `isComparable()` and `isOrderable()` return false, so the type cannot be used
  as a grouping, join, or sort key. Spark has the same restriction.
- The `ROW` keeps both byte strings, rather than a single `VARBINARY`, because:
  - The Parquet writer writes them as two columns. Joining them into one blob
    would need a Velox-specific framing format that the writer must undo.
  - `variant_get` with a non-constant path is not pushed into the scan and
    must navigate both byte strings for each row.
  - Iceberg models the type the same way.

### Package

Until now, custom types were defined by function packages and only functions
understood them. `VARIANT` must be understood by functions (Spark, later
Presto) and by file readers and writers (Parquet, later Nimble). To avoid
Parquet depending on Spark functions or the reverse, the type and the binary
codec live in their own library, `velox/type/variant` (`velox_type_variant`),
which depends only on `velox_type`.

| File | Contents |
| --- | --- |
| `VariantType.h` | `VariantType`, `VARIANT()`, `isVariantType()`. |
| `VariantRegistration.h` | `registerVariantType()`. |
| `VariantEncoding.h` | `VariantMetadata` and `VariantValue`, read-only views over the encoding. |
| `VariantBuilder.h` | `VariantBuilder`, which encodes values byte for byte like Spark's. |

`VariantValue` navigates the bytes in place: `kind()`, typed accessors
(`asInteger()`, `asString()`, `asDecimal()`, ...), `field(name)`, and
`elementAt(index)`. `field(name)` scans objects with fewer than 32 fields and
uses binary search on larger ones, as Spark does. It does not allocate. Malformed input throws a
user error instead of reading out of bounds. Converting between kinds (for
example, `variant_get(v, '$.a', 'bigint')` on an `INT8`) is left to the
function packages because the rules depend on the engine.

## Data lifecycle

| Stage | Component | Notes |
| --- | --- | --- |
| Create | `parse_json`, `to_variant_object` (Spark functions) | Use `VariantBuilder`. |
| Read | Parquet reader | Unshredded groups are read as `VARIANT` today. Later: take the requested subfields from the scan spec, return typed columns for path accesses, and reassemble `(metadata, value)` for full accesses. |
| Transform | `variant_get`, `try_variant_get`, `is_variant_null`, `schema_of_variant`, `schema_of_variant_agg`, `variant_explode`, `to_json`, casts | Spark functions package. All run on the binary encoding. |
| Exchange / spill | Existing `ROW` serialization | Nothing Variant-specific is needed. |
| Write | Parquet writer | Unshredded first. Shredding on write comes later. |

Variant functions are a separate set from the JSON functions
(`get_json_object`, `json_object_keys`, ...) that work on `VARCHAR`. They can
share path parsing, but not evaluation.

## Plan

Each item is one or more small PRs.

Done on branch `feat/variant-type`:

1. **Type and decoder:** `VariantType`, registration, `VariantMetadata` and
   `VariantValue`.
2. **Encoder:** `VariantBuilder`. Tests compare its output with bytes from
   Spark 4.0.2.
3. **Spark functions:** `parse_json`, `try_parse_json`, `variant_get`,
   `try_variant_get`, `is_variant_null`, `schema_of_variant`,
   `to_variant_object`, and `to_json` on `VARIANT`. Expected values come from
   Spark 4.0.2.
4. **Parquet reader, unshredded:** a group is read as `VARIANT` when the table
   schema requests `VARIANT` or the group has the `VARIANT` annotation. Spark
   4.0 writes no annotation, and it writes `value` before `metadata`, so the
   two fields are matched by name.

Remaining:

5. **Parquet reader, shredded:** reassemble the full value from `typed_value`
   and the remainder `value`. Then support subfield requests that return typed
   columns, and push filters into the shredded columns so their statistics can
   skip row groups. Reading by field id, which Iceberg uses, also belongs here.
6. **Parquet writer:** unshredded first, shredding later.
7. **More Spark support:** casts to and from `VARIANT`, `schema_of_variant_agg`,
   and `variant_explode` (a generator, which maps to an unnest in Velox).
8. **Gluten / Substrait** mapping.
9. **Benchmark** `variant_get` against `get_json_object` on the same data.

## Known differences from Spark

- `to_json` prints some doubles with more digits than Spark: `1e-5` becomes
  `1.0000000000000001E-5` instead of `1.0E-5`. The cause is Velox's shared
  double to string conversion, which Spark `cast` and `to_json` in Velox also
  use. Fixing it there fixes all three.
- `VariantBuilder` sorts object fields by UTF-8 bytes. Spark sorts by UTF-16
  code units. The orders differ only for some non-BMP field names.
- Spark 4.0 rejects the TIME and nanosecond timestamp types of the Parquet
  spec. The decoder accepts them, and the Spark functions reject them.

## Open questions

- **Naming.** `velox::Variant` already exists as a runtime-typed value holder,
  so `VariantType` and `VariantValue` can be confusing. The SQL name has to be
  `VARIANT` to match Spark, Iceberg and Parquet. C++ alternatives raised in the
  discussion include renaming the existing class to `DynamicValue`, or a
  `SemiStructured` prefix for the new classes. This draft keeps the `Variant`
  prefix until reviewers decide. A side effect: the function-registration alias
  (`using X = CustomType<VariantT>`) is not defined yet, because the natural
  name `Variant` is taken.
- **Casts.** `getCastOperator()` returns `nullptr`, so casts treat `VARIANT` as
  its `ROW`. Until Spark cast semantics are added, Gluten can map
  `CAST(v AS type)` to `variant_get(v, '$')` and `TRY_CAST(v AS type)` to
  `try_variant_get(v, '$')`, with that result type. A cast from `VARIANT`
  fails on invalid values in both ANSI and legacy mode.
- **Parquet C++.** The discussion noted that Arrow's Parquet library, which the
  Velox Parquet writer uses, did not support Variant yet. Check its status
  before step 6.
- **Fuzzer.** `getInputGenerator()` returns `nullptr`. A generator of valid
  encoded values can be added once `VariantBuilder` exists.
