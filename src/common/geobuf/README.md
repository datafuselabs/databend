**Summary**

A columnar Geometry/Geography format is proposed here to provide support for the storage and computation of geospatial features.

This column format has five columns, namely the point column, which consists of the x column, the y column, and the point_offset column,
and the rest of the information is serialized in a binary column, which consists of the data column and the offset column, see Column

**Format compatibility**

EWKB supports up to 4 dimensions, here only 2 dimensions are supported, in line with snowflake, and new types can be added to support higher dimensions.
https://docs.snowflake.com/en/sql-reference/data-types-geospatial#geometry-data-type

See geo.fbs for details on other compatibility designs.

References WKT WKB EWKT EWKB GeoJSON Spec.
https://libgeos.org/specifications/wkb/#standard-wkb
https://datatracker.ietf.org/doc/html/rfc7946
https://geojson.org/geojson-spec.html#coordinate-reference-system-objects

**Why the columnar Geometry/Geography format?**

- For smaller storage volumes, see the test below.
- The problem of compression coding of floating-point columns has a large number of readily available research results that can be introduced at low cost,
  which is conducive to compression of storage space and improvement of io efficiency.
- Floating-point columns have min max sparse indexes, very similar to the R tree indexes widely used in geospatial features, and are very cheap to implement and maintain.
- Facilitates the implementation of filtered push-down to the storage layer, and vectorized computation.
- On the downside, geospatial functions become significantly more expensive to implement and maintain, requiring a significant amount of work.

**Why use flatbuffer?**

flatbuffer provides delayed deserialization , and partial deserialization capabilities , to facilitate the writing of high-performance implementation .
In addition, code generation is easy to adapt to the design.
However, flatbuffer has its drawbacks. Similar to protobuffer, flatbuffer is an extensible, compatible format, and in order to provide extensibility and compatibility, a vtable is deposited in the data, resulting in an inflated size.
Once the design is stabilized, consider implementing it in other ways.

**Other implementations**
https://help.aliyun.com/document_detail/442156.html
