# DataKitten Default Storage Engine File Format

## File Header

Every DataKitten file starts with four static bytes (`0x4D 0x65 0x6F 0x77`) , followed by a 1-byte version number (currently `0x00`), followed by an UInt64 which points to the location of the first byte of the Metadata Document.

## Metadata Document

```json
{
  'free': .binary([
    (UInt64, UInt32)... // first byte location, length
  ]),
  'cols': {
    'collection_name': {
      dlts: [4124, 12341212512, 613431531513513] // Locations of parts of the DLTs
      //indexes: [{
      // name: "ids",
      // fields: [{
      //   field: "_id",
      //   'type': "asc",
      //}],
      //'pos': 2131231232,
      //'len': 10342
	  }]
    }
  }
}
```

## Data Location Table

(UInt64, Document) // the uint64 is the position of the next document in the collection

```json
UInt64 UInt32...
```
