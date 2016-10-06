# DataKitten Default Storage Engine File Format

## Files
- cat (Common Administrative Table)
- `collection._id`.collection (`collection._id` can be found in the CAT)

## File Header

Every DataKitten file starts with four static bytes (`0x4D 0x65 0x6F 0x77`) , followed by a 1-byte version number (currently `0x00`).

## Common Administrative Table (`CAT`)

The first thing after the file header is a BSON Document.

This BSON Document contains necessary information, but allows for additional information that's reserved for future usage.

The space reserved for the header document is always 1000 bytes.

This means the first page starts at byte 1005 where the start of the file = 0.

```json
{
  "collections": {
     collectionName: {
        "_id": ObjectId("afafafafafafafafafafafaf"), // the hexstring will be used for the collection filename
        "pages": [{
          s: 0,   // Start position
          l: 1053 // page Length
          t: indexIdentifier
        },{
          s: 1053, // Start position
          l: 41023 // page length
          t: dataIdentifier
        }]
     }
  }
}
```
 
## Collection file

The collection is split up into pages, every page start and length can be found in the CAT

A page purely exists of con*cat*enated BSON Documents