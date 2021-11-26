# Baselet

## 0.2.1 (2021-11-26)
- Upgrade memlet to 0.1.x and integrate memlet exclusively as the backend datastore.

## 0.2.0 (2021-10-19)

### Added 
- TypeScript types for each baselet (`CountBase`, `HashBase`, etc).
- New `createOrOpen*` APIs for each baselet type.
- Always include partition key on returned data from `dumpData` on `HashBase` instances.
- Include `rangeKey` and `idKey` config values on `RangeBase` instances
### Fixes
- Return empty result set for empty queries in `HashBase`.

## 0.0.9 (2021-03-16)

* Return default value `0` for `RangeBase` functions: `min`, `max`, `size`

## 0.0.8 (2021-03-12)

* Add `dumpData` function for all base types
  - Returns data in the format:
  ```json
  {
    "config": {},
    "data": {}
  }
  ```

## 0.0.6 (2021-02-12)

* Add `size` getter function for `RangeBase`

## 0.0.5 (2020-12-07)

* Add support for `RangeBase` pagination

## 0.0.4 (2020-12-06)

* Revert to previous TypeScript output path

## 0.0.3 (2020-11-19)

* Fixing searching logic
* Wrap `baselet` reference in `memlet`
* Rename function `move` to `update` 
* Added required parameter `range` for `RangeBase` functions: `queryById`, `delete`, `update`
* Exported `BaseType`
* Change TypeScript output path

## 0.0.1 (2020-06-25)

Initial release
