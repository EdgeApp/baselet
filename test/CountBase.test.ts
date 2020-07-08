import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { createCountBase } from '../src/CountBase'
import { BaseType } from '../src/types'

describe('CountBase baselet', function () {
  it('create countbase', async function () {
    const dbName = 'testdb'
    const dbBucketSize = 10
    const expectedTest = JSON.stringify({
      type: BaseType.COUNT_BASE,
      bucketSize: dbBucketSize,
      length: 0,
      partitions: {
        '/': {
          length: 0
        }
      }
    })
    const disklet = makeMemoryDisklet()
    await createCountBase(disklet, dbName, dbBucketSize)
    expect(await disklet.getText(`${dbName}/config.json`)).equals(expectedTest)
  })
})
