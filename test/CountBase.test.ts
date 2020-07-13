import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { CountBase, createCountBase } from '../src/CountBase'
import { BaseType } from '../src/types'

describe('CountBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let countbaseDb: CountBase
  const dbName = 'testdb'
  const dbBucketSize = 10
  it('create countbase', async function () {
    const expectedTest = JSON.stringify({
      type: BaseType.CountBase,
      bucketSize: dbBucketSize,
      length: 0,
      partitions: {
        '': {
          length: 0
        }
      }
    })
    countbaseDb = await createCountBase(disklet, dbName, dbBucketSize)
    expect(await disklet.getText(`${dbName}/config.json`)).equals(expectedTest)
  })
  it('insert data', async function () {
    const partitionName = 'users'
    const index = 0
    const bucketNumber = Math.floor(index / dbBucketSize)
    const testData = { name: 'jerry', age: '2' }
    await countbaseDb.insert(partitionName, index, testData)
    const storedConfig = await disklet.getText(`${dbName}/config.json`)
    const storedData = await disklet.getText(
      `${dbName}/${partitionName}/${bucketNumber}.json`
    )
    expect(
      JSON.parse(storedConfig).partitions[`/${partitionName}`].length
    ).equals(1)
    expect(JSON.stringify(JSON.parse(storedData)[index])).equals(
      JSON.stringify(testData)
    )
  })
})
