import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { CountBase, createCountBase } from '../src/CountBase'
import { BaseType } from '../src/types'

describe('CountBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let countbaseDb: CountBase
  const dbName = 'testCountdb'
  const dbBucketSize = 10
  const partitionName = 'users'
  const dataSet = [
    { name: 'jerry', age: '2', index: 0 },
    { name: 'max', age: '12', index: 1 },
    { name: 'ana', age: '26', index: 2 },
    { name: 'lucy', age: '17', index: 3 },
    { name: 'bobby', age: '9', index: 4 }
  ]

  it('create countbase', async function () {
    const expectedTest = JSON.stringify({
      type: BaseType.CountBase,
      bucketSize: dbBucketSize,
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
    for (let i = 0; i < dataSet.length; i++) {
      const data = dataSet[i]
      await countbaseDb.insert(partitionName, data.index, data)
    }

    const storedConfig = await disklet.getText(`${dbName}/config.json`)
    const bucketNumber = Math.floor(dataSet[0].index / dbBucketSize)
    const storedData = await disklet.getText(
      `${dbName}/${partitionName}/${bucketNumber}.json`
    )
    expect(
      JSON.parse(storedConfig).partitions[`/${partitionName}`].length
    ).equals(dataSet.length)
    expect(JSON.stringify(JSON.parse(storedData)[dataSet[0].index])).equals(
      JSON.stringify(dataSet[0])
    )
  })
  it('query data', async function () {
    const queriedData1 = await countbaseDb.query(
      partitionName,
      0,
      dataSet.length - 1
    )
    const queriedData2 = await countbaseDb.query(partitionName, 3)
    const queriedData3 = await countbaseDb.query(partitionName, 3, 4)
    expect(JSON.stringify(queriedData1)).equals(JSON.stringify(dataSet))
    expect(JSON.stringify(queriedData2)).equals(
      JSON.stringify(dataSet.slice(3, 4))
    )
    expect(JSON.stringify(queriedData3)).equals(
      JSON.stringify(dataSet.slice(3, 5))
    )
  })
  it('get length of partition', async function () {
    const partitionLength = await countbaseDb.length(partitionName)
    expect(partitionLength).equals(dataSet.length)
  })
})
