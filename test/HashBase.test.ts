import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { createHashBase, HashBase } from '../src/HashBase'
import { BaseType } from '../src/types'

describe('HashBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let hashbaseDb: HashBase
  const dbName = 'testdb'
  const prefixSize = 2
  it('create hashbase', async function () {
    const expectedTest = JSON.stringify({
      type: BaseType.HashBase,
      prefixSize
    })
    hashbaseDb = await createHashBase(disklet, dbName, prefixSize)
    expect(await disklet.getText(`${dbName}/config.json`)).equals(expectedTest)
  })
  it('insert data', async function () {
    const partitionName = 'users'
    const hash = 'abcd-efgh-ijkl-mnop'
    const prefix = hash.substring(0, 2)
    const testData = { id: hash, name: 'jerry', age: '2' }
    await hashbaseDb.insert(partitionName, hash, testData)
    const storedData = await disklet.getText(
      `${dbName}/${partitionName}/${prefix}.json`
    )
    expect(JSON.stringify(JSON.parse(storedData)[hash])).equals(
      JSON.stringify(testData)
    )
  })
})
