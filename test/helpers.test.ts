import { expect } from 'chai'

import { checkAndFormatPartition } from '../src/helpers'

describe('checkAndformatPartition', function () {
  it('prefix partition with slash', function () {
    const partition = 'transactions'
    expect(checkAndFormatPartition(partition)).eql(partition)
  })
  it('fail if argument is not string', function () {
    const partition = 5
    expect(
      checkAndFormatPartition.bind(this, partition as unknown as string)
    ).to.throw(TypeError)
  })
  it('fail if partition contains invalid characters', function () {
    const partition = '/transactions'
    expect(checkAndFormatPartition.bind(this, partition)).to.throw(Error)
  })
})
