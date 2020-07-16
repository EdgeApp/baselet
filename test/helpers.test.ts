import { expect } from 'chai'

import { checkAndformatPartition } from '../src/helpers'

describe('checkAndformatPartition', function () {
  it('prefix partition with slash', function () {
    const partition = 'transactions'
    expect(checkAndformatPartition(partition)).equals(`/${partition}`)
  })
  it('fail if argument is not string', function () {
    const partition = 5
    expect(
      checkAndformatPartition.bind(this, (partition as unknown) as string)
    ).to.throw(TypeError)
  })
  it('fail if partition contains invalid characters', function () {
    const partition = '/transactions'
    expect(checkAndformatPartition.bind(this, partition)).to.throw(Error)
  })
})
