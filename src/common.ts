import { Disklet } from 'disklet'

import { CountBase, createCountBase, openCountBase } from './CountBase'
import { createHashBase, HashBase, openHashBase } from './HashBase'
import { createRangeBase, openRangeBase, RangeBase } from './RangeBase'
import { BaseType } from './types'

export { createCountBase, createHashBase, createRangeBase }

export function openBase(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase | HashBase | RangeBase> {
  return disklet
    .getText(`${databaseName}/config.json`)
    .then(serializedConfig => JSON.parse(serializedConfig))
    .then(configData => {
      let baselet: CountBase | HashBase | RangeBase
      switch (configData.type) {
        case BaseType.CountBase:
          baselet = openCountBase(disklet, databaseName)
          break
        case BaseType.HashBase:
          baselet = openHashBase(disklet, databaseName)
          break
        case BaseType.RangeBase:
          baselet = openRangeBase(disklet, databaseName)
          break
        default:
          throw new Error('Database is of an unknown type')
      }
      return baselet
    })
}
