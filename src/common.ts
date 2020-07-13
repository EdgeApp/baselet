import { Disklet } from 'disklet'

import { CountBase, openCountBase } from './CountBase'
import { HashBase, openHashBase } from './HashBase'
import { BaseType } from './types'

export function openBase(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase | HashBase> {
  // check that database exists
  return disklet
    .getText(`${databaseName}/config.json`)
    .then(serializedConfig => JSON.parse(serializedConfig))
    .then(configData => {
      let baselet: Promise<CountBase | HashBase>
      switch (configData.type) {
        case BaseType.CountBase:
          baselet = openCountBase(disklet, databaseName)
          break
        case BaseType.HashBase:
          baselet = openHashBase(disklet, databaseName)
          break
        default:
          throw new Error('Database is of an unknown type')
      }
      return baselet
    })
}
