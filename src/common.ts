import { Disklet } from 'disklet'

import { CountBase, openCountBase } from './CountBase'
import { HashBase, openHashBase } from './HashBase'
import { openRangeBase, RangeBase } from './RangeBase'
import { BaseletConfig, BaseType } from './types'

export async function openBase<
  K extends any,
  RangeKey extends string,
  IdKey extends string
>(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase<K> | HashBase<K> | RangeBase<RangeKey, IdKey, K>> {
  const config: BaseletConfig = await disklet
    .getText(`${databaseName}/config.json`)
    .then(serializedConfig => JSON.parse(serializedConfig))
  switch (config.type) {
    case BaseType.CountBase:
      return openCountBase(disklet, databaseName)
    case BaseType.HashBase:
      return openHashBase(disklet, databaseName)
    case BaseType.RangeBase:
      return openRangeBase<RangeKey, IdKey, K>(disklet, databaseName)
    default:
      throw new Error(`Unknown base type: ${config.type}`)
  }
}
