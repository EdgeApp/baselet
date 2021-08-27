import { Disklet } from 'disklet'

import { CountBase, openCountBase } from './CountBase'
import { HashBase, openHashBase } from './HashBase'
import { openRangeBase, RangeBase, RangeData } from './RangeBase'
import { BaseletConfig, BaseType } from './types'

export async function openBase<K>(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase<K> | HashBase<K>>
export async function openBase<
  K extends RangeData = any,
  RangeKey extends string = 'rangeKey',
  IdKey extends string = 'idKey'
>(
  disklet: Disklet,
  databaseName: string
): Promise<RangeBase<K, RangeKey, IdKey>>
export async function openBase<
  K extends RangeData = any,
  RangeKey extends string = 'rangeKey',
  IdKey extends string = 'idKey'
>(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase<K> | HashBase<K> | RangeBase<K, RangeKey, IdKey>> {
  const config: BaseletConfig = await disklet
    .getText(`${databaseName}/config.json`)
    .then(serializedConfig => JSON.parse(serializedConfig))
  switch (config.type) {
    case BaseType.CountBase:
      return openCountBase<K>(disklet, databaseName)
    case BaseType.HashBase:
      return openHashBase<K>(disklet, databaseName)
    case BaseType.RangeBase:
      return openRangeBase<K, RangeKey, IdKey>(disklet, databaseName)
    default:
      throw new Error(`Unknown base type: ${config.type}`)
  }
}
