import { Disklet } from 'disklet'

import { CountBase, openCountBase } from './CountBase'
import { HashBase, openHashBase } from './HashBase'
import { openRangeBase, RangeBase, RangeRecord } from './RangeBase'
import { AnyBaseletConfig, BaseType } from './types'

export async function openBase<K>(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase<K> | HashBase<K>>
export async function openBase<
  K extends RangeRecord<any, RangeKey, IdKey>,
  RangeKey extends string,
  IdKey extends string
>(
  disklet: Disklet,
  databaseName: string
): Promise<RangeBase<K, RangeKey, IdKey>>
export async function openBase<
  K extends RangeRecord<any, RangeKey, IdKey>,
  RangeKey extends string,
  IdKey extends string
>(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase<K> | HashBase<K> | RangeBase<K, RangeKey, IdKey>> {
  const config: AnyBaseletConfig = await disklet
    .getText(`${databaseName}/config.json`)
    .then(serializedConfig => JSON.parse(serializedConfig))
  switch (config.type) {
    case BaseType.CountBase:
      return openCountBase(disklet, databaseName)
    case BaseType.HashBase:
      return openHashBase(disklet, databaseName)
    case BaseType.RangeBase:
      return openRangeBase(disklet, databaseName)
    default: {
      const type: string = (config as any)?.type
      throw new Error(`Unknown base type: ${type}`)
    }
  }
}
