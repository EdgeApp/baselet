import { RangeBase } from './RangeBase'

export enum BaseType {
  CountBase = 'COUNT_BASE',
  HashBase = 'HASH_BASE',
  RangeBase = 'RANGE_BASE'
}

/**
 * Config types describe the baselet configuration and are saved to the disk.
 * This allows for baselet type checking and configuration persistence
 * at runtime.
 */
export interface CountBaseConfig {
  type: BaseType.CountBase
  bucketSize: number
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}
export interface HashBaseConfig {
  type: BaseType.HashBase
  prefixSize: number
}
export interface RangeBaseConfig<
  B extends RangeBase<any, string, string> = RangeBase<any, string, string>
> {
  type: BaseType.RangeBase
  bucketSize: number
  rangeKey: B['rangeKey']
  idKey: B['idKey']
  idPrefixLength: number
  limits: PartitionLimits
  sizes: { [partition: string]: number }
}
export type BaseletConfig = CountBaseConfig | HashBaseConfig | RangeBaseConfig

interface PartitionLimits {
  [partition: string]: undefined | PartitionLimit
}
interface PartitionLimit {
  minRange?: number
  maxRange?: number
}

/**
 * The data format for baselet dump methods.
 */
export interface DataDump<C extends BaseletConfig, D> {
  config: C
  data: D
}
