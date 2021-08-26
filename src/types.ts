import { RangeBase } from './RangeBase'

export enum BaseType {
  CountBase = 'COUNT_BASE',
  HashBase = 'HASH_BASE',
  RangeBase = 'RANGE_BASE'
}

/**
 * Option types are parameter types for create functions.
 */
export interface HashBaseOptions {
  name: string
  prefixSize: number
}
export interface CountBaseOptions {
  name: string
  bucketSize: number
}
export interface RangeBaseOptions<RangeKey, IdKey> {
  name: string
  bucketSize: number
  rangeKey: RangeKey
  idKey: IdKey
  idPrefixLength?: number
}

/**
 * Config types describe the baselet configuration and are saved to the disk.
 * This allows for baselet type checking and configuration persistence
 * at runtime.
 */
export interface CountBaseConfig {
  type: BaseType.CountBase
  name: string
  bucketSize: number
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}
export interface HashBaseConfig {
  type: BaseType.HashBase
  name: string
  prefixSize: number
}
export interface RangeBaseConfig<
  B extends RangeBase<any, string, string> = RangeBase<any, string, string>
> {
  type: BaseType.RangeBase
  name: string
  bucketSize: number
  rangeKey: B['rangeKey']
  idKey: B['idKey']
  idPrefixLength: number
  limits: PartitionLimits
  sizes: { [partition: string]: number }
}
interface PartitionLimits {
  [partition: string]: undefined | PartitionLimit
}
interface PartitionLimit {
  minRange?: number
  maxRange?: number
}
export type AnyBaseletConfig =
  | CountBaseConfig
  | HashBaseConfig
  | RangeBaseConfig

/**
 * The data format for baselet dump methods.
 */
export interface DataDump<C extends AnyBaseletConfig, D> {
  config: C
  data: D
}
