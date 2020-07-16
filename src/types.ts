export enum BaseType {
  CountBase = 'COUNT_BASE',
  HashBase = 'HASH_BASE',
  RangeBase = 'RANGE_BASE'
}

export interface BaseletConfig {
  type: BaseType
}
