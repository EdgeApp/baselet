export enum BaseType {
  CountBase = 'COUNT_BASE',
  HashBase = 'HASH_BASE',
  RangeBase = 'RANGE_BASE'
}

export interface BaseletConfig {
  type: BaseType
}

export interface DataDump<C extends BaseletConfig, D> {
  config: C
  data: D
}
