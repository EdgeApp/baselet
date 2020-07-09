export enum BaseType {
  COUNT_BASE,
  HASH_BASE,
  RANGE_BASE
}

export interface BaseletConfig {
  type: BaseType
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}
