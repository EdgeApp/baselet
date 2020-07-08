export enum BaseType {
  COUNT_BASE,
  HASH_BASE,
  RANGE_BASE
}

export interface HashBase {
  insert(partition: string, hash: string, data: any): Promise<unknown>
  query(partition: string, hash: string): Promise<any>
}

export interface RangeBase {
  insert(partition: string, hash: string, data: any): Promise<unknown>
  query(partition: string, rangeStart: number, rangeEnd: number): Promise<any>
  queryByte(partition: string, range: number, id: string): Promise<any>
}

export interface BaseletConfig {
  type: BaseType
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}
