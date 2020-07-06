export interface CountBase {
  insert(partition: string, index: number, data: any): Promise<unknown>
  query(partition: string, range: number): Promise<any>
  length(): number
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

export const BaseTypes = {
  COUNT_BASE: 'count_base',
  HASH_BASE: 'hash_base',
  RANGE_BASE: 'range_base'
}
