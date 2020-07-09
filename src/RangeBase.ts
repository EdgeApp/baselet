export interface RangeBase {
  insert(partition: string, data: any): Promise<unknown>
  query(partition: string, rangeStart: number, rangeEnd: number): Promise<any[]>
  queryByte(partition: string, range: number, id: string): Promise<any[]>
}
