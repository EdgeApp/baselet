export interface HashBase {
  insert(partition: string, hash: string, data: any): Promise<unknown>
  query(partition: string, hashes: string[]): Promise<any[]>
}
