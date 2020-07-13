// check that partition only contains letters, numbers, and underscores
// if partition was provided, prefix with a slash
export function checkAndformatPartition(partition: string = ''): string {
  if (typeof partition !== 'string') {
    throw new TypeError('partition must be of type string')
  }
  const fPartition = partition.trim()
  return fPartition === '' ? fPartition : `/${fPartition}`
}
