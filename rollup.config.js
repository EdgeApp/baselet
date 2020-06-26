import babel from 'rollup-plugin-babel'
import filesize from 'rollup-plugin-filesize'
import flowEntry from 'rollup-plugin-flow-entry'
import resolve from 'rollup-plugin-node-resolve'

import packageJson from './package.json'

const extensions = ['.ts']
const babelOpts = {
  babelrc: false,
  extensions,
  include: ['src/**/*'],
  presets: [
    [
      '@babel/preset-env',
      {
        exclude: ['transform-regenerator'],
        loose: true
      }
    ],
    '@babel/preset-typescript'
  ],
  plugins: [
    ['@babel/plugin-transform-for-of', { assumeArray: true }],
    '@babel/plugin-transform-object-assign'
  ]
}
const resolveOpts = { extensions }
const flowOpts = { types: 'src/index.flow.js' }

const external = [
  'fs',
  'path',
  'react-native',
  ...Object.keys(packageJson.dependencies),
  ...Object.keys(packageJson.devDependencies)
]

export default [
  // Normal build:
  {
    external,
    input: 'src/index.ts',
    output: [
      { file: packageJson.main, format: 'cjs', sourcemap: true },
      { file: packageJson.module, format: 'es', sourcemap: true }
    ],
    plugins: [
      resolve(resolveOpts),
      babel(babelOpts),
      flowEntry(flowOpts),
      filesize()
    ]
  },
  // Browser build:
  {
    external,
    input: 'src/browser.ts',
    output: [{ file: packageJson.browser, format: 'cjs', sourcemap: true }],
    plugins: [resolve(resolveOpts), babel(babelOpts), filesize()]
  }
]
