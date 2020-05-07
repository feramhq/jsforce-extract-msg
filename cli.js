const { stripIndent } = require('common-tags')
const updateFiles = require('./index')
const pw = require('./pw')

function usage() {
  console.info(stripIndent`
    usage:
    node cli.js limit
    node cli.js nolimit
  `)
}

const main = updateFiles.main
// const main = updateFiles.undoMyWork
const credentials = pw.sandboxData

if (process.argv.length === 3) {
  const arg = process.argv[2]
  if (arg === 'limit') {
    main(credentials, true)
  }
  else if (arg === 'nolimit') {
    main(credentials, false)
  }
  else {
    usage()
  }
}
else {
  usage()
}
