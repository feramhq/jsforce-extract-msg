const jsforce = require('jsforce')
const { oneLine, oneLineTrim } = require('common-tags')
const fs = require('fs')
const fetch = require("node-fetch")
const msg2txt = require('msg2txt')
const path = require('path')
const pw = require('./pw')

const loginData = pw.loginData
const sandboxData = pw.sandboxData

const logNames = {
  missingVersionData: `missingVersionData.txt`,
  requestErrors: `requestErrors.txt`,
  convertErrors: 'convertErros.txt',
  fetchErrors: 'fetchErrors.txt',
}

function downloadDocument(connection, serverFilename, localFilename) {
  const headers = {
    'Authorization': 'Bearer ' + connection.accessToken,
    'Content-Type': 'application/octet-stream',
  }
  const options = {
    method: 'GET',
    headers: headers,
  }
  return new Promise((resolve, reject) => {
    fetch(connection.instanceUrl + serverFilename, options)
      .then(result => {
        if (!result.body) {
          throw new Error(oneLine`
            The request body of the document ${localFilename} was empty
          `)
        }
        result.body.pipe(fs.createWriteStream(localFilename))
          .on('close', () => resolve())
      })
      .catch(error => {
        fs.appendFileSync(
          logNames.fetchErrors,
          `${localFilename} - ${document.Title}: ${error}\n`
        )
        reject(error)
      })
  })
}

function uploadDocument(connection, localFilename) {
  const file = fs.readFileSync(localFilename)
  const record = {
    PathOnClient: localFilename,
    Title: path.basename(localFilename, path.extname(localFilename)),
    Description: '',
    VersionData: file.toString('base64'),
  }
  const headers = {
    'Authorization': 'Bearer ' + connection.accessToken,
    'Content-Type': 'application/json',
  }
  const options = {
    method: 'POST',
    headers: headers,
    body: JSON.stringify(record),
  }
  const uploadPath = `/services/data/v${connection.version}/sobjects/ContentVersion/`
  return fetch(connection.instanceUrl + uploadPath, options)
    .then(response => response.json())
}

function createDocumentLinkRecords(
  connection,
  currentUserId,
  msgFileId,
  newVersionRecordId,
) {
  return connection.queryAll(oneLine`
    SELECT LinkedEntityId, ShareType, Visibility
    FROM ContentDocumentLink
    WHERE ContentDocumentId = '${msgFileId}'
  `)
    .then( queryResult => {
      const linkRecords = queryResult.records
      return connection.queryAll(oneLine`
        SELECT ContentDocumentId
        FROM ContentVersion
        WHERE Id = '${newVersionRecordId}'
      `)
        .then(result => {
          if (
            !result ||
            !result.records ||
            !result.records.length ||
            !result.records[0].ContentDocumentId
          ) {
            throw new Error(
              `Could not query DocumentLinks of the uploaded file
                ${newVersionRecordId}`,
            )
          }
          const newDocumentId = result.records[0].ContentDocumentId
          uploadPath = oneLineTrim`
            /services/data/v${connection.version}/
            sobjects/ContentDocumentLink/
          `
          linkRecords
            .filter(linkRecord => {
              return currentUserId !== linkRecord.LinkedEntityId
            })
            .map(linkRecord => {
              const record = {
                ContentDocumentId: newDocumentId,
                LinkedEntityId: linkRecord.LinkedEntityId,
                ShareType: linkRecord.ShareType,
                Visibility: linkRecord.Visibility,
              }

              connection.create('ContentDocumentLink', record)
                .catch(createLinkError => {
                  throw createLinkError
                })
            })
        })
        .catch(getNewDocumentIdError => {
          throw getNewDocumentIdError
        })
    })
    .catch(getAllDocumentLinksError => {
      throw getAllDocumentLinksError
    })
}

function getMsgFiles (connection, state) {
  return connection.queryAll(oneLine`
    SELECT
      Id,
      Title,
      FileExtension,
      LatestPublishedVersion.VersionData,
      LatestPublishedVersion.Id
    FROM ContentDocument
    WHERE FileExtension = 'msg'
  `)
    .then(result => {
      if (
        !result ||
        !result.records ||
        !result.records.length
      ) {
        throw new Error(oneLine`
          Querying msg files returns a empty set of records.
        `)
      }
      state.msgRecords = result.records.filter(versionDataExists)
      return state
    })
}

function versionDataExists(record) {
  if (
    !record.LatestPublishedVersion ||
    !record.LatestPublishedVersion.VersionData
  ) {
    fs.appendFileSync(
      logNames.missingVersionData,
      `${record.Id}\t${record.Title}\n`,
    )
    return false
  }
  return true
}

function extractMsgFile(localFilename) {
  return new Promise((resolve, reject) => {
    try {
      msg2txt(localFilename)
      resolve()
    }
    catch (error) {
      fs.appendFileSync(
        logNames.convertErrors,
        `${localFilename}: ${error}\n`,
      )
    }
  })
}

function main(credentials) {

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  Object.entries(logNames).map(([key, value]) => {
    fs.unlink(value, () => {})
  })

  connection
    .login(credentials.user, credentials.pw + credentials.token)
    .then(async loginInformation => {
      const state = {userId: loginInformation.id}
      return state
    })
    .then(state => {
      return getMsgFiles(connection, state)
    })
    .then(state => {
      state.msgRecords.map(record => {
        const localFilename = `./files/${record.Id}.${record.FileExtension}`
        const serverFilename = record.LatestPublishedVersion.VersionData
        const extractionDir = `./files/${record.Id}`
        downloadDocument(connection, serverFilename, localFilename)
          .then(() => {
            return extractMsgFile(localFilename)
          })
          .then(() => {
            fs.readdirSync(extractionDir).map(filename => {
              const fileToUpload = path.join(extractionDir, filename)
              uploadDocument(connection, fileToUpload)
                .then(versionRecord => {
                  return createDocumentLinkRecords(
                    connection,
                    state.userId,
                    record.Id,
                    versionRecord.id,
                  )
                })
            })
          })
      })
    })
}

function justDonwloadAndConvert(credentials) {
  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  Object.entries(logNames).map(([key, value]) => {
    fs.unlink(value, () => {})
  })

  connection
    .login(credentials.user, credentials.pw + credentials.token)
    .then(async loginInformation => {
      const state = {userId: loginInformation.id}
      return state
    })
    .then(state => {
      return getMsgFiles(connection, state)
    })
    .then(state => {
      state.msgRecords.map(record => {
        const localFilename = `./files/${record.Id}.${record.FileExtension}`
        const serverFilename = record.LatestPublishedVersion.VersionData
        downloadDocument(connection, serverFilename, localFilename)
          .then(() => extractMsgFile(localFilename))
      })
    })
}


justDonwloadAndConvert(loginData)
// main(sandboxData)
