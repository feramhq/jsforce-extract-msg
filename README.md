# Usage
Add a file in the base directory named pw.js with the following:
```javascript
const loginData = {
  url: 'https://login.salesforce.com',
  pw: '<password>',
  token: '<token>',
  user: '<user>',
}

const sandboxData = {
  url: 'https://test.salesforce.com',
  pw: '<password>',
  token: '<token>',
  user: '<user>',
}

module.exports = {
  loginData: loginData,
  sandboxData: sandboxData,
}
```

From there, start the script with `node index.js`
