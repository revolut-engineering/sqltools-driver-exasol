# SQLTools exasol-driver Driver

References:

- Creating a new SQLTools driver: https://vscode-sqltools.mteixeira.dev/contributing/support-new-drivers
- Exasol WebSocket API and Javascript implementation: https://github.com/exasol/websocket-api
  - Details of the protocol: https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties

## Package the driver

```
npm install
vsce package
```

Output is a `.vsix` file that can be installed in VS code.
