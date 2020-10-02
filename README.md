# SQLTools Exasol Driver

This extension for Visual Studio Code's [SQLTools](https://vscode-sqltools.mteixeira.dev/) allows you to connect to an Exasol database.

## Installing

Currently, you need to download the release from github and install the `.vsix` file manually. We will start publishing into the VSCode's extension marketplace if there is demand for it.


## Package the driver

```
npm install
vsce package
```

Output is a `.vsix` file that can be installed in VS code.

## Known limitations

- This driver only sends one query at a time, including queries used for the auto-complete feature.
- Queries cannot be canceled once started. There is however a configurable query timeout in the connection settings.
- There is no auto-complete support for schemas.
- The support for sending multiple queries at once (separated by `;`) is experimental.

## References
- Creating a new SQLTools driver: https://vscode-sqltools.mteixeira.dev/contributing/support-new-drivers
- Exasol WebSocket API and Javascript implementation: https://github.com/exasol/websocket-api
  - Details of the protocol: https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties

## License

This project is released under MIT license.
