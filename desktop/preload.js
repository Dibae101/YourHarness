const { contextBridge, ipcRenderer } = require('electron');
// Only the app's own pairing page gets the bridge. Pool pages come from the network
// over plain HTTP, and a script there must never be able to move this computer to
// another pool (which would hand that pool a shell here).
if (location.protocol === 'file:') {
  contextBridge.exposeInMainWorld('yh', {
    pair: (info) => ipcRenderer.invoke('pair', info),
    servers: () => ipcRenderer.invoke('servers'),
    startOwn: () => ipcRenderer.invoke('start-own'),
  });
}
