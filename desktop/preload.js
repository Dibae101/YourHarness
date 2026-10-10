const { contextBridge, ipcRenderer } = require('electron');
contextBridge.exposeInMainWorld('yh', {
  pair: (info) => ipcRenderer.invoke('pair', info),
  servers: () => ipcRenderer.invoke('servers'),
  startOwn: () => ipcRenderer.invoke('start-own'),
});
