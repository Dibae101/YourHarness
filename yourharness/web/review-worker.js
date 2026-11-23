// Rasterize one fixed-format input. A packet contains geometry or a JPEG, never code.
self.onmessage = async ({ data: { input, kind, width, height } }) => {
  try {
    const canvas = new OffscreenCanvas(width, height);
    const ctx = canvas.getContext('2d', { alpha: false });
    ctx.fillStyle = '#182327';
    ctx.fillRect(0, 0, width, height);
    if (kind === 'images') {
      const bitmap = await createImageBitmap(new Blob([input], { type: 'image/jpeg' }));
      const scale = Math.min(width / bitmap.width, height / bitmap.height);
      const w = bitmap.width * scale,
        h = bitmap.height * scale;
      ctx.drawImage(bitmap, (width - w) / 2, (height - h) / 2, w, h);
      bitmap.close();
    } else {
      const buffer = new DataView(input);
      const img = ctx.getImageData(0, 0, width, height);
      const depth = new Float32Array(width * height).fill(-Infinity);
      for (let i = 0; i < input.byteLength; i += 40) {
        const x0 = buffer.getFloat32(i, true) * width,
          y0 = buffer.getFloat32(i + 4, true) * height;
        const x1 = buffer.getFloat32(i + 12, true) * width,
          y1 = buffer.getFloat32(i + 16, true) * height;
        const x2 = buffer.getFloat32(i + 24, true) * width,
          y2 = buffer.getFloat32(i + 28, true) * height;
        const z0 = buffer.getFloat32(i + 8, true),
          z1 = buffer.getFloat32(i + 20, true),
          z2 = buffer.getFloat32(i + 32, true);
        const det = (y1 - y2) * (x0 - x2) + (x2 - x1) * (y0 - y2);
        if (Math.abs(det) < 1e-8) continue;
        const left = Math.max(0, Math.floor(Math.min(x0, x1, x2))),
          right = Math.min(width - 1, Math.ceil(Math.max(x0, x1, x2)));
        const top = Math.max(0, Math.floor(Math.min(y0, y1, y2))),
          bottom = Math.min(height - 1, Math.ceil(Math.max(y0, y1, y2)));
        for (let y = top; y <= bottom; y++) {
          for (let x = left; x <= right; x++) {
            const a = ((y1 - y2) * (x + 0.5 - x2) + (x2 - x1) * (y + 0.5 - y2)) / det;
            const b = ((y2 - y0) * (x + 0.5 - x2) + (x0 - x2) * (y + 0.5 - y2)) / det;
            const c = 1 - a - b;
            if (Math.min(a, b, c) < 0) continue;
            const z = a * z0 + b * z1 + c * z2,
              p = y * width + x;
            if (z <= depth[p]) continue;
            depth[p] = z;
            img.data[p * 4] = buffer.getUint8(i + 36);
            img.data[p * 4 + 1] = buffer.getUint8(i + 37);
            img.data[p * 4 + 2] = buffer.getUint8(i + 38);
          }
        }
      }
      ctx.putImageData(img, 0, 0);
    }
    self.postMessage({ frame: await canvas.convertToBlob({ type: 'image/jpeg', quality: 0.9 }) });
  } catch (err) {
    self.postMessage({ error: err.message || 'This browser cannot render a frame.' });
  }
};
