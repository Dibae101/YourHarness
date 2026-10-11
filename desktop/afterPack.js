// Ad-hoc sign the Mac app, so macOS doesn't refuse it as damaged.
//
// Electron's prebuilt binaries arrive ad-hoc signed. electron-builder then renames the
// bundle, writes the Info.plist keys from package.json and copies the Python engine into
// Resources -- which invalidates that signature. An invalid signature is worse than no
// signature: on Apple silicon the kernel refuses to run the executable at all, and macOS
// reports "YourHarness is damaged and can't be opened", which reads to everyone like a
// corrupt download rather than a missing certificate.
//
// Signing ad-hoc (the "-" identity) needs no Apple certificate and costs nothing. It does
// NOT make the app trusted -- the first launch still needs right-click -> Open, because
// that takes a paid Developer ID and notarisation. It only stops the app looking broken.
//
// This runs after the .app is packed and before the .dmg is built, so the signature is
// what ends up inside the disk image.

const { execFileSync } = require('node:child_process');
const path = require('node:path');

exports.default = async function afterPack(context) {
  if (context.electronPlatformName !== 'darwin') return;

  const app = path.join(context.appOutDir, `${context.packager.appInfo.productFilename}.app`);

  // --deep reaches the nested Electron helpers and the bundled Python engine. Apple
  // discourages it for notarised builds, which this is not; leaving those inner binaries
  // unsigned would invalidate the outer bundle all over again.
  execFileSync('codesign', ['--force', '--deep', '--sign', '-', app], { stdio: 'inherit' });

  // Fail the build rather than ship another "damaged" disk image.
  execFileSync('codesign', ['--verify', '--deep', '--strict', app], { stdio: 'inherit' });

  console.log(`afterPack: ad-hoc signed ${path.basename(app)}`);
};
