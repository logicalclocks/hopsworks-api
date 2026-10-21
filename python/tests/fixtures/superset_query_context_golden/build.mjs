// Bundles entry.ts with the Superset frontend at SUPERSET_FRONTEND into golden.cjs.
import * as esbuild from 'esbuild';
import path from 'path';

const here = path.dirname(new URL(import.meta.url).pathname);
const frontend = process.env.SUPERSET_FRONTEND;
const local = (f) => ({ path: path.join(here, f) });
await esbuild.build({
  entryPoints: [path.join(here, 'entry.ts')], bundle: true, platform: 'node', format: 'cjs',
  banner: { js: 'globalThis.window = globalThis;' },
  // Superset's tsconfig.json would switch JSX to @emotion/react, which the query code never renders.
  tsconfigRaw: '{}',
  outfile: path.join(here, 'golden.cjs'), logLevel: 'error', nodePaths: [path.join(here, 'node_modules')],
  plugins: [{
    name: 'alias',
    setup(b) {
      b.onResolve({ filter: /^superset-frontend\// }, (a) =>
        b.resolve('./' + a.path.slice('superset-frontend/'.length), { resolveDir: frontend, kind: a.kind }));
      b.onResolve({ filter: /^@superset-ui\/core$/ }, () => local('core-shim.ts'));
      b.onResolve({ filter: /^@superset-ui\/chart-controls$/ }, () => local('cc-shim.ts'));
      b.onResolve({ filter: /externalAPIs$/ }, () => local('stub.ts'));
      b.onResolve({ filter: /^(nanoid|react|react-dom|xss|seedrandom|\.\.\/translation)$/ }, () => local('libstub.cjs'));
    },
  }],
});
