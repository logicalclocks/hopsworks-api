// Hops Run scene, PAPER style. The track is a 3D curve with a moving frame (forward, up, right),
// generated ahead of the hops from random segments: straights, banked turns, hills, corkscrews and
// loops, so no two runs are the same. Everything lives in track coordinates (s along the track,
// x across, h above the surface) and is placed in the world through the frame at s: the hops,
// obstacles, speed gates, debris and the chase camera, which rolls with the track, upside down
// included. Gravity is magnetic, towards the track; over a crest at speed the hops lifts off.
// After a crash the player can put their name and distance on the leaderboard.

import * as THREE from 'three';

const P = { bg: 0xf1efea, ink: 0x151513, dim: 0x8a867d };
const PAPER = { top: 0xfcfbf8, side: 0xeae7e0, end: 0xd9d5cc };
const GREEN = { top: 0x1eb182, side: 0x168a65, end: 0x10664b };
const RUST = { top: 0xff6a3d, side: 0xc4502d, end: 0x933b20 };
const MARK = 0x1cb182; // the Hopsworks mark's own green

const LANES = ['left', 'centre', 'right'];
const LANE_X = 2.6;
const TRACK_W = LANE_X * 3 + 1.2;
const HX = TRACK_W / 2;
const TRACK_T = 0.7; // slab thickness
const AHEAD = 360, BEHIND = 60;
const SPEED = { start: 45, max: 160, gain: 1.6 }; // m/s, m/s per s
const BOOST = { kick: 45, decay: 18 }; // m/s added by a gate, m/s lost per s
const GAP = { min: 42, max: 74, floor: 0.55, over: 5000 }; // metres between rows, shrinking to floor x over `over` m
const PAD_GAP = { min: 140, max: 260 };
const STEP = 1, CHUNK = 64, RIB = 4, ARCH = 160; // metres per track sample, per mesh chunk, between ribs, between arches
const ALTITUDE = 15; // metres the generator steers the track back towards
const UNLOCK = { side: 300, wallride: 400, cork: 500, invert: 700, loop: 900 }; // metres before each segment can appear
// Segments where the frame is meant to lean: the upright correction stays off through them.
const LEANING = new Set(['turn', 'side', 'cork', 'loop', 'roll', 'held']);
const HOVER = 1.3, GRAVITY = 45;
const SPRING = { k: 260, c: 26 }; // lateral spring stiffness and damping
const JUMP = 16, DUCK = { hover: 0.45, time: 0.7, flat: 0.35, narrow: 0.15 }; // m/s up; hover height, seconds, and how much the hops squeezes when ducking
// Jump charge: fills while flying and with every cleared row; a jump spends all of it, up to
// (1 + power) times the base jump.
const CHARGE = { perSecond: 1 / 25, perRow: 0.08, power: 1.6 };
// The hops as an ellipsoid for collisions: radii across, up and along, centred where its body is.
const HULL = { x: 1.0, y: 1.0, z: 1.5 };
// Obstacle kinds: a wall is dodged sideways, a low block jumped, a bar ducked under or jumped.
const KIND = { wall: { p: 0.5, h: [4.6, 5.6] }, low: { p: 0.25, h: [0.9, 1.1] }, bar: { p: 0.25, bottom: 1.7, t: 0.6 } };

// --- renderer, camera ---------------------------------------------------------------------------
const canvas = document.getElementById('scene');
const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
renderer.setClearColor(P.bg);
const scene = new THREE.Scene();
scene.fog = new THREE.Fog(P.bg, 180, 350);

// Chase camera, WipEout style: behind and above the hops in the track's frame, looking down the
// track, its up following the track's up. The field of view widens with speed and kicks on a
// boost: the sense of speed, and more track in view when there is less time to read it.
const camera = new THREE.PerspectiveCamera(60, 1, 0.1, 800);
const CHASE = { back: 12, up: 3.6, ahead: 30, lag: 6 };
const FOV = { base: 58, perSpeed: 0.11, boost: 0.25, max: 92 };
function resize() {
  camera.aspect = innerWidth / innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(innerWidth, innerHeight, false);
}
addEventListener('resize', resize);
resize();

scene.add(new THREE.HemisphereLight(0xffffff, 0x8a867d, 2.4));
const sun = new THREE.DirectionalLight(0xffffff, 1.4);
sun.position.set(-3, 10, 4);
scene.add(sun);

// --- randomness ----------------------------------------------------------------------------------
// The run (track and obstacles) draws from a generator seeded fresh each run; visual noise
// (particles, grain, shake) draws from Math.random.
let seed = 1;
const rng = () => { seed = (seed + 0x6d2b79f5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const between = (a, b) => a + rng() * (b - a);
const fx = Math.random;

// --- the track: a curve sampled every STEP metres, with its frame ---------------------------------
// Each segment gives yaw, pitch and roll rates (rad per metre) over its length. `ease` turns by a
// total angle and ends straight; `wave` swings out to a peak angle and back to zero.
const ease = (total, len) => (u) => (total / len) * (1 - Math.cos(2 * Math.PI * u));
const wave = (peak, len) => (u) => ((peak * Math.PI) / len) * Math.sin(2 * Math.PI * u);
const zero = () => 0;
const track = { P: [], Q: [], kind: [], pitch: [] };
const gen = { p: new THREE.Vector3(), q: new THREE.Quaternion(), seg: null, at: 0, minY: 0, queue: [] };
const V = (x, y, z) => new THREE.Vector3(x, y, z);
const AX = { fwd: V(0, 0, -1), up: V(0, 1, 0), right: V(1, 0, 0) };
const tv = new THREE.Vector3(), tq = new THREE.Quaternion(), te = new THREE.Euler(0, 0, 0, 'YXZ');

function pickSegment() {
  const s = track.P.length * STEP;
  const fwd = tv.copy(AX.fwd).applyQuaternion(gen.q);
  const heading = Math.atan2(-fwd.x, -fwd.z); // 0 when the track runs down -z
  const kinds = [['straight', 3], ['turn', 4], ['hill', 4]];
  if (s > UNLOCK.side) kinds.push(['side', 2]);
  if (s > UNLOCK.wallride) kinds.push(['wallride', 1.2]);
  if (s > UNLOCK.cork) kinds.push(['cork', 1.2]);
  if (s > UNLOCK.invert) kinds.push(['invert', 1]);
  if (s > UNLOCK.loop) kinds.push(['loop', 1]);
  let roll = rng() * kinds.reduce((a, [, w]) => a + w, 0), kind = 'straight';
  for (const [k, w] of kinds) { if ((roll -= w) <= 0) { kind = k; break; } }
  const side = () => (rng() < 0.5 ? 1 : -1);
  if (kind === 'turn') {
    // Turn back towards -z once the heading has wandered; bank into the turn.
    const dir = Math.abs(heading) > 0.7 ? -Math.sign(heading) : side(), len = between(110, 200);
    return { kind, len, yaw: ease(dir * between(0.5, 1.3), len), pitch: zero, roll: wave(dir * between(0.3, 0.65), len) };
  }
  if (kind === 'side') {
    // A hard turn banked onto its side, roller coaster style: near vertical mid-corner.
    const dir = Math.abs(heading) > 0.7 ? -Math.sign(heading) : side(), len = between(200, 300);
    return { kind, len, yaw: ease(dir * between(0.9, 1.6), len), pitch: zero, roll: wave(dir * between(1.3, 1.57), len) };
  }
  if (kind === 'hill') {
    // Climb or dive, steering the altitude back into a band.
    const dir = gen.p.y < -5 ? 1 : gen.p.y > 35 ? -1 : side(), len = between(90, 170);
    return { kind, len, yaw: zero, pitch: wave(dir * between(0.22, 0.45), len), roll: zero };
  }
  if (kind === 'wallride' || kind === 'invert') {
    // Roll onto the side or upside down, stay there a while, roll back.
    const angle = kind === 'invert' ? Math.PI * side() : (Math.PI / 2) * side(), turn = between(120, 170);
    return [
      { kind: 'roll', len: turn, yaw: zero, pitch: zero, roll: ease(angle, turn) },
      { kind: 'held', len: between(200, 400), yaw: zero, pitch: zero, roll: zero },
      { kind: 'roll', len: turn, yaw: zero, pitch: zero, roll: ease(-angle, turn) },
    ];
  }
  if (kind === 'cork') { const len = between(400, 560); return { kind, len, yaw: zero, pitch: zero, roll: ease(side() * Math.PI * 2, len) }; }
  // Loops are 400 to 500 m round (radius 64 to 80 m), so the chase camera sees round them.
  if (kind === 'loop') { const len = between(400, 500); return { kind, len, yaw: ease(side() * between(0.3, 0.5), len), pitch: ease(Math.PI * 2, len), roll: zero }; }
  return { kind, len: between(50, 140), yaw: zero, pitch: zero, roll: zero };
}

function stepTrack() {
  if (!gen.seg || gen.at >= gen.seg.len) {
    if (!gen.queue.length) gen.queue.push(...[].concat(track.P.length < 220 ? { kind: 'straight', len: 220, yaw: zero, pitch: zero, roll: zero } : pickSegment()));
    gen.seg = gen.queue.shift(); gen.at = 0;
  }
  const { seg } = gen, u = (gen.at + STEP / 2) / seg.len;
  let yaw = seg.yaw(u) * STEP, pitch = seg.pitch(u) * STEP, roll = seg.roll(u) * STEP;
  // Outside corkscrews and loops, ease the frame back upright and level, so turns and hills
  // never accumulate a lean.
  if (seg.kind !== 'cork' && seg.kind !== 'loop') {
    const right = tv.copy(AX.right).applyQuaternion(gen.q);
    if (!LEANING.has(seg.kind)) roll -= right.y * 0.03 * STEP;
    // Hold the nose towards a slope that brings the altitude back to the band around ALTITUDE,
    // through the local up: no effect on a side, reversed upside down.
    const upY = tv.copy(AX.up).applyQuaternion(gen.q).y;
    const want = THREE.MathUtils.clamp(-(gen.p.y - ALTITUDE) * 0.004, -0.12, 0.12);
    if (seg.kind !== 'hill') pitch -= (tv.copy(AX.fwd).applyQuaternion(gen.q).y - want) * upY * 0.02 * STEP;
  }
  // Yaw turns about the world vertical, so a corner banked onto its side still turns on the level;
  // pitch and roll are about the track's own axes.
  gen.q.premultiply(tq.setFromAxisAngle(AX.up, yaw)).multiply(tq.setFromEuler(te.set(pitch, 0, roll, 'YXZ'))).normalize();
  gen.p.addScaledVector(tv.copy(AX.fwd).applyQuaternion(gen.q), STEP);
  gen.minY = Math.min(gen.minY, gen.p.y);
  track.P.push(gen.p.clone()); track.Q.push(gen.q.clone()); track.kind.push(seg.kind); track.pitch.push(seg.pitch(u));
  gen.at += STEP;
}
function extend(s) { while (track.P.length * STEP < s + 2) stepTrack(); }

// Frame at s: position, orientation and its axes, interpolated between samples.
const F = { p: new THREE.Vector3(), q: new THREE.Quaternion(), fwd: new THREE.Vector3(), up: new THREE.Vector3(), right: new THREE.Vector3() };
function frameAt(s, out = F) {
  s = Math.max(0, s);
  extend(s);
  const i = Math.floor(s / STEP), f = s / STEP - i;
  out.p.lerpVectors(track.P[i], track.P[i + 1], f);
  out.q.slerpQuaternions(track.Q[i], track.Q[i + 1], f);
  out.fwd.copy(AX.fwd).applyQuaternion(out.q); out.up.copy(AX.up).applyQuaternion(out.q); out.right.copy(AX.right).applyQuaternion(out.q);
  return out;
}
const pitchAt = (s) => { extend(s); return track.pitch[Math.floor(Math.max(0, s) / STEP)]; };
const kindAt = (s) => { extend(s); return track.kind[Math.floor(Math.max(0, s) / STEP)]; };
// World point at track coordinates.
const at = (s, x, h, out = new THREE.Vector3()) => { const f = frameAt(s); return out.copy(f.p).addScaledVector(f.right, x).addScaledVector(f.up, h); };
function place(obj, s, x, h) { const f = frameAt(s); obj.position.copy(f.p).addScaledVector(f.right, x).addScaledVector(f.up, h); obj.quaternion.copy(f.q); return obj; }

// --- materials -----------------------------------------------------------------------------------
// BoxGeometry groups: +x, -x, +y, -y, +z, -z. The chase camera sees top (+y), the sides and +z.
const faces = (p) => [p.end, p.end, p.top, p.top, p.side, p.side].map((color) => new THREE.MeshBasicMaterial({ color }));
const inkMat = new THREE.LineBasicMaterial({ color: P.ink, transparent: true, opacity: 0.85 });
const ribMat = new THREE.LineBasicMaterial({ color: P.ink, transparent: true, opacity: 0.18 });
const unitBox = new THREE.BoxGeometry(1, 1, 1);
const unitEdges = new THREE.EdgesGeometry(unitBox);
const shared = new Set([unitBox, unitEdges]);

function block(w, h, d, palette = PAPER) {
  const mesh = new THREE.Mesh(unitBox, faces(palette));
  mesh.scale.set(w, h, d);
  mesh.add(new THREE.LineSegments(unitEdges, inkMat));
  return mesh;
}
function paint(mesh, palette) {
  const [x0, x1, top, bottom, z0, z1] = mesh.material;
  x0.color.setHex(palette.end); x1.color.setHex(palette.end);
  top.color.setHex(palette.top); bottom.color.setHex(palette.top);
  z0.color.setHex(palette.side); z1.color.setHex(palette.side);
}
function drop(obj) {
  scene.remove(obj);
  obj.traverse((o) => { if (o.geometry && !shared.has(o.geometry)) o.geometry.dispose(); });
}
const segments = (pts, mat) => { const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.Float32BufferAttribute(pts, 3)); return new THREE.LineSegments(g, mat); };

// --- track meshes: a floating slab swept along the frame, in chunks ------------------------------
const chunks = new Map(); // chunk index -> group
const tint = new THREE.Color();
const A = { tl: V(), tr: V(), bl: V(), br: V() }, B = { tl: V(), tr: V(), bl: V(), br: V() };
function corners(s, o) {
  const f = frameAt(s);
  o.tl.copy(f.p).addScaledVector(f.right, -HX); o.tr.copy(f.p).addScaledVector(f.right, HX);
  o.bl.copy(o.tl).addScaledVector(f.up, -TRACK_T); o.br.copy(o.tr).addScaledVector(f.up, -TRACK_T);
  return f.up.y;
}
function buildChunk(i) {
  const s0 = i * CHUNK, pos = [], col = [], ribs = [], rails = [];
  const quad = (a, b, c, d, hex, shade = 1) => {
    tint.setHex(hex).multiplyScalar(shade);
    for (const v of [a, b, c, a, c, d]) { pos.push(v.x, v.y, v.z); col.push(tint.r, tint.g, tint.b); }
  };
  const line = (arr, a, b) => arr.push(a.x, a.y, a.z, b.x, b.y, b.z);
  for (let s = s0; s < s0 + CHUNK; s += STEP) {
    const upY = corners(s, A); corners(s + STEP, B);
    quad(A.tl, A.tr, B.tr, B.tl, PAPER.top, 0.88 + 0.12 * Math.max(0, upY)); // faces facing down read darker
    quad(A.tr, A.br, B.br, B.tr, PAPER.end);
    quad(A.bl, A.tl, B.tl, B.bl, PAPER.end);
    quad(A.br, A.bl, B.bl, B.br, PAPER.side);
    line(rails, A.tl, B.tl); line(rails, A.tr, B.tr); line(rails, A.br, B.br); line(rails, A.bl, B.bl);
    for (const lx of [-LANE_X / 2, LANE_X / 2]) line(ribs, at(s, lx, 0.02), at(s + STEP, lx, 0.02));
    if (s % RIB === 0) line(ribs, at(s, -HX, 0.02), at(s, HX, 0.02));
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  g.setAttribute('color', new THREE.Float32BufferAttribute(col, 3));
  const group = new THREE.Group();
  group.add(new THREE.Mesh(g, new THREE.MeshBasicMaterial({ vertexColors: true, side: THREE.DoubleSide })));
  group.add(segments(rails, inkMat), segments(ribs, ribMat));
  // Arches over the track every ARCH metres.
  for (let s = Math.ceil(s0 / ARCH) * ARCH; s < s0 + CHUNK; s += ARCH) {
    const post = 5.6;
    for (const gx of [-HX - 0.4, HX + 0.4]) group.add(place(block(0.6, post, 0.6), s, gx, post / 2 - TRACK_T));
    group.add(place(block(TRACK_W + 1.4, 0.6, 0.6), s, 0, post - TRACK_T));
  }
  return group;
}
function syncTrack(s) {
  const first = Math.max(0, Math.floor((s - BEHIND) / CHUNK)), last = Math.floor((s + AHEAD) / CHUNK);
  for (const [i, g] of chunks) if (i < first || i > last) { drop(g); chunks.delete(i); }
  for (let i = first; i <= last; i++) if (!chunks.has(i)) { const g = buildChunk(i); chunks.set(i, g); scene.add(g); }
}

// Ground far below the lowest point of the track: unit grid lines, the paper style's floor.
const ground = new THREE.GridHelper(1400, 175, P.dim, P.dim);
ground.material.transparent = true; ground.material.opacity = 0.22;
scene.add(ground);

// --- speed gates: a turning hex ring over one lane, chevrons beneath, a boost when flown through ---
const PAD_LEN = 10;
const chevrons = (() => {
  const c = document.createElement('canvas');
  c.width = 64; c.height = 64;
  const g = c.getContext('2d');
  g.fillStyle = '#151513';
  g.beginPath(); g.moveTo(6, 52); g.lineTo(32, 14); g.lineTo(58, 52); g.lineTo(46, 52); g.lineTo(32, 32); g.lineTo(18, 52); g.closePath(); g.fill();
  const t = new THREE.CanvasTexture(c);
  t.wrapS = t.wrapT = THREE.RepeatWrapping;
  t.repeat.set(1, 4);
  t.anisotropy = 4;
  return t;
})();
const padMat = new THREE.MeshBasicMaterial({ map: chevrons, transparent: true, opacity: 0.8, depthWrite: false, side: THREE.DoubleSide });
const padGeo = new THREE.PlaneGeometry(LANE_X - 0.7, PAD_LEN);
padGeo.rotateX(-Math.PI / 2); // lie flat in the frame; the texture's up points along the way of travel
const GATE_R = 1.7; // ring radius, centred at hover height
const gateFrame = new THREE.TorusGeometry(GATE_R, 0.2, 4, 6);
const gateInner = new THREE.TorusGeometry(GATE_R - 0.32, 0.07, 3, 6);
const gateEdges = new THREE.EdgesGeometry(gateFrame, 20);
for (const g of [padGeo, gateFrame, gateInner, gateEdges]) shared.add(g);
const gateMat = new THREE.MeshLambertMaterial({ color: PAPER.top, flatShading: true });
const gateGlow = new THREE.MeshBasicMaterial({ color: MARK });
let pads = [];
function spawnPad(ps) {
  const l = Math.floor(rng() * 3), lx = (l - 1) * LANE_X;
  const floor = place(new THREE.Mesh(padGeo, padMat), ps, lx, 0.03);
  const ring = new THREE.Group();
  ring.add(new THREE.Mesh(gateFrame, gateMat), new THREE.Mesh(gateInner, gateGlow), new THREE.LineSegments(gateEdges, inkMat));
  const spin = new THREE.Group();
  spin.add(ring);
  place(spin, ps, lx, HOVER);
  scene.add(floor, spin);
  pads.push({ s: ps, lane: l, floor, ring, spin, used: false });
}

// --- the hops: the Hopsworks mark in low poly ------------------------------------------------------
// An ovoid of revolution around z, nose at -z. The front band is one smooth cap; the bands behind
// are cut into staggered, domed scales that overlap like shingles (each band starts under the
// previous one, its rear edge lifted), over a dark green core that reads as shadow between them.
const LENGTH = 3.0, RADIUS = 1.0;
const profile = (t) => RADIUS * Math.pow(Math.sin(Math.PI * Math.min(Math.max(t, 0), 1) ** 0.85), 0.7); // t: 0 nose, 1 tail
const along = (t) => LENGTH * (t - 0.42);
const markMat = new THREE.MeshLambertMaterial({ color: MARK, flatShading: true, side: THREE.DoubleSide });
// A closed solid patch of the ovoid: an outer face (domed by puff, rear edge lifted by lift), an
// inner face THICK below it, and the four walls between them, so no edge is ever paper thin.
const THICK = 0.09;
function tile(t0, t1, a0, a1, lift, segA = 3, segT = 3, puff = 0.08) {
  const pt = (u, w, inner) => {
    const t = t0 + (t1 - t0) * w, a = a0 + (a1 - a0) * u;
    const dome = inner ? 0 : puff * Math.sin(Math.PI * u) * Math.sin(Math.PI * w);
    const r = Math.max(0, profile(t) * (1 + dome) + lift * w * w - (inner ? THICK : 0));
    return [Math.cos(a) * r, Math.sin(a) * r, along(t)];
  };
  const pos = [];
  const quad = (p00, p10, p01, p11) => pos.push(...p00, ...p10, ...p01, ...p10, ...p11, ...p01);
  for (let i = 0; i < segA; i++) for (let j = 0; j < segT; j++) {
    const [u0, u1, w0, w1] = [i / segA, (i + 1) / segA, j / segT, (j + 1) / segT];
    quad(pt(u0, w0), pt(u1, w0), pt(u0, w1), pt(u1, w1)); // outer
    quad(pt(u1, w0, 1), pt(u0, w0, 1), pt(u1, w1, 1), pt(u0, w1, 1)); // inner, reversed
  }
  for (let i = 0; i < segA; i++) {
    const [u0, u1] = [i / segA, (i + 1) / segA];
    quad(pt(u1, 0), pt(u0, 0), pt(u1, 0, 1), pt(u0, 0, 1)); // front wall
    quad(pt(u0, 1), pt(u1, 1), pt(u0, 1, 1), pt(u1, 1, 1)); // rear wall
  }
  for (let j = 0; j < segT; j++) {
    const [w0, w1] = [j / segT, (j + 1) / segT];
    quad(pt(0, w0, 1), pt(0, w0), pt(0, w1, 1), pt(0, w1)); // side walls
    quad(pt(1, w0), pt(1, w0, 1), pt(1, w1), pt(1, w1, 1));
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  g.computeVertexNormals();
  return new THREE.Mesh(g, markMat);
}
function hops() {
  const group = new THREE.Group(), tilt = new THREE.Group(), body = new THREE.Group();
  body.add(tile(0, 0.34, 0, Math.PI * 2, 0.1, 12, 4, 0)); // the cap
  const bands = [{ t: [0.27, 0.56], n: 6 }, { t: [0.5, 0.77], n: 6 }, { t: [0.71, 0.93], n: 5 }, { t: [0.87, 1], n: 4 }];
  bands.forEach(({ t: [t0, t1], n }, k) => {
    const step = (Math.PI * 2) / n, off = (k % 2) * step / 2, gap = step * 0.025;
    for (let j = 0; j < n; j++) body.add(tile(t0, t1, off + j * step + gap, off + (j + 1) * step - gap, 0.14 - k * 0.02, 3, 3, 0.14));
  });
  const core = tile(0.02, 0.99, 0, Math.PI * 2, 0, 12, 10, 0);
  core.material = new THREE.MeshLambertMaterial({ color: GREEN.end, flatShading: true });
  core.scale.setScalar(0.94);
  body.add(core);
  tilt.add(body);
  group.add(tilt);
  const shadow = new THREE.Mesh(new THREE.CircleGeometry(1.0, 6), new THREE.MeshBasicMaterial({ color: PAPER.end, side: THREE.DoubleSide }));
  shadow.rotation.x = -Math.PI / 2;
  group.add(shadow);
  group.userData = { tilt, body, shadow };
  return group;
}
let ship = hops();
scene.add(ship);
const TAIL = along(1) + 0.05; // thruster origin, in body space

// --- thruster: a saber plume, shock rings, sparks ------------------------------------------------
// Nested open cones from the tail backwards: a white core inside translucent greens. On paper a
// white core only reads against colour around it, so the glow layers carry it.
const plume = new THREE.Group();
const flame = [
  { r: 0.62, color: 0x1eb182, opacity: 0.28 },
  { r: 0.44, color: 0x4fd0a3, opacity: 0.55 },
  { r: 0.27, color: 0xc8f5e4, opacity: 0.9 },
  { r: 0.13, color: 0xffffff, opacity: 1 },
].map(({ r, color, opacity }, i) => {
  const g = new THREE.ConeGeometry(r, 1, 7, 1, true);
  g.rotateX(Math.PI / 2); g.translate(0, 0, 0.5); // base at the tail, apex trailing at +z
  const m = new THREE.Mesh(g, new THREE.MeshBasicMaterial({ color, transparent: true, opacity, depthWrite: false, side: THREE.DoubleSide }));
  m.renderOrder = 10 + i;
  plume.add(m);
  return m;
});
const nozzle = new THREE.Mesh(new THREE.CircleGeometry(0.4, 7), new THREE.MeshBasicMaterial({ color: 0xffffff, side: THREE.DoubleSide }));
nozzle.renderOrder = 15;
plume.add(nozzle);
plume.position.z = TAIL;
function rebuildShip() {
  scene.remove(ship);
  ship = hops();
  ship.userData.tilt.add(plume);
  scene.add(ship);
  plume.visible = true;
}
ship.userData.tilt.add(plume);

const hexPts = Array.from({ length: 7 }, (_, i) => { const a = (i / 6) * Math.PI * 2; return V(Math.cos(a) * 0.5, Math.sin(a) * 0.5, 0); });
const hexGeo = new THREE.BufferGeometry().setFromPoints(hexPts);
const rings = Array.from({ length: 36 }, () => {
  const line = new THREE.Line(hexGeo, new THREE.LineBasicMaterial({ color: GREEN.side, transparent: true, opacity: 0 }));
  line.visible = false;
  scene.add(line);
  return { line, age: 0, life: 1, grow: 1 };
});
let ringNext = 0, ringClock = 0;
const shipQ = new THREE.Quaternion(); // the hops' world orientation this frame
function emitRing(where, life, grow) {
  const r = rings[ringNext = (ringNext + 1) % rings.length];
  r.line.position.copy(where); r.line.quaternion.copy(shipQ); r.line.rotateZ(fx() * Math.PI);
  r.age = 0; r.life = life; r.grow = grow; r.line.visible = true;
}

const puffGeo = new THREE.OctahedronGeometry(0.2, 0);
const puffEdges = new THREE.EdgesGeometry(puffGeo);
const SPARK = [0xffffff, 0xc8f5e4, 0x4fd0a3, 0x1eb182];
const puffs = Array.from({ length: 96 }, (_, i) => {
  const mesh = new THREE.Mesh(puffGeo, new THREE.MeshBasicMaterial({ color: SPARK[i % SPARK.length] }));
  if (i % 4 === 3) mesh.add(new THREE.LineSegments(puffEdges, inkMat));
  mesh.visible = false;
  scene.add(mesh);
  return { mesh, v: V(), spin: V(), age: 0, life: 1 };
});
let puffNext = 0;
// Velocity in the hops' frame: x right, y up, z backwards.
function emitPuff(where, vx, vy, vz, life = 0.5) {
  const p = puffs[puffNext = (puffNext + 1) % puffs.length];
  p.mesh.position.copy(where); p.v.set(vx, vy, vz).applyQuaternion(shipQ); p.spin.set(fx() * 12, fx() * 12, fx() * 12);
  p.age = 0; p.life = life; p.mesh.visible = true;
}
const tailWorld = V();
function burst(n, spread, back, life = 0.6) {
  for (let i = 0; i < n; i++) emitPuff(tailWorld, (fx() - 0.5) * spread, (fx() - 0.3) * spread, back * (0.5 + fx()), life);
}

// --- air streaks: ink dashes fixed in space along the track ---------------------------------------
const streakMat = new THREE.LineBasicMaterial({ color: P.ink, transparent: true, opacity: 0.3 });
let streaks = [], nextStreakAt = 0;
function spawnStreak(ss) {
  const len = 4 + fx() * 9, a = at(ss, (fx() - 0.5) * (TRACK_W + 8), 1.2 + fx() * 4.5), b = a.clone().addScaledVector(frameAt(ss).fwd, -len);
  const line = segments([a.x, a.y, a.z, b.x, b.y, b.z], streakMat);
  scene.add(line);
  streaks.push({ s: ss, line });
}

// --- debris: what a crash leaves -----------------------------------------------------------------
// Pieces move in track coordinates under magnetic gravity, bounce on the track, and fall away
// once they leave its edge.
let debris = [];
function piece(mesh, s, x, h, vs, vx, vh) {
  scene.add(mesh);
  debris.push({ mesh, s, x, h, vs, vx, vh, rot: new THREE.Euler(fx() * 6, fx() * 6, fx() * 6), spin: V((fx() - 0.5) * 16, (fx() - 0.5) * 16, (fx() - 0.5) * 16) });
}
const spinQ = new THREE.Quaternion();
function stepDebris(dt) {
  for (const p of debris) {
    p.vh -= GRAVITY * dt;
    p.s += p.vs * dt; p.x += p.vx * dt; p.h += p.vh * dt;
    if (Math.abs(p.x) < HX && p.h < 0.25 && p.h > -1) {
      p.h = 0.25; p.vh = Math.abs(p.vh) * 0.38; p.vx *= 0.7; p.vs *= 0.7; p.spin.multiplyScalar(0.6);
    }
    p.rot.x += p.spin.x * dt; p.rot.y += p.spin.y * dt; p.rot.z += p.spin.z * dt;
    place(p.mesh, p.s, p.x, p.h);
    p.mesh.quaternion.multiply(spinQ.setFromEuler(p.rot));
  }
}
function clearDebris() { for (const p of debris) drop(p.mesh); debris = []; }
function shatterShip(fwd) {
  const { body, tilt } = ship.userData;
  for (const m of [...body.children]) {
    if (m.material !== markMat) continue;
    m.geometry.computeBoundingSphere();
    const c = m.geometry.boundingSphere.center.clone();
    const mesh = new THREE.Mesh(m.geometry.clone().translate(-c.x, -c.y, -c.z), new THREE.MeshLambertMaterial({ color: RUST.top, flatShading: true, side: THREE.DoubleSide }));
    const o = c.clone().applyEuler(tilt.rotation);
    const out = V(c.x, c.y, 0).normalize();
    piece(mesh, s - o.z, x + o.x, h + o.y, fwd * (0.35 + fx() * 0.4), out.x * 10 + (fx() - 0.5) * 8, 6 + out.y * 6 + fx() * 10);
  }
  plume.visible = false;
  ship.visible = false;
}
function shatterBlock(m, fwd) {
  const { x: w, y: hh, z: d } = m.scale, { s: bs, x: bx, h: bh } = m.userData, n = [3, Math.max(2, Math.round(hh / 1.2)), 2];
  for (let i = 0; i < n[0]; i++) for (let j = 0; j < n[1]; j++) for (let k = 0; k < n[2]; k++) {
    const c = block(w / n[0] * 0.92, hh / n[1] * 0.92, d / n[2] * 0.92, (i + j + k) % 3 ? PAPER : RUST);
    piece(c, bs - (k - 0.5) * d * 0.5, bx + (i / (n[0] - 1) - 0.5) * w * 0.66, bh + (j / (n[1] - 1) - 0.5) * hh * 0.66,
      fwd * (0.25 + fx() * 0.5), (fx() - 0.5) * 14, 4 + fx() * 14 + j * 2);
  }
  scene.remove(m);
}
const flash = document.querySelector('.flash');

// --- the leaderboard in the world -----------------------------------------------------------------
// A paper slab floating over the track. On the start and crash screens it hovers ahead of the hops;
// once a run starts it stays put and the hops flies under it. Its face is drawn from the board the
// server rendered into the page (#board): the HTML list stays the content, the slab is its view.
const SLAB = { w: 16, h: 9, d: 0.6, ahead: 30, lift: 7 }; // bottom edge 2.5 m up: the hops (top 2.3 m) flies under // metres; lift is the centre above the track
const slabCanvas = document.createElement('canvas');
slabCanvas.width = 1600; slabCanvas.height = 900;
const slabTex = new THREE.CanvasTexture(slabCanvas);
slabTex.colorSpace = THREE.SRGBColorSpace; slabTex.anisotropy = 8;
const slab = new THREE.Group(), slabFloat = new THREE.Group();
slabFloat.add(block(SLAB.w, SLAB.h, SLAB.d));
const slabFace = new THREE.Mesh(new THREE.PlaneGeometry(SLAB.w - 0.24, SLAB.h - 0.24), new THREE.MeshBasicMaterial({ map: slabTex }));
slabFace.position.z = SLAB.d / 2 + 0.01; // +z faces the chase camera
slabFloat.add(slabFace);
slab.add(slabFloat);
const slabShadow = new THREE.Mesh(new THREE.PlaneGeometry(SLAB.w * 0.9, SLAB.d * 3).rotateX(-Math.PI / 2), new THREE.MeshBasicMaterial({ color: PAPER.end, transparent: true, opacity: 0.7, depthWrite: false }));
scene.add(slab, slabShadow);
let slabS = 0;

function drawSlab() {
  const g = slabCanvas.getContext('2d'), W = slabCanvas.width, H = slabCanvas.height, pad = 64;
  g.fillStyle = '#FCFBF8'; g.fillRect(0, 0, W, H);
  g.textBaseline = 'alphabetic';
  g.font = '500 38px "Geist Mono"'; g.letterSpacing = '6px'; g.fillStyle = '#8A867D';
  g.fillText('LEADERBOARD', pad, pad + 30);
  g.textAlign = 'right'; g.fillText(document.querySelector('.hud .label b')?.textContent?.toUpperCase() ?? '', W - pad, pad + 30); g.textAlign = 'left';
  g.fillStyle = '#151513'; g.fillRect(pad, pad + 58, W - pad * 2, 4);
  const items = [...document.querySelectorAll('#board li')];
  const rowH = (H - pad * 2 - 80) / Math.max(10, items.length);
  g.letterSpacing = '0px';
  items.forEach((li, i) => {
    const y = pad + 84 + i * rowH;
    if (li.classList.contains('empty')) { g.font = '400 48px "Geist Mono"'; g.fillStyle = '#8A867D'; g.fillText(li.textContent, pad, y + rowH * 0.7); return; }
    if (li.classList.contains('gap')) { g.font = '400 40px "Geist Mono"'; g.fillStyle = '#8A867D'; g.textAlign = 'center'; g.fillText(li.textContent, W / 2, y + rowH * 0.6); g.textAlign = 'left'; return; }
    if (li.classList.contains('you')) { g.fillStyle = 'rgba(14,143,101,0.14)'; g.fillRect(pad - 12, y + 4, W - pad * 2 + 24, rowH - 4); }
    const rank = li.querySelector('.rank')?.textContent ?? '', pilot = li.querySelector('.pilot')?.textContent ?? '';
    const who = li.querySelector('.who'), bot = who?.querySelector('.bot path')?.getAttribute('d');
    const name = ([...(who?.childNodes ?? [])].find((n) => n.nodeType === Node.TEXT_NODE)?.textContent ?? '').trim(), dist = (li.querySelector('.dist')?.firstChild?.textContent ?? '').trim();
    const base = y + rowH * 0.68;
    let nx = pad + 96;
    g.font = '400 40px "Geist Mono"'; g.fillStyle = '#8A867D'; g.fillText(rank, pad, base);
    if (bot) {
      // The board's robot (16 px viewBox) at 40 px, sitting on the baseline.
      g.save(); g.translate(nx, base - 36); g.scale(2.5, 2.5);
      g.strokeStyle = '#0E8F65'; g.lineWidth = 1.5; g.lineCap = g.lineJoin = 'round'; g.stroke(new Path2D(bot));
      g.restore(); nx += 56;
    }
    g.font = '500 48px "Geist Mono"'; g.fillStyle = '#151513'; g.fillText(name, nx, base);
    if (pilot) { const w = g.measureText(name).width; g.font = '500 30px "Geist Mono"'; g.fillStyle = '#0E8F65'; g.fillText(pilot.toUpperCase(), nx + w + 20, base); }
    g.font = '500 48px "Geist Mono"'; g.fillStyle = '#151513'; g.textAlign = 'right'; g.fillText(dist, W - pad, base); g.textAlign = 'left';
    g.fillStyle = '#D9D5CC'; g.fillRect(pad, y + rowH, W - pad * 2, 2);
  });
  slabTex.needsUpdate = true;
}
document.fonts.load('500 48px "Geist Mono"').then(drawSlab, drawSlab);
new MutationObserver(drawSlab).observe(document.getElementById('board'), { childList: true, subtree: true, attributes: true });
document.body.classList.add('in-world'); // the 3D slab now carries the board; the HTML list stays for no-JS and crawlers

// --- state ---------------------------------------------------------------------------------------
const ui = Object.fromEntries(['distance', 'speed', 'speedbar', 'reticle', 'status', 'prompt', 'charge'].map((id) => [id, document.getElementById(id)]));
const grain = document.querySelector('.grain');
let mode = 'ready'; // ready | flying | crashed
let flightMs = 0;
let speed = 0, boost = 0, distance = 0, s = 0, x = 0, xv = 0, h = HOVER, hv = 0, lane = 1;
let crashV = 0, timeScale = 1, camX = 0, camH = 0, charge = 0, duckAmt = 0;
let nextRowAt = 0, nextPadAt = 0, rows = [], shake = 0, squash = 0, airborne = false, hover = HOVER, duckT = 0;
const START = CHASE.back + 10; // so the camera starts on the track

const inLoop = (ps) => { const k = kindAt(ps); return k === 'cork' || k === 'loop'; };
function spawnRow(rs) {
  // One or two lanes hold an obstacle, never all three.
  const taken = [...LANES].sort(() => rng() - 0.5).slice(0, rng() < 0.45 ? 2 : 1);
  const lanes = {}, meshes = [];
  for (const l of taken) {
    const roll = rng(), kind = roll < KIND.wall.p ? 'wall' : roll < KIND.wall.p + KIND.low.p ? 'low' : 'bar';
    lanes[l] = kind;
    const lx = (LANES.indexOf(l) - 1) * LANE_X, w = LANE_X - 0.4;
    if (kind === 'bar') {
      // Full lane wide on thin posts at the lane edges, so a ducking hops squeezes through.
      const { bottom, t } = KIND.bar, bw = LANE_X - 0.1;
      const beam = place(block(bw, t, 1.0), rs, lx, bottom + t / 2);
      beam.userData = { kind, s: rs, x: lx, h: bottom + t / 2, hw: bw / 2, hh: t / 2, hd: 0.5 };
      for (const px of [-bw / 2 + 0.08, bw / 2 - 0.08]) { const post = place(block(0.16, bottom, 0.16), rs, lx + px, bottom / 2); post.userData = { kind: 'post', s: rs, x: lx + px, h: bottom / 2, hw: 0.08, hh: bottom / 2, hd: 0.08 }; meshes.push(post); scene.add(post); }
      meshes.push(beam); scene.add(beam);
    } else {
      const [lo, hi] = KIND[kind].h, bh = between(lo, hi);
      const d = kind === 'wall' ? 1.6 : 2.2;
      const b = place(block(w, bh, d), rs, lx, bh / 2);
      b.userData = { kind, s: rs, x: lx, h: bh / 2, hw: w / 2, hh: bh / 2, hd: d / 2 };
      meshes.push(b); scene.add(b);
    }
  }
  rows.push({ s: rs, lanes, meshes, cleared: false, flash: 0 });
}

function reset() {
  for (const r of rows) for (const m of r.meshes) drop(m);
  for (const p of pads) { drop(p.floor); drop(p.spin); }
  for (const st of streaks) drop(st.line);
  for (const [, g] of chunks) drop(g);
  chunks.clear();
  clearDebris();
  rebuildShip();
  rows = []; pads = []; streaks = [];
  seed = (Math.random() * 2 ** 31) | 0;
  track.P = []; track.Q = []; track.kind = []; track.pitch = [];
  gen.p.set(0, 0, 0); gen.q.identity(); gen.seg = null; gen.at = 0; gen.minY = 0; gen.queue = [];
  track.P.push(gen.p.clone()); track.Q.push(gen.q.clone()); track.kind.push('straight'); track.pitch.push(0);
  timeScale = 1;
  speed = SPEED.start; boost = 0; distance = 0; s = START; x = 0; xv = 0; lane = 1;
  h = HOVER; hv = 0; airborne = false; hover = HOVER; duckT = 0; camX = 0; camH = 0; charge = 0;
  nextRowAt = START + 110; nextPadAt = START + 200; nextStreakAt = START;
  slabS = s + SLAB.ahead;
}

// A model pilot flies the page when the jevworks runner drives it: the runner exposes
// jevworksDecide (state in, move probabilities out) and jevworksFinished (posts the run).
const PILOT = typeof window.jevworksDecide === 'function';
// Umami custom events, when the page loads the tracker; a model pilot's runs are not visits.
const analytics = (event, data) => { if (!PILOT) window.umami?.track(event, data); };

// The pilot's live preview is unloaded during a run, so it never takes frames from the game.
const live = document.getElementById('live'), liveFrame = live?.querySelector('iframe');
function showLive(on) {
  if (!liveFrame) return;
  live.hidden = !on;
  liveFrame.src = on ? liveFrame.dataset.src : 'about:blank';
}

// Takeoff asks the server for the run's key: the server times the run from now, and the run
// posted with that key may not last longer. A model pilot is trusted by its token and draws its own.
let runKey = null;
async function takeoff() {
  if (PILOT) return crypto.randomUUID();
  try {
    const res = await fetch('api/runs/start', { method: 'POST' });
    return res.ok ? (await res.json()).runKey : null;
  } catch { return null; } // offline: the run can be flown, not posted
}

function start() {
  if (seat.state !== 'play') return;
  analytics('run-start');
  showLive(false);
  reset();
  mode = 'flying';
  document.body.classList.add('flying');
  flightMs = 0; lastRun = null; submitted = false; pilot.armed = null;
  runKey = takeoff();
  ui.prompt.hidden = true; form.hidden = true; result.hidden = true;
  ui.status.textContent = 'Flying'; ui.status.className = 'label flying';
}

function crash(row, hitMesh) {
  mode = 'crashed';
  document.body.classList.remove('flying');
  crashV = speed + boost; timeScale = 0.25;
  for (const m of row.meshes) if (m.userData.kind !== 'post') paint(m, RUST);
  shatterBlock(hitMesh, crashV);
  shatterShip(crashV);
  shake = 1.6;
  for (let i = 0; i < 4; i++) emitRing(tailWorld, 0.6 + i * 0.25, 8 + i * 6);
  burst(40, 22, -crashV * 0.3, 1.2);
  flash.style.transition = 'none'; flash.style.opacity = '0.35';
  requestAnimationFrame(() => { flash.style.transition = 'opacity 0.9s ease-out'; flash.style.opacity = '0'; });
  ui.status.textContent = `Crashed at ${Math.round(distance)} m`; ui.status.className = 'label crash';
  lastRun = { distance: Math.round(distance), durationMs: Math.round(flightMs), runKey };
  analytics('crash', { distance: lastRun.distance });
  if (PILOT) finished(lastRun);
  setTimeout(() => {
    if (mode !== 'crashed') return;
    if (!PILOT) showLive(true);
    ui.prompt.querySelector('h1').textContent = `${lastRun.distance} m`;
    if (PILOT) { result.hidden = false; ui.prompt.hidden = false; return; }
    result.hidden = false; result.textContent = 'Enter: add to board · Space: fly again';
    form.hidden = false;
    ui.prompt.hidden = false;
  }, 900);
}

function steer(move) {
  const before = lane;
  if (move === 'left') lane = Math.max(0, lane - 1);
  if (move === 'right') lane = Math.min(2, lane + 1);
  if (move === 'up' && !airborne) {
    hv += JUMP * (1 + CHARGE.power * charge); airborne = true; squash = 0.3 + charge * 0.2;
    for (let i = 0; i <= Math.round(charge * 3); i++) emitRing(tailWorld, 0.45 + i * 0.15, 5 + i * 4);
    burst(6 + Math.round(charge * 18), 5 + charge * 8, 2 + charge * 6);
    shake = Math.max(shake, charge * 0.5);
    charge = 0;
  }
  if (move === 'down') { duckT = DUCK.time; squash = Math.max(squash, 0.2); }
  if (lane !== before) for (let i = 0; i < 5; i++) emitPuff(tailWorld, (before - lane) * (4 + fx() * 4), fx() * 2, 3 + fx() * 3, 0.45);
}

addEventListener('keydown', (e) => {
  seat.active = true;
  if (document.activeElement === nameInput) { // typing a name; Escape leaves the field
    if (e.code === 'Escape') nameInput.blur();
    return;
  }
  if (mode !== 'flying') {
    if (e.code === 'Space') { e.preventDefault(); if (seat.state === 'gone') joinSeat(); else start(); }
    // After a crash, Enter puts the run on the board: straight away with a saved name, else via the field.
    if (e.code === 'Enter' && lastRun && !submitted && !form.hidden) { e.preventDefault(); if (nameInput.value.trim()) form.requestSubmit(); else nameInput.focus(); }
    return;
  }
  if (e.code === 'ArrowLeft' || e.code === 'KeyA') steer('left');
  if (e.code === 'ArrowRight' || e.code === 'KeyD') steer('right');
  if (e.code === 'ArrowUp' || e.code === 'KeyW') { e.preventDefault(); steer('up'); }
  if (e.code === 'ArrowDown' || e.code === 'KeyS') { e.preventDefault(); steer('down'); }
});

// --- leaderboard ---------------------------------------------------------------------------------
// A run is posted with the key drawn at its crash, so it is safe to post again: while the game
// server restarts (a deploy) or the network drops, the post is retried; the server records a key once.
const RETRY = { attempts: 5, waitMs: 2000 };
async function postRun(run) {
  for (let attempt = 1; ; attempt++) {
    let res = null;
    try { res = await fetch('api/runs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(run) }); } catch { /* network down */ }
    if (res && ![502, 503, 504].includes(res.status)) {
      // Anything in front of the game (a proxy, a login) may answer with something other than JSON.
      const d = await res.json().catch(() => ({ error: `Leaderboard unreachable (HTTP ${res.status}). Your run is kept: try again.` }));
      if (!res.ok || d.error) throw new Error(d.error);
      return d;
    }
    if (attempt === RETRY.attempts) throw new Error('Leaderboard unreachable. Your run is kept: try again.');
    result.textContent = `Board restarting, retrying (${attempt}/${RETRY.attempts - 1})`;
    await new Promise((r) => setTimeout(r, RETRY.waitMs));
  }
}
// The board is rendered by the server into the page; after a crash the form posts the run and
// the server returns the board, ranked, as HTML.
const form = document.getElementById('sign'), nameInput = document.getElementById('name');
const boardEl = document.getElementById('board'), result = document.getElementById('result');
try { nameInput.value = localStorage.getItem('hops-run-name') ?? ''; } catch { /* storage blocked */ }
let lastRun = null, submitted = false, posting = false;
form.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!lastRun || submitted || posting) return;
  posting = true;
  const name = nameInput.value.trim();
  try { localStorage.setItem('hops-run-name', name); } catch { /* storage blocked */ }
  form.querySelector('button').disabled = true;
  try {
    const key = await lastRun.runKey;
    if (!key) throw new Error('The server could not time this run (offline at takeoff), so it cannot go on the board.');
    const d = await postRun({ name, ...lastRun, runKey: key });
    submitted = true;
    analytics('board-submit', { distance: lastRun.distance, rank: d.rank });
    boardEl.innerHTML = d.html;
    boardEl.querySelector(`li[data-rank="${d.rank}"]`)?.classList.add('you');
    const top = d.runs.filter((r) => !r.below).length;
    result.textContent = d.rank <= top ? `${lastRun.distance} m · rank ${d.rank}` : `${lastRun.distance} m · rank ${d.rank}, outside the top ${top}`;
    form.hidden = true;
    nameInput.blur();
  } catch (err) {
    result.innerHTML = `<span class="err">${String(err.message).replace(/[<>&]/g, '')}</span>`;
  } finally {
    posting = false;
    form.querySelector('button').disabled = false;
  }
});

// --- seat ----------------------------------------------------------------------------------------
// The server seats a limited number of players at once; beyond it the page waits in line and
// shows its place. A heartbeat holds the seat. A seat lost while playing (a server restart) is
// joined again at once; one released while idle (others were waiting) waits for Space.
const seatEl = document.getElementById('seat');
const seat = { id: null, state: null, position: 0, active: false, every: 10_000, timer: 0, waited: false };
function showSeat(error) {
  seatEl.hidden = !error && (seat.state === 'play' || seat.state === null);
  if (error) seatEl.innerHTML = `<span class="err">${String(error).replace(/[<>&]/g, '')}</span>`;
  else if (seat.state === 'wait') seatEl.textContent = `The track is full. You are number ${seat.position} in line`;
  else if (seat.state === 'gone') seatEl.textContent = 'Seat released while you were away. Press Space to get back in line';
}
async function beat() {
  clearTimeout(seat.timer);
  const active = seat.active || mode === 'flying';
  seat.active = false;
  try {
    const res = await fetch('api/seat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id: seat.id, active }) });
    const d = await res.json().catch(() => ({ error: `Game server unreachable (HTTP ${res.status}). Retrying.` }));
    if (!res.ok || d.error) throw new Error(d.error);
    if (d.state === 'gone') {
      seat.id = null; seat.state = 'gone';
      if (active) return joinSeat();
      return showSeat();
    }
    Object.assign(seat, { id: d.id, state: d.state, position: d.position ?? 0, every: d.heartbeatMs });
    if (d.state === 'wait' && !seat.waited) { seat.waited = true; analytics('queue-wait', { position: d.position }); }
    showSeat();
  } catch (err) {
    showSeat(err.message);
  }
  seat.timer = setTimeout(beat, seat.every);
}
function joinSeat() { seat.id = null; seat.state = null; beat(); }
addEventListener('pagehide', () => {
  if (seat.id) navigator.sendBeacon('api/seat/leave', new Blob([JSON.stringify({ id: seat.id })], { type: 'application/json' }));
  clearTimeout(seat.timer);
});
addEventListener('pageshow', (e) => { if (e.persisted) joinSeat(); }); // back from the page cache
joinSeat();

// --- model pilot ---------------------------------------------------------------------------------
// The pilot is asked for a move whenever no request is in flight. Jump and duck are armed against
// the nearest row and fired LEAD seconds before it, so the hops tops its arc, or is lowest, as it
// crosses; lane changes apply at once. After a crash the run is posted, and the pilot takes off
// again RESTART_MS later, time enough to see the crash and the result.
const LEAD = { up: 0.3, down: 0.25 }, RESTART_MS = 4000;
const mind = document.getElementById('mind'), pilotEl = document.getElementById('pilot');
const moveEls = Object.fromEntries([...mind.querySelectorAll('.move')].map((el) => [el.dataset.move, el]));
const pilot = { asking: false, armed: null, restartAt: 0, name: '', model: '', record: '' };
const clean = (text) => String(text).replace(/[<>&]/g, '');
function showPilot(timing = '') {
  pilotEl.textContent = [pilot.name, pilot.model, pilot.record, timing].filter(Boolean).join(' · ');
}
async function ask() {
  if (pilot.asking || mode !== 'flying') return;
  pilot.asking = true;
  const ahead = rows.filter((r) => r.s > s + 0.8).sort((a, b) => a.s - b.s).map((r) => ({ distance: r.s - s, lanes: r.lanes }));
  try {
    const d = await window.jevworksDecide({ lane: LANES[lane], airborne, ahead });
    const best = d.moves[d.probabilities.indexOf(Math.max(...d.probabilities))];
    for (const [move, el] of Object.entries(moveEls)) {
      const i = d.moves.indexOf(move);
      el.classList.toggle('off', i < 0);
      el.classList.toggle('pick', move === best);
      el.querySelector('i').style.width = i < 0 ? '0' : `${(d.probabilities[i] * 100).toFixed(1)}%`;
      el.querySelector('.p').textContent = i < 0 ? '-' : d.probabilities[i].toFixed(2);
    }
    pilot.name = d.pilot; pilot.model = d.model;
    showPilot(`${d.forwardMs.toFixed(0)} ms`);
    if (mode === 'flying') {
      const next = rows.find((r) => !r.cleared && r.s > s);
      if ((best === 'up' || best === 'down') && next) pilot.armed = { move: best, row: next };
      else steer(best);
    }
  } catch (err) {
    pilotEl.innerHTML = `<span class="err">Pilot unavailable: ${clean(err.message).slice(0, 80)}</span>`;
  } finally {
    pilot.asking = false;
  }
}
async function finished(run) {
  pilot.restartAt = Infinity;
  result.textContent = 'Posting the run';
  try {
    const d = await window.jevworksFinished({ ...run, runKey: await run.runKey });
    if (d.error) throw new Error(d.error);
    boardEl.innerHTML = d.html;
    pilot.record = `run ${d.number} · best ${d.best} m`;
    result.textContent = `Run ${d.number} · ${run.distance} m · best ${d.best} m`;
    showPilot();
  } catch (err) {
    result.innerHTML = `<span class="err">Run not posted: ${clean(err.message).slice(0, 80)}</span>`;
  }
  pilot.restartAt = performance.now() + RESTART_MS;
}
if (PILOT) {
  live?.remove(); // the pilot's own page is the stream
  mind.hidden = false; pilotEl.hidden = false;
  document.querySelector('.keys').hidden = true;
  showPilot();
}

// --- loop ----------------------------------------------------------------------------------------
const clock = new THREE.Timer();
const camUp = V(0, 1, 0), look = V(), camF = { p: V(), q: new THREE.Quaternion(), fwd: V(), up: V(), right: V() };
reset();

function frame(now) {
  clock.update(now);
  const raw = Math.min(clock.getDelta(), 0.05);
  if (mode === 'crashed') timeScale = Math.min(1, timeScale + raw * 0.5);
  const dt = raw * timeScale;
  const t = clock.getElapsed();
  const flying = mode === 'flying';
  if (flying) {
    speed = Math.min(SPEED.max, speed + SPEED.gain * dt);
    boost = Math.max(0, boost - BOOST.decay * dt);
    charge = Math.min(1, charge + CHARGE.perSecond * dt);
    flightMs += dt * 1000;
    if (PILOT) ask();
  }
  if (PILOT && !flying && seat.state === 'play' && now >= pilot.restartAt) start();
  if (mode === 'crashed') crashV = Math.max(0, crashV - crashV * 2.5 * dt - 4 * dt);
  const v = flying ? speed + boost : mode === 'ready' ? 22 : crashV;
  distance += flying ? v * dt : 0;
  s += v * dt;
  syncTrack(s);

  // Lateral spring towards the target lane (in ready mode, a slow weave).
  const tx = mode === 'ready' ? Math.sin(t * 0.9) * LANE_X : (lane - 1) * LANE_X;
  xv += ((tx - x) * SPRING.k - xv * SPRING.c) * dt;
  x += xv * dt;

  // Height above the track under magnetic gravity. Where the track curves away beneath the hops
  // (a crest, pitch rate below zero) faster than gravity pulls, the hops lifts off; where it
  // curves up into it (a dip, a loop) it is pressed down.
  duckT = Math.max(0, duckT - dt);
  hover = THREE.MathUtils.lerp(hover, duckT > 0 ? DUCK.hover : HOVER, Math.min(1, dt * 18));
  hv += (-GRAVITY - v * v * pitchAt(s)) * dt;
  h += hv * dt;
  if (h <= hover) {
    if (airborne && -hv > 4) {
      squash = Math.min(-hv / 25, 0.45);
      shake = Math.max(shake, Math.min(-hv / 40, 0.8));
      emitRing(tailWorld, 0.5, 6); burst(8, 8, 2);
    }
    h = hover; hv = 0; airborne = false;
  } else if (h > hover + 0.3) airborne = true;

  const f = frameAt(s);
  ship.position.copy(f.p).addScaledVector(f.right, x);
  ship.quaternion.copy(f.q);
  const { tilt, body, shadow } = ship.userData;
  tilt.position.y = h + (airborne ? 0 : Math.sin(t * 5) * 0.06);
  tilt.rotation.order = 'YXZ';
  tilt.rotation.x = Math.atan2(hv, Math.max(v, 1)); // nose follows the flight path
  tilt.rotation.y = -xv * 0.018;
  tilt.rotation.z = THREE.MathUtils.clamp(-xv * 0.07, -1.1, 1.1); // bank into the turn
  body.rotation.z = Math.sin(t * 2.3) * 0.08;
  squash = Math.max(0, squash - dt * 2.5);
  duckAmt = THREE.MathUtils.lerp(duckAmt, duckT > 0 ? 1 : 0, Math.min(1, dt * 18));
  body.scale.set((1 + squash * 0.5) * (1 - DUCK.narrow * duckAmt), (1 - squash) * (1 - DUCK.flat * duckAmt), 1 + v / 700);
  shadow.position.y = 0.03;
  shadow.scale.setScalar(1 / (1 + (h - hover) * 0.15));
  tilt.getWorldQuaternion(shipQ);

  // Thruster: plume length and flicker follow speed and boost; rings and sparks stream behind.
  tilt.localToWorld(tailWorld.set(0, 0, TAIL + 0.2));
  const thrust = (v / SPEED.max) + boost / BOOST.kick;
  const flicker = 0.85 + Math.sin(t * 61) * 0.08 + fx() * 0.12;
  const len = (1.6 + thrust * 4.5) * flicker;
  flame.forEach((m, i) => {
    const w = 1 + thrust * 0.3 + (fx() - 0.5) * 0.12;
    m.scale.set(w, w, len * (1 - i * 0.16) * (0.9 + fx() * 0.2));
  });
  nozzle.scale.setScalar(0.9 + thrust * 0.3 + fx() * 0.1);
  if (v > 0 && ship.visible) {
    ringClock -= dt;
    if (ringClock <= 0) { emitRing(tailWorld, 0.35 + thrust * 0.2, 2 + thrust * 2.5); ringClock = 1 / (6 + v * 0.18 + boost * 0.4); }
    for (let i = 0; i < 1 + thrust * 2; i++) if (fx() < 0.6) emitPuff(tailWorld, (fx() - 0.5) * 3, (fx() - 0.5) * 3, 4 + fx() * 8, 0.25 + fx() * 0.25);
  }
  for (const r of rings) {
    if (!r.line.visible) continue;
    r.age += dt;
    const k = r.age / r.life;
    if (k >= 1) { r.line.visible = false; continue; }
    r.line.scale.setScalar(0.6 + k * r.grow);
    r.line.material.opacity = 0.9 * (1 - k);
  }
  for (const p of puffs) {
    if (!p.mesh.visible) continue;
    p.age += dt;
    const k = p.age / p.life;
    if (k >= 1) { p.mesh.visible = false; continue; }
    p.mesh.position.addScaledVector(p.v, dt);
    p.mesh.rotation.x += p.spin.x * dt; p.mesh.rotation.y += p.spin.y * dt;
    p.mesh.scale.setScalar(1 - k);
  }

  // Speed gates: on straights and gentle sections, a boost once flown through.
  chevrons.offset.y = (chevrons.offset.y + dt * 2.5) % 1;
  while (flying && nextPadAt < s + AHEAD) {
    if (inLoop(nextPadAt)) { nextPadAt += 20; continue; }
    spawnPad(nextPadAt); nextPadAt += between(PAD_GAP.min, PAD_GAP.max);
  }
  for (const p of pads) {
    p.ring.rotation.z += dt * (p.used ? 9 : 1.6);
    if (flying && !p.used && Math.abs(p.s - s) < 1.2 && Math.abs(x - (p.lane - 1) * LANE_X) < GATE_R && Math.abs(h - HOVER) < GATE_R) {
      p.used = true; boost = BOOST.kick; shake = Math.max(shake, 0.35);
      emitRing(tailWorld, 0.6, 7); emitRing(tailWorld, 0.8, 10); burst(12, 6, 10);
    }
  }
  for (const p of pads.filter((p) => s - p.s > BEHIND)) { drop(p.floor); drop(p.spin); }
  pads = pads.filter((p) => s - p.s <= BEHIND);

  // Air streaks.
  while (v > 0 && nextStreakAt < s + AHEAD * 0.6) { spawnStreak(nextStreakAt); nextStreakAt += 3 + fx() * 6; }
  for (const st of streaks.filter((st) => s - st.s > BEHIND)) drop(st.line);
  streaks = streaks.filter((st) => s - st.s <= BEHIND);

  // Rows: spawn ahead (never inside a loop or corkscrew, closer together the further the run),
  // collide, flash green when cleared, drop behind. Above a low block or under a bar, the hops passes.
  while (flying && nextRowAt < s + AHEAD) {
    if (inLoop(nextRowAt)) { nextRowAt += 20; continue; }
    spawnRow(nextRowAt);
    const tighten = Math.max(GAP.floor, 1 - distance / GAP.over);
    nextRowAt += between(GAP.min, GAP.max) * tighten;
  }
  if (pilot.armed && flying) {
    if (pilot.armed.row.cleared) pilot.armed = null;
    else if ((pilot.armed.row.s - s) / Math.max(v, 1) <= LEAD[pilot.armed.move]) { steer(pilot.armed.move); pilot.armed = null; }
  }

  // Collision: touch and you crash, miss and you pass. The hops is an ellipsoid (squashed when it
  // ducks or lands), each obstacle its own box, posts included; the test is exact.
  const sq = ship.userData.body.scale, cs = s - (along(0) + along(1)) / 2;
  const rx = HULL.x * sq.x, ry = HULL.y * sq.y, rz = HULL.z * sq.z;
  const touches = (u) => {
    const dz = (Math.max(u.s - u.hd, Math.min(cs, u.s + u.hd)) - cs) / rz;
    const dx = (Math.max(u.x - u.hw, Math.min(x, u.x + u.hw)) - x) / rx;
    const dy = (Math.max(u.h - u.hh, Math.min(h, u.h + u.hh)) - h) / ry;
    return dx * dx + dy * dy + dz * dz < 1;
  };
  for (const r of rows) {
    if (flying && !r.cleared && Math.abs(r.s - cs) < 4) {
      const hit = r.meshes.find((m) => touches(m.userData));
      if (hit) crash(r, hit);
    }
    if (flying && !r.cleared && cs - r.s > 1.2 + rz) { r.cleared = true; r.flash = 1; charge = Math.min(1, charge + CHARGE.perRow); for (const m of r.meshes) if (m.userData.kind !== 'post') paint(m, GREEN); }
    if (r.flash > 0) { r.flash -= dt * 1.2; if (r.flash <= 0) for (const m of r.meshes) if (m.userData.kind !== 'post') paint(m, PAPER); }
  }
  for (const r of rows.filter((r) => s - r.s > BEHIND)) for (const m of r.meshes) drop(m);
  rows = rows.filter((r) => s - r.s <= BEHIND);

  // Leaderboard slab: hovers ahead between runs, stays put during one.
  if (!flying) slabS = THREE.MathUtils.lerp(slabS, s + SLAB.ahead, Math.min(1, raw * 2));
  place(slab, slabS, 0, SLAB.lift);
  slabFloat.position.y = Math.sin(t * 1.1) * 0.25;
  slabFloat.rotation.z = Math.sin(t * 0.7) * 0.025;
  slabFloat.rotation.y = Math.sin(t * 0.5) * 0.04;
  place(slabShadow, slabS, 0, 0.03);
  slab.visible = slabShadow.visible = Math.abs(slabS - s) < AHEAD;

  // Camera: behind and above in the track's frame, its up easing towards the track's up so the
  // world turns over in a loop; lagging a little on lane changes, rolled with the bank, shaken
  // by impacts.
  shake = Math.max(0, shake - dt * 2.2);
  const k = Math.min(1, raw * CHASE.lag);
  camX = THREE.MathUtils.lerp(camX, x * 0.75, k);
  camH = THREE.MathUtils.lerp(camH, Math.max(0, h - HOVER), k);
  frameAt(s - CHASE.back, camF);
  camera.position.copy(camF.p).addScaledVector(camF.right, camX).addScaledVector(camF.up, CHASE.up + HOVER + camH);
  camera.position.x += (fx() - 0.5) * shake; camera.position.y += (fx() - 0.5) * shake;
  camUp.lerp(camF.up, Math.min(1, raw * 5)).normalize();
  camera.up.copy(camUp);
  camera.lookAt(at(s + CHASE.ahead, x * 0.5, HOVER * 0.6, look));
  camera.rotateZ(THREE.MathUtils.clamp(-xv * 0.012, -0.18, 0.18));
  const fov = Math.min(FOV.max, FOV.base + Math.max(0, v - SPEED.start) * FOV.perSpeed + boost * FOV.boost);
  if (Math.abs(camera.fov - fov) > 0.01) { camera.fov = THREE.MathUtils.lerp(camera.fov, fov, Math.min(1, raw * 4)); camera.updateProjectionMatrix(); }
  ground.position.set(Math.round(f.p.x / 8) * 8, gen.minY - 45, Math.round(f.p.z / 8) * 8);

  stepDebris(dt);

  // Grain, re-seeded every frame.
  grain.style.transform = `translate(${(fx() * 200) | 0}px, ${(fx() * 200) | 0}px)`;

  ui.distance.innerHTML = `${Math.round(distance)}<small>m</small>`;
  ui.speed.textContent = Math.round(v);
  ui.speedbar.style.width = `${Math.min(100, (v / (SPEED.max + BOOST.kick)) * 100).toFixed(1)}%`;
  ui.speedbar.classList.toggle('full', boost > 1);
  ui.reticle.style.setProperty('--bank', `${THREE.MathUtils.radToDeg(tilt.rotation.z) * 0.4}deg`);
  ui.charge.style.width = `${(charge * 100).toFixed(1)}%`;
  ui.charge.classList.toggle('full', charge >= 1);
  renderer.render(scene, camera);
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
