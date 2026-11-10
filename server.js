require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const path = require('path');
const axios = require('axios');
const fetch = require('node-fetch');
const twilio = require('twilio');
const helmet = require('helmet');
const jwt = require('jsonwebtoken');
const zlib = require('zlib');
const { promisify } = require('util');
const nodeCrypto = require('crypto');
const dns = require('dns').promises;
const net = require('net');
const https = require('https');
const dnsCb = require('dns');
const { Transform, pipeline } = require('stream');
const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Firebase Admin SDK
const admin = require('firebase-admin');
try {
    const { getFirestore, FieldPath, FieldValue } = require('firebase-admin/firestore');
    if (!admin.firestore) {
        admin.firestore = getFirestore;
        admin.firestore.FieldPath = FieldPath;
        admin.firestore.FieldValue = FieldValue;
    }
} catch (e) { }

if (process.env.FIREBASE_PROJECT_ID && process.env.FIREBASE_PRIVATE_KEY) {
    admin.initializeApp({
        credential: admin.credential.cert({
            projectId: process.env.FIREBASE_PROJECT_ID,
            clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
            privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
        })
    });
} else {
    try {
        admin.initializeApp();
    } catch (e) {
        console.warn('[WARN] Firebase credentials not configured in env.');
    }
}

const db = admin.firestore();
const usersCol = () => db.collection('users');
const chatsCol = () => db.collection('chats');
const appsCol = () => db.collection('store_apps');
const commentsCol = () => db.collection('store_comments');
const mailCol = () => db.collection('mail_messages');
const authCodesCol = () => db.collection('auth_codes');
const collabDocsCol = () => db.collection('collab_docs');           // { appType, ownerEmail, title, collaborators:{email:role}, updatedAt }
const collabGuestsCol = () => db.collection('collab_guests');       // { email, displayName, color, createdAt, lastSeenAt } アカウント無しの仮ユーザー
const collabInvitesCol = () => db.collection('collab_invites');     // { code, docId, role, createdBy, expiresAt, maxUses, uses }

async function getUser(email) {
    const snap = await usersCol().doc(email).get();
    return snap.exists ? snap.data() : null;
}

async function getChat(chatID) {
    const snap = await chatsCol().doc(chatID).get();
    return snap.exists ? snap.data() : null;
}

const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) console.warn('[WARN] JWT_SECRET が設定されていません。認証エンドポイントは機能しません。');

const ALLOWED_ORIGINS = [
    'https://rivift.app',
    'https://rivift-os-html.web.app',
    'https://rivift-os-html.firebaseapp.com',
    'http://localhost:5500',
    'http://localhost:3000',
    'http://127.0.0.1:5500',
    'null', // Chromebook等のローカルファイル (file://) 起動をサポート
];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SSRF対策（/proxy・/extract 共通）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// [SEC] 1) URL文字列の検証  2) DNS解決結果の検証  3) 実際に接続する瞬間のIP検証（DNSリバインディング対策）
//       4) リダイレクトを1ホップずつ手動で追跡して毎回検証  5) ポート制限  6) サイズ・時間の上限
class SsrfError extends Error { constructor(msg) { super(msg); this.name = 'SsrfError'; } }

function _isBlockedIPv4(ip) {
    const parts = ip.split('.').map(Number);
    if (parts.length !== 4 || parts.some(p => !Number.isInteger(p) || p < 0 || p > 255)) return true;
    const [a, b, c] = parts;
    if (a === 0 || a === 10 || a === 127) return true;               // 0/8, 10/8, 127/8
    if (a === 100 && b >= 64 && b <= 127) return true;               // 100.64/10 (CGNAT)
    if (a === 169 && b === 254) return true;                         // 169.254/16 (リンクローカル・クラウドメタデータ)
    if (a === 172 && b >= 16 && b <= 31) return true;                // 172.16/12
    if (a === 192 && b === 0 && (c === 0 || c === 2)) return true;   // 192.0.0/24, 192.0.2/24
    if (a === 192 && b === 88 && c === 99) return true;              // 6to4 relay
    if (a === 192 && b === 168) return true;                         // 192.168/16
    if (a === 198 && (b === 18 || b === 19)) return true;            // ベンチマーク用
    if (a === 198 && b === 51 && c === 100) return true;             // TEST-NET-2
    if (a === 203 && b === 0 && c === 113) return true;              // TEST-NET-3
    if (a >= 224) return true;                                       // マルチキャスト・予約・ブロードキャスト
    return false;
}

// IPv6 文字列を 8個の16bit値に展開（"::" 省略・末尾IPv4表記・ゼロ埋めの揺れに対応）。失敗したら null
function _parseIPv6ToGroups(ip) {
    let str = ip.toLowerCase();
    const zone = str.indexOf('%');
    if (zone >= 0) str = str.slice(0, zone);
    if (str.includes('.')) {
        const lastColon = str.lastIndexOf(':');
        const v4 = str.slice(lastColon + 1);
        if (!net.isIPv4(v4)) return null;
        const p = v4.split('.').map(Number);
        str = `${str.slice(0, lastColon + 1)}${((p[0] << 8) | p[1]).toString(16)}:${((p[2] << 8) | p[3]).toString(16)}`;
    }
    const halves = str.split('::');
    if (halves.length > 2) return null;
    const head = halves[0] ? halves[0].split(':') : [];
    const rest = (halves.length === 2 && halves[1]) ? halves[1].split(':') : [];
    let groups;
    if (halves.length === 2) {
        const fill = 8 - head.length - rest.length;
        if (fill < 1) return null;
        groups = [...head, ...Array(fill).fill('0'), ...rest];
    } else {
        groups = head;
    }
    if (groups.length !== 8) return null;
    const out = groups.map(g => /^[0-9a-f]{1,4}$/.test(g) ? parseInt(g, 16) : NaN);
    return out.some(Number.isNaN) ? null : out;
}

// SSRF対策: プライベート/ループバック/リンクローカル/メタデータ/マルチキャストIPの判定
function isPrivateOrBlockedIp(ipStr) {
    if (!ipStr || typeof ipStr !== 'string') return true;
    let ip = ipStr.trim();
    if (ip.startsWith('[') && ip.endsWith(']')) ip = ip.slice(1, -1);
    if (net.isIPv4(ip)) return _isBlockedIPv4(ip);
    if (net.isIPv6(ip)) {
        const g = _parseIPv6ToGroups(ip);
        if (!g) return true;
        const zeros = (from, to) => g.slice(from, to).every(x => x === 0);
        const v4 = (hi, lo) => `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`;
        if (zeros(0, 6)) return true;                                              // ::, ::1, ::a.b.c.d（IPv4互換）
        if (zeros(0, 5) && g[5] === 0xffff) return _isBlockedIPv4(v4(g[6], g[7])); // ::ffff:a.b.c.d（IPv4射影）
        if (g[0] === 0x64 && g[1] === 0xff9b && zeros(2, 6)) return _isBlockedIPv4(v4(g[6], g[7])); // NAT64
        if (g[0] === 0x2002) return _isBlockedIPv4(v4(g[1], g[2]));                // 6to4
        if (g[0] === 0x2001 && g[1] === 0) return true;                            // Teredo
        if (g[0] === 0x2001 && g[1] === 0x0db8) return true;                       // ドキュメント用
        if ((g[0] & 0xfe00) === 0xfc00) return true;                               // fc00::/7 ユニークローカル
        if ((g[0] & 0xffc0) === 0xfe80) return true;                               // fe80::/10 リンクローカル
        if ((g[0] & 0xffc0) === 0xfec0) return true;                               // fec0::/10 サイトローカル
        if ((g[0] & 0xff00) === 0xff00) return true;                               // マルチキャスト
        if (g[0] === 0x0100 && zeros(1, 4)) return true;                           // 100::/64
        return false;
    }
    return true;
}

// 外向き通信で許可するポート（環境変数 SSRF_ALLOWED_PORTS で上書き可）
const SSRF_ALLOWED_PORTS = new Set(
    (process.env.SSRF_ALLOWED_PORTS || '80,443,8080,8443').split(',').map(x => Number(x.trim())).filter(Boolean)
);
const SSRF_BLOCKED_HOSTS = ['localhost', 'metadata.google.internal', 'metadata', 'instance-data', '169.254.169.254'];
const SSRF_BLOCKED_SUFFIXES = ['.localhost', '.local', '.internal', '.lan', '.intranet', '.corp', '.home', '.home.arpa'];

// SSRF検証ヘルパー (URL形式・ポート・ホスト名・DNS解決結果の全IPを検証)
async function assertSafeUrl(rawUrl) {
    if (!rawUrl || typeof rawUrl !== 'string' || rawUrl.length > 4096) throw new SsrfError('Invalid URL');
    let parsed;
    try {
        parsed = new URL(rawUrl);
    } catch {
        throw new SsrfError('Invalid URL format');
    }
    if (!['http:', 'https:'].includes(parsed.protocol)) throw new SsrfError('Only HTTP/HTTPS protocols are allowed');
    if (parsed.username || parsed.password) throw new SsrfError('URLs with embedded credentials are not allowed');

    const port = parsed.port ? Number(parsed.port) : (parsed.protocol === 'https:' ? 443 : 80);
    if (!SSRF_ALLOWED_PORTS.has(port)) throw new SsrfError('This port is not allowed');

    let hostname = parsed.hostname.toLowerCase().replace(/\.+$/, '');
    if (hostname.startsWith('[') && hostname.endsWith(']')) hostname = hostname.slice(1, -1);
    if (!hostname) throw new SsrfError('Invalid hostname');
    if (SSRF_BLOCKED_HOSTS.includes(hostname) || SSRF_BLOCKED_SUFFIXES.some(sfx => hostname.endsWith(sfx))) {
        throw new SsrfError('Access to internal hostnames is blocked');
    }
    if (net.isIP(hostname)) {
        if (isPrivateOrBlockedIp(hostname)) throw new SsrfError('Access to private/local IP is blocked');
        return parsed;
    }
    const addresses = await dns.lookup(hostname, { all: true, verbatim: true });
    if (!addresses || addresses.length === 0) throw new SsrfError('Could not resolve hostname');
    for (const record of addresses) {
        if (isPrivateOrBlockedIp(record.address)) throw new SsrfError('Access to private/local IP is blocked');
    }
    return parsed;
}

// [SEC] DNSリバインディング対策: 「検証した時のDNS結果」と「実際に接続する時のDNS結果」が違う、という攻撃を防ぐため
// 接続の瞬間に使うDNS解決そのものを検証付きにする（http/https Agent の lookup に差し込む）。
function _ssrfSafeLookup(hostname, options, callback) {
    if (typeof options === 'function') { callback = options; options = {}; }
    options = options || {};
    dnsCb.lookup(hostname, { family: options.family, hints: options.hints, all: true, verbatim: true }, (err, addrs) => {
        if (err) return callback(err);
        if (!addrs || addrs.length === 0 || addrs.some(a => isPrivateOrBlockedIp(a.address))) {
            return callback(new SsrfError('Access to private/local IP is blocked'));
        }
        if (options.all) return callback(null, addrs);
        callback(null, addrs[0].address, addrs[0].family);
    });
}
const _ssrfHttpAgent = new http.Agent({ lookup: _ssrfSafeLookup });
const _ssrfHttpsAgent = new https.Agent({ lookup: _ssrfSafeLookup });
const _ssrfAgentFor = (parsedUrl) => parsedUrl.protocol === 'http:' ? _ssrfHttpAgent : _ssrfHttpsAgent;

const SAFE_FETCH_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36';

// クライアントから渡された任意ヘッダーのうち、中継に不適切なものを除去する（大文字小文字の違いも吸収）
const _OUTGOING_BLOCKED_HEADERS = new Set([
    'host', 'origin', 'referer', 'content-length', 'connection', 'transfer-encoding', 'upgrade', 'te', 'trailer',
    'keep-alive', 'expect', 'forwarded', 'via', 'x-real-ip', 'cf-connecting-ip', 'true-client-ip', 'x-client-ip',
    'x-original-url', 'x-rewrite-url', 'x-http-method-override', 'user-agent',
]);
function _cleanOutgoingHeaders(input) {
    const out = {};
    if (!input || typeof input !== 'object' || Array.isArray(input)) return out;
    let n = 0;
    for (const [k, v] of Object.entries(input)) {
        if (++n > 50) break;
        const name = String(k).toLowerCase();
        if (!/^[a-z0-9!#$%&'*+.^_`|~-]{1,64}$/.test(name)) continue;
        if (_OUTGOING_BLOCKED_HEADERS.has(name) || name.startsWith('proxy-') || name.startsWith('x-forwarded-') || name.startsWith('sec-')) continue;
        if (typeof v !== 'string' || v.length > 4096 || /[\r\n\0]/.test(v)) continue;
        out[name] = v;
    }
    return out;
}

// 検証つきfetch。リダイレクトは手動で1ホップずつ追い、毎回 assertSafeUrl を通す。
// 返す finish() は、レスポンス本文を読み終えた/中断した時に必ず呼ぶ（全体タイムアウトの解除）。
async function safeFetch(startUrl, { method = 'GET', headers = {}, body = null, maxRedirects = 5, timeoutMs = 15000 } = {}) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    const finish = () => clearTimeout(timer);
    try {
        let currentUrl = startUrl;
        let m = method;
        let b = body;
        let h = { ...headers };
        for (let hop = 0; hop <= maxRedirects; hop++) {
            const parsed = await assertSafeUrl(currentUrl);
            const response = await fetch(parsed.href, {
                method: m,
                headers: h,
                body: (m === 'GET' || m === 'HEAD') ? undefined : (b || undefined),
                redirect: 'manual',
                agent: _ssrfAgentFor,
                signal: ac.signal,
                compress: true,
            });
            const loc = response.headers.get('location');
            if (response.status >= 300 && response.status < 400 && loc) {
                try { response.body.destroy(); } catch { /* ignore */ }
                if (hop === maxRedirects) throw new SsrfError('Too many redirects');
                const next = new URL(loc, parsed);
                if (next.origin !== parsed.origin) {
                    // 別オリジンへのリダイレクトでは認証情報・Cookieを持ち越さない
                    h = { ...h };
                    delete h['authorization']; delete h['cookie']; delete h['proxy-authorization'];
                }
                if (response.status === 303 || ((response.status === 301 || response.status === 302) && m === 'POST')) {
                    m = 'GET'; b = null;
                    h = { ...h }; delete h['content-type'];
                }
                currentUrl = next.href;
                continue;
            }
            return { response, finalUrl: parsed.href, finish };
        }
        throw new SsrfError('Too many redirects');
    } catch (e) {
        finish();
        throw e;
    }
}

// 受信バイト数に上限をかけるTransform（上限超過でストリームをエラー終了させる）
function _byteLimiter(maxBytes, onBytes) {
    let total = 0;
    return new Transform({
        transform(chunk, _enc, cb) {
            total += chunk.length;
            if (onBytes) onBytes(chunk.length);
            if (total > maxBytes) return cb(new Error('Response too large'));
            cb(null, chunk);
        },
    });
}
async function _readBodyLimited(stream, maxBytes) {
    const chunks = [];
    let total = 0;
    for await (const c of stream) {
        total += c.length;
        if (total > maxBytes) { try { stream.destroy(); } catch { /* ignore */ } throw new Error('Response too large'); }
        chunks.push(c);
    }
    return Buffer.concat(chunks);
}

// 抽出したHTMLからスクリプト・イベントハンドラ・危険なURLを除去（XSS対策）
function _sanitizeExtractedHtml(html) {
    if (typeof html !== 'string') return html;
    const dom = new JSDOM(`<body>${html}</body>`);
    const doc = dom.window.document;
    doc.querySelectorAll('script,style,iframe,frame,frameset,object,embed,applet,form,input,button,textarea,select,link,meta,base,svg,math,noscript,template')
        .forEach(n => n.remove());
    doc.querySelectorAll('*').forEach(el => {
        for (const attr of Array.from(el.attributes)) {
            const name = attr.name.toLowerCase();
            if (name.startsWith('on') || name === 'style' || name === 'srcdoc' || name === 'formaction') { el.removeAttribute(attr.name); continue; }
            if (['href', 'src', 'xlink:href', 'action', 'poster', 'data'].includes(name)) {
                const compact = attr.value.replace(/[\u0000-\u0020\u007f-\u009f]/g, '').toLowerCase();
                const okDataImg = name === 'src' && /^data:image\/(png|jpe?g|gif|webp);/.test(compact);
                if (/^(javascript|vbscript|data):/.test(compact) && !okDataImg) el.removeAttribute(attr.name);
            }
        }
    });
    return doc.body.innerHTML;
}

// [SEC] "/" と "\" を含むメールはFirestoreのドキュメントパスを壊す（別パスを指せてしまう）ので受け付けない
function isValidEmail(email) {
    return typeof email === 'string' && /^[^\s@\/\\]+@[^\s@\/\\]+\.[^\s@\/\\]+$/.test(email) && email.length <= 254;
}

function sanitizeString(str, maxLen) {
    return typeof str === 'string' ? str.trim().slice(0, maxLen) : '';
}

// [SEC] JWT検証は必ずアルゴリズムを固定する（alg 差し替え攻撃の余地を消す）。sign側は既定の HS256。
function verifyJwt(token) {
    return jwt.verify(token, JWT_SECRET, { algorithms: ['HS256'] });
}

// [SEC] Firestoreのドキュメントidとして安全か（"/" や "." ".." "__x__" はパス/予約名になるため拒否）
function _isSafeDocId(id) {
    return typeof id === 'string' && id.length >= 1 && id.length <= 200
        && !/[\/\\\u0000-\u001f\u007f]/.test(id) && id !== '.' && id !== '..' && !/^__.*__$/.test(id);
}

// 表示名などから制御文字と < > を除く（クライアントが innerHTML で描画しても事故らないように）
function _cleanLabel(str, maxLen) {
    return sanitizeString(str, maxLen * 2).replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, maxLen);
}

// 失敗回数ベースの制限（招待コードの総当たり対策）。成功は数えない。
const _failMap = new Map();
function _failBlocked(key, max, windowMs) {
    const now = Date.now();
    const arr = (_failMap.get(key) || []).filter(t => now - t < windowMs);
    if (arr.length) _failMap.set(key, arr); else _failMap.delete(key);
    return arr.length >= max;
}
function _failRecord(key) {
    const arr = _failMap.get(key) || [];
    arr.push(Date.now());
    _failMap.set(key, arr);
    if (_failMap.size > 5000) {
        const now = Date.now();
        for (const [k, ts] of _failMap.entries()) if (ts.every(t => now - t > 3_600_000)) _failMap.delete(k);
    }
}

// 汎用の「キー単位」レート制限（ユーザー単位など）。_nearbyRateLimit と同じ仕組みを流用。
const _keyedRateLimit = (key, action, maxRequests, windowMs) => _nearbyRateLimit(key, action, maxRequests, windowMs);

// Socket.IO ハンドシェイクからクライアントIPを取る。trust proxy=1 なので、信頼できるのは
// X-Forwarded-For の「いちばん右」（直前のプロキシが付けた値）。左側はクライアントが偽装できる。
function _clientIpFromHandshake(handshake) {
    const xff = handshake.headers?.['x-forwarded-for'];
    if (typeof xff === 'string' && xff) {
        const parts = xff.split(',').map(x => x.trim()).filter(Boolean);
        if (parts.length) return parts[parts.length - 1];
    }
    return handshake.address || 'unknown';
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 共同編集（Office系アプリ共通基盤）: 権限・招待コード
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// role階層: viewer(閲覧のみ) < commenter(コメント可) < editor(編集可) < owner(権限管理可)
const COLLAB_ROLES = ['viewer', 'commenter', 'editor', 'owner'];
const COLLAB_ROLE_RANK = { viewer: 0, commenter: 1, editor: 2, owner: 3 };
const COLLAB_INVITE_MAX_USES = 200;
const COLLAB_INVITE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 招待コードの既定有効期限: 7日
const COLLAB_INVITE_MAX_ACTIVE_PER_DOC = 30;           // 1ドキュメントで同時に有効な招待コードの上限

// [FIX] Firestoreの update({ 'collaborators.user@gmail.com': role }) は「.」をフィールドパスの区切りと
// 解釈するため、実際には collaborators.user@gmail.com という入れ子 { 'user@gmail': { com: role } } で
// 保存されてしまい、権限が一切読み出せなくなっていた（→「アクセス権がありません」）。
// 書き込みは FieldPath で行い、読み取りは過去に壊れた形式で保存されたデータも救済できるよう正規化する。
function _collabNormalize(collaborators) {
    const out = {};
    const walk = (obj, prefix) => {
        for (const [k, v] of Object.entries(obj || {})) {
            const key = prefix ? `${prefix}.${k}` : k;
            if (typeof v === 'string') { if (COLLAB_ROLES.includes(v)) out[key] = v; }
            else if (v && typeof v === 'object') walk(v, key);
        }
    };
    walk(collaborators, '');
    return out;
}

function _collabRoleOf(docData, email) {
    if (!docData || !email) return null;
    if (docData.ownerEmail === email) return 'owner';
    return _collabNormalize(docData.collaborators)[email] || null;
}

// ── ゲスト（Rivift Connect アカウント無しの仮ユーザー）─────────────────────────
// 招待リンク/コードで参加した未ログインの人に、自動で仮ユーザーを発行する。
// 仮ユーザーは「g-xxxx@guest.rivift」という擬似メールで識別し、JWT に guest:true を付ける。
// guest トークンは共同編集(collab)の API / Socket イベントだけに使える（他の API は requireAuth が拒否）。
const COLLAB_GUEST_DOMAIN = 'guest.rivift';
const _collabIsGuestEmail = (email) => typeof email === 'string' && email.endsWith('@' + COLLAB_GUEST_DOMAIN);
const COLLAB_LINK_ROLES = ['none', 'viewer', 'commenter', 'editor'];
const COLLAB_CONTENT_CHUNK = 250000;       // 1チャンクの文字数（日本語3byteでも Firestore の1MiB制限に収まる）
const COLLAB_CONTENT_MAX_CHARS = 8000000;  // 1ドキュメントの保存上限
const GUEST_ANIMALS = ['ねこ', 'いぬ', 'うさぎ', 'きつね', 'たぬき', 'ぱんだ', 'くま', 'ふくろう', 'いるか', 'ぺんぎん', 'りす', 'かめ'];
const GUEST_COLORS = ['#e57373', '#f06292', '#ba68c8', '#7986cb', '#4fc3f7', '#4db6ac', '#81c784', '#ffb74d', '#a1887f', '#90a4ae'];

function _generateLinkCode() {
    const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    const bytes = nodeCrypto.randomBytes(12);
    let code = '';
    for (let i = 0; i < 12; i++) code += CHARS[bytes[i] % CHARS.length];
    return `${code.slice(0, 4)}-${code.slice(4, 8)}-${code.slice(8)}`;
}

async function _collabCreateGuest(name) {
    const id = nodeCrypto.randomBytes(8).toString('hex');
    const email = `g-${id}@${COLLAB_GUEST_DOMAIN}`;
    const rnd = nodeCrypto.randomBytes(2);
    const displayName = sanitizeString(name, 30) || `ゲスト・${GUEST_ANIMALS[rnd[0] % GUEST_ANIMALS.length]}`;
    const color = GUEST_COLORS[rnd[1] % GUEST_COLORS.length];
    await collabGuestsCol().doc(email).set({ email, displayName, color, createdAt: Date.now(), lastSeenAt: Date.now() });
    const token = jwt.sign({ email, displayName, guest: true }, JWT_SECRET, { expiresIn: '30d' });
    return { email, displayName, color, token };
}

// クライアントへ返すドキュメント情報（内部用フィールドは除く／collaborators は正規化済みの { email: role }）
function _collabPublicDoc(id, data, email) {
    const role = _collabRoleOf(data, email);
    const { memberEmails, linkCode, ...rest } = data;
    const pub = {
        id, ...rest,
        collaborators: _collabNormalize(data.collaborators),
        yourRole: role, yourEmail: email,
        linkRole: COLLAB_LINK_ROLES.includes(data.linkRole) ? data.linkRole : 'none',
    };
    if (role === 'owner' && linkCode && pub.linkRole !== 'none') pub.linkCode = linkCode;
    return pub;
}

// 共有メンバー（オーナー含む）を表示用に解決: 名前・アイコン・ゲスト判定・オンライン状況
async function _collabResolveMembers(data) {
    const roles = { [data.ownerEmail]: 'owner', ..._collabNormalize(data.collaborators) };
    const emails = Object.keys(roles).slice(0, 200);
    const accounts = emails.filter(e => !_collabIsGuestEmail(e));
    const guests = emails.filter(_collabIsGuestEmail);
    const [uSnaps, gSnaps] = await Promise.all([
        accounts.length ? db.getAll(...accounts.map(e => usersCol().doc(e))) : [],
        guests.length ? db.getAll(...guests.map(e => collabGuestsCol().doc(e))) : [],
    ]);
    const info = {};
    uSnaps.forEach(sn => { if (sn.exists) { const d = sn.data(); info[sn.id] = { displayName: d.displayName || sn.id, icon: (typeof d.icon === 'string' && d.icon.length <= 150000) ? d.icon : null }; } });
    gSnaps.forEach(sn => { if (sn.exists) { const d = sn.data(); info[sn.id] = { displayName: d.displayName || 'ゲスト', color: d.color || '#90a4ae' }; } });
    const members = emails.map(e => ({
        email: e, role: roles[e], guest: _collabIsGuestEmail(e),
        displayName: info[e]?.displayName || (_collabIsGuestEmail(e) ? 'ゲスト' : e.split('@')[0]),
        icon: info[e]?.icon || null, color: info[e]?.color || null,
        online: isUserOnline(e),
    }));
    members.sort((a, b) => (COLLAB_ROLE_RANK[b.role] - COLLAB_ROLE_RANK[a.role]) || a.displayName.localeCompare(b.displayName, 'ja'));
    return members;
}

// コラボレーターの権限を書き込む（role が null なら削除）。memberEmails（一覧検索用）も同時に更新する。
async function _collabSetRole(docRef, docData, email, role) {
    const raw = docData.collaborators || {};
    const hasLegacy = Object.values(raw).some(v => v && typeof v === 'object');
    if (hasLegacy) {
        // 過去の壊れた形式が残っている場合は、正規化したマップで丸ごと置き換えて修復する
        const next = _collabNormalize(raw);
        if (role) next[email] = role; else delete next[email];
        await docRef.update({ collaborators: next, memberEmails: [docData.ownerEmail, ...Object.keys(next)], updatedAt: Date.now() });
        _collabInvalidateRoles(docRef.id);
        return;
    }
    await docRef.update(
        new admin.firestore.FieldPath('collaborators', email),
        role ? role : admin.firestore.FieldValue.delete(),
        'memberEmails', role ? admin.firestore.FieldValue.arrayUnion(email) : admin.firestore.FieldValue.arrayRemove(email),
        'updatedAt', Date.now()
    );
    _collabInvalidateRoles(docRef.id);
}

class CollabJoinError extends Error { constructor(status, msg) { super(msg); this.status = status; } }

// 招待コード or 共有リンクのコードから、参加先ドキュメントと付与する権限を解決する
async function _collabResolveJoinCode(rawCode) {
    const code = sanitizeString(rawCode, 40).toUpperCase();
    if (!code) return { error: [400, '招待コードを入力してください'] };
    // [SEC] コードはFirestoreのドキュメントidとして使うので、生成される文字種だけに絞る（"/" 等でパスを壊されない）
    if (!/^[A-Z0-9-]{4,40}$/.test(code)) return { error: [404, '招待コードが無効です'] };
    const inviteSnap = await collabInvitesCol().doc(code).get();
    if (inviteSnap.exists) {
        const invite = inviteSnap.data();
        // 数値でない/欠けている値は「無効」扱い（比較が常に false になって素通りするのを防ぐ）
        if (!(Number(invite.expiresAt) >= Date.now())) return { error: [410, '招待コードの有効期限が切れています'] };
        if (!((Number(invite.uses) || 0) < (Number(invite.maxUses) || 0))) return { error: [410, '招待コードの利用上限に達しています'] };
        if (!COLLAB_LINK_ROLES.includes(invite.role) || invite.role === 'none' || !_isSafeDocId(invite.docId)) {
            return { error: [404, '招待コードが無効です'] };
        }
        const docRef = collabDocsCol().doc(invite.docId);
        const docSnap = await docRef.get();
        if (!docSnap.exists) return { error: [404, 'ドキュメントが見つかりません'] };
        return { docRef, docData: docSnap.data(), docId: invite.docId, role: invite.role, inviteRef: inviteSnap.ref };
    }
    const q = await collabDocsCol().where('linkCode', '==', code).limit(1).get();
    if (q.empty) return { error: [404, '招待コードが無効です'] };
    const d = q.docs[0], data = d.data();
    if (!COLLAB_LINK_ROLES.includes(data.linkRole) || data.linkRole === 'none') {
        return { error: [410, 'このドキュメントのリンク共有は停止されています'] };
    }
    return { docRef: d.ref, docData: data, docId: d.id, role: data.linkRole };
}

// 解決済みの招待で email を参加させる（既存の権限より低くは下げない）。更新後のドキュメントを返す。
// [SEC] 招待コードの使用回数は「権限を付ける前に」トランザクションで確保する。
// 以前は「権限付与 → あとから +1」だったため、同時に何人も参加すると maxUses を超えて参加できた。
// すでに同等以上の権限を持つ人（オーナー含む）は、使用回数を消費しない。
async function _collabJoinWith(resolved, email) {
    const { docRef, docData, role, inviteRef } = resolved;
    const cur = docData.ownerEmail === email ? 'owner' : _collabRoleOf(docData, email);
    const needsGrant = !cur || COLLAB_ROLE_RANK[role] > (COLLAB_ROLE_RANK[cur] ?? -1);
    if (needsGrant) {
        if (inviteRef) {
            const reserved = await db.runTransaction(async (tx) => {
                const snap = await tx.get(inviteRef);
                if (!snap.exists) return false;
                const inv = snap.data();
                const uses = Number(inv.uses) || 0;
                if (!(Number(inv.expiresAt) >= Date.now()) || uses >= (Number(inv.maxUses) || 0)) return false;
                tx.update(inviteRef, { uses: uses + 1 });
                return true;
            });
            if (!reserved) throw new CollabJoinError(410, '招待コードの有効期限が切れているか、利用上限に達しています');
            try {
                await _collabSetRole(docRef, docData, email, role);
            } catch (e) {
                // 権限付与に失敗したら、確保した使用回数を返す
                inviteRef.update({ uses: admin.firestore.FieldValue.increment(-1) }).catch(() => { });
                throw e;
            }
        } else {
            await _collabSetRole(docRef, docData, email, role);
        }
    }
    return (await docRef.get()).data();
}

function _collabNotifyJoined(docId, updated, email) {
    io.to(`collab:${docId}`).emit('collab_collaborators_updated', { docId });
    [updated.ownerEmail, ...Object.keys(_collabNormalize(updated.collaborators))].forEach(e => {
        if (e !== email) io.to(_userRoom(e)).emit('collab_collaborator_joined', { docId, email });
    });
}

function _collabHasAtLeast(role, required) {
    if (!role) return false;
    return (COLLAB_ROLE_RANK[role] ?? -1) >= (COLLAB_ROLE_RANK[required] ?? 99);
}

function _generateInviteCode() {
    // 見間違いにくい文字だけを使う（0/O, 1/I を除外）、CSPRNGで暗号論的に安全に生成
    const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    const bytes = nodeCrypto.randomBytes(8);
    for (let i = 0; i < 8; i++) code += CHARS[bytes[i] % CHARS.length];
    return `${code.slice(0, 4)}-${code.slice(4)}`;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 共同編集: カーソル状態追跡 & オペレーション変換（OT競合解消）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const _collabRoomCursors = new Map(); // docId -> Map<email, { docId, from, displayName, color, cursor, updatedAt }>
const _collabDocStates = new Map();   // docId -> { lastActive, pages: Map<pageIndex, { rev, opsHistory: [] }> }

// [SEC] 共同編集のリアルタイム操作で使う入力の上限
const COLLAB_MAX_PAGES = 500;                  // 1ドキュメントのページ数上限（PUT /content と同じ）
const COLLAB_OP_MAX_HTML = 4_000_000;          // full 操作で送れるHTMLの最大文字数
const COLLAB_OP_MAX_INSERT = 600_000;          // patch.insert の最大文字数（クライアントの閾値 500,000 に余裕を持たせた値）
const COLLAB_SNAPSHOT_MAX_CHARS = 5_000_000;   // スナップショット全体の最大文字数
const COLLAB_MAX_ROOMS_PER_SOCKET = 30;        // 1接続が同時に参加できるドキュメント数

// [SEC] 権限を毎イベントFirestoreで読むと重いので、ごく短時間だけキャッシュする。
// 権限を変える全ての経路（_collabSetRole / 一括操作 / 解除）で invalidate するので、反映の遅れは出ない。
// （サーバーを複数台にする場合は、TTL（5秒）が最大の反映遅延になる）
const COLLAB_ROLE_CACHE_TTL = 5000;
const _collabRoleCache = new Map(); // `${docId}\n${email}` -> { role, exp }
function _collabRoleCacheSet(docId, email, role) {
    if (_collabRoleCache.size > 20000) _collabRoleCache.clear();
    _collabRoleCache.set(`${docId}\n${email}`, { role, exp: Date.now() + COLLAB_ROLE_CACHE_TTL });
}
function _collabInvalidateRoles(docId) {
    const prefix = `${docId}\n`;
    for (const k of _collabRoleCache.keys()) if (k.startsWith(prefix)) _collabRoleCache.delete(k);
}
async function _collabGetRole(docId, email) {
    const key = `${docId}\n${email}`;
    const hit = _collabRoleCache.get(key);
    if (hit && hit.exp > Date.now()) return hit.role;
    const snap = await collabDocsCol().doc(docId).get();
    const role = snap.exists ? _collabRoleOf(snap.data(), email) : null;
    _collabRoleCacheSet(docId, email, role);
    return role;
}

// 共有解除などで権限を失ったユーザーを、リアルタイムの部屋・カーソル表示から即座に外す
function _collabKick(docId, email) {
    _collabInvalidateRoles(docId);
    io.in(_userRoom(email)).socketsLeave(`collab:${docId}`);
    const rc = _collabRoomCursors.get(docId);
    if (rc && rc.delete(email)) {
        io.to(`collab:${docId}`).emit('collab_cursor_remove', { docId, from: email });
        if (rc.size === 0) _collabRoomCursors.delete(docId);
    }
    io.to(`collab:${docId}`).emit('collab_presence_left', { docId, email });
}

// 誰もいない部屋の編集状態（リビジョン・履歴）がメモリに溜まり続けないよう、30分使われなければ捨てる
setInterval(() => {
    const now = Date.now();
    for (const [docId, st] of _collabDocStates) {
        const occupied = (io.sockets.adapter.rooms.get(`collab:${docId}`)?.size || 0) > 0;
        if (!occupied && now - (st.lastActive || 0) > 30 * 60_000) _collabDocStates.delete(docId);
    }
}, 10 * 60_000).unref();

// collab_op の中身を検証し、既知のフィールドだけを取り出した新しいオブジェクトを返す（不正なら null）
function _collabValidateOp(op) {
    if (!op || typeof op !== 'object' || Array.isArray(op)) return null;
    const pageIndex = op.pageIndex === undefined ? 0 : op.pageIndex;
    if (!Number.isInteger(pageIndex) || pageIndex < 0 || pageIndex >= COLLAB_MAX_PAGES) return null;
    const clean = { pageIndex };
    if (typeof op.clientOpId === 'string' && op.clientOpId.length <= 100) clean.clientOpId = op.clientOpId;
    if (op.baseRev !== undefined && op.baseRev !== null) {
        const b = Number(op.baseRev);
        if (!Number.isInteger(b) || b < 0) return null;
        clean.baseRev = b;
    }
    if (op.baseLen !== undefined && op.baseLen !== null) {
        const bl = Number(op.baseLen);
        if (Number.isFinite(bl) && bl >= 0) clean.baseLen = bl;
    }
    if (op.full) {
        if (typeof op.html !== 'string' || op.html.length > COLLAB_OP_MAX_HTML) return null;
        clean.full = true;
        clean.html = op.html;
        return clean;
    }
    const p = op.patch;
    if (!p || typeof p !== 'object') return null;
    const start = Number(p.start);
    const delCount = Number(p.delCount ?? 0);
    const insert = p.insert === undefined ? '' : p.insert;
    if (!Number.isInteger(start) || start < 0 || start > 50_000_000) return null;
    if (!Number.isInteger(delCount) || delCount < 0 || delCount > 50_000_000) return null;
    if (typeof insert !== 'string' || insert.length > COLLAB_OP_MAX_INSERT) return null;
    clean.patch = { start, delCount, insert };
    return clean;
}

function _transformPatch(p1, p2) {
    if (!p1 || !p2) return p1;
    let start = Number(p1.start ?? 0);
    let delCount = Number(p1.delCount ?? 0);
    const insert = typeof p1.insert === 'string' ? p1.insert : '';
    const p2Start = Number(p2.start ?? 0);
    const p2Del = Number(p2.delCount ?? 0);
    const p2Ins = typeof p2.insert === 'string' ? p2.insert.length : 0;
    const delta2 = p2Ins - p2Del;

    if (p2Start <= start) {
        if (p2Start + p2Del <= start) {
            start += delta2;
        } else {
            const overlap = Math.min(p2Start + p2Del, start + delCount) - start;
            start = p2Start + p2Ins;
            delCount = Math.max(0, delCount - overlap);
        }
    } else if (p2Start < start + delCount) {
        delCount += delta2;
    }
    return { start: Math.max(0, start), delCount: Math.max(0, delCount), insert };
}

const app = express();
app.set('trust proxy', 1);
app.use(helmet());

app.use(cors({
    origin: (origin, cb) => {
        if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
        cb(new Error('Not allowed by CORS'));
    },
    credentials: true,
}));

app.use((err, req, res, next) => {
    if (err && err.message === 'Not allowed by CORS') {
        return res.status(403).json({ error: '許可されていないオリジンからのアクセスです' });
    }
    next(err);
});

app.use((req, res, next) => (req.method === 'PUT' && /^\/collab\/docs\/[^/]+\/content$/.test(req.path)) ? next() : express.json({ limit: '1mb' })(req, res, next));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: ALLOWED_ORIGINS, methods: ["GET", "POST"] },
    maxHttpBufferSize: 5e6,
    // [FIX] 死んだ接続(ゴースト)を早く検出する。既定(25s+20s)だと切断検出が最大45秒遅れる
    pingInterval: 10000,
    pingTimeout: 10000,
    perMessageDeflate: {
        threshold: 1024,
    },
});
const PORT = 3000;

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;

// [FIX] オンライン管理を「メール→socketId 1個」から Socket.IO のルーム方式に変更。
// 旧方式は、古い接続の disconnect が遅れて届くと「新しい接続の登録」まで消してしまい、
// 相手がオンラインなのにオフライン扱い（通話・メッセージが届かない）になっていた。
// ルームは接続ごとに自動で出入りするので競合しない。複数タブ/複数端末にも同時に届く。
// 既存コードの `onlineUsers[email]` → `io.to(...)` はそのまま動くよう Proxy で互換を保つ。
const _userRoom = (email) => `user:${email}`;
const isUserOnline = (email) => !!email && ((io.sockets.adapter.rooms.get(_userRoom(email))?.size || 0) > 0);
const onlineUsers = new Proxy({}, {
    get: (_t, email) => (typeof email === 'string' && isUserOnline(email)) ? _userRoom(email) : undefined,
    set: () => true,
    deleteProperty: () => true,
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 近くの人モード（Nearby Share）: ログイン不要・匿名の状態管理
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ip: 同一ネットワーク（≒同じグローバルIP）ごとにグルーピングするためのキー
// nearbyRooms: Map<ip, Map<nickname, { socketId, icon }>>  — そのIPグループ内で今ONの端末一覧
// nearbySocketInfo: Map<socketId, { nickname, ip }>  — 逆引き用（disconnect/リレー時に使用）
const nearbyRooms = new Map();
const nearbySocketInfo = new Map();
const NEARBY_ROOM_MAX = 200; // 1グループあたりの同時参加上限（乱用・過負荷対策）
const NEARBY_ANIMALS = [
    'しろくま', 'うさぎ', 'ぺんぎん', 'こあら', 'たぬき', 'きつね', 'ぱんだ', 'いるか',
    'からす', 'ふくろう', 'とら', 'らいおん', 'ぞう', 'きりん', 'かめ', 'いぬ', 'ねこ',
    'うま', 'ひつじ', 'やぎ', 'くじゃく', 'はりねずみ', 'かわうそ', 'あざらし', 'わに',
    'とかげ', 'りす', 'はむすたー', 'もぐら', 'うぐいす',
];

function _getNearbyGroupKey(handshake) {
    // [SEC] プロキシ配下では handshake.address (trust proxy有効時) または x-forwarded-for を安全に参照
    const raw = handshake.address || handshake.headers['x-forwarded-for']?.split(',')[0]?.trim() || 'unknown';
    return raw;
}

function _generateNearbyNickname(existingNicknames) {
    for (let i = 0; i < 20; i++) {
        const animal = NEARBY_ANIMALS[Math.floor(Math.random() * NEARBY_ANIMALS.length)];
        const num = Math.floor(1000 + Math.random() * 9000);
        const nickname = `${animal}-${num}`;
        if (!existingNicknames.has(nickname)) return nickname;
    }
    return `ゲスト-${Date.now() % 10000}`; // フォールバック（衝突しても実用上は問題ない一意性）
}

function _nearbyPeerList(ip, excludeSocketId) {
    const room = nearbyRooms.get(ip);
    if (!room) return [];
    const peers = [];
    for (const [nickname, info] of room.entries()) {
        if (info.socketId === excludeSocketId) continue;
        peers.push({ nickname, icon: info.icon || null });
    }
    return peers;
}

function _broadcastNearbyPeers(nsp, ip) {
    const room = nearbyRooms.get(ip);
    if (!room) return;
    for (const [nickname, info] of room.entries()) {
        nsp.to(info.socketId).emit('peers_updated', { peers: _nearbyPeerList(ip, info.socketId) });
    }
}

// 近くの人モード専用の簡易レート制限（IPごと）
const _nearbyRateMap = new Map();
function _nearbyRateLimit(ip, action, maxRequests, windowMs) {
    const key = `${ip}:${action}`;
    const now = Date.now();
    const win = windowMs || 60_000;
    const timestamps = (_nearbyRateMap.get(key) || []).filter(t => now - t < win);
    if (timestamps.length >= maxRequests) return false;
    timestamps.push(now);
    _nearbyRateMap.set(key, timestamps);
    if (_nearbyRateMap.size > 2000) {
        for (const [k, ts] of _nearbyRateMap.entries()) {
            if (ts.every(t => now - t > win)) _nearbyRateMap.delete(k);
        }
    }
    return true;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App Store: HTML圧縮ヘルパー
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const APP_HTML_MAX_SIZE = 3_000_000;

async function compressHtmlContent(html) {
    try {
        const buf = await gzip(Buffer.from(html, 'utf8'), { level: 9 });
        return 'gz:' + buf.toString('base64');
    } catch (e) {
        console.warn('[AppStore] HTML圧縮失敗、平文で保存します:', e.message);
        return html;
    }
}

async function decompressHtmlContent(stored) {
    if (typeof stored === 'string' && stored.startsWith('gz:')) {
        try {
            const buf = await gunzip(Buffer.from(stored.slice(3), 'base64'));
            return buf.toString('utf8');
        } catch (e) {
            console.warn('[AppStore] HTML解凍失敗:', e.message);
            return stored;
        }
    }
    return stored;
}

const _listCache = new Map();
const LIST_CACHE_TTL = 30_000;
const LIST_CACHE_MAX = 20;

function _cacheGet(key) {
    const entry = _listCache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expireAt) { _listCache.delete(key); return null; }
    return entry.data;
}

function _cacheSet(key, data) {
    if (_listCache.size >= LIST_CACHE_MAX) {
        const oldest = [..._listCache.entries()].sort((a, b) => a[1].expireAt - b[1].expireAt)[0];
        if (oldest) _listCache.delete(oldest[0]);
    }
    _listCache.set(key, { data, expireAt: Date.now() + LIST_CACHE_TTL });
}

function _cacheInvalidate() {
    _listCache.clear();
}

const _commentCache = new Map();
const COMMENT_CACHE_TTL = 30_000;
const COMMENT_CACHE_MAX = 50;

function _commentCacheGet(key) {
    const entry = _commentCache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expireAt) { _commentCache.delete(key); return null; }
    return entry.data;
}

function _commentCacheSet(key, data) {
    if (_commentCache.size >= COMMENT_CACHE_MAX) {
        const oldest = [..._commentCache.entries()].sort((a, b) => a[1].expireAt - b[1].expireAt)[0];
        if (oldest) _commentCache.delete(oldest[0]);
    }
    _commentCache.set(key, { data, expireAt: Date.now() + COMMENT_CACHE_TTL });
}

function _commentCacheInvalidate(appId) {
    for (const key of _commentCache.keys()) {
        if (key.startsWith(`${appId}:`)) _commentCache.delete(key);
    }
}

const _rateLimitMap = new Map();
const RATE_LIMIT_MAX = 10;
const RATE_LIMIT_WINDOW = 60_000;

function _rateLimit(req, endpoint, maxRequests) {
    // [SEC] app.set('trust proxy', 1) により信頼されたプロキシ経由のクライアント実IP (req.ip) を使用
    const ip = req.ip || req.socket?.remoteAddress || 'unknown';
    const key = `${ip}:${endpoint}`;
    const now = Date.now();
    const limit = maxRequests || RATE_LIMIT_MAX;
    const timestamps = (_rateLimitMap.get(key) || []).filter(t => now - t < RATE_LIMIT_WINDOW);
    if (timestamps.length >= limit) return false;
    timestamps.push(now);
    _rateLimitMap.set(key, timestamps);
    if (_rateLimitMap.size > 1000) {
        for (const [k, ts] of _rateLimitMap.entries()) {
            if (ts.every(t => now - t > RATE_LIMIT_WINDOW)) _rateLimitMap.delete(k);
        }
    }
    return true;
}

function globalRateLimit(req, res, next) {
    if (req.path.startsWith('/chat/blob/')) return next(); // ファイル分割転送は専用の上限(_rateLimit 'blob')で数える
    if (!_rateLimit(req, 'global', 120)) {
        return res.status(429).json({ error: 'リクエストが多すぎます。しばらく待ってから再試行してください。' });
    }
    next();
}
app.use(globalRateLimit);

function requireAuth(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: '認証が必要です' });
    if (!JWT_SECRET) return res.status(500).json({ error: 'サーバー設定エラー' });
    try {
        req.user = verifyJwt(token);
    } catch {
        return res.status(401).json({ error: 'トークンが無効または期限切れです' });
    }
    // [SEC] ゲスト（仮ユーザー）のトークンは共同編集以外のAPIでは使えない
    if (req.user.guest) return res.status(403).json({ error: 'ゲストはこの操作を行えません。Rivift Connect アカウントでサインインしてください' });
    next();
}

// 共同編集(collab)用: ゲストのトークンも許可する
function requireCollabAuth(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: '認証が必要です' });
    if (!JWT_SECRET) return res.status(500).json({ error: 'サーバー設定エラー' });
    try {
        req.user = verifyJwt(token);
        next();
    } catch {
        res.status(401).json({ error: 'トークンが無効または期限切れです' });
    }
}

// [SEC] JWTを「あれば検証、無くても先に進む」任意認証ヘルパー。
// /getUserData のように、未ログイン状態(パスワード移行ブートストラップ)でも
// 呼ばれる必要があるエンドポイントで、「本人かどうか」を判定するために使う。
function optionalAuth(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '');
    req.user = null;
    if (token && JWT_SECRET) {
        try { req.user = verifyJwt(token); if (req.user.guest) req.user = null; } catch { /* 未認証として扱う */ }
    }
    next();
}

app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/ping', (req, res) => res.json({ ok: true }));

app.post('/createUser', async (req, res) => {
    if (!_rateLimit(req, 'create-user', 3)) return res.status(429).json({ error: 'Too many requests' });
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, passwordHash } = req.body;
        if (!isValidEmail(email)) return res.status(400).json({ error: 'メールアドレスが無効です' });
        const safeDisplayName = sanitizeString(displayName, 50);
        if (!safeDisplayName) return res.status(400).json({ error: '表示名は必須です' });
        const existing = await getUser(email);
        if (existing) return res.status(400).json({ error: 'User already exists' });
        const userData = {
            email, displayName: safeDisplayName, publicKeyJwk, encryptedPrivateKeyPayload,
            icon: icon || null, friends: [], requests: [], sentRequests: []
        };
        if (passwordHash && passwordHash.salt && passwordHash.hash) {
            userData.passwordHash = passwordHash;
        }
        await usersCol().doc(email).set(userData);
        res.json({ success: true });
    } catch (e) { console.error('createUser error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ユーザーデータ取得
// [SEC] このエンドポイントは「自分のアカウント情報の取得」と「他人（友達・チャット相手）の
// 公開プロフィール取得」の両方に使われる。かつ、パスワード未移行の古いアカウントは
// ここで返す encryptedPrivateKeyPayload をクライアント側で復号できるかどうかが
// 本人確認の唯一の手段になっている（移行ブートストラップ）ため、完全に認証必須化はできない。
// そのため optionalAuth で「JWTがあれば本人確認に使う」方式にし、
// 本人確認できたか・対象アカウントの状態に応じて返すフィールドを絞る。
app.post('/getUserData', optionalAuth, async (req, res) => {
    try {
        const { email } = req.body;
        if (!isValidEmail(email)) return res.status(400).json({ error: 'メールアドレスが無効です' });
        const userData = await getUser(email);
        if (!userData) return res.json({ userData: null });

        const isSelf = req.user && req.user.email === email;

        // [SEC] passwordHashは、本人かどうかに関わらず絶対に誰にも返さない
        const { passwordHash, friends, requests, sentRequests, encryptedPrivateKeyPayload, ...publicFields } = userData;

        if (isSelf) {
            return res.json({ userData: { ...publicFields, friends, requests, sentRequests, encryptedPrivateKeyPayload } });
        }

        if (!userData.passwordHash) {
            // [互換] パスワード未移行のレガシーアカウント：
            // encryptedPrivateKeyPayloadの復号成功自体が本人確認の手段になっている
            // 既存フローのため、ここだけ例外的に返す。
            return res.json({ userData: { ...publicFields, encryptedPrivateKeyPayload } });
        }

        // 他人の情報（友達・チャット相手の公開プロフィール取得）：公開情報のみ
        res.json({ userData: publicFields });
    } catch (e) { console.error('getUserData error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/getSidebarData', requireAuth, async (req, res) => {
    try {
        const { email } = req.body;
        // [SEC] friends/requests/sentRequestsは本人の非公開データなので、
        // 認証済み本人以外が他人のemailを指定して取得できないようにする。
        if (req.user.email !== email) return res.status(403).json({ error: '権限がありません' });
        const user = await getUser(email);
        if (!user) return res.status(404).json({ error: 'User not found' });

        const { friends = [], requests = [], sentRequests = [] } = user;
        const contactEmails = [...new Set([...friends, ...requests, ...sentRequests])];

        const usersData = {};
        const chunks = [];
        for (let i = 0; i < contactEmails.length; i += 30) chunks.push(contactEmails.slice(i, i + 30));
        for (const chunk of chunks) {
            if (chunk.length === 0) continue;
            const snaps = await usersCol().where(admin.firestore.FieldPath.documentId(), 'in', chunk).get();
            snaps.forEach(doc => {
                const d = doc.data();
                usersData[doc.id] = { displayName: d.displayName, icon: d.icon };
            });
        }

        const unreadCounts = {};
        await Promise.all((friends || []).map(async (friendEmail) => {
            try {
                const chatID = [email, friendEmail].sort().join('__');
                const messagesSnap = await chatsCol().doc(chatID).collection('messages')
                    .where('to', '==', email)
                    .where('read', '==', false)
                    .limit(100)
                    .get();
                unreadCounts[friendEmail] = messagesSnap.size;
            } catch (err) {
                // インデックス未作成等のフォールバック
                try {
                    const chatID = [email, friendEmail].sort().join('__');
                    const messagesSnap = await chatsCol().doc(chatID).collection('messages')
                        .where('to', '==', email)
                        .limit(50)
                        .get();
                    unreadCounts[friendEmail] = messagesSnap.docs.filter(doc => doc.data().read !== true).length;
                } catch (e2) {
                    unreadCounts[friendEmail] = 0;
                }
            }
        }));

        res.json({ friends, requests, sentRequests, usersData, unreadCounts });
    } catch (e) { console.error('getSidebarData error:', e); res.status(500).json({ error: 'Server error' }); }
});

// [SEC] 認証必須 + 「自分・友達・受信/送信中の友達申請」の相手だけに限定。
// 以前は認証なしで任意のメールアドレス一覧を渡すと、表示名・アイコン・公開鍵をまとめて取得できた（アカウント列挙）。
app.post('/getUsersData', requireAuth, async (req, res) => {
    try {
        const me = req.user.email;
        const raw = Array.isArray(req.body?.emails) ? req.body.emails : [];
        const emails = [...new Set(raw.filter(isValidEmail))].slice(0, 100);
        const usersData = {};
        if (emails.length === 0) return res.json({ usersData });
        if (!_keyedRateLimit(`u:${me}`, 'getUsersData', 60, 60_000)) return res.status(429).json({ error: 'リクエストが多すぎます' });

        const meData = await getUser(me);
        const allowed = new Set([me, ...(meData?.friends || []), ...(meData?.requests || []), ...(meData?.sentRequests || [])]);
        const targets = emails.filter(e => allowed.has(e));

        const chunks = [];
        for (let i = 0; i < targets.length; i += 30) chunks.push(targets.slice(i, i + 30));
        for (const chunk of chunks) {
            const snaps = await usersCol().where(admin.firestore.FieldPath.documentId(), 'in', chunk).get();
            snaps.forEach(doc => {
                const d = doc.data();
                usersData[doc.id] = { displayName: d.displayName, icon: d.icon, publicKeyJwk: d.publicKeyJwk };
            });
        }
        res.json({ usersData });
    } catch (e) { console.error('getUsersData error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/getMessageHistory', requireAuth, async (req, res) => {
    try {
        const { user1, user2, limit = 50, skip = 0, lastTimestamp = null } = req.body;
        // [SEC] 会話の当事者本人以外はメッセージ履歴を取得できないようにする
        if (req.user.email !== user1 && req.user.email !== user2) return res.status(403).json({ error: '権限がありません' });
        const chatID = [user1, user2].sort().join('__');
        let q = chatsCol().doc(chatID).collection('messages')
            .orderBy('timestamp', 'desc');
        if (lastTimestamp) {
            q = q.startAfter(lastTimestamp);
        } else if (skip && Number.isInteger(skip) && skip > 0) {
            q = q.offset(skip);
        }
        q = q.limit(Math.min(limit, 100));
        const snap = await q.get();
        const history = snap.docs.map(d => d.data()).reverse();
        res.json({ history });
    } catch (e) { console.error('getMessageHistory error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/saveMessage', requireAuth, async (req, res) => {
    try {
        const { user1, user2, message } = req.body;
        // [SEC] 当事者本人以外が他人のチャットにメッセージを書き込めてしまう問題を修正。
        // message.fromも信用せず、認証済み本人のメールで上書きする。
        if (req.user.email !== user1 && req.user.email !== user2) return res.status(403).json({ error: '権限がありません' });
        const chatID = [user1, user2].sort().join('__');
        const chatRef = chatsCol().doc(chatID);
        await chatRef.set({
            users: [user1, user2],
            messages: admin.firestore.FieldValue.arrayUnion({ chatID, ...message, from: req.user.email })
        }, { merge: true });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/search', async (req, res) => {
    const query = req.query.q;
    if (!query) return res.status(400).json({ error: 'Query parameter "q" is required.' });
    try {
        const response = await axios.get('https://api.duckduckgo.com/', {
            params: { q: query, format: 'json', no_html: 1, skip_disambig: 1 },
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
        });
        const results = (response.data.RelatedTopics || [])
            .filter(t => t.Result)
            .map(t => ({ title: t.Text, description: t.Result.replace(/<[^>]*>?/gm, ''), url: t.FirstURL }));
        res.json({ results });
    } catch (e) { res.status(500).json({ error: 'Failed to fetch search results.' }); }
});

let totalDataProxied = 0;
// [SEC] 認証必須（以前は誰でも利用状況を取得できた）
app.get('/getProxyStats', requireAuth, (req, res) => {
    res.json({ totalDataProxied, totalDataProxiedMB: (totalDataProxied / (1024 * 1024)).toFixed(2) });
});

function rewriteHtmlPaths(html, baseUrl) {
    const base = new URL(baseUrl);
    const domain = base.origin;
    const rewrite = (content) => {
        content = content.replace(/(src|href|action|data-src)=(["'])(\/(?!\/)[^"']+)(["'])/g,
            (match, attr, q1, path, q2) => `${attr}=${q1}${domain}${path}${q2}`);
        content = content.replace(/(src|href|action|data-src)=(["'])(?!\/|https?:|data:)([^"']+)(["'])/g,
            (match, attr, q1, path, q2) => {
                const newUrl = new URL(path, baseUrl).href;
                return `${attr}=${q1}${newUrl}${q2}`;
            });
        return content;
    };
    let rewrittenHtml = rewrite(html);
    rewrittenHtml = rewrittenHtml.replace(/<style([^>]*)>([\s\S]*?)<\/style>/gi, (match, attrs, css) => {
        const rewrittenCss = css.replace(/url\((["']?)(?!\/|https?:|data:)([^"'\)]+)(["']?)\)/g, (match, q1, path, q2) => {
            const newUrl = new URL(path, baseUrl).href;
            return `url(${q1}${newUrl}${q2})`;
        });
        return `<style${attrs}>${rewrittenCss}</style>`;
    });
    return rewrittenHtml;
}

app.get('/iframe-helper.js', (req, res) => {
    res.sendFile(path.join(__dirname, 'iframe-helper.js'));
});

const PROXY_MAX_BYTES = 25 * 1024 * 1024;   // 1リクエストで中継するレスポンスの上限
const PROXY_MAX_REQUEST_BODY = 1_400_000;    // 中継するリクエストボディ(base64文字数)の上限
const _PROXY_ALLOWED_METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'];

function _proxyHeadersJson(headers) {
    const obj = {};
    for (const [k, v] of headers) {
        const lk = k.toLowerCase();
        if (lk === 'set-cookie' || lk === 'set-cookie2') continue;   // 第三者サイトのCookieは中継しない
        obj[lk] = v;
    }
    // HTTPヘッダーに載せられるよう ASCII にエスケープ（JSONとしては同じ内容）
    let json = JSON.stringify(obj).replace(/[\u007f-\uffff]/g, c => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'));
    if (json.length > 8000) json = '{}';
    return json;
}

// [SEC] 認証必須 + SSRF対策（DNSリバインディング・リダイレクト・ポート・サイズ/時間の上限）
app.post('/proxy', requireAuth, async (req, res) => {
    let finish = () => { };
    try {
        const { url, method, headers, body } = req.body || {};
        if (!url || typeof url !== 'string') return res.status(400).send('URL is required.');
        if (!_rateLimit(req, 'proxy', 30) || !_keyedRateLimit(`u:${req.user.email}`, 'proxy', 60, 60_000)) {
            return res.status(429).send('Rate limit exceeded.');
        }
        const m = (typeof method === 'string' && _PROXY_ALLOWED_METHODS.includes(method.toUpperCase())) ? method.toUpperCase() : 'GET';
        let bodyBuf = null;
        if (body && typeof body === 'string' && m !== 'GET' && m !== 'HEAD') {
            if (body.length > PROXY_MAX_REQUEST_BODY) return res.status(413).send('Request body too large.');
            bodyBuf = Buffer.from(body, 'base64');
        }
        const outHeaders = _cleanOutgoingHeaders(headers);
        outHeaders['user-agent'] = SAFE_FETCH_UA;

        const r = await safeFetch(url, { method: m, headers: outHeaders, body: bodyBuf, maxRedirects: 5, timeoutMs: 20000 });
        finish = r.finish;
        const { response } = r;

        res.setHeader('x-proxy-headers', _proxyHeadersJson(response.headers));
        const ct = response.headers.get('content-type');
        if (ct && /^[\x20-\x7e]{1,200}$/.test(ct)) res.setHeader('Content-Type', ct);
        // 中継した第三者のHTMLがこのサーバーのオリジンでスクリプト実行されないよう、サンドボックス化する
        res.setHeader('Content-Security-Policy', "sandbox; default-src 'none'");
        res.setHeader('X-Content-Type-Options', 'nosniff');

        pipeline(response.body, _byteLimiter(PROXY_MAX_BYTES, (n) => { totalDataProxied += n; }), res, (err) => {
            finish();
            if (err && err.message !== 'Premature close') console.warn('Proxy stream ended early:', err.message);
        });
    } catch (e) {
        finish();
        console.error('Proxy error:', e.message);
        if (res.headersSent) return res.end();
        if (e instanceof SsrfError) return res.status(400).send('このURLにはアクセスできません。');
        if (e && e.name === 'AbortError') return res.status(504).send('Upstream timeout.');
        res.status(502).send('Proxy error');
    }
});

const google = require('google-it');
app.get('/youtube-search', async (req, res) => {
    const query = req.query.q;
    if (!query) return res.status(400).json({ error: 'Query is required' });
    try {
        const results = await google({ query: `${query} site:youtube.com`, 'no-display': true });
        const videoResults = results.map(r => {
            const m = r.link.match(/watch\?v=([a-zA-Z0-9_-]{11})/);
            return { title: r.title, link: r.link, videoId: m ? m[1] : null };
        }).filter(r => r.videoId);
        res.json({ results: videoResults });
    } catch (e) { res.status(500).json({ error: 'Failed to search YouTube' }); }
});

const { JSDOM } = require('jsdom');
const { Readability } = require('@mozilla/readability');
const EXTRACT_MAX_BYTES = 5 * 1024 * 1024;
// [SEC] 認証必須 + SSRF対策。以前は axios の beforeRedirect が async で、検証が待たれず
// リダイレクト経由で内部アドレスに到達できた。safeFetch で1ホップずつ検証する方式に変更。
// 返す content もスクリプト等を除去（サニタイズ）してから返す。
app.get('/extract', requireAuth, async (req, res) => {
    const url = req.query.url;
    if (!url || typeof url !== 'string') return res.status(400).json({ error: 'URL parameter is required.' });
    if (!_rateLimit(req, 'extract', 20) || !_keyedRateLimit(`u:${req.user.email}`, 'extract', 30, 60_000)) {
        return res.status(429).json({ error: 'Rate limit exceeded.' });
    }
    let finish = () => { };
    try {
        const r = await safeFetch(url, {
            headers: { 'user-agent': SAFE_FETCH_UA, 'accept': 'text/html,application/xhtml+xml' },
            maxRedirects: 5, timeoutMs: 12000,
        });
        finish = r.finish;
        const ct = (r.response.headers.get('content-type') || '').toLowerCase();
        if (ct && !/^(text\/html|application\/xhtml\+xml|text\/plain)/.test(ct)) {
            try { r.response.body.destroy(); } catch { /* ignore */ }
            return res.status(415).json({ error: 'HTMLページ以外は読み込めません。' });
        }
        if (!r.response.ok) {
            try { r.response.body.destroy(); } catch { /* ignore */ }
            return res.status(502).json({ error: 'ページの読み込みまたは解析に失敗しました。' });
        }
        const html = (await _readBodyLimited(r.response.body, EXTRACT_MAX_BYTES)).toString('utf8');
        const dom = new JSDOM(html, { url: r.finalUrl });
        const article = new Readability(dom.window.document).parse();
        if (article) {
            res.json({
                title: article.title, content: _sanitizeExtractedHtml(article.content),
                textContent: article.textContent, author: article.byline, length: article.length,
            });
        } else {
            res.status(404).json({ error: '記事のコンテンツを抽出できませんでした。' });
        }
    } catch (e) {
        console.error('Extract error:', e.message);
        if (e instanceof SsrfError) return res.status(400).json({ error: 'このURLにはアクセスできません。' });
        res.status(500).json({ error: 'ページの読み込みまたは解析に失敗しました。' });
    } finally {
        finish();
    }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Rivift App Store API
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.get('/store/apps', async (req, res) => {
    try {
        const { category = 'all', sort = 'newest', limit = 30, offset = 0 } = req.query;
        const cacheKey = `${category}|${sort}|${limit}|${offset}`;
        const cached = _cacheGet(cacheKey);
        if (cached) return res.json({ apps: cached, _cached: true });

        const sortField = sort === 'popular' ? 'downloads' : sort === 'liked' ? 'likeCount' : 'createdAt';
        const q = appsCol().orderBy(sortField, 'desc');
        const snap = await q.get();
        let apps = snap.docs.map(d => {
            const { htmlContent, ...rest } = d.data();
            return { id: d.id, ...rest };
        });
        if (category && category !== 'all') apps = apps.filter(a => a.category === category);
        apps = apps.slice(Number(offset), Number(offset) + Number(limit));
        _cacheSet(cacheKey, apps);
        res.json({ apps });
    } catch (e) { console.error('store/apps error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.get('/store/apps/:id', async (req, res) => {
    try {
        const snap = await appsCol().doc(req.params.id).get();
        if (!snap.exists) return res.status(404).json({ error: 'App not found' });
        const data = snap.data();
        if (data.htmlContent && typeof data.htmlContent === 'string' && data.htmlContent.startsWith('gz:')) {
            data.htmlContentGz = data.htmlContent.slice(3);
            delete data.htmlContent;
        } else if (data.htmlContent) {
            // 平文保存のもの（旧データ）はそのまま返す
        }
        res.json({ app: { id: snap.id, ...data } });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/store/apps', express.json({ limit: '10mb' }), requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { title, description, category, htmlContent, htmlContentGz, iconEmoji, iconImage, version } = req.body;
        if (!title || (!htmlContent && !htmlContentGz)) return res.status(400).json({ error: 'title, htmlContent は必須です' });
        const user = await getUser(email);
        if (!user) return res.status(401).json({ error: 'Connectアカウントが見つかりません' });
        let compressedHtml;
        if (htmlContentGz) {
            const estimatedSize = Math.floor(htmlContentGz.length * 0.75);
            if (estimatedSize > APP_HTML_MAX_SIZE * 1.05) return res.status(400).json({ error: 'HTMLが大きすぎます（最大3MB）' });
            compressedHtml = 'gz:' + htmlContentGz;
        } else {
            if (Buffer.byteLength(htmlContent, 'utf8') > APP_HTML_MAX_SIZE) return res.status(400).json({ error: 'HTMLが大きすぎます（最大3MB）' });
            compressedHtml = await compressHtmlContent(htmlContent);
        }
        if (iconImage && Buffer.byteLength(iconImage, 'utf8') > 400_000) return res.status(400).json({ error: 'アイコン画像が大きすぎます（最大300KB）' });
        const now = Date.now();
        const docRef = appsCol().doc();
        await docRef.set({
            title: title.slice(0, 60),
            description: (description || '').slice(0, 500),
            category: ['tool', 'game', 'entertainment', 'other'].includes(category) ? category : 'other',
            authorEmail: email,
            authorName: user.displayName || email,
            htmlContent: compressedHtml,
            iconEmoji: iconImage ? '' : (iconEmoji || '📦').slice(0, 2),
            iconImage: iconImage || null,
            version: (version || '1.0.0').slice(0, 20),
            downloads: 0,
            likeCount: 0,
            ratingSum: 0,
            ratingCount: 0,
            createdAt: now,
            updatedAt: now,
        });
        _cacheInvalidate();
        res.json({ success: true, id: docRef.id });
    } catch (e) { console.error('store publish error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.put('/store/apps/:id', express.json({ limit: '10mb' }), requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { title, description, category, htmlContent, iconEmoji, iconImage, version } = req.body;
        const snap = await appsCol().doc(req.params.id).get();
        if (!snap.exists) return res.status(404).json({ error: 'App not found' });
        if (snap.data().authorEmail !== email) return res.status(403).json({ error: '権限がありません' });
        const update = { updatedAt: Date.now() };
        if (title) update.title = title.slice(0, 60);
        if (description !== undefined) update.description = description.slice(0, 500);
        if (category) update.category = ['tool', 'game', 'entertainment', 'other'].includes(category) ? category : 'other';
        const { htmlContentGz: putHtmlContentGz } = req.body;
        if (htmlContent || putHtmlContentGz) {
            if (putHtmlContentGz) {
                const estimatedSize = Math.floor(putHtmlContentGz.length * 0.75);
                if (estimatedSize > APP_HTML_MAX_SIZE * 1.05) return res.status(400).json({ error: 'HTMLが大きすぎます' });
                update.htmlContent = 'gz:' + putHtmlContentGz;
            } else {
                if (Buffer.byteLength(htmlContent, 'utf8') > APP_HTML_MAX_SIZE) return res.status(400).json({ error: 'HTMLが大きすぎます' });
                update.htmlContent = await compressHtmlContent(htmlContent);
            }
        }
        if (iconImage !== undefined) {
            if (iconImage && Buffer.byteLength(iconImage, 'utf8') > 400_000) return res.status(400).json({ error: 'アイコン画像が大きすぎます' });
            update.iconImage = iconImage || null;
            update.iconEmoji = iconImage ? '' : (iconEmoji || '📦').slice(0, 2);
        } else if (iconEmoji) {
            update.iconEmoji = iconEmoji.slice(0, 2);
        }
        if (version) update.version = version.slice(0, 20);
        await appsCol().doc(req.params.id).update(update);
        _cacheInvalidate();
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.delete('/store/apps/:id', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const snap = await appsCol().doc(req.params.id).get();
        if (!snap.exists) return res.status(404).json({ error: 'App not found' });
        if (snap.data().authorEmail !== email) return res.status(403).json({ error: '権限がありません' });
        await appsCol().doc(req.params.id).delete();
        _cacheInvalidate();
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/store/apps/:id/download', async (req, res) => {
    try {
        await appsCol().doc(req.params.id).update({
            downloads: admin.firestore.FieldValue.increment(1)
        });
        _cacheInvalidate();
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/store/my-apps', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(400).json({ error: 'email is required' });
        const snap = await appsCol().where('authorEmail', '==', email).get();
        const apps = snap.docs.map(d => {
            const { htmlContent, iconImage, ...rest } = d.data();
            return { id: d.id, ...rest };
        });
        apps.sort((a, b) => b.createdAt - a.createdAt);
        res.json({ apps });
    } catch (e) { console.error('my-apps error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/store/apps/:id/like', requireAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'like')) return res.status(429).json({ error: 'Too many requests' });
        const email = req.user.email;

        const appRef = appsCol().doc(req.params.id);
        const likeRef = appRef.collection('likes').doc(email);
        const likeSnap = await likeRef.get();

        let liked;
        if (likeSnap.exists) {
            await Promise.all([
                likeRef.delete(),
                appRef.update({ likeCount: admin.firestore.FieldValue.increment(-1) }),
            ]);
            liked = false;
        } else {
            await Promise.all([
                likeRef.set({ likedAt: Date.now() }),
                appRef.update({ likeCount: admin.firestore.FieldValue.increment(1) }),
            ]);
            liked = true;
        }
        _cacheInvalidate();

        const appSnap = await appRef.get();
        res.json({ liked, likeCount: appSnap.data()?.likeCount || 0 });
    } catch (e) { console.error('like error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.get('/store/apps/:id/like', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(400).json({ error: 'email is required' });
        const likeSnap = await appsCol().doc(req.params.id).collection('likes').doc(email).get();
        res.json({ liked: likeSnap.exists });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/store/apps/:id/rating', requireAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'rating')) return res.status(429).json({ error: 'Too many requests' });
        const email = req.user.email;
        const { score } = req.body;
        const s = Number(score);
        if (!s || s < 1 || s > 5 || !Number.isInteger(s)) return res.status(400).json({ error: 'score must be 1-5 integer' });

        const appRef = appsCol().doc(req.params.id);
        const ratingRef = appRef.collection('ratings').doc(email);
        const ratingSnap = await ratingRef.get();

        if (ratingSnap.exists) {
            const oldScore = ratingSnap.data().score;
            const diff = s - oldScore;
            await Promise.all([
                ratingRef.set({ score: s, updatedAt: Date.now() }),
                diff !== 0 ? appRef.update({ ratingSum: admin.firestore.FieldValue.increment(diff) }) : Promise.resolve(),
            ]);
        } else {
            await Promise.all([
                ratingRef.set({ score: s, updatedAt: Date.now() }),
                appRef.update({
                    ratingSum: admin.firestore.FieldValue.increment(s),
                    ratingCount: admin.firestore.FieldValue.increment(1),
                }),
            ]);
        }
        _cacheInvalidate();

        const appSnap = await appRef.get();
        const { ratingSum = 0, ratingCount = 0 } = appSnap.data();
        res.json({
            myScore: s,
            ratingSum,
            ratingCount,
            ratingAvg: ratingCount > 0 ? Math.round((ratingSum / ratingCount) * 10) / 10 : 0,
        });
    } catch (e) { console.error('rating error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.get('/store/apps/:id/rating', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(400).json({ myScore: null });
        const snap = await appsCol().doc(req.params.id).collection('ratings').doc(email).get();
        res.json({ myScore: snap.exists ? snap.data().score : null });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

const _reportCooldown = new Map();

app.post('/store/apps/:id/report', optionalAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'report', 5)) return res.status(429).json({ error: 'Too many requests' });
        const { reason, detail } = req.body;
        const reporterEmail = req.user ? req.user.email : req.body.email;
        if (!isValidEmail(reporterEmail)) return res.status(400).json({ error: '有効なメールアドレスが必要です' });
        if (!reason) return res.status(400).json({ error: 'reason is required' });

        const VALID_REASONS = ['spam', 'malware', 'inappropriate', 'phishing', 'other'];
        if (!VALID_REASONS.includes(reason)) return res.status(400).json({ error: 'Invalid reason' });

        const coolKey = `${reporterEmail}:${req.params.id}`;
        const lastReport = _reportCooldown.get(coolKey);
        if (lastReport && Date.now() - lastReport < 24 * 60 * 60 * 1000) {
            return res.status(429).json({ error: 'すでにこのアプリを通報済みです。24時間後に再通報できます。' });
        }

        const appSnap = await appsCol().doc(req.params.id).get();
        if (!appSnap.exists) return res.status(404).json({ error: 'App not found' });

        await db.collection('store_reports').add({
            appId: req.params.id,
            appTitle: appSnap.data().title,
            reporterEmail,
            reason,
            detail: (detail || '').slice(0, 500),
            reportedAt: Date.now(),
            status: 'pending',
        });

        _reportCooldown.set(coolKey, Date.now());
        console.warn(`[REPORT] app="${appSnap.data().title}" (${req.params.id}) by=${reporterEmail} reason=${reason}`);

        res.json({ success: true });
    } catch (e) { console.error('report error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.get('/store/apps/:id/comments', async (req, res) => {
    try {
        const appId = req.params.id;
        const limit = Math.min(Number(req.query.limit) || 20, 50);
        const before = req.query.before ? Number(req.query.before) : null;

        const cacheKey = `${appId}:${limit}:${before || 'latest'}`;
        const cached = _commentCacheGet(cacheKey);
        if (cached) return res.json({ comments: cached, _cached: true });

        const snap = await commentsCol().where('appId', '==', appId).get();

        let comments = snap.docs.map(d => {
            const { email, ...rest } = d.data();
            return { id: d.id, ...rest };
        });

        comments.sort((a, b) => b.createdAt - a.createdAt);
        if (before) comments = comments.filter(c => c.createdAt < before);
        comments = comments.slice(0, limit);

        _commentCacheSet(cacheKey, comments);
        res.json({ comments });
    } catch (e) { console.error('comments GET error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/store/apps/:id/comments', requireAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'comments-post', 3)) return res.status(429).json({ error: 'コメントは1分間に3件までです。しばらく待ってから再試行してください。' });

        const email = req.user.email;
        const { body } = req.body;
        if (!body || typeof body !== 'string') return res.status(400).json({ error: 'body is required' });
        const trimmed = body.trim();
        if (trimmed.length < 1 || trimmed.length > 200) return res.status(400).json({ error: 'コメントは1〜200文字で入力してください' });

        const user = await getUser(email);
        if (!user) return res.status(401).json({ error: 'アカウントが見つかりません' });

        const appSnap = await appsCol().doc(req.params.id).get();
        if (!appSnap.exists) return res.status(404).json({ error: 'App not found' });

        const now = Date.now();
        const docRef = await commentsCol().add({
            appId: req.params.id,
            email,
            displayName: user.displayName || email,
            icon: user.icon || null,
            body: trimmed,
            createdAt: now,
        });

        res.json({
            success: true,
            comment: {
                id: docRef.id,
                appId: req.params.id,
                displayName: user.displayName || email,
                icon: user.icon || null,
                body: trimmed,
                createdAt: now,
            },
        });
        setTimeout(() => _commentCacheInvalidate(req.params.id), 500);
    } catch (e) { console.error('comments POST error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.delete('/store/apps/:id/comments/:commentId', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;

        const commentRef = commentsCol().doc(req.params.commentId);
        const commentSnap = await commentRef.get();
        if (!commentSnap.exists) return res.status(404).json({ error: 'Comment not found' });
        if (commentSnap.data().email !== email) return res.status(403).json({ error: '削除する権限がありません' });
        if (commentSnap.data().appId !== req.params.id) return res.status(400).json({ error: 'App ID mismatch' });

        await commentRef.delete();
        res.json({ success: true });
        setTimeout(() => _commentCacheInvalidate(req.params.id), 500);
    } catch (e) { console.error('comments DELETE error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Socket.io（リアルタイム通信）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// [SEC] Socket.io接続自体にJWT認証を必須化する。
// 認証済みでも「emailを名乗るだけで他人になりすませる」問題を防ぐため、
// ここで検証したJWTのemailだけを socket.verifiedEmail として以後ずっと使う。
// 接続時にトークンが無い/無効なクライアントは接続自体を拒否する。
// [SEC] 接続時の認証を強化:
//  - トークンは handshake.auth だけから受け取る（URLのクエリは、アクセスログ・履歴にトークンが残るため廃止）
//  - JWTのアルゴリズムを固定して検証
//  - トークンの中身（email形式・guest フラグとメールドメインの整合）を検証
//  - 「トークンは有効だがアカウント/ゲストがもう存在しない」接続を拒否（削除済みアカウントの居座り防止）
//  - IPごとの接続試行回数・アカウントごとの同時接続数に上限
const SOCKET_MAX_PER_USER = 20;
const _authExistCache = new Map(); // email -> expireAt（Firestoreの読み取りを減らすための短いキャッシュ）
async function _accountStillExists(email, isGuest) {
    const hit = _authExistCache.get(email);
    if (hit && hit > Date.now()) return true;
    const snap = isGuest ? await collabGuestsCol().doc(email).get() : await usersCol().doc(email).get();
    if (!snap.exists) return false;
    if (_authExistCache.size > 5000) _authExistCache.clear();
    _authExistCache.set(email, Date.now() + 60_000);
    return true;
}

io.use(async (socket, next) => {
    try {
        if (!JWT_SECRET) return next(new Error('サーバー設定エラー'));
        const ip = _clientIpFromHandshake(socket.handshake);
        if (!_keyedRateLimit(`ip:${ip}`, 'socket-connect', 60, 60_000)) return next(new Error('接続の試行が多すぎます。しばらく待ってください'));

        const token = socket.handshake.auth?.token;
        if (!token || typeof token !== 'string' || token.length > 4096) return next(new Error('認証が必要です'));
        let payload;
        try { payload = verifyJwt(token); } catch { return next(new Error('トークンが無効または期限切れです')); }

        const isGuest = payload.guest === true;
        if (!isValidEmail(payload.email) || isGuest !== _collabIsGuestEmail(payload.email)) {
            return next(new Error('トークンが無効または期限切れです'));
        }
        if (!(await _accountStillExists(payload.email, isGuest))) return next(new Error('トークンが無効または期限切れです'));
        if ((io.sockets.adapter.rooms.get(_userRoom(payload.email))?.size || 0) >= SOCKET_MAX_PER_USER) {
            return next(new Error('同時接続数の上限に達しています'));
        }
        socket.verifiedEmail = payload.email;
        socket.isGuest = isGuest;
        socket.tokenExpiresAt = payload.exp ? payload.exp * 1000 : null;
        next();
    } catch (e) {
        console.error('socket auth error:', e);
        next(new Error('サーバーエラー'));
    }
});

io.on('connection', (socket) => {
    // [SEC] currentUserEmail / socket.email は、接続時にJWTで検証されたメールアドレス
    // (socket.verifiedEmail) 由来のものだけを使う。クライアントから送られてくる
    // payloadの email / from を鵜呑みにしない。
    let currentUserEmail = socket.verifiedEmail;
    socket.email = socket.verifiedEmail;

    // [SEC] ゲスト（仮ユーザー）の接続は共同編集(collab_*)イベントしか使えない
    if (socket.isGuest) {
        socket.use(([ev], next) => (typeof ev === 'string' && ev.startsWith('collab_')) ? next() : next(new Error('ゲストはこの操作を行えません')));
    }

    // [SEC] イベントのレート制限（接続ごとのトークンバケット）。通常利用では到達しない余裕のある値にしてあり、
    // 超えた分は黙って捨てる（ackがあれば rate_limited を返す）。メモリ・DB・帯域の浪費を防ぐ。
    const _bucket = (capacity, refillPerSec) => {
        let tokens = capacity, last = Date.now();
        return () => {
            const now = Date.now();
            tokens = Math.min(capacity, tokens + ((now - last) / 1000) * refillPerSec);
            last = now;
            if (tokens >= 1) { tokens -= 1; return true; }
            return false;
        };
    };
    const allowAny = _bucket(300, 150);
    const allowCollabOp = _bucket(100, 50);
    const allowCollabCursor = _bucket(80, 40);
    const allowCollabOther = _bucket(30, 10);
    socket.use(([ev, ...args], next) => {
        let ok = allowAny();
        if (ok && typeof ev === 'string' && ev.startsWith('collab_')) {
            ok = ev === 'collab_op' ? allowCollabOp() : ev === 'collab_cursor' ? allowCollabCursor() : allowCollabOther();
        }
        if (ok) return next();
        const cb = args[args.length - 1];
        if (typeof cb === 'function') { try { cb({ error: 'rate_limited' }); } catch (_) { } }
    });

    // [SEC] 接続したままトークンの期限が切れても居座れないよう、期限が来たら切断する
    if (socket.tokenExpiresAt) {
        const ms = socket.tokenExpiresAt - Date.now();
        if (ms <= 0) { socket.disconnect(true); return; }
        const expTimer = setTimeout(() => socket.disconnect(true), Math.min(ms, 2 ** 31 - 1));
        expTimer.unref?.();
        socket.on('disconnect', () => clearTimeout(expTimer));
    }
    socket.collabSnapReq = new Map(); // docId -> スナップショットを要求した時刻（collab_snapshot_response の検証用）

    const DEFAULT_ICE_SERVERS = [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        { urls: 'stun:stun3.l.google.com:19302' },
        { urls: 'stun:stun4.l.google.com:19302' }
    ];

    socket.on('get-ice-servers', async (payload, callback) => {
        if (typeof callback !== 'function') return;
        if (!accountSid || !authToken) {
            return callback({ iceServers: DEFAULT_ICE_SERVERS });
        }
        try {
            const client = twilio(accountSid, authToken);
            const token = await client.tokens.create({ ttl: 3600 });
            const combined = [...(token.iceServers || []), ...DEFAULT_ICE_SERVERS];
            callback({ iceServers: combined });
        } catch (e) {
            console.warn(`Twilioトークン取得失敗、公開STUNにフォールバック: ${e.message}`);
            callback({ iceServers: DEFAULT_ICE_SERVERS });
        }
    });

    const notifyFriendsOfStatusChange = async (email, isOnline) => {
        try {
            const user = await getUser(email);
            if (user && user.friends) {
                user.friends.forEach(friendEmail => {
                    const friendSocketId = onlineUsers[friendEmail];
                    if (friendSocketId) io.to(friendSocketId).emit('friend_status_changed', { email, isOnline });
                });
            }
        } catch (e) { console.error('notifyFriendsOfStatusChange error:', e); }
    };

    // [SEC] 'login' はJWTで検証済みの本人（socket.verifiedEmail）としてのみ扱う。
    // クライアントから送られてくる payload.email は無視する（なりすまし防止）。
    socket.join(_userRoom(currentUserEmail));
    // 同じアカウントの2つ目以降の接続ではオンライン通知を重複させない
    if ((io.sockets.adapter.rooms.get(_userRoom(currentUserEmail))?.size || 0) === 1) {
        notifyFriendsOfStatusChange(currentUserEmail, true);
    }
    socket.on('login', () => {
        socket.join(_userRoom(currentUserEmail)); // 冪等。念のための再参加
    });

    socket.on('get_initial_statuses', async (payload, callback) => {
        const user = await getUser(currentUserEmail);
        if (user && user.friends) {
            const statuses = {};
            user.friends.forEach(friendEmail => { statuses[friendEmail] = !!onlineUsers[friendEmail]; });
            callback(statuses);
        }
    });

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // 共同編集（Office系アプリ共通基盤）: ルーム参加・操作同期・カーソル共有
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    const _collabJoinedRooms = new Set(); // このsocketが参加中のdocIdの集合（disconnect時の後片付け用）
    const _collabRoom = (docId) => `collab:${docId}`;

    // [SEC] 全 collab_* ハンドラ共通のラッパー:
    //  - payload が null・配列・関数などでも落ちない（分割代入の TypeError でプロセスごと落ちるのを防ぐ）
    //  - ackコールバックが関数でない場合も安全に扱う
    //  - 例外は握りつぶさずログに残し、ackがあれば Server error を返す
    const onCollab = (event, handler) => {
        socket.on(event, async (...args) => {
            const cb = typeof args[args.length - 1] === 'function' ? args[args.length - 1] : null;
            const payload = (args[0] && typeof args[0] === 'object' && !Array.isArray(args[0])) ? args[0] : {};
            try {
                await handler(payload, cb);
            } catch (e) {
                console.error(`${event} error:`, e);
                if (cb) { try { cb({ error: 'Server error' }); } catch (_) { } }
            }
        });
    };

    const _cleanupCollabRoom = (docId) => {
        socket.to(_collabRoom(docId)).emit('collab_presence_left', { docId, email: currentUserEmail });
        const rc = _collabRoomCursors.get(docId);
        if (rc) {
            rc.delete(currentUserEmail);
            if (rc.size === 0) _collabRoomCursors.delete(docId);
        }
        socket.to(_collabRoom(docId)).emit('collab_cursor_remove', { docId, from: currentUserEmail });
    };

    // 権限を失っているのにまだ部屋に残っていた接続を、その場で外す（イベント到着時の自己修復）
    const _dropFromRoom = (docId) => {
        if (!_collabJoinedRooms.has(docId)) return;
        socket.leave(_collabRoom(docId));
        _collabJoinedRooms.delete(docId);
        _cleanupCollabRoom(docId);
        socket.emit('collab_permission_changed', { docId, role: null });
    };

    socket.on('disconnect', () => {
        if (currentUserEmail) {
            // 再接続の一瞬の切れ目で「オフライン」がチカチカしないよう、3秒待って本当に全接続が無い時だけ通知
            const _em = currentUserEmail;
            setTimeout(() => { if (!isUserOnline(_em)) notifyFriendsOfStatusChange(_em, false); }, 3000);
        }
        _collabJoinedRooms.forEach(docId => {
            _cleanupCollabRoom(docId);
        });
        _collabJoinedRooms.clear();
    });

    // [SEC] 参加時は必ずFirestoreの最新の権限を確認する（キャッシュは使わない）。
    onCollab('collab_join_room', async ({ docId }, callback) => {
        const reply = (r) => { if (callback) callback(r); };
        if (!_isSafeDocId(docId)) return reply({ error: 'docId が不正です' });
        if (!_collabJoinedRooms.has(docId) && _collabJoinedRooms.size >= COLLAB_MAX_ROOMS_PER_SOCKET) {
            return reply({ error: '同時に開けるドキュメント数の上限に達しています' });
        }
        const snap = await collabDocsCol().doc(docId).get();
        if (!snap.exists) return reply({ error: 'ドキュメントが見つかりません' });
        const data = snap.data();
        const role = _collabRoleOf(data, currentUserEmail);
        if (!role) return reply({ error: 'このドキュメントへのアクセス権がありません' });
        _collabRoleCacheSet(docId, currentUserEmail, role);

        socket.join(_collabRoom(docId));
        _collabJoinedRooms.add(docId);

        // 既に部屋にいる人たちのプレゼンスを新規参加者に返す
        const roomSockets = await io.in(_collabRoom(docId)).fetchSockets();
        const peers = roomSockets
            .filter(s => s.id !== socket.id)
            .map(s => ({ email: s.email, role: _collabRoleOf(data, s.email) }));

        // 既存参加者のカーソル一覧
        const rc = _collabRoomCursors.get(docId);
        const cursors = rc ? Array.from(rc.values()) : [];

        // ページごとの現在リビジョン番号
        const docState = _collabDocStates.get(docId);
        const pageRevs = {};
        if (docState) {
            for (const [pIdx, pState] of docState.pages.entries()) {
                pageRevs[pIdx] = pState.rev;
            }
        }

        socket.to(_collabRoom(docId)).emit('collab_presence_joined', { docId, email: currentUserEmail, role });
        reply({ success: true, role, peers, cursors, pageRevs });
    });

    onCollab('collab_leave_room', async ({ docId }) => {
        if (!_isSafeDocId(docId)) return;
        socket.leave(_collabRoom(docId));
        if (_collabJoinedRooms.delete(docId)) _cleanupCollabRoom(docId);
    });

    // 実際の編集内容（テキスト差分・全文置換）を同室の他ユーザーへ中継。
    // [SEC] 「部屋に参加している」「今この瞬間 editor 以上」「中身の形式が正しい」を毎回サーバー側で確認する。
    // [OT] 競合対策: 送信元の baseRev を考慮し、サーバー間で発生した直近の操作と照らし合わせて位置を自動補正 (OT)
    onCollab('collab_op', async ({ docId, op }) => {
        if (!_isSafeDocId(docId) || !socket.rooms.has(_collabRoom(docId))) return;
        const role = await _collabGetRole(docId, currentUserEmail);
        if (!role) return _dropFromRoom(docId);
        if (!_collabHasAtLeast(role, 'editor')) return; // editor未満は変更を送れない
        const clean = _collabValidateOp(op);
        if (!clean) return;

        let docState = _collabDocStates.get(docId);
        if (!docState) {
            docState = { lastActive: Date.now(), pages: new Map() };
            _collabDocStates.set(docId, docState);
        }
        docState.lastActive = Date.now();
        const pageIndex = clean.pageIndex;
        let pageState = docState.pages.get(pageIndex);
        if (!pageState) {
            pageState = { rev: 0, opsHistory: [] };
            docState.pages.set(pageIndex, pageState);
        }

        if (clean.full) {
            pageState.rev++;
            pageState.opsHistory = [];
            socket.to(_collabRoom(docId)).emit('collab_op', {
                docId,
                op: { ...clean, rev: pageState.rev },
                from: currentUserEmail,
                at: Date.now()
            });
            return;
        }

        let effectivePatch = clean.patch;
        // クライアントが「未来のリビジョン」を主張しても、サーバーの現在値を上限とする
        const baseRev = Math.min(clean.baseRev ?? pageState.rev, pageState.rev);

        // OT変換: クライアントが古いリビジョンで打鍵していた場合、その間に起きたパッチで位置を補正
        if (baseRev < pageState.rev && pageState.opsHistory.length > 0 && effectivePatch) {
            const missedOps = pageState.opsHistory.filter(o => o.rev > baseRev);
            for (const pastOp of missedOps) {
                if (pastOp.patch) {
                    effectivePatch = _transformPatch(effectivePatch, pastOp.patch);
                }
            }
        }

        pageState.rev++;
        pageState.opsHistory.push({ rev: pageState.rev, patch: effectivePatch, from: currentUserEmail });
        if (pageState.opsHistory.length > 100) pageState.opsHistory.shift();

        const transformedOp = {
            ...clean,
            patch: effectivePatch,
            rev: pageState.rev,
            baseRev: baseRev
        };

        socket.to(_collabRoom(docId)).emit('collab_op', {
            docId,
            op: transformedOp,
            from: currentUserEmail,
            at: Date.now()
        });

        // 送信クライアントへ確定通知（ack）
        socket.emit('collab_op_ack', {
            docId,
            pageIndex,
            clientOpId: clean.clientOpId,
            rev: pageState.rev
        });
    });

    // コメント権限があればコメントの追加/返信も中継（編集はできないがコメントは可、を許容）
    onCollab('collab_comment', async ({ docId, comment }) => {
        if (!_isSafeDocId(docId) || !socket.rooms.has(_collabRoom(docId))) return;
        if (!comment || typeof comment !== 'object') return;
        if (JSON.stringify(comment).length > 20000) return;
        const role = await _collabGetRole(docId, currentUserEmail);
        if (!role) return _dropFromRoom(docId);
        if (!_collabHasAtLeast(role, 'commenter')) return;
        socket.to(_collabRoom(docId)).emit('collab_comment', { docId, comment, from: currentUserEmail, at: Date.now() });
    });

    // カーソル位置・選択範囲の共有（閲覧権限だけでもOK。キャレットとテキスト選択ハイライト情報を含む）
    // [SEC] 部屋に入っていて、かつ「今も」閲覧権限があること。表示名・色は検証/無害化して中継する。
    onCollab('collab_cursor', async ({ docId, cursor, displayName, color }) => {
        if (!_isSafeDocId(docId) || !socket.rooms.has(_collabRoom(docId))) return;
        const role = await _collabGetRole(docId, currentUserEmail);
        if (!role) return _dropFromRoom(docId);
        if (cursor !== undefined && cursor !== null) {
            if (typeof cursor !== 'object' || JSON.stringify(cursor).length > 20000) return;
        }
        let rc = _collabRoomCursors.get(docId);
        if (!rc) {
            rc = new Map();
            _collabRoomCursors.set(docId, rc);
        }
        const cursorData = {
            docId,
            from: currentUserEmail,
            displayName: _cleanLabel(displayName, 50) || currentUserEmail.split('@')[0],
            color: (typeof color === 'string' && /^#[0-9a-fA-F]{3,8}$/.test(color)) ? color : '#3b82f6',
            cursor,
            updatedAt: Date.now()
        };
        rc.set(currentUserEmail, cursorData);
        socket.to(_collabRoom(docId)).emit('collab_cursor', cursorData);
    });

    // 新規参加者が「今の最新内容」を持っていないため、同室の他の人に一度だけ呼びかけて取り直す。
    // サーバーは中身を保持せず、あくまで橋渡しだけを行う（誰かがdocを開いていないと解決できない点に注意）。
    // [SEC] 要求した時刻を記録しておき、collab_snapshot_response は「実際に要求した相手」にだけ届ける。
    onCollab('collab_request_snapshot', async ({ docId }) => {
        if (!_isSafeDocId(docId) || !socket.rooms.has(_collabRoom(docId))) return;
        const role = await _collabGetRole(docId, currentUserEmail);
        if (!role) return _dropFromRoom(docId);
        const last = socket.collabSnapReq.get(docId) || 0;
        if (Date.now() - last < 2000) return; // 連打防止
        socket.collabSnapReq.set(docId, Date.now());
        socket.to(_collabRoom(docId)).emit('collab_snapshot_request', { docId, requesterSocketId: socket.id });
    });
    // [SEC] 部屋に参加している編集者(editor以上)だけが、「要求してきた」同じ部屋の相手にだけスナップショットを返せる。
    // 以前は、編集者が部屋内の誰にでも、頼まれていないスナップショットを送って内容を丸ごと差し替えられた。
    onCollab('collab_snapshot_response', async ({ docId, toSocketId, title, pages }) => {
        if (!_isSafeDocId(docId) || typeof toSocketId !== 'string' || !Array.isArray(pages)) return;
        if (pages.length < 1 || pages.length > COLLAB_MAX_PAGES || pages.some(p => typeof p !== 'string')) return;
        if (pages.reduce((n, p) => n + p.length, 0) > COLLAB_SNAPSHOT_MAX_CHARS) return;
        const room = _collabRoom(docId);
        if (!socket.rooms.has(room)) return;
        const target = io.sockets.sockets.get(toSocketId);
        if (!target || !target.rooms.has(room)) return;
        const requestedAt = target.collabSnapReq?.get(docId);
        if (!requestedAt || Date.now() - requestedAt > 30_000) return;
        if (!_collabHasAtLeast(await _collabGetRole(docId, currentUserEmail), 'editor')) return;
        if (!(await _collabGetRole(docId, target.email))) return;
        target.collabSnapReq.delete(docId); // 1回の要求につき最初の1件だけ受け付ける
        io.to(toSocketId).emit('collab_snapshot', { docId, title: sanitizeString(title, 200), pages });
    });

    const notifyUsers = (userEmails) => {
        userEmails.forEach(email => {
            const socketId = onlineUsers[email];
            if (socketId) io.to(socketId).emit('db_updated_notification');
        });
    };

    // [SEC] 以降の全イベントで、"from"（自分側）は必ず currentUserEmail（JWT検証済み）を使う。
    // payloadに from が含まれていても無視し、他人になりすませないようにする。

    socket.on('send_friend_request', async ({ to }, callback) => {
        try {
            const from = (currentUserEmail || '').trim();
            const target = (to || '').trim();
            if (!target || !from || target.toLowerCase() === from.toLowerCase()) {
                if (typeof callback === 'function') callback({ error: '無効なメールアドレスです' });
                return;
            }
            const targetUser = await getUser(target);
            if (!targetUser) {
                if (typeof callback === 'function') callback({ error: 'ユーザーが見つかりません' });
                return;
            }
            // 相手の requests に from を追加
            await usersCol().doc(target).update({ requests: admin.firestore.FieldValue.arrayUnion(from) });
            // 自分の sentRequests に target を追加
            await usersCol().doc(from).update({ sentRequests: admin.firestore.FieldValue.arrayUnion(target) });
            notifyUsers([from, target]);
            if (typeof callback === 'function') callback({ success: true });
        } catch (e) {
            console.error('send_friend_request error:', e);
            if (typeof callback === 'function') callback({ error: e.message });
        }
    });

    socket.on('accept_friend_request', async ({ from: requesterEmail }, callback) => {
        try {
            const to = (currentUserEmail || '').trim();
            const from = (requesterEmail || '').trim();
            if (!from || !to || from.toLowerCase() === to.toLowerCase()) {
                if (typeof callback === 'function') callback({ error: '無効な申請です' });
                return;
            }
            const fromLower = from.toLowerCase();
            const toLower = to.toLowerCase();

            // 1. 自分のドキュメント: requestsから削除 & friendsに追加
            const toDocRef = usersCol().doc(to);
            const toDoc = await toDocRef.get();
            if (toDoc.exists) {
                const curRequests = toDoc.data().requests || [];
                const updatedRequests = curRequests.filter(e => (e || '').trim().toLowerCase() !== fromLower);
                const curFriends = toDoc.data().friends || [];
                const updatedFriends = curFriends.some(e => (e || '').trim().toLowerCase() === fromLower)
                    ? curFriends
                    : [...curFriends, from];
                await toDocRef.update({
                    requests: updatedRequests,
                    friends: updatedFriends
                });
            }

            // 2. 相手のドキュメント: sentRequestsから削除 & friendsに追加
            const fromDocRef = usersCol().doc(from);
            const fromDoc = await fromDocRef.get();
            if (fromDoc.exists) {
                const curSent = fromDoc.data().sentRequests || [];
                const updatedSent = curSent.filter(e => (e || '').trim().toLowerCase() !== toLower);
                const curFriends = fromDoc.data().friends || [];
                const updatedFriends = curFriends.some(e => (e || '').trim().toLowerCase() === toLower)
                    ? curFriends
                    : [...curFriends, to];
                await fromDocRef.update({
                    sentRequests: updatedSent,
                    friends: updatedFriends
                });
            }

            notifyUsers([from, to]);
            if (typeof callback === 'function') callback({ success: true });
        } catch (e) {
            console.error('accept_friend_request error:', e);
            if (typeof callback === 'function') callback({ error: e.message });
        }
    });

    socket.on('reject_friend_request', async ({ from: requesterEmail }, callback) => {
        try {
            const to = (currentUserEmail || '').trim();
            const from = (requesterEmail || '').trim();
            if (!from || !to || from.toLowerCase() === to.toLowerCase()) return;
            const fromLower = from.toLowerCase();
            const toLower = to.toLowerCase();

            const toDocRef = usersCol().doc(to);
            const toDoc = await toDocRef.get();
            if (toDoc.exists) {
                const curRequests = toDoc.data().requests || [];
                await toDocRef.update({
                    requests: curRequests.filter(e => (e || '').trim().toLowerCase() !== fromLower)
                });
            }

            const fromDocRef = usersCol().doc(from);
            const fromDoc = await fromDocRef.get();
            if (fromDoc.exists) {
                const curSent = fromDoc.data().sentRequests || [];
                await fromDocRef.update({
                    sentRequests: curSent.filter(e => (e || '').trim().toLowerCase() !== toLower)
                });
            }

            notifyUsers([from, to]);
            if (typeof callback === 'function') callback({ success: true });
        } catch (e) {
            console.error('reject_friend_request error:', e);
            if (typeof callback === 'function') callback({ error: e.message });
        }
    });

    socket.on('cancel_friend_request', async ({ to: targetEmail }, callback) => {
        try {
            const from = (currentUserEmail || '').trim();
            const target = (targetEmail || '').trim();
            if (!target || !from || target.toLowerCase() === from.toLowerCase()) return;
            const fromLower = from.toLowerCase();
            const targetLower = target.toLowerCase();

            const fromDocRef = usersCol().doc(from);
            const fromDoc = await fromDocRef.get();
            if (fromDoc.exists) {
                const curSent = fromDoc.data().sentRequests || [];
                await fromDocRef.update({
                    sentRequests: curSent.filter(e => (e || '').trim().toLowerCase() !== targetLower)
                });
            }

            const targetDocRef = usersCol().doc(target);
            const targetDoc = await targetDocRef.get();
            if (targetDoc.exists) {
                const curRequests = targetDoc.data().requests || [];
                await targetDocRef.update({
                    requests: curRequests.filter(e => (e || '').trim().toLowerCase() !== fromLower)
                });
            }

            notifyUsers([from, target]);
            if (typeof callback === 'function') callback({ success: true });
        } catch (e) {
            console.error('cancel_friend_request error:', e);
            if (typeof callback === 'function') callback({ error: e.message });
        }
    });

    socket.on('delete_friend', async ({ to }) => {
        try {
            const from = currentUserEmail;
            await usersCol().doc(from).update({ friends: admin.firestore.FieldValue.arrayRemove(to) });
            await usersCol().doc(to).update({ friends: admin.firestore.FieldValue.arrayRemove(from) });
            _friendCacheInvalidate(from, to); // [SEC] 解除直後からファイル送信を許可しない
            const chatID = [from, to].sort().join('__');
            await chatsCol().doc(chatID).delete();
            deleteBlobs(chatID).catch(e => console.error('deleteBlobs error:', e));
            notifyUsers([from, to]);
        } catch (e) { console.error('delete_friend error:', e); }
    });

    socket.on('private_message', async (payload, ack) => {
        const reply = (r) => { if (typeof ack === 'function') { try { ack(r); } catch (_) {} } };
        const from = currentUserEmail;
        const to = payload && payload.to;
        if (!isValidEmail(to)) return reply({ ok: false, error: 'invalid_recipient' });
        const chatID = [from, to].sort().join('__');
        // [SEC] クライアントが指定するidはFirestoreのドキュメントidになるため、パスを壊す文字を含むものは採用しない
        const msgId = (typeof payload.id === 'string' && payload.id.length <= 200 && _isSafeDocId(payload.id))
            ? payload.id : `${Date.now()}_${nodeCrypto.randomBytes(4).toString('hex')}`;
        const doc = { ...payload, from, id: msgId, read: false };
        if (doc.fileBlob) {
            const b = doc.fileBlob;
            if (typeof b !== 'object' || b.id !== msgId || !Number.isInteger(b.chunks) || b.chunks < 1 || b.chunks > BLOB_MAX_CHUNKS) {
                return reply({ ok: false, error: 'invalid_blob' });
            }
            // [SEC] 添付は「自分がアップロードした、全チャンクが揃ったもの」だけ送れる。
            // （他人のアップロードや、存在しないチャンクを指す添付メッセージを作れないようにする）
            try {
                const prior = await chatsCol().doc(chatID).collection('messages').doc(msgId).get();
                if (prior.exists) {
                    if (prior.data().from !== from) return reply({ ok: false, error: 'invalid_blob' }); // 他人のメッセージidは使えない
                } else if (!(await _verifyBlobUpload(chatID, msgId, from, b.chunks))) {
                    return reply({ ok: false, error: 'invalid_blob' });
                }
            } catch (e) {
                console.error('blob verify error:', e);
                return reply({ ok: false, error: 'persist_failed' });
            }
        }

        // Firestoreの1ドキュメント上限(1MiB)を超えるものは保存できない。
        // 以前は set() が失敗して握りつぶされ、送信者には「送信済み」に見えていた。
        if (Buffer.byteLength(JSON.stringify(doc), 'utf8') > 1_000_000) {
            return reply({ ok: false, error: 'too_large' });
        }

        // [PERF] 先に相手へ配信（DB書き込みを待たない）。保存は並列で行う。
        const delivered = isUserOnline(to);
        if (delivered) io.to(_userRoom(to)).emit('private_message', doc);

        try {
            await Promise.all([
                chatsCol().doc(chatID).set({ users: [from, to], lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }),
                chatsCol().doc(chatID).collection('messages').doc(msgId).set(doc),
            ]);
        } catch (e) {
            console.error('private_message persist error:', e);
            // 保存に失敗したのに相手の画面にだけ出ている状態を避けるため取り消す
            if (delivered) io.to(_userRoom(to)).emit('message_deleted', { messageId: msgId });
            if (doc.fileBlob) deleteBlobs(chatID, msgId).then(() => uploadMarkersCol().doc(_markerId(chatID, msgId)).delete()).catch(() => {});
            return reply({ ok: false, error: 'persist_failed' });
        }
        // 送信が確定したので「未完了アップロード」の目印は不要
        if (doc.fileBlob) uploadMarkersCol().doc(_markerId(chatID, msgId)).delete().catch(() => {});
        reply({ ok: true, id: msgId, delivered });

        // 以降は送信結果に影響しない後処理（失敗しても無視）
        try {
            io.to(_userRoom(from)).emit('db_updated_notification');
            if (delivered) {
                const unreadSnap = await chatsCol().doc(chatID).collection('messages')
                    .where('to', '==', to).where('read', '==', false).get();
                io.to(_userRoom(to)).emit('unread_update', { partnerEmail: from, count: unreadSnap.size });
            }
        } catch (e) { console.error('private_message post error:', e); }
    });

    socket.on('read_receipt', async (payload) => {
        try {
            const readerEmail = currentUserEmail;
            const writerEmail = payload.to;
            const chatID = [readerEmail, writerEmail].sort().join('__');
            const unreadSnap = await chatsCol().doc(chatID).collection('messages').where('to', '==', readerEmail).where('read', '==', false).get();
            const batch = db.batch();
            unreadSnap.docs.filter(d => d.data().read !== true).forEach(d => batch.update(d.ref, { read: true }));
            await batch.commit();
            const readerSocketId = onlineUsers[readerEmail];
            const writerSocketId = onlineUsers[writerEmail];
            if (readerSocketId) io.to(readerSocketId).emit('messages_marked_as_read', { chatPartner: writerEmail });
            if (writerSocketId) io.to(writerSocketId).emit('messages_marked_as_read', { chatPartner: readerEmail });

            if (readerSocketId) io.to(readerSocketId).emit('unread_update', { partnerEmail: writerEmail, count: 0 });
        } catch (e) { console.error('read_receipt error:', e); }
    });

    socket.on('delete_message', async ({ to, messageId }) => {
        try {
            const from = currentUserEmail;
            const chatID = [from, to].sort().join('__');
            const msgRef = chatsCol().doc(chatID).collection('messages').doc(messageId);
            const msgSnap = await msgRef.get();
            if (msgSnap.exists && msgSnap.data().from === from) {
                // メッセージをDBから完全に削除
                const _hadBlob = !!msgSnap.data().fileBlob;
                await msgRef.delete();
                if (_hadBlob) deleteBlobs(chatID, messageId).catch(e => console.error('deleteBlobs error:', e));
            }
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) io.to(recipientSocketId).emit('message_deleted', { messageId });
            notifyUsers([from, to]);
        } catch (e) { console.error('delete_message error:', e); }
    });

    socket.on('delete_chat', async ({ user1, user2 }) => {
        try {
            // [SEC] 自分が関与していないチャットを削除できないようにする
            if (currentUserEmail !== user1 && currentUserEmail !== user2) return;
            const chatID = [user1, user2].sort().join('__');
            const msgsSnap = await chatsCol().doc(chatID).collection('messages').get();
            const batch = db.batch();
            msgsSnap.docs.forEach(d => batch.delete(d.ref));
            batch.delete(chatsCol().doc(chatID));
            await batch.commit();
            deleteBlobs(chatID).catch(e => console.error('deleteBlobs error:', e));
            notifyUsers([user1, user2]);
        } catch (e) { console.error('delete_chat error:', e); }
    });

    socket.on('edit_message', async ({ to, messageId, newText }) => {
        try {
            const from = currentUserEmail;
            const chatID = [from, to].sort().join('__');
            const msgRef = chatsCol().doc(chatID).collection('messages').doc(messageId);
            const msgSnap = await msgRef.get();
            if (msgSnap.exists && msgSnap.data().from === from) {
                await msgRef.update({ editedText: newText, edited: true });
            }
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) io.to(recipientSocketId).emit('message_edited', { messageId, newText });
        } catch (e) { console.error('edit_message error:', e); }
    });

    socket.on('add_reaction', async ({ to, messageId, emoji }) => {
        try {
            const from = currentUserEmail;
            const chatID = [from, to].sort().join('__');
            const msgRef = chatsCol().doc(chatID).collection('messages').doc(messageId);
            const msgSnap = await msgRef.get();
            if (!msgSnap.exists) return;
            const reactions = msgSnap.data().reactions || [];
            const existingIndex = reactions.findIndex(r => r.from === from && r.emoji === emoji);
            const newReactions = existingIndex >= 0
                ? reactions.filter((_, i) => i !== existingIndex)
                : [...reactions, { from, emoji }];
            await msgRef.update({ reactions: newReactions });
            [onlineUsers[from], onlineUsers[to]].forEach(socketId => {
                if (socketId) io.to(socketId).emit('reaction_updated', { messageId, reactions: newReactions });
            });
        } catch (e) { console.error('add_reaction error:', e); }
    });

    socket.on('update_profile', async ({ newDisplayName, newIcon }) => {
        try {
            const email = currentUserEmail; // なりすまし防止：自分のプロフィールしか更新できない
            const updateData = {};
            if (newDisplayName) updateData.displayName = newDisplayName;
            if (newIcon) updateData.icon = newIcon;
            await usersCol().doc(email).update(updateData);
            const user = await getUser(email);
            if (user) notifyUsers([email, ...(user.friends || [])]);
        } catch (e) { console.error('update_profile error:', e); }
    });

    socket.on('webrtc-offer', (payload, ack) => {
        // [FIX] 届いたかどうかを発信側に返す（以前はオフラインだと黙って捨てていた）
        const delivered = isUserOnline(payload && payload.to);
        if (delivered) {
            io.to(_userRoom(payload.to)).emit('webrtc-offer', {
                from: socket.email,
                offer: payload.offer,
                callType: payload.callType || 'audio'
            });
        }
        if (typeof ack === 'function') ack({ delivered });
    });
    socket.on('webrtc-answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-answer', { from: socket.email, answer: payload.answer });
    });
    socket.on('webrtc-ice-candidate', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-ice-candidate', { from: socket.email, candidate: payload.candidate });
    });
    socket.on('webrtc-end-call', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-end-call', { from: socket.email });
    });
    socket.on('webrtc-reject-call', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-reject-call', { from: socket.email });
    });
    socket.on('webrtc-busy', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-busy', { from: socket.email });
    });
    socket.on('webrtc-glare-resolve', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-glare-resolve', { from: socket.email, winner: payload.winner });
    });
    socket.on('webrtc-turn-confirm-request', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-turn-confirm-request', { from: socket.email });
    });
    socket.on('mute_status_changed', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('mute_status_changed', { from: socket.email, isMuted: payload.isMuted });
    });
    socket.on('camera_status_changed', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('camera_status_changed', { from: socket.email, isCameraOff: payload.isCameraOff });
    });

    socket.on('live_canvas_invite', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('live_canvas_invite', { from: socket.email });
    });
    socket.on('live_canvas_draw', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('live_canvas_draw', {
            from: socket.email, x: payload.x, y: payload.y,
            lastX: payload.lastX, lastY: payload.lastY, color: payload.color, size: payload.size
        });
    });
    socket.on('live_canvas_clear', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('live_canvas_clear', { from: socket.email });
    });

    socket.on('typing_start', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('typing_start', { from: socket.email });
    });

    socket.on('typing_stop', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('typing_stop', { from: socket.email });
    });

    socket.on('proximity_offer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_share_invite', {
            from: socket.email,
            fileName: payload.fileName,
            fileSize: payload.fileSize,
            fileType: payload.fileType,
            destType: payload.destType,
            batchAutoAccept: !!payload.batchAutoAccept,
        });
    });

    // [ADD] 複数ファイル一括共有: 送信前にファイル一覧をまとめて相手に提示し、
    // 「受け取る（全部）」「拒否」「選択してダウンロード」をまとめて判断できるようにする。
    socket.on('proximity_batch_offer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_batch_invite', {
            from: socket.email,
            files: payload.files, // [{name, size, type}]
            destType: payload.destType,
            batchId: payload.batchId,
        });
    });

    socket.on('proximity_batch_response', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (!recipientSocketId) return;
        io.to(recipientSocketId).emit('proximity_batch_response', {
            from: socket.email,
            decision: payload.decision, // 'acceptAll' | 'reject' | 'selected'
            selectedNames: payload.selectedNames || null,
            batchId: payload.batchId,
        });
    });

    socket.on('proximity_share_response', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (!recipientSocketId) return;
        io.to(recipientSocketId).emit('proximity_share_response', { from: socket.email, accepted: payload.accepted });
    });

    socket.on('proximity_ready', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_do_offer', { from: socket.email });
    });

    socket.on('proximity_do_answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_do_answer', {
            from: socket.email,
            offer: payload.offer,
        });
    });

    socket.on('proximity_answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_answer', {
            from: socket.email,
            answer: payload.answer,
        });
    });

    socket.on('proximity_ice', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_ice', {
            from: socket.email,
            candidate: payload.candidate,
        });
    });

    socket.on('proximity_received', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_received', {
            from: socket.email,
            fileName: payload.fileName,
        });
    });

    socket.on('proximity_cancel', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_cancel', {
            from: socket.email,
        });
    });
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 近くの人モード（Nearby Share）: 匿名・ログイン不要のP2Pファイル共有
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// [SEC] このnamespaceは意図的に上の io.use() (JWT必須ミドルウェア) の対象外。
// Connectアプリへのログインなしで使えるようにするための設計であり、
// その代わりに要求できることは「同じネットワーク内での発見とシグナリングの中継」のみに限定する。
// ファイルの中身は一切サーバーを経由しない（WebRTC DataChannelで直接P2P、DTLSで暗号化済み）。
const nearbyNsp = io.of('/nearby');

nearbyNsp.on('connection', (socket) => {
    const ip = _getNearbyGroupKey(socket.handshake);
    let myNickname = null;

    socket.on('join', (payload, callback) => {
        if (typeof callback !== 'function') return;
        if (!_nearbyRateLimit(ip, 'join', 20)) return callback({ error: 'しばらく待ってから再試行してください' });
        if (myNickname) return callback({ nickname: myNickname }); // 二重join対策

        let room = nearbyRooms.get(ip);
        if (!room) { room = new Map(); nearbyRooms.set(ip, room); }
        if (room.size >= NEARBY_ROOM_MAX) return callback({ error: '近くの端末が多すぎます。しばらくしてからお試しください' });

        // 設定アプリで名前を設定していればそれを使う（同室内で重複する場合のみ末尾に番号を振る）。
        // 未設定なら従来通りランダム生成。
        const requestedName = sanitizeString(payload?.nickname, 20);
        let nickname;
        if (requestedName) {
            nickname = requestedName;
            if (room.has(nickname)) {
                let n = 2;
                while (room.has(`${requestedName} (${n})`) && n < 100) n++;
                nickname = `${requestedName} (${n})`;
            }
        } else {
            nickname = _generateNearbyNickname(new Set(room.keys()));
        }
        // [FIX] 以前はここで一律8文字に切り詰めており、絵文字は問題なかったが
        // VFSの画像から作ったデータURL（数千〜数万文字）が壊れて相手に表示されなかった。
        // 絵文字（短い文字列）と画像データURL（サイズ上限つき）の両方を許可する。
        const NEARBY_ICON_IMAGE_MAX_LEN = 40000; // クライアント側のICON_IMAGE_MAX_LENと合わせる
        let icon = null;
        if (typeof payload?.icon === 'string') {
            const rawIcon = payload.icon.trim();
            if (rawIcon.startsWith('data:image/')) {
                if (rawIcon.length <= NEARBY_ICON_IMAGE_MAX_LEN) icon = rawIcon;
            } else {
                icon = sanitizeString(rawIcon, 8) || null;
            }
        }
        room.set(nickname, { socketId: socket.id, icon });
        nearbySocketInfo.set(socket.id, { nickname, ip });
        myNickname = nickname;

        callback({ nickname });
        socket.emit('peers_updated', { peers: _nearbyPeerList(ip, socket.id) });
        _broadcastNearbyPeers(nearbyNsp, ip);
    });

    function _leaveNearby() {
        if (!myNickname) return;
        const room = nearbyRooms.get(ip);
        if (room) {
            room.delete(myNickname);
            if (room.size === 0) nearbyRooms.delete(ip);
        }
        nearbySocketInfo.delete(socket.id);
        myNickname = null;
        _broadcastNearbyPeers(nearbyNsp, ip);
    }

    socket.on('leave', () => _leaveNearby());
    socket.on('disconnect', () => _leaveNearby());

    // [SEC] 送信元(from)は必ずサーバー側で管理している myNickname を使う。
    // クライアントから送られてくる from は一切信用しない（なりすまし防止）。
    function _relayTo(eventOut, payload) {
        if (!myNickname) return; // join前のリレーは無視
        const room = nearbyRooms.get(ip);
        const target = room?.get(payload.toNickname);
        if (!target) return; // 相手が見つからない（既にOFF/圏外）
        const { toNickname, ...rest } = payload;
        nearbyNsp.to(target.socketId).emit(eventOut, { ...rest, from: myNickname });
    }

    socket.on('send_offer', (payload) => {
        if (!_nearbyRateLimit(ip, 'offer', 20)) return;
        const fileName = sanitizeString(payload?.fileName, 255);
        const fileSize = Number(payload?.fileSize) || 0;
        const fileType = sanitizeString(payload?.fileType, 100);
        if (!fileName || fileSize <= 0 || fileSize > 500 * 1024 * 1024) return;
        // [FIX] batchAutoAccept が中継時に落ちており、一括承認済みのファイルでも
        // 受信側で毎回個別の確認トーストが出てしまっていた。他のフィールドと同様に中継する。
        _relayTo('incoming_offer', { toNickname: payload.toNickname, fileName, fileSize, fileType, batchAutoAccept: !!payload.batchAutoAccept });
    });

    socket.on('respond', (payload) => _relayTo('offer_response', { toNickname: payload.toNickname, accepted: !!payload.accepted }));
    socket.on('ready', (payload) => _relayTo('do_offer', { toNickname: payload.toNickname }));
    socket.on('webrtc_offer', (payload) => _relayTo('webrtc_offer', { toNickname: payload.toNickname, offer: payload.offer }));
    socket.on('webrtc_answer', (payload) => _relayTo('webrtc_answer', { toNickname: payload.toNickname, answer: payload.answer }));
    socket.on('ice', (payload) => _relayTo('ice', { toNickname: payload.toNickname, candidate: payload.candidate }));
    socket.on('received', (payload) => _relayTo('received', { toNickname: payload.toNickname, fileName: sanitizeString(payload?.fileName, 255) }));
    socket.on('cancel', (payload) => _relayTo('cancel', { toNickname: payload.toNickname }));

    // [ADD] 複数ファイル一括共有（バッチ）の意思確認オファーをリレーする。
    // ファイルの中身は含まず、名前/サイズ/種類の一覧（マニフェスト）のみを中継する。
    socket.on('send_batch_offer', (payload) => {
        if (!_nearbyRateLimit(ip, 'batch_offer', 20)) return;
        const files = Array.isArray(payload?.files) ? payload.files.slice(0, 200).map(f => ({
            name: sanitizeString(f?.name, 255),
            size: Number(f?.size) || 0,
            type: sanitizeString(f?.type, 100),
        })).filter(f => f.name && f.size > 0 && f.size <= 500 * 1024 * 1024) : [];
        if (files.length === 0) return;
        const batchId = sanitizeString(payload?.batchId, 64) || `${Date.now()}_${nodeCrypto.randomBytes(4).toString('hex')}`;
        _relayTo('incoming_batch_offer', { toNickname: payload.toNickname, files, batchId });
    });

    // バッチ提案への回答（全部受け取る/選択/拒否）を提案元に中継する。
    socket.on('batch_response', (payload) => {
        const decision = sanitizeString(payload?.decision, 20);
        if (!['acceptAll', 'selected', 'reject'].includes(decision)) return;
        _relayTo('batch_response', {
            toNickname: payload.toNickname,
            decision,
            selectedNames: Array.isArray(payload?.selectedNames)
                ? payload.selectedNames.slice(0, 200).map(n => sanitizeString(n, 255))
                : null,
            batchId: sanitizeString(payload?.batchId, 64),
        });
    });
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Phase 1: 認証エンドポイント（/auth/*）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.post('/auth/login', async (req, res) => {
    if (!_rateLimit(req, 'auth-login', 5)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const { email, password } = req.body;
        if (!isValidEmail(email) || !password) return res.status(400).json({ error: 'email と password は必須です' });

        const user = await getUser(email);
        if (!user) return res.status(401).json({ error: 'メールアドレスまたはパスワードが違います' });
        if (!user.passwordHash) return res.status(401).json({ error: 'このアカウントはパスワードが設定されていません' });

        const ok = await verifyPassword(password, user.passwordHash);
        if (!ok) return res.status(401).json({ error: 'メールアドレスまたはパスワードが違います' });

        const token = jwt.sign(
            { email: user.email, displayName: user.displayName },
            JWT_SECRET,
            { expiresIn: '30d' }
        );
        res.json({ token, user: { email: user.email, displayName: user.displayName, icon: user.icon } });
    } catch (e) { console.error('auth/login error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/auth/verify', requireAuth, async (req, res) => {
    try {
        const user = await getUser(req.user.email);
        res.json({ valid: true, user: { email: req.user.email, displayName: req.user.displayName, icon: user?.icon || '' } });
    } catch (e) {
        res.json({ valid: true, user: { email: req.user.email, displayName: req.user.displayName, icon: '' } });
    }
});

app.post('/auth/migrate-password', async (req, res) => {
    if (!_rateLimit(req, 'migrate-password', 3)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const { email, password } = req.body;
        if (!isValidEmail(email) || typeof password !== 'string' || password.length < 8) {
            return res.status(400).json({ error: '入力が無効です（パスワードは8文字以上必要です）' });
        }
        const user = await getUser(email);
        if (!user) return res.status(404).json({ error: 'ユーザーが見つかりません' });
        if (user.passwordHash) return res.status(400).json({ error: 'このアカウントは既に移行済みです' });

        const passwordHash = await hashPassword(password);
        await usersCol().doc(email).update({ passwordHash });

        const token = jwt.sign(
            { email: user.email, displayName: user.displayName },
            JWT_SECRET,
            { expiresIn: '30d' }
        );
        res.json({ success: true, token, user: { email: user.email, displayName: user.displayName, icon: user.icon } });
    } catch (e) { console.error('migrate-password error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Phase 2: アカウント管理
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const { Resend } = require('resend');
let _resend = null;
function getResend() {
    if (!process.env.RESEND_API_KEY) return null;
    if (!_resend) _resend = new Resend(process.env.RESEND_API_KEY);
    return _resend;
}
if (!process.env.RESEND_API_KEY) {
    console.warn('[WARN] RESEND_API_KEY が設定されていません。確認コードのメール送信は機能しません。');
}

async function sendAuthCode(email, purpose) {
    const resend = getResend();
    if (!resend) throw new Error('メール送信が設定されていません（RESEND_API_KEY未設定）。サーバー管理者にお問い合わせください。');

    // [SEC] 暗号論的疑似乱数生成器 (CSPRNG) を使用し、予測不可能な6桁PINを発行
    const code = nodeCrypto.randomInt(100000, 1000000).toString();
    const expireAt = Date.now() + 10 * 60 * 1000;
    // attempts: 0 を初期化してブルートフォース試行回数を管理
    await authCodesCol().doc(email).set({ purpose, code, expireAt, attempts: 0 });

    const purposeLabels = {
        'change-password': 'パスワード変更',
        'reset-password': 'パスワードリセット',
        'delete-account': 'アカウント削除',
    };
    const label = purposeLabels[purpose] || '操作';

    await resend.emails.send({
        // [TEMP] rivift.app ドメインがResend側で未認証のため、認証不要な共有アドレスを暫定使用。
        // ドメイン認証（DNSにTXT/CNAME追加）が完了したら 'Rivift <noreply@rivift.app>' に戻すこと。
        from: 'Rivift <onboarding@resend.dev>',
        to: email,
        subject: `Rivift ${label}の確認コード`,
        html: `
            <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:32px;background:#fff;">
                <h2 style="font-size:20px;font-weight:600;color:#1a1a1a;margin:0 0 8px;">${label}の確認</h2>
                <p style="color:#555;margin:0 0 24px;">Riviftアカウントの${label}リクエストを受け付けました。</p>
                <div style="background:#f5f5f5;border-radius:12px;padding:24px;text-align:center;margin:0 0 24px;">
                    <p style="color:#888;font-size:13px;margin:0 0 8px;">確認コード</p>
                    <p style="font-size:36px;font-weight:700;letter-spacing:8px;color:#1a1a1a;margin:0;">${code}</p>
                    <p style="color:#888;font-size:12px;margin:8px 0 0;">10分間有効</p>
                </div>
                <p style="color:#888;font-size:12px;margin:0;">このメールに心当たりがない場合は無視してください。</p>
            </div>
        `,
    });
    return code;
}

async function verifyAuthCode(email, code, purpose) {
    if (!email || !code || typeof code !== 'string') return false;
    const ref = authCodesCol().doc(email);
    const snap = await ref.get();
    if (!snap.exists) return false;
    const data = snap.data();
    if (data.purpose !== purpose) return false;
    if (Date.now() > data.expireAt) {
        await ref.delete().catch(() => {});
        return false;
    }
    // [SEC] ブルートフォース制限: 試行回数が5回以上の場合はコードを破棄して拒否
    if ((data.attempts || 0) >= 5) {
        await ref.delete().catch(() => {});
        return false;
    }

    // [SEC] タイミング攻撃防止 (定時間比較)
    const codeBuf = Buffer.from(code.trim());
    const expectedBuf = Buffer.from(String(data.code).trim());
    const isMatch = codeBuf.length === expectedBuf.length && nodeCrypto.timingSafeEqual(codeBuf, expectedBuf);

    if (!isMatch) {
        const nextAttempts = (data.attempts || 0) + 1;
        if (nextAttempts >= 5) {
            await ref.delete().catch(() => {}); // 5回連続失敗で即座に無効化
        } else {
            await ref.update({ attempts: admin.firestore.FieldValue.increment(1) }).catch(() => {});
        }
        return false;
    }

    // 成功したらワンタイムコードを即座に削除
    await ref.delete().catch(() => {});
    return true;
}

async function hashPassword(password) {
    if (typeof password !== 'string') throw new Error('Password must be a string');
    const salt = nodeCrypto.randomBytes(32).toString('hex');
    const encoder = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey(
        'raw', encoder.encode(password), { name: 'PBKDF2' }, false, ['deriveBits']
    );
    const derivedBits = await crypto.subtle.deriveBits(
        { name: 'PBKDF2', salt: Buffer.from(salt, 'hex'), iterations: 100000, hash: 'SHA-256' },
        keyMaterial, 256
    );
    const hash = Buffer.from(derivedBits).toString('hex');
    return { salt, hash };
}

async function verifyPassword(plainPassword, passwordHash) {
    if (typeof plainPassword !== 'string' || !passwordHash) return false;
    const { salt, hash: storedHash } = passwordHash;
    if (typeof salt !== 'string' || typeof storedHash !== 'string') return false;
    try {
        const encoder = new TextEncoder();
        const keyMaterial = await crypto.subtle.importKey(
            'raw', encoder.encode(plainPassword), { name: 'PBKDF2' }, false, ['deriveBits']
        );
        const derivedBits = await crypto.subtle.deriveBits(
            { name: 'PBKDF2', salt: Buffer.from(salt, 'hex'), iterations: 100000, hash: 'SHA-256' },
            keyMaterial, 256
        );
        const computedHash = Buffer.from(derivedBits).toString('hex');

        // [SEC] タイミング攻撃防止 (定時間比較)
        const compBuf = Buffer.from(computedHash, 'hex');
        const storeBuf = Buffer.from(storedHash, 'hex');
        if (compBuf.length !== storeBuf.length) return false;
        return nodeCrypto.timingSafeEqual(compBuf, storeBuf);
    } catch (e) {
        console.error('verifyPassword error:', e);
        return false;
    }
}

app.post('/auth/send-code', async (req, res) => {
    if (!_rateLimit(req, 'auth-send-code', 3)) return res.status(429).json({ error: 'しばらく待ってから再試行してください' });
    const { email, purpose } = req.body;
    if (!isValidEmail(email) || purpose !== 'reset-password') return res.status(400).json({ error: '入力が無効です' });

    // [SEC] 同一メールアドレス宛の送信頻度制限 (メールフラッディング対策)
    const emailKey = `send-code-email:${email}`;
    const now = Date.now();
    const timestamps = (_rateLimitMap.get(emailKey) || []).filter(t => now - t < 60_000);
    if (timestamps.length >= 2) {
        return res.status(429).json({ error: 'このメールアドレスへの送信頻度が高すぎます。しばらく待ってから再試行してください。' });
    }
    timestamps.push(now);
    _rateLimitMap.set(emailKey, timestamps);

    const user = await getUser(email);
    if (user) await sendAuthCode(email, purpose).catch(e => console.error('sendAuthCode error:', e));
    res.json({ success: true });
});

app.post('/auth/change-password', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'change-password', 5)) return res.status(429).json({ error: 'リクエストが多すぎます。しばらく待ってから再試行してください' });
    try {
        const email = req.user.email;
        const { currentPassword, newPassword } = req.body;
        if (!currentPassword || !newPassword || typeof newPassword !== 'string' || newPassword.length < 8) {
            return res.status(400).json({ error: '現在のパスワードと新しいパスワード（8文字以上）は必須です' });
        }

        const user = await getUser(email);
        if (!user || !user.passwordHash) return res.status(400).json({ error: 'このアカウントはパスワードが設定されていません' });

        const ok = await verifyPassword(currentPassword, user.passwordHash);
        if (!ok) return res.status(401).json({ error: '現在のパスワードが正しくありません' });

        const passwordHash = await hashPassword(newPassword);
        await usersCol().doc(email).update({ passwordHash });
        res.json({ success: true });
    } catch (e) { console.error('change-password error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/auth/update-icon', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'update-icon', 20)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const email = req.user.email;
        const { icon } = req.body;
        if (typeof icon !== 'string' || icon.length > 400000) {
            return res.status(400).json({ error: 'iconが無効です' });
        }
        await usersCol().doc(email).update({ icon });
        res.json({ success: true, icon });
    } catch (e) { console.error('update-icon error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── パスワードリセット（未ログイン）
// [一時停止中] クライアント側の「パスワードを忘れた場合」UIは現在無効化されています。
// 理由：Resendの送信元アドレスがサンドボックスドメイン(onboarding@resend.dev)のままで、
// Resend側の仕様により実際に送信できるのはResendアカウント登録者本人のメールアドレスのみ。
// rivift.appドメインをResend側でDNS認証すれば、任意の宛先に送れるようになりUIを復活できる。
// このエンドポイント自体は残しているが、上記の理由で通常のユーザーには届かない。
// [SEC/RECOVERY] 秘密鍵は「昔のパスワードで暗号化されたまま」なので、パスワードだけ
// 変えても古い鍵は復号できない。そのためリセット時はクライアント側で新しい鍵ペアを
// 生成させ、newPublicKeyJwk/newEncryptedPrivateKeyPayloadが渡された場合は
// まとめて置き換える（過去のE2E暗号化データは読めなくなるが、アカウントは使えるようになる）。
app.post('/auth/reset-password', async (req, res) => {
    if (!_rateLimit(req, 'reset-password', 5)) {
        return res.status(429).json({ error: 'リクエストが多すぎます。しばらく待ってから再試行してください' });
    }
    try {
        const { email, code, newPassword, newPublicKeyJwk, newEncryptedPrivateKeyPayload } = req.body;
        if (!isValidEmail(email) || typeof code !== 'string' || typeof newPassword !== 'string' || newPassword.length < 8) {
            return res.status(400).json({ error: '入力が無効です（パスワードは8文字以上必要です）' });
        }

        const valid = await verifyAuthCode(email, code, 'reset-password');
        if (!valid) return res.status(400).json({ error: 'コードが無効または期限切れです（試行回数上限を超えた場合は再発行してください）' });

        const user = await getUser(email);
        if (!user) return res.status(400).json({ error: '入力が無効です' });

        const passwordHash = await hashPassword(newPassword);
        const update = { passwordHash };
        if (newPublicKeyJwk && newEncryptedPrivateKeyPayload
            && newEncryptedPrivateKeyPayload.salt && newEncryptedPrivateKeyPayload.iv && newEncryptedPrivateKeyPayload.encryptedData) {
            update.publicKeyJwk = newPublicKeyJwk;
            update.encryptedPrivateKeyPayload = newEncryptedPrivateKeyPayload;
        }
        await usersCol().doc(email).update(update);

        const token = jwt.sign({ email: user.email, displayName: user.displayName }, JWT_SECRET, { expiresIn: '30d' });
        res.json({ success: true, token, user: { email: user.email, displayName: user.displayName, icon: user.icon }, keysReset: !!update.encryptedPrivateKeyPayload });
    } catch (e) { console.error('reset-password error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/auth/delete-account', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'delete-account', 5)) return res.status(429).json({ error: 'リクエストが多すぎます。しばらく待ってから再試行してください' });
    try {
        const email = req.user.email;
        const { currentPassword } = req.body;
        if (!currentPassword) return res.status(400).json({ error: '現在のパスワードは必須です' });

        const user = await getUser(email);
        if (!user) return res.status(404).json({ error: 'ユーザーが見つかりません' });
        if (!user.passwordHash) return res.status(400).json({ error: 'このアカウントはパスワードが設定されていません' });

        const ok = await verifyPassword(currentPassword, user.passwordHash);
        if (!ok) return res.status(401).json({ error: '現在のパスワードが正しくありません' });

        const batchSize = 400;

        const myAppsSnap = await appsCol().where('authorEmail', '==', email).get();
        for (let i = 0; i < myAppsSnap.docs.length; i += batchSize) {
            const batch = db.batch();
            myAppsSnap.docs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }

        const myCommentsSnap = await commentsCol().where('email', '==', email).get();
        for (let i = 0; i < myCommentsSnap.docs.length; i += batchSize) {
            const batch = db.batch();
            myCommentsSnap.docs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }

        const mailFromSnap = await mailCol().where('from', '==', email).get();
        const mailToSnap = await mailCol().where('to', '==', email).get();
        const mailDocs = [...new Map([...mailFromSnap.docs, ...mailToSnap.docs].map(d => [d.id, d])).values()];
        for (let i = 0; i < mailDocs.length; i += batchSize) {
            const batch = db.batch();
            mailDocs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }

        if (user && user.friends) {
            for (const friendEmail of user.friends) {
                const chatID = [email, friendEmail].sort().join('__');
                const msgsSnap = await chatsCol().doc(chatID).collection('messages').get();
                for (let i = 0; i < msgsSnap.docs.length; i += batchSize) {
                    const batch = db.batch();
                    msgsSnap.docs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
                    await batch.commit();
                }
                await deleteBlobs(chatID).catch(() => {});
                await chatsCol().doc(chatID).delete();
                await usersCol().doc(friendEmail).update({ friends: admin.firestore.FieldValue.arrayRemove(email) });
            }
        }

        await authCodesCol().doc(email).delete().catch(() => {});
        await usersCol().doc(email).delete();

        res.json({ success: true });
    } catch (e) { console.error('delete-account error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Phase 4: Rivift Mail エンドポイント
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.get('/mail/inbox', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const limit = Math.min(Number(req.query.limit) || 20, 50);
        const before = req.query.before ? Number(req.query.before) : null;

        let q = mailCol()
            .where('to', '==', email)
            .orderBy('sentAt', 'desc')
            .limit(limit);
        if (before) q = q.where('sentAt', '<', before);

        const snap = await q.get();
        const messages = snap.docs
            .map(d => ({ id: d.id, ...d.data() }))
            .filter(m => !(m.deletedBy || []).includes(email) && !(m.trashBy || []).includes(email));

        res.json({ messages });
    } catch (e) { console.error('mail/inbox error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.get('/mail/sent', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const limit = Math.min(Number(req.query.limit) || 20, 50);
        const before = req.query.before ? Number(req.query.before) : null;

        let q = mailCol()
            .where('from', '==', email)
            .orderBy('sentAt', 'desc')
            .limit(limit);
        if (before) q = q.where('sentAt', '<', before);

        const snap = await q.get();
        const messages = snap.docs
            .map(d => ({ id: d.id, ...d.data() }))
            .filter(m => !(m.deletedBy || []).includes(email));

        res.json({ messages });
    } catch (e) { console.error('mail/sent error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.get('/mail/trash', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const limit = Math.min(Number(req.query.limit) || 20, 50);

        let q = mailCol()
            .where('trashBy', 'array-contains', email)
            .orderBy('sentAt', 'desc')
            .limit(limit);

        const snap = await q.get();
        const messages = snap.docs
            .map(d => ({ id: d.id, ...d.data() }))
            .filter(m => !(m.deletedBy || []).includes(email));

        res.json({ messages });
    } catch (e) { console.error('mail/trash error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/mail/send', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'mail-send', 10)) return res.status(429).json({ error: 'メール送信が多すぎます' });
    try {
        const from = req.user.email;
        const { to, encryptedPayload, keyForRecipient, keyForSender } = req.body;

        if (!isValidEmail(to)) return res.status(400).json({ error: '宛先メールアドレスが無効です' });
        if (from === to) return res.status(400).json({ error: '自分自身には送れません' });

        if (!encryptedPayload || typeof encryptedPayload.iv !== 'string' || typeof encryptedPayload.data !== 'string') {
            return res.status(400).json({ error: '暗号化データが無効です' });
        }
        if (typeof keyForRecipient !== 'string' || typeof keyForSender !== 'string') {
            return res.status(400).json({ error: '暗号化鍵が無効です' });
        }
        if (encryptedPayload.data.length > 50000 || keyForRecipient.length > 2000 || keyForSender.length > 2000) {
            return res.status(400).json({ error: 'データサイズが大きすぎます' });
        }

        const recipient = await getUser(to);
        if (!recipient) return res.status(404).json({ error: 'Riviftアカウントが見つかりません' });

        const now = Date.now();
        const docRef = await mailCol().add({
            from, to,
            encryptedPayload,
            keyForRecipient,
            keyForSender,
            sentAt: now, readAt: null, deletedBy: [], trashBy: [],
        });

        const recipientSocketId = onlineUsers[to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('new_mail', {
                messageId: docRef.id, from, sentAt: now,
            });
        }

        res.json({ success: true, messageId: docRef.id });
    } catch (e) { console.error('mail/send error:', e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/mail/read', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { messageId } = req.body;
        if (!messageId) return res.status(400).json({ error: 'messageId は必須です' });

        const ref = mailCol().doc(messageId);
        const snap = await ref.get();
        if (!snap.exists) return res.status(404).json({ error: 'メッセージが見つかりません' });
        if (snap.data().to !== email) return res.status(403).json({ error: '権限がありません' });

        await ref.update({ readAt: Date.now() });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/mail/trash', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { messageId } = req.body;
        if (!messageId) return res.status(400).json({ error: 'messageId は必須です' });

        const ref = mailCol().doc(messageId);
        const snap = await ref.get();
        if (!snap.exists) return res.status(404).json({ error: 'メッセージが見つかりません' });
        const data = snap.data();
        if (data.from !== email && data.to !== email) return res.status(403).json({ error: '権限がありません' });

        await ref.update({ trashBy: admin.firestore.FieldValue.arrayUnion(email) });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/mail/delete', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { messageId } = req.body;
        if (!messageId) return res.status(400).json({ error: 'messageId は必須です' });

        const ref = mailCol().doc(messageId);
        const snap = await ref.get();
        if (!snap.exists) return res.status(404).json({ error: 'メッセージが見つかりません' });
        const data = snap.data();
        if (data.from !== email && data.to !== email) return res.status(403).json({ error: '権限がありません' });

        const newDeletedBy = [...new Set([...(data.deletedBy || []), email])];
        const bothDeleted = [data.from, data.to].every(e => newDeletedBy.includes(e));
        if (bothDeleted) {
            await ref.delete();
        } else {
            await ref.update({ deletedBy: newDeletedBy });
        }
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/mail/unread-count', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const snap = await mailCol()
            .where('to', '==', email)
            .where('readAt', '==', null)
            .get();
        const count = snap.docs.filter(d => {
            const data = d.data();
            return !(data.deletedBy || []).includes(email) && !(data.trashBy || []).includes(email);
        }).length;
        res.json({ count });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 共同編集（Office系アプリ共通基盤）エンドポイント
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// [SEC] URLの :docId は "%2F" で "/" を含められ、Firestoreのパスが別のドキュメントを指してしまうため、全ルート共通で検証する
app.param('docId', (req, res, next, docId) => {
    if (!_isSafeDocId(docId)) return res.status(400).json({ error: 'docId が不正です' });
    next();
});

// ドキュメントの共同編集セッションを作成（まだ無ければ）。appType: 'cell' | 'docs' | 'slides' 等
app.post('/collab/docs', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { docId, appType, title } = req.body;
        if (!_isSafeDocId(docId)) {
            return res.status(400).json({ error: 'docId が不正です' });
        }
        if (!['cell', 'docs', 'slides'].includes(appType)) {
            return res.status(400).json({ error: 'appType が不正です' });
        }
        const ref = collabDocsCol().doc(docId);
        const snap = await ref.get();
        if (snap.exists) {
            const data = snap.data();
            const role = _collabRoleOf(data, email);
            if (!role) return res.status(403).json({ error: 'このドキュメントへのアクセス権がありません' });
            return res.json({ success: true, doc: _collabPublicDoc(docId, data, email) });
        }
        const doc = {
            appType, ownerEmail: email,
            title: sanitizeString(title, 200) || '無題',
            collaborators: {}, // { email: role } ※ownerは含めない
            memberEmails: [email], // 「共有されたドキュメント」一覧の検索用（owner + collaborators）
            linkRole: 'none',
            createdAt: Date.now(), updatedAt: Date.now(),
        };
        await ref.set(doc);
        res.json({ success: true, doc: { id: docId, ...doc, yourRole: 'owner' } });
    } catch (e) { console.error('collab/docs create error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ドキュメントの共有状態（メンバー一覧・各自の権限・リンク共有設定）を取得
app.get('/collab/docs/:docId', requireCollabAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const ref = collabDocsCol().doc(req.params.docId);
        const snap = await ref.get();
        if (!snap.exists) return res.status(404).json({ error: 'ドキュメントが見つかりません' });
        const data = snap.data();
        if (!_collabRoleOf(data, email)) return res.status(403).json({ error: 'アクセス権がありません' });
        const members = await _collabResolveMembers(data);
        if (!Array.isArray(data.memberEmails)) ref.update({ memberEmails: members.map(m => m.email) }).catch(() => { }); // 旧データの補完
        res.json({ doc: { ..._collabPublicDoc(req.params.docId, data, email), members } });
    } catch (e) { console.error('collab/docs get error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 自分が参加している（共有された／自分が作った）ドキュメントの一覧
app.get('/collab/my-docs', requireCollabAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const snap = await collabDocsCol().where('memberEmails', 'array-contains', email).limit(200).get();
        const rows = snap.docs.filter(d => d.data().appType === 'docs').map(d => ({ id: d.id, data: d.data() }));
        const owners = [...new Set(rows.map(r => r.data.ownerEmail).filter(o => o && !_collabIsGuestEmail(o)))];
        const oSnaps = owners.length ? await db.getAll(...owners.map(o => usersCol().doc(o))) : [];
        const ownerName = {};
        oSnaps.forEach(sn => { if (sn.exists) ownerName[sn.id] = sn.data().displayName || sn.id; });
        const docs = rows.map(({ id, data }) => ({
            id, title: data.title || '無題', ownerEmail: data.ownerEmail, ownerName: ownerName[data.ownerEmail] || data.ownerEmail,
            yourRole: _collabRoleOf(data, email), updatedAt: data.contentUpdatedAt || data.updatedAt || 0,
            memberCount: 1 + Object.keys(_collabNormalize(data.collaborators)).length, hasContent: !!data.contentRev,
        })).sort((a, b) => b.updatedAt - a.updatedAt);
        res.json({ docs });
    } catch (e) { console.error('collab/my-docs error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ドキュメント本文の取得（オーナーがオフラインでも、権限があれば誰でも読める）
app.get('/collab/docs/:docId/content', requireCollabAuth, async (req, res) => {
    try {
        const ref = collabDocsCol().doc(req.params.docId);
        const snap = await ref.get();
        if (!snap.exists) return res.status(404).json({ error: 'ドキュメントが見つかりません' });
        const data = snap.data();
        if (!_collabRoleOf(data, req.user.email)) return res.status(403).json({ error: 'アクセス権がありません' });
        if (!data.contentRev) return res.json({ content: null });
        const chunks = await ref.collection('content').orderBy('i').get();
        let content = null;
        try { content = JSON.parse(chunks.docs.map(d => d.data().t).join('')); } catch { /* 破損時は null */ }
        res.json({ content, rev: data.contentRev, updatedAt: data.contentUpdatedAt || null, by: data.contentBy || null });
    } catch (e) { console.error('collab/content get error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ドキュメント本文の保存（editor 以上）。チャンクに分割して原子的に置き換える
app.put('/collab/docs/:docId/content', express.json({ limit: '12mb' }), requireCollabAuth, async (req, res) => {
    if (!_rateLimit(req, 'collab-content', 90)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const email = req.user.email;
        const ref = collabDocsCol().doc(req.params.docId);
        const snap = await ref.get();
        if (!snap.exists) return res.status(404).json({ error: 'ドキュメントが見つかりません' });
        const data = snap.data();
        if (!_collabHasAtLeast(_collabRoleOf(data, email), 'editor')) return res.status(403).json({ error: '編集権限がありません' });
        const { title, pageConfig, pages } = req.body || {};
        if (!Array.isArray(pages) || pages.length < 1 || pages.length > 500 || pages.some(pg => typeof pg !== 'string')) {
            return res.status(400).json({ error: 'pages が不正です' });
        }
        const safeTitle = sanitizeString(title, 200) || data.title || '無題';
        const payload = JSON.stringify({ title: safeTitle, pageConfig: (pageConfig && typeof pageConfig === 'object') ? pageConfig : null, pages });
        if (payload.length > COLLAB_CONTENT_MAX_CHARS) return res.status(413).json({ error: 'ドキュメントが大きすぎて保存できません' });

        const col = ref.collection('content');
        const old = await col.get();
        const batch = db.batch();
        old.docs.forEach(d => batch.delete(d.ref));
        for (let i = 0, n = 0; i < payload.length; i += COLLAB_CONTENT_CHUNK, n++) {
            batch.set(col.doc(String(n).padStart(4, '0')), { i: n, t: payload.slice(i, i + COLLAB_CONTENT_CHUNK) });
        }
        const rev = (data.contentRev || 0) + 1;
        const now = Date.now();
        batch.update(ref, { contentRev: rev, contentUpdatedAt: now, contentBy: email, title: safeTitle, updatedAt: now });
        await batch.commit();
        res.json({ success: true, rev, updatedAt: now });
    } catch (e) { console.error('collab/content put error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 招待コードを発行（オーナーのみ）。role: 招待経由で参加した人に付与する初期権限
app.post('/collab/docs/:docId/invite', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'collab-invite', 20)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const email = req.user.email;
        const { role = 'editor', maxUses = 20 } = req.body;
        if (!['viewer', 'commenter', 'editor'].includes(role)) {
            return res.status(400).json({ error: 'role が不正です' });
        }
        const docRef = collabDocsCol().doc(req.params.docId);
        const snap = await docRef.get();
        if (!snap.exists) return res.status(404).json({ error: 'ドキュメントが見つかりません' });
        if (snap.data().ownerEmail !== email) return res.status(403).json({ error: 'オーナーのみ招待コードを発行できます' });

        // [SEC] 有効な招待コードの数に上限を設け、期限切れのものはここで掃除する
        const existing = await collabInvitesCol().where('docId', '==', req.params.docId).limit(100).get();
        const nowTs = Date.now();
        const expired = existing.docs.filter(d => !(Number(d.data().expiresAt) > nowTs));
        if (expired.length) {
            const cleanup = db.batch();
            expired.forEach(d => cleanup.delete(d.ref));
            await cleanup.commit().catch(() => { });
        }
        if (existing.size - expired.length >= COLLAB_INVITE_MAX_ACTIVE_PER_DOC) {
            return res.status(429).json({ error: '有効な招待コードが多すぎます。不要なコードの期限が切れるのを待つか、共有リンクを使ってください' });
        }

        const record = {
            docId: req.params.docId, role,
            createdBy: email,
            createdAt: nowTs,
            expiresAt: nowTs + COLLAB_INVITE_TTL_MS,
            maxUses: Math.min(Math.max(Number(maxUses) || 20, 1), COLLAB_INVITE_MAX_USES),
            uses: 0,
        };
        // [SEC] 既存コードを上書きしないよう create()（衝突したら別のコードで再試行）
        let code = null;
        for (let attempt = 0; attempt < 5 && !code; attempt++) {
            const candidate = _generateInviteCode();
            try {
                await collabInvitesCol().doc(candidate).create({ code: candidate, ...record });
                code = candidate;
            } catch (e) {
                if (!(e && (e.code === 6 || /ALREADY_EXISTS/.test(String(e.message))))) throw e;
            }
        }
        if (!code) throw new Error('invite code generation failed');
        res.json({ success: true, code, role, expiresAt: record.expiresAt });
    } catch (e) { console.error('collab/invite error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 招待コード or 共有リンクのコードでドキュメントに参加（Connect アカウント / ゲスト どちらでも可）
// [SEC] コードの総当たり対策として、「失敗」だけを別枠で数えて制限する（成功は数えない）。
app.post('/collab/join', requireCollabAuth, async (req, res) => {
    if (!_rateLimit(req, 'collab-join', 20)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    const failKeys = [`join-fail:ip:${req.ip}`, `join-fail:user:${req.user.email}`];
    try {
        if (failKeys.some(k => _failBlocked(k, 15, 10 * 60_000))) {
            return res.status(429).json({ error: '失敗が続いたため、しばらく待ってからお試しください' });
        }
        const email = req.user.email;
        const resolved = await _collabResolveJoinCode(req.body?.code);
        if (resolved.error) {
            failKeys.forEach(_failRecord);
            return res.status(resolved.error[0]).json({ error: resolved.error[1] });
        }
        const updated = await _collabJoinWith(resolved, email);
        res.json({ success: true, doc: _collabPublicDoc(resolved.docId, updated, email) });
        _collabNotifyJoined(resolved.docId, updated, email);
    } catch (e) {
        if (e instanceof CollabJoinError) { failKeys.forEach(_failRecord); return res.status(e.status).json({ error: e.message }); }
        console.error('collab/join error:', e); res.status(500).json({ error: 'Server error' });
    }
});

// アカウントを持っていない人向け: 共有リンク/招待コードで、自動的に仮ユーザー（ゲスト）を作って参加する
app.post('/collab/guest-join', async (req, res) => {
    if (!_rateLimit(req, 'collab-guest', 10)) return res.status(429).json({ error: 'リクエストが多すぎます。しばらくしてからお試しください' });
    const failKey = `join-fail:ip:${req.ip}`;
    let guest = null;
    try {
        if (!JWT_SECRET) return res.status(500).json({ error: 'サーバー設定エラー' });
        if (_failBlocked(failKey, 15, 10 * 60_000)) {
            return res.status(429).json({ error: '失敗が続いたため、しばらくしてからお試しください' });
        }
        const resolved = await _collabResolveJoinCode(req.body?.code);
        if (resolved.error) {
            _failRecord(failKey);
            return res.status(resolved.error[0]).json({ error: resolved.error[1] });
        }
        guest = await _collabCreateGuest(_cleanLabel(req.body?.name, 30));
        const updated = await _collabJoinWith(resolved, guest.email);
        res.json({
            success: true, token: guest.token,
            user: { email: guest.email, displayName: guest.displayName, color: guest.color, guest: true },
            doc: _collabPublicDoc(resolved.docId, updated, guest.email),
        });
        _collabNotifyJoined(resolved.docId, updated, guest.email);
    } catch (e) {
        // 参加に失敗したら、作りかけのゲストを残さない
        if (guest) collabGuestsCol().doc(guest.email).delete().catch(() => { });
        if (e instanceof CollabJoinError) { _failRecord(failKey); return res.status(e.status).json({ error: e.message }); }
        console.error('collab/guest-join error:', e); res.status(500).json({ error: 'Server error' });
    }
});

// コラボレーターの権限を個別に変更、または削除（オーナーのみ）
app.post('/collab/docs/:docId/permission', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const { targetEmail, role } = req.body; // role が null/'' なら共有解除
        const docRef = collabDocsCol().doc(req.params.docId);
        const snap = await docRef.get();
        if (!snap.exists) return res.status(404).json({ error: 'ドキュメントが見つかりません' });
        const data = snap.data();
        if (data.ownerEmail !== email) return res.status(403).json({ error: 'オーナーのみ権限を変更できます' });
        if (!isValidEmail(targetEmail) || targetEmail === email) return res.status(400).json({ error: '対象メールアドレスが不正です' });

        if (!role) {
            await _collabSetRole(docRef, data, targetEmail, null);
        } else {
            if (!['viewer', 'commenter', 'editor'].includes(role)) return res.status(400).json({ error: 'role が不正です' });
            await _collabSetRole(docRef, data, targetEmail, role);
        }

        io.to(_userRoom(targetEmail)).emit('collab_permission_changed', { docId: req.params.docId, role: role || null });
        if (!role) _collabKick(req.params.docId, targetEmail); // 共有解除したら即座にルームとカーソル表示から外す
        // 同じドキュメントを開いている全員に、コラボレーター一覧の更新を通知
        io.to(`collab:${req.params.docId}`).emit('collab_collaborators_updated', { docId: req.params.docId });

        res.json({ success: true });
    } catch (e) { console.error('collab/permission error:', e); res.status(500).json({ error: 'Server error' }); }
});

// オーナー専用ルートの共通チェック
async function _collabOwnerDoc(req, res) {
    const docRef = collabDocsCol().doc(req.params.docId);
    const snap = await docRef.get();
    if (!snap.exists) { res.status(404).json({ error: 'ドキュメントが見つかりません' }); return null; }
    const data = snap.data();
    if (data.ownerEmail !== req.user.email) { res.status(403).json({ error: 'オーナーのみ操作できます' }); return null; }
    return { docRef, data };
}

// Rivift Connect アカウントを持つ人をメールアドレスで直接招待（オーナーのみ）
app.post('/collab/docs/:docId/members', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'collab-members', 40)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const ctx = await _collabOwnerDoc(req, res); if (!ctx) return;
        const { docRef, data } = ctx;
        const role = req.body?.role || 'editor';
        let target = sanitizeString(req.body?.email, 254);
        if (!['viewer', 'commenter', 'editor'].includes(role)) return res.status(400).json({ error: 'role が不正です' });
        if (!isValidEmail(target) || _collabIsGuestEmail(target)) return res.status(400).json({ error: 'メールアドレスが不正です' });
        if (target === data.ownerEmail) return res.status(400).json({ error: '自分自身は招待できません' });
        let user = await getUser(target);
        if (!user && target !== target.toLowerCase()) { target = target.toLowerCase(); user = await getUser(target); }
        if (!user) return res.status(404).json({ error: 'このメールアドレスの Rivift Connect アカウントが見つかりません。共有リンクを送れば、アカウント無しでもゲストとして参加できます' });
        await _collabSetRole(docRef, data, target, role);
        io.to(_userRoom(target)).emit('collab_invited', { docId: req.params.docId, title: data.title, role, by: req.user.displayName || req.user.email });
        io.to(`collab:${req.params.docId}`).emit('collab_collaborators_updated', { docId: req.params.docId });
        res.json({ success: true, email: target });
    } catch (e) { console.error('collab/members error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 一括操作（オーナーのみ）: 権限の一括変更 / 一括で共有解除
// body: { action: 'set-role'|'remove', role?, scope: 'all'|'guests'|'accounts' }
app.post('/collab/docs/:docId/members/bulk', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'collab-bulk', 30)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const ctx = await _collabOwnerDoc(req, res); if (!ctx) return;
        const { docRef, data } = ctx;
        const { action, role, scope = 'all' } = req.body || {};
        if (!['set-role', 'remove'].includes(action)) return res.status(400).json({ error: 'action が不正です' });
        if (action === 'set-role' && !['viewer', 'commenter', 'editor'].includes(role)) return res.status(400).json({ error: 'role が不正です' });
        if (!['all', 'guests', 'accounts'].includes(scope)) return res.status(400).json({ error: 'scope が不正です' });
        const cur = _collabNormalize(data.collaborators);
        const targets = Object.keys(cur).filter(e => scope === 'all' || (scope === 'guests') === _collabIsGuestEmail(e));
        const next = { ...cur };
        targets.forEach(e => { if (action === 'remove') delete next[e]; else next[e] = role; });
        await docRef.update({ collaborators: next, memberEmails: [data.ownerEmail, ...Object.keys(next)], updatedAt: Date.now() });
        _collabInvalidateRoles(req.params.docId);
        targets.forEach(e => {
            io.to(_userRoom(e)).emit('collab_permission_changed', { docId: req.params.docId, role: action === 'remove' ? null : role });
            if (action === 'remove') _collabKick(req.params.docId, e);
        });
        io.to(`collab:${req.params.docId}`).emit('collab_collaborators_updated', { docId: req.params.docId });
        res.json({ success: true, changed: targets.length });
    } catch (e) { console.error('collab/bulk error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 共有リンク（「リンクを知っている全員」）の設定（オーナーのみ）
// body: { role: 'none'|'viewer'|'commenter'|'editor', regenerate?: boolean }
app.post('/collab/docs/:docId/link', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'collab-link', 30)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const ctx = await _collabOwnerDoc(req, res); if (!ctx) return;
        const { docRef, data } = ctx;
        const role = req.body?.role ?? data.linkRole ?? 'none';
        if (!COLLAB_LINK_ROLES.includes(role)) return res.status(400).json({ error: 'role が不正です' });
        let code = data.linkCode;
        if (!code || req.body?.regenerate) code = _generateLinkCode();
        await docRef.update({ linkRole: role, linkCode: code, updatedAt: Date.now() });
        res.json({ success: true, linkRole: role, linkCode: role === 'none' ? null : code });
    } catch (e) { console.error('collab/link error:', e); res.status(500).json({ error: 'Server error' }); }
});


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 大きいファイルの分割保存（チャットのファイル送信）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// クライアントが暗号化済みのバイト列を約700KBずつに分けて送り、Firestoreに保存する。
// メッセージ本体(private_message)には id・分割数・サイズなど小さな情報だけが入る。
// サーバーは暗号化済みデータしか見えない（中身は読めない）。
const BLOB_MAX_CHUNKS = 24;               // 24 × 約700KB ≒ 16MBまで
const BLOB_CHUNK_LIMIT = 780 * 1024;      // 1チャンクの受付上限（Firestore 1MiB/doc に収まる範囲）
const BLOB_MAX_TOTAL_BYTES = BLOB_MAX_CHUNKS * BLOB_CHUNK_LIMIT; // 1つの添付ファイル全体の上限
const BLOB_MAX_PENDING_PER_USER = 8;      // 1人が同時に持てる「未送信のアップロード」数
const BLOB_DAILY_QUOTA_BYTES = (Number(process.env.BLOB_DAILY_QUOTA_MB) || 1024) * 1024 * 1024; // 1人あたり24時間のアップロード上限
const BLOB_ORPHAN_TTL_MS = 60 * 60 * 1000;

const _friendCache = new Map();           // `${a}|${b}` → expireAt
async function _isFriendOf(me, partner) {
    const key = `${me}|${partner}`, hit = _friendCache.get(key);
    if (hit && hit > Date.now()) return true;
    const u = await getUser(me);
    const ok = !!(u && Array.isArray(u.friends) && u.friends.includes(partner));
    if (ok) {
        if (_friendCache.size > 2000) _friendCache.clear();
        _friendCache.set(key, Date.now() + 60_000);
    }
    return ok;
}
function _friendCacheInvalidate(a, b) {
    _friendCache.delete(`${a}|${b}`);
    _friendCache.delete(`${b}|${a}`);
}
function _blobParams(req) {
    const msgId = req.params.msgId, index = Number(req.params.index), partner = String(req.query.partner || '');
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(msgId)) return null;
    if (!Number.isInteger(index) || index < 0 || index >= BLOB_MAX_CHUNKS) return null;
    if (!isValidEmail(partner)) return null;
    return { msgId, index, partner, chatID: [req.user.email, partner].sort().join('__') };
}
const blobsCol = (chatID) => chatsCol().doc(chatID).collection('blobs');
const uploadMarkersCol = () => db.collection('blob_uploads');
const _markerId = (chatID, msgId) => `${chatID}__${msgId}`;

// 1人あたりの24時間アップロード量の上限（メモリ上の概算。再起動でリセット）
const _blobQuota = new Map(); // email -> { resetAt, bytes }
function _blobQuotaTake(email, bytes) {
    const now = Date.now();
    let q = _blobQuota.get(email);
    if (!q || q.resetAt < now) {
        if (_blobQuota.size > 5000) for (const [k, v] of _blobQuota) if (v.resetAt < now) _blobQuota.delete(k);
        q = { resetAt: now + 24 * 3600_000, bytes: 0 };
        _blobQuota.set(email, q);
    }
    if (q.bytes + bytes > BLOB_DAILY_QUOTA_BYTES) return false;
    q.bytes += bytes;
    return true;
}
function _blobQuotaRefund(email, bytes) {
    const q = _blobQuota.get(email);
    if (q) q.bytes = Math.max(0, q.bytes - bytes);
}

// 「送信済みメッセージに紐づいたチャンクか」の確認（送信済みと分かったものだけ短時間キャッシュ）
const _blobSentCache = new Map(); // `${chatID}__${msgId}` → expireAt
async function _blobMessageSent(chatID, msgId) {
    const key = _markerId(chatID, msgId), hit = _blobSentCache.get(key);
    if (hit && hit > Date.now()) return true;
    const snap = await chatsCol().doc(chatID).collection('messages').doc(msgId).get();
    if (!snap.exists) return false;
    if (_blobSentCache.size > 2000) _blobSentCache.clear();
    _blobSentCache.set(key, Date.now() + 60_000);
    return true;
}

// private_message で添付ファイルを送る時の確認: 本人がアップロードした全チャンクが揃っているか
async function _verifyBlobUpload(chatID, msgId, uploader, chunks) {
    const m = await uploadMarkersCol().doc(_markerId(chatID, msgId)).get();
    if (!m.exists) return false;
    const d = m.data();
    if (d.uploader !== uploader) return false;
    if (d.sizes === undefined) return true; // この修正より前に始まったアップロード（サイズ記録なし）は従来どおり通す
    for (let i = 0; i < chunks; i++) if (!(Number(d.sizes[i]) > 0)) return false;
    return true;
}

// 送信を途中でやめた時など、クライアントから「このアップロードを破棄して」と頼むための窓口。
// すでにメッセージとして送信済みのものは消さない（409）。自分がアップロードした分だけ消せる。
app.delete('/chat/blob/:msgId', requireAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'blob', 600)) return res.status(429).json({ error: 'Too many requests' });
        const msgId = req.params.msgId, partner = String(req.query.partner || '');
        if (!/^[A-Za-z0-9_-]{1,64}$/.test(msgId) || !isValidEmail(partner)) return res.status(400).json({ error: 'Bad request' });
        const chatID = [req.user.email, partner].sort().join('__');
        const sent = await chatsCol().doc(chatID).collection('messages').doc(msgId).get();
        if (sent.exists) return res.status(409).json({ error: 'Already sent' });
        const snap = await blobsCol(chatID).where('msgId', '==', msgId).get();
        const mine = snap.docs.filter(d => d.data().uploader === req.user.email);
        for (let i = 0; i < mine.length; i += 400) {
            const batch = db.batch();
            mine.slice(i, i + 400).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }
        // [SEC] 目印（marker）も、自分のものだけ消す（相手が消すと残骸の掃除が効かなくなる）
        const markerRef = uploadMarkersCol().doc(_markerId(chatID, msgId));
        const marker = await markerRef.get();
        if (marker.exists && marker.data().uploader === req.user.email) await markerRef.delete().catch(() => { });
        res.json({ ok: true, deleted: mine.length });
    } catch (e) { console.error('blob discard error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 保険の掃除: 最後のアップロードから60分以上たっても送信されていないものを、定期的に削除する。
// （ネットワーク断・ブラウザ強制終了・キャンセル直後に遅れて届いたチャンクなど、クライアントから消せなかった分）
async function sweepOrphanBlobs() {
    try {
        const cutoff = Date.now() - BLOB_ORPHAN_TTL_MS;
        for (let round = 0; round < 5; round++) {
            const snap = await uploadMarkersCol().where('updatedAt', '<', cutoff).limit(100).get();
            if (snap.empty) break;
            for (const m of snap.docs) {
                try {
                    const { chatID, msgId } = m.data();
                    if (!chatID || !msgId) { await m.ref.delete(); continue; } // 壊れた目印は捨てる
                    const sent = await chatsCol().doc(chatID).collection('messages').doc(msgId).get();
                    if (!sent.exists) await deleteBlobs(chatID, msgId);
                    await m.ref.delete();
                } catch (e) { console.error('sweep item error:', e); }
            }
            if (snap.size < 100) break;
        }
    } catch (e) { console.error('sweepOrphanBlobs error:', e); }
}
setInterval(sweepOrphanBlobs, 15 * 60 * 1000).unref();
setTimeout(sweepOrphanBlobs, 60 * 1000).unref();

// チャンクのアップロード。
// [SEC] 以前は、会話の相手が「同じmsgId・同じindex」で上書きでき、送信済みの添付ファイルの中身まで差し替えられた。
//  - すでにメッセージとして送信済みなら受け付けない（409）
//  - 目印(marker)とチャンクの「アップロード者」を記録し、本人以外の上書きを拒否（403）
//  - 添付1つの合計サイズ・同時進行数・1日の総量に上限
app.post('/chat/blob/:msgId/:index', requireAuth, express.raw({ type: 'application/octet-stream', limit: BLOB_CHUNK_LIMIT }), async (req, res) => {
    const me = req.user.email;
    let taken = 0;
    try {
        if (!_rateLimit(req, 'blob', 600)) return res.status(429).json({ error: 'Too many requests' });
        const p = _blobParams(req);
        if (!p) return res.status(400).json({ error: 'Bad request' });
        if (!Buffer.isBuffer(req.body) || req.body.length === 0) return res.status(400).json({ error: 'Empty body' });
        if (!(await _isFriendOf(me, p.partner))) return res.status(403).json({ error: '権限がありません' });

        const markerRef = uploadMarkersCol().doc(_markerId(p.chatID, p.msgId));
        const chunkRef = blobsCol(p.chatID).doc(`${p.msgId}_${p.index}`);
        const msgRef = chatsCol().doc(p.chatID).collection('messages').doc(p.msgId);

        // 新しいアップロードの開始時だけ、未送信のアップロード数の上限を確認する
        const pre = await markerRef.get();
        if (!pre.exists) {
            const pending = await uploadMarkersCol().where('uploader', '==', me).limit(BLOB_MAX_PENDING_PER_USER).select().get();
            if (pending.size >= BLOB_MAX_PENDING_PER_USER) {
                return res.status(429).json({ error: '未送信のアップロードが多すぎます。送信またはキャンセルしてから再試行してください' });
            }
        }
        if (!_blobQuotaTake(me, req.body.length)) {
            return res.status(429).json({ error: '本日のアップロード上限に達しました' });
        }
        taken = req.body.length;

        const now = Date.now();
        const result = await db.runTransaction(async (tx) => {
            const [marker, chunk, sent] = await Promise.all([tx.get(markerRef), tx.get(chunkRef), tx.get(msgRef)]);
            if (sent.exists) return 'sent';
            if (marker.exists && marker.data().uploader !== me) return 'forbidden';
            if (chunk.exists && chunk.data().uploader !== me) return 'forbidden';
            const sizes = { ...((marker.exists && marker.data().sizes) || {}), [p.index]: req.body.length };
            const total = Object.values(sizes).reduce((a, b) => a + Number(b || 0), 0);
            if (total > BLOB_MAX_TOTAL_BYTES) return 'too_big';
            tx.set(chunkRef, { msgId: p.msgId, index: p.index, data: req.body, uploader: me, createdAt: now });
            // 「送信が完了していないアップロード」の目印。メッセージの保存に成功すると消える。
            // 途中でやめた・アプリを閉じた場合に残るので、掃除処理がこれを頼りに残骸を消す。
            tx.set(markerRef, {
                chatID: p.chatID, msgId: p.msgId, uploader: me, updatedAt: now, sizes,
                createdAt: (marker.exists && marker.data().createdAt) || now,
            });
            return 'ok';
        });
        if (result !== 'ok') {
            _blobQuotaRefund(me, taken); taken = 0;
            if (result === 'sent') return res.status(409).json({ error: 'Already sent' });
            if (result === 'too_big') return res.status(413).json({ error: 'ファイルが大きすぎます' });
            return res.status(403).json({ error: '権限がありません' });
        }
        taken = 0;
        res.json({ ok: true });
    } catch (e) {
        if (taken) _blobQuotaRefund(me, taken);
        console.error('blob upload error:', e); res.status(500).json({ error: 'Server error' });
    }
});

// チャンクのダウンロード。
// chatID は「認証済み本人 + 相手」から作るので、当事者以外は他人の会話のデータに到達できない。
// [SEC] さらに、相手が「まだ送信していない（途中の）アップロード」は読めないようにする（本人のものか、送信済みのものだけ）。
app.get('/chat/blob/:msgId/:index', requireAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'blob', 600)) return res.status(429).json({ error: 'Too many requests' });
        const p = _blobParams(req);
        if (!p) return res.status(400).json({ error: 'Bad request' });
        const snap = await blobsCol(p.chatID).doc(`${p.msgId}_${p.index}`).get();
        if (!snap.exists) return res.status(404).json({ error: 'Not found' });
        const meta = snap.data();
        if (meta.uploader !== req.user.email && !(await _blobMessageSent(p.chatID, p.msgId))) {
            return res.status(404).json({ error: 'Not found' });
        }
        res.set('Content-Type', 'application/octet-stream');
        res.set('X-Content-Type-Options', 'nosniff');
        res.set('Cache-Control', 'private, max-age=86400, immutable');
        res.send(Buffer.from(meta.data));
    } catch (e) { console.error('blob download error:', e); res.status(500).json({ error: 'Server error' }); }
});

async function deleteBlobs(chatID, msgId) {
    let q = blobsCol(chatID);
    const snap = msgId ? await q.where('msgId', '==', msgId).get() : await q.get();
    for (let i = 0; i < snap.docs.length; i += 400) {
        const batch = db.batch();
        snap.docs.slice(i, i + 400).forEach(d => batch.delete(d.ref));
        await batch.commit();
    }
}

// [SEC] 最後尾の共通エラーハンドラ: 想定外のエラーでスタックトレース等を返さない（サイズ超過/JSON不正は適切なステータスで返す）
app.use((err, req, res, next) => {
    if (res.headersSent) return next(err);
    if (err && err.type === 'entity.too.large') return res.status(413).json({ error: 'データが大きすぎます' });
    if (err && (err.type === 'entity.parse.failed' || err instanceof SyntaxError)) return res.status(400).json({ error: 'リクエストの形式が正しくありません' });
    console.error('Unhandled error:', err);
    res.status(500).json({ error: 'Server error' });
});

// 想定外の非同期エラーでプロセスごと落ちないようにログだけ残す
process.on('unhandledRejection', (reason) => console.error('[unhandledRejection]', reason));

server.listen(PORT, () => console.log(`Rivift Connect Server v6.0 (Firebase) is running on port ${PORT}`));
