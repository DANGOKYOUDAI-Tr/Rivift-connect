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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Firebase Admin SDK
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const admin = require('firebase-admin');

admin.initializeApp({
    credential: admin.credential.cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    })
});

const db = admin.firestore();
const usersCol = () => db.collection('users');
const chatsCol = () => db.collection('chats');
const appsCol = () => db.collection('store_apps');
const commentsCol = () => db.collection('store_comments');
const mailCol = () => db.collection('mail_messages');
const authCodesCol = () => db.collection('auth_codes');

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Firestore ヘルパー
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async function getUser(email) {
    const snap = await usersCol().doc(email).get();
    return snap.exists ? snap.data() : null;
}

async function getChat(chatID) {
    const snap = await chatsCol().doc(chatID).get();
    return snap.exists ? snap.data() : null;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// セキュリティ定数
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) console.warn('[WARN] JWT_SECRET が設定されていません。認証エンドポイントは機能しません。');

const ALLOWED_ORIGINS = [
    'https://rivift.app',
    'https://rivift-os-html.web.app',
    'https://rivift-os-html.firebaseapp.com',
    'http://localhost:5500',
    'http://localhost:3000',
    'http://127.0.0.1:5500',
    'null', // file:// で開いた場合
];

// SSRF対策ブロックリスト
const BLOCKED_HOSTNAMES = ['localhost', '127.0.0.1', '0.0.0.0', '::1'];
const BLOCKED_PREFIXES = ['169.254.', '10.', '192.168.', '172.16.', '172.17.', '172.18.',
    '172.19.', '172.20.', '172.21.', '172.22.', '172.23.', '172.24.', '172.25.',
    '172.26.', '172.27.', '172.28.', '172.29.', '172.30.', '172.31.'];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// バリデーションヘルパー
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function isValidEmail(email) {
    return typeof email === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) && email.length <= 254;
}

function sanitizeString(str, maxLen) {
    return typeof str === 'string' ? str.trim().slice(0, maxLen) : '';
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Express / Socket.io セットアップ
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const app = express();

// Helmet（HTTPヘッダー強化）
app.use(helmet());

// CORS 厳格化
app.use(cors({
    origin: (origin, cb) => {
        // originなし（同一オリジン・curl等）は許可
        if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
        cb(new Error('Not allowed by CORS'));
    },
    credentials: true,
}));

// [FIX] CORSで拒否された場合、corsミドルウェアが投げるErrorが未処理のまま
//       生の500エラーになっていた（ブラウザ側ではCORSエラーとしか見えず原因が分かりにくい）。
//       ここで明示的に捕捉し、分かりやすい403を返す。
app.use((err, req, res, next) => {
    if (err && err.message === 'Not allowed by CORS') {
        return res.status(403).json({ error: '許可されていないオリジンからのアクセスです' });
    }
    next(err);
});

// グローバルリクエストサイズ制限（1MB）
app.use(express.json({ limit: '1mb' }));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: ALLOWED_ORIGINS, methods: ["GET", "POST"] },
    maxHttpBufferSize: 5e6, // 5MB（旧50MBから縮小）
});
const PORT = process.env.PORT || 3001;

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;

let onlineUsers = {};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App Store: インメモリキャッシュ（Firestore読み取り節約）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const _listCache = new Map();
const LIST_CACHE_TTL = 30_000; // 30秒
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App Store: コメントキャッシュ
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const _commentCache = new Map();
const COMMENT_CACHE_TTL = 30_000; // 30秒
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
    // 対象アプリのコメントキャッシュをすべて削除
    for (const key of _commentCache.keys()) {
        if (key.startsWith(`${appId}:`)) _commentCache.delete(key);
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App Store: レートリミット（IPベース・メモリ内）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const _rateLimitMap = new Map();
const RATE_LIMIT_MAX = 10;
const RATE_LIMIT_WINDOW = 60_000; // 1分

function _rateLimit(req, endpoint, maxRequests) {
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || 'unknown';
    const key = `${ip}:${endpoint}`;
    const now = Date.now();
    const limit = maxRequests || RATE_LIMIT_MAX;
    const timestamps = (_rateLimitMap.get(key) || []).filter(t => now - t < RATE_LIMIT_WINDOW);
    if (timestamps.length >= limit) return false;
    timestamps.push(now);
    _rateLimitMap.set(key, timestamps);
    // メモリ肥大化防止
    if (_rateLimitMap.size > 1000) {
        for (const [k, ts] of _rateLimitMap.entries()) {
            if (ts.every(t => now - t > RATE_LIMIT_WINDOW)) _rateLimitMap.delete(k);
        }
    }
    return true;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// グローバルレートリミットミドルウェア
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function globalRateLimit(req, res, next) {
    if (!_rateLimit(req, 'global', 120)) {
        return res.status(429).json({ error: 'リクエストが多すぎます。しばらく待ってから再試行してください。' });
    }
    next();
}
app.use(globalRateLimit);

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// JWT 認証ミドルウェア
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function requireAuth(req, res, next) {
    const token = req.headers['authorization']?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: '認証が必要です' });
    if (!JWT_SECRET) return res.status(500).json({ error: 'サーバー設定エラー' });
    try {
        req.user = jwt.verify(token, JWT_SECRET); // { email, displayName }
        next();
    } catch {
        res.status(401).json({ error: 'トークンが無効または期限切れです' });
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// REST API
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v6.0 (Firebase) is Active!</h1>'));

// スリープ対策 ping
app.get('/ping', (req, res) => res.json({ ok: true }));

// ユーザー作成
app.post('/createUser', async (req, res) => {
    if (!_rateLimit(req, 'create-user', 3)) return res.status(429).json({ error: 'Too many requests' });
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, passwordHash } = req.body;
        if (!isValidEmail(email)) return res.status(400).json({ error: 'メールアドレスが無効です' });
        const safeDisplayName = sanitizeString(displayName, 50);
        if (!safeDisplayName) return res.status(400).json({ error: '表示名は必須です' });
        const existing = await getUser(email);
        if (existing) return res.status(400).json({ error: 'User already exists' });
        // passwordHash があれば保存（Rivift ConnectアカウントのJWT認証で使う）
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
app.post('/getUserData', async (req, res) => {
    try {
        const { email } = req.body;
        const userData = await getUser(email);
        res.json({ userData });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// サイドバーデータ取得
// [PERF] 未読数の算出はここでのみ行う（初回ロード用）。
//        以降の未読数更新はSocket.ioの 'unread_update' イベントで差分通知されるため、
//        メッセージ送受信のたびにこのエンドポイントを叩く必要はない。
app.post('/getSidebarData', async (req, res) => {
    try {
        const { email } = req.body;
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
        for (const friendEmail of friends) {
            const chatID = [email, friendEmail].sort().join('__');
            const chat = await getChat(chatID);
            if (chat && chat.messages) {
                unreadCounts[friendEmail] = chat.messages.filter(m => m.to === email && !m.read).length;
            } else {
                unreadCounts[friendEmail] = 0;
            }
        }

        res.json({ friends, requests, sentRequests, usersData, unreadCounts });
    } catch (e) { console.error('getSidebarData error:', e); res.status(500).json({ error: 'Server error' }); }
});

// 複数ユーザーデータ取得（公開鍵含む）
app.post('/getUsersData', async (req, res) => {
    try {
        const { emails = [] } = req.body;
        const usersData = {};
        if (emails.length === 0) return res.json({ usersData });
        const chunks = [];
        for (let i = 0; i < emails.length; i += 30) chunks.push(emails.slice(i, i + 30));
        for (const chunk of chunks) {
            if (chunk.length === 0) continue;
            const snaps = await usersCol().where(admin.firestore.FieldPath.documentId(), 'in', chunk).get();
            snaps.forEach(doc => {
                const d = doc.data();
                usersData[doc.id] = { displayName: d.displayName, icon: d.icon, publicKeyJwk: d.publicKeyJwk };
            });
        }
        res.json({ usersData });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// メッセージ履歴取得
app.post('/getMessageHistory', async (req, res) => {
    try {
        const { user1, user2, limit = 50, lastTimestamp = null } = req.body;
        const chatID = [user1, user2].sort().join('__');
        let q = chatsCol().doc(chatID).collection('messages')
            .orderBy('timestamp', 'desc')
            .limit(limit);
        if (lastTimestamp) {
            q = q.startAfter(lastTimestamp);
        }
        const snap = await q.get();
        const history = snap.docs.map(d => d.data()).reverse();
        res.json({ history });
    } catch (e) { console.error('getMessageHistory error:', e); res.status(500).json({ error: 'Server error' }); }
});

// メッセージ保存
app.post('/saveMessage', async (req, res) => {
    try {
        const { user1, user2, message } = req.body;
        const chatID = [user1, user2].sort().join('__');
        const chatRef = chatsCol().doc(chatID);
        await chatRef.set({
            users: [user1, user2],
            messages: admin.firestore.FieldValue.arrayUnion({ chatID, ...message })
        }, { merge: true });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// DuckDuckGo検索
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

// プロキシ統計
let totalDataProxied = 0;
app.get('/getProxyStats', (req, res) => {
    res.json({ totalDataProxied, totalDataProxiedMB: (totalDataProxied / (1024 * 1024)).toFixed(2) });
});

// HTMLパス書き換え
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

// iframe-helper.js
app.get('/iframe-helper.js', (req, res) => {
    res.sendFile(path.join(__dirname, 'iframe-helper.js'));
});

// Webプロキシ
app.post('/proxy', async (req, res) => {
    try {
        const { url, method, headers, body } = req.body;
        if (!url) return res.status(400).send('URL is required.');

        // URLスキームチェック
        if (!/^https?:\/\//i.test(url)) return res.status(400).send('Invalid URL scheme.');

        // SSRF対策：内部IPブロック
        let hostname;
        try { hostname = new URL(url).hostname; } catch { return res.status(400).send('Invalid URL.'); }
        if (BLOCKED_HOSTNAMES.includes(hostname) || BLOCKED_PREFIXES.some(p => hostname.startsWith(p))) {
            return res.status(403).send('Blocked.');
        }

        // レートリミット（30回/分）
        if (!_rateLimit(req, 'proxy', 30)) return res.status(429).send('Rate limit exceeded.');

        const fetchOptions = {
            method, headers: { ...headers }, body: body ? Buffer.from(body, 'base64') : null, redirect: 'manual'
        };
        delete fetchOptions.headers['host'];
        delete fetchOptions.headers['origin'];
        delete fetchOptions.headers['referer'];
        delete fetchOptions.headers['content-length'];
        fetchOptions.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36';

        // リダイレクトを手動で追跡し、各ホップで内部IPチェックを行う（DNS rebinding/リダイレクトSSRF対策）
        let currentUrl = url;
        let response;
        for (let hop = 0; hop < 5; hop++) {
            response = await fetch(currentUrl, fetchOptions);
            if (response.status >= 300 && response.status < 400 && response.headers.get('location')) {
                const nextUrl = new URL(response.headers.get('location'), currentUrl).href;
                if (!/^https?:\/\//i.test(nextUrl)) return res.status(400).send('Invalid redirect URL scheme.');
                const nextHostname = new URL(nextUrl).hostname;
                if (BLOCKED_HOSTNAMES.includes(nextHostname) || BLOCKED_PREFIXES.some(p => nextHostname.startsWith(p))) {
                    return res.status(403).send('Blocked (redirect target).');
                }
                currentUrl = nextUrl;
                continue;
            }
            break;
        }

        res.setHeader('x-proxy-headers', JSON.stringify(Object.fromEntries(response.headers)));
        response.body.pipe(res);
    } catch (e) { console.error('Proxy error:', e.message); res.status(500).send(e.message); }
});

// YouTube検索
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

// 記事抽出
const { JSDOM } = require('jsdom');
const { Readability } = require('@mozilla/readability');
app.get('/extract', async (req, res) => {
    const url = req.query.url;
    if (!url) return res.status(400).json({ error: 'URL parameter is required.' });
    try {
        const response = await axios.get(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } });
        const dom = new JSDOM(response.data, { url });
        const article = new Readability(dom.window.document).parse();
        if (article) {
            res.json({ title: article.title, content: article.content, textContent: article.textContent, author: article.byline, length: article.length });
        } else {
            res.status(404).json({ error: '記事のコンテンツを抽出できませんでした。' });
        }
    } catch (e) { res.status(500).json({ error: 'ページの読み込みまたは解析に失敗しました。' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Rivift App Store API
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ── アプリ一覧取得（キャッシュ付き・htmlContentは返さない）
app.get('/store/apps', async (req, res) => {
    try {
        const { category = 'all', sort = 'newest', limit = 30, offset = 0 } = req.query;
        const cacheKey = `${category}|${sort}|${limit}|${offset}`;
        const cached = _cacheGet(cacheKey);
        if (cached) return res.json({ apps: cached, _cached: true });

        let q = appsCol();
        if (category && category !== 'all') q = q.where('category', '==', category);
        const sortField = sort === 'popular' ? 'downloads' : sort === 'liked' ? 'likeCount' : 'createdAt';
        q = q.orderBy(sortField, 'desc').limit(Number(limit)).offset(Number(offset));
        const snap = await q.get();
        const apps = snap.docs.map(d => {
            const { htmlContent, ...rest } = d.data();
            return { id: d.id, ...rest };
        });
        _cacheSet(cacheKey, apps);
        res.json({ apps });
    } catch (e) { console.error('store/apps error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── アプリ詳細（htmlContent含む）
app.get('/store/apps/:id', async (req, res) => {
    try {
        const snap = await appsCol().doc(req.params.id).get();
        if (!snap.exists) return res.status(404).json({ error: 'App not found' });
        res.json({ app: { id: snap.id, ...snap.data() } });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── アプリ投稿
app.post('/store/apps', express.json({ limit: '10mb' }), requireAuth, async (req, res) => {
    try {
        const email = req.user.email; // JWTから取得（body.emailは信用しない）
        const { title, description, category, htmlContent, iconEmoji, iconImage, version } = req.body;
        if (!title || !htmlContent) return res.status(400).json({ error: 'title, htmlContent は必須です' });
        const user = await getUser(email);
        if (!user) return res.status(401).json({ error: 'Connectアカウントが見つかりません' });
        if (Buffer.byteLength(htmlContent, 'utf8') > 1_000_000) return res.status(400).json({ error: 'HTMLが大きすぎます（最大1MB）' });
        if (iconImage && Buffer.byteLength(iconImage, 'utf8') > 400_000) return res.status(400).json({ error: 'アイコン画像が大きすぎます（最大300KB）' });
        const now = Date.now();
        const docRef = appsCol().doc();
        await docRef.set({
            title: title.slice(0, 60),
            description: (description || '').slice(0, 500),
            category: ['tool', 'game', 'entertainment', 'other'].includes(category) ? category : 'other',
            authorEmail: email,
            authorName: user.displayName || email,
            htmlContent,
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

// ── アプリ更新
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
        if (htmlContent) {
            if (Buffer.byteLength(htmlContent, 'utf8') > 1_000_000) return res.status(400).json({ error: 'HTMLが大きすぎます' });
            update.htmlContent = htmlContent;
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

// ── アプリ削除
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

// ── ダウンロード数インクリメント
app.post('/store/apps/:id/download', async (req, res) => {
    try {
        await appsCol().doc(req.params.id).update({
            downloads: admin.firestore.FieldValue.increment(1)
        });
        _cacheInvalidate();
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── 自分の投稿アプリ一覧
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

// ── ハート（いいね）トグル
// サブコレクション likes/{email} で管理。1ユーザー1いいねを確実に保証。
app.post('/store/apps/:id/like', requireAuth, async (req, res) => {
    try {
        if (!_rateLimit(req, 'like')) return res.status(429).json({ error: 'Too many requests' });
        const email = req.user.email;

        const appRef = appsCol().doc(req.params.id);
        const likeRef = appRef.collection('likes').doc(email);
        const likeSnap = await likeRef.get();

        let liked;
        if (likeSnap.exists) {
            // いいね取り消し
            await Promise.all([
                likeRef.delete(),
                appRef.update({ likeCount: admin.firestore.FieldValue.increment(-1) }),
            ]);
            liked = false;
        } else {
            // いいね追加
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

// ── 自分がいいねしているか確認
app.get('/store/apps/:id/like', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(400).json({ error: 'email is required' });
        const likeSnap = await appsCol().doc(req.params.id).collection('likes').doc(email).get();
        res.json({ liked: likeSnap.exists });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── 星評価（1〜5）
// サブコレクション ratings/{email} で1人1評価。変更可。
// 集計は ratingSum/ratingCount に保持し毎回全件集計しない。
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
            // 既存評価を変更: 差分だけ加算
            const oldScore = ratingSnap.data().score;
            const diff = s - oldScore;
            await Promise.all([
                ratingRef.set({ score: s, updatedAt: Date.now() }),
                diff !== 0 ? appRef.update({ ratingSum: admin.firestore.FieldValue.increment(diff) }) : Promise.resolve(),
            ]);
        } else {
            // 新規評価
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

// ── 自分の星評価を取得
app.get('/store/apps/:id/rating', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(400).json({ myScore: null });
        const snap = await appsCol().doc(req.params.id).collection('ratings').doc(email).get();
        res.json({ myScore: snap.exists ? snap.data().score : null });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── 通報
// store_reports コレクションに保存。同一ユーザー×同一アプリは24h1回まで。
const _reportCooldown = new Map(); // `${email}:${appId}` → timestamp

app.post('/store/apps/:id/report', async (req, res) => {
    try {
        if (!_rateLimit(req, 'report')) return res.status(429).json({ error: 'Too many requests' });
        const { email, reason, detail } = req.body;
        if (!email) return res.status(400).json({ error: 'email is required' });
        if (!reason) return res.status(400).json({ error: 'reason is required' });

        const VALID_REASONS = ['spam', 'malware', 'inappropriate', 'phishing', 'other'];
        if (!VALID_REASONS.includes(reason)) return res.status(400).json({ error: 'Invalid reason' });

        // 24hクールダウン
        const coolKey = `${email}:${req.params.id}`;
        const lastReport = _reportCooldown.get(coolKey);
        if (lastReport && Date.now() - lastReport < 24 * 60 * 60 * 1000) {
            return res.status(429).json({ error: 'すでにこのアプリを通報済みです。24時間後に再通報できます。' });
        }

        const appSnap = await appsCol().doc(req.params.id).get();
        if (!appSnap.exists) return res.status(404).json({ error: 'App not found' });

        await db.collection('store_reports').add({
            appId: req.params.id,
            appTitle: appSnap.data().title,
            reporterEmail: email,
            reason,
            detail: (detail || '').slice(0, 500),
            reportedAt: Date.now(),
            status: 'pending', // pending | reviewed | dismissed
        });

        _reportCooldown.set(coolKey, Date.now());
        console.warn(`[REPORT] app="${appSnap.data().title}" (${req.params.id}) by=${email} reason=${reason}`);

        res.json({ success: true });
    } catch (e) { console.error('report error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// App Store: コメント機能
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ── コメント一覧取得（新着順・カーソルページネーション・30秒キャッシュ）
app.get('/store/apps/:id/comments', async (req, res) => {
    try {
        const appId = req.params.id;
        const limit = Math.min(Number(req.query.limit) || 20, 50);
        const before = req.query.before ? Number(req.query.before) : null;

        const cacheKey = `${appId}:${limit}:${before || 'latest'}`;
        const cached = _commentCacheGet(cacheKey);
        if (cached) return res.json({ comments: cached, _cached: true });

        // 複合インデックス不要：appId のみで絞り込み、ソートはメモリで行う
        // （コメント数が数百件規模なら十分実用的）
        const snap = await commentsCol().where('appId', '==', appId).get();

        let comments = snap.docs.map(d => {
            const { email, ...rest } = d.data();
            return { id: d.id, ...rest };
        });

        // メモリでソート・カーソル適用
        comments.sort((a, b) => b.createdAt - a.createdAt);
        if (before) comments = comments.filter(c => c.createdAt < before);
        comments = comments.slice(0, limit);

        _commentCacheSet(cacheKey, comments);
        res.json({ comments });
    } catch (e) { console.error('comments GET error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── コメント投稿
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

        // レスポンスを先に返してからキャッシュ無効化
        // （Firestoreの書き込み反映を待ってから次のGETが正しい値を取れるよう少し遅延）
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

// ── コメント削除（投稿者本人のみ）
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
io.on('connection', (socket) => {
    let currentUserEmail = null;

    socket.on('get-ice-servers', async (payload, callback) => {
        if (!accountSid || !authToken) return callback({ error: 'Twilioの認証情報が設定されていません。' });
        try {
            const client = twilio(accountSid, authToken);
            const token = await client.tokens.create({ ttl: 3600 });
            callback({ iceServers: token.iceServers });
        } catch (e) { callback({ error: `Twilioとの通信に失敗しました: ${e.message}` }); }
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

    socket.on('login', ({ email }) => {
        onlineUsers[email] = socket.id;
        currentUserEmail = email;
        socket.email = email;
        notifyFriendsOfStatusChange(email, true);
    });

    socket.on('get_initial_statuses', async (payload, callback) => {
        const user = await getUser(payload.email);
        if (user && user.friends) {
            const statuses = {};
            user.friends.forEach(friendEmail => { statuses[friendEmail] = !!onlineUsers[friendEmail]; });
            callback(statuses);
        }
    });

    socket.on('disconnect', () => {
        if (currentUserEmail) { delete onlineUsers[currentUserEmail]; notifyFriendsOfStatusChange(currentUserEmail, false); }
        if (socket.email) delete onlineUsers[socket.email];
    });

    const notifyUsers = (userEmails) => {
        userEmails.forEach(email => {
            const socketId = onlineUsers[email];
            if (socketId) io.to(socketId).emit('db_updated_notification');
        });
    };

    socket.on('send_friend_request', async ({ from, to }) => {
        await usersCol().doc(to).update({ requests: admin.firestore.FieldValue.arrayUnion(from) });
        await usersCol().doc(from).update({ sentRequests: admin.firestore.FieldValue.arrayUnion(to) });
        notifyUsers([from, to]);
    });

    socket.on('accept_friend_request', async ({ from, to }) => {
        await usersCol().doc(from).update({
            requests: admin.firestore.FieldValue.arrayRemove(to),
            friends: admin.firestore.FieldValue.arrayUnion(to)
        });
        await usersCol().doc(to).update({
            sentRequests: admin.firestore.FieldValue.arrayRemove(from),
            friends: admin.firestore.FieldValue.arrayUnion(from)
        });
        notifyUsers([from, to]);
    });

    socket.on('delete_friend', async ({ from, to }) => {
        try {
            await usersCol().doc(from).update({ friends: admin.firestore.FieldValue.arrayRemove(to) });
            await usersCol().doc(to).update({ friends: admin.firestore.FieldValue.arrayRemove(from) });
            const chatID = [from, to].sort().join('__');
            await chatsCol().doc(chatID).delete();
            notifyUsers([from, to]);
        } catch (e) { console.error('delete_friend error:', e); }
    });

    socket.on('private_message', async (payload) => {
        try {
            const { from, to } = payload;
            const chatID = [from, to].sort().join('__');
            const msgId = payload.id || `${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
            await chatsCol().doc(chatID).set({ users: [from, to], lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
            await chatsCol().doc(chatID).collection('messages').doc(msgId).set({ ...payload, id: msgId });
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) io.to(recipientSocketId).emit('private_message', { ...payload, id: msgId });
            const senderSocketId = onlineUsers[from];
            if (senderSocketId) io.to(senderSocketId).emit('db_updated_notification');

            // [PERF] 未読数はSocket.io差分通知（getSidebarDataの再取得を避ける）
            if (recipientSocketId) {
                const unreadSnap = await chatsCol().doc(chatID).collection('messages')
                    .where('to', '==', to).where('read', '==', false).get();
                io.to(recipientSocketId).emit('unread_update', { partnerEmail: from, count: unreadSnap.size });
            }
        } catch (e) { console.error('private_message error:', e); }
    });

    socket.on('read_receipt', async (payload) => {
        try {
            const readerEmail = payload.from;
            const writerEmail = payload.to;
            const chatID = [readerEmail, writerEmail].sort().join('__');
            const unreadSnap = await chatsCol().doc(chatID).collection('messages')
                .where('to', '==', readerEmail).where('read', '==', false).get();
            const batch = db.batch();
            unreadSnap.docs.forEach(d => batch.update(d.ref, { read: true }));
            await batch.commit();
            const readerSocketId = onlineUsers[readerEmail];
            const writerSocketId = onlineUsers[writerEmail];
            if (readerSocketId) io.to(readerSocketId).emit('messages_marked_as_read', { chatPartner: writerEmail });
            if (writerSocketId) io.to(writerSocketId).emit('messages_marked_as_read', { chatPartner: readerEmail });

            // [PERF] 未読数差分通知：このチャットの未読は0になった
            if (readerSocketId) io.to(readerSocketId).emit('unread_update', { partnerEmail: writerEmail, count: 0 });
        } catch (e) { console.error('read_receipt error:', e); }
    });

    socket.on('delete_message', async ({ from, to, messageId }) => {
        try {
            const chatID = [from, to].sort().join('__');
            const msgRef = chatsCol().doc(chatID).collection('messages').doc(messageId);
            const msgSnap = await msgRef.get();
            if (msgSnap.exists && msgSnap.data().from === from) {
                await msgRef.update({ type: 'deleted', content: 'メッセージが削除されました' });
            }
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) io.to(recipientSocketId).emit('message_deleted', { messageId });
            notifyUsers([from, to]);
        } catch (e) { console.error('delete_message error:', e); }
    });

    socket.on('delete_chat', async ({ user1, user2 }) => {
        try {
            const chatID = [user1, user2].sort().join('__');
            const msgsSnap = await chatsCol().doc(chatID).collection('messages').get();
            const batch = db.batch();
            msgsSnap.docs.forEach(d => batch.delete(d.ref));
            batch.delete(chatsCol().doc(chatID));
            await batch.commit();
            notifyUsers([user1, user2]);
        } catch (e) { console.error('delete_chat error:', e); }
    });

    socket.on('edit_message', async ({ from, to, messageId, newText }) => {
        try {
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

    socket.on('add_reaction', async ({ from, to, messageId, emoji }) => {
        try {
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

    socket.on('update_profile', async ({ email, newDisplayName, newIcon }) => {
        try {
            const updateData = {};
            if (newDisplayName) updateData.displayName = newDisplayName;
            if (newIcon) updateData.icon = newIcon;
            await usersCol().doc(email).update(updateData);
            const user = await getUser(email);
            if (user) notifyUsers([email, ...(user.friends || [])]);
        } catch (e) { console.error('update_profile error:', e); }
    });

    // WebRTC
    socket.on('webrtc-offer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('webrtc-offer', { from: socket.email, offer: payload.offer });
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

    // LiveCanvas
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
        if (recipientSocketId) io.to(recipientSocketId).emit('typing_start', { from: payload.from });
    });

    socket.on('typing_stop', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('typing_stop', { from: payload.from });
    });

    // 近距離共有 シグナリング
    socket.on('proximity_offer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_share_invite', {
            from: socket.email,
            fileName: payload.fileName,
            fileSize: payload.fileSize,
            fileType: payload.fileType,
            destType: payload.destType,
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
// Phase 1: 認証エンドポイント（/auth/*）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ── サインイン → JWT発行
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

// ── JWT検証（トークン更新・自動サインイン確認）
app.post('/auth/verify', requireAuth, async (req, res) => {
    // requireAuthが成功した時点でトークンは有効。iconはJWTに含めていないため軽く1回だけ補完する。
    try {
        const user = await getUser(req.user.email);
        res.json({ valid: true, user: { email: req.user.email, displayName: req.user.displayName, icon: user?.icon || '' } });
    } catch (e) {
        res.json({ valid: true, user: { email: req.user.email, displayName: req.user.displayName, icon: '' } });
    }
});

// ── パスワード移行（Phase1以前に作成された古いアカウント用）
// /createUser 経由で作られたアカウントは passwordHash が存在しないため /auth/login が使えない。
// クライアント側で encryptedPrivateKeyPayload の復号に成功した（= 本人確認済み）後にこのエンドポイントを呼ぶ。
// [SEC] passwordHash が既に存在するアカウントには絶対に上書きしない（乗っ取り防止）。
app.post('/auth/migrate-password', async (req, res) => {
    if (!_rateLimit(req, 'migrate-password', 3)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const { email, password } = req.body;
        if (!isValidEmail(email) || !password || typeof password !== 'string') {
            return res.status(400).json({ error: '入力が無効です' });
        }
        const user = await getUser(email);
        if (!user) return res.status(404).json({ error: 'ユーザーが見つかりません' });
        // [重要] すでに passwordHash がある場合は絶対に上書きしない
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
// Phase 2: アカウント管理（確認コード・パスワード変更・削除）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const { Resend } = require('resend');
// [FIX] APIキー未設定時に new Resend() がコンストラクタで即時例外を投げ、
//       サーバープロセス全体が起動できなくなる問題を防ぐため遅延初期化にする。
//       実際にメール送信が必要になった時点で初めてインスタンス化し、
//       キーが無ければ明確なエラーメッセージを返す（サーバー全体は落とさない）。
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

    const code = Math.floor(100000 + Math.random() * 900000).toString();
    const expireAt = Date.now() + 10 * 60 * 1000; // 10分
    await authCodesCol().doc(email).set({ purpose, code, expireAt });

    const purposeLabels = {
        'change-password': 'パスワード変更',
        'reset-password': 'パスワードリセット',
        'delete-account': 'アカウント削除',
    };
    const label = purposeLabels[purpose] || '操作';

    await resend.emails.send({
        from: 'Rivift <noreply@rivift.app>',
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
    const snap = await authCodesCol().doc(email).get();
    if (!snap.exists) return false;
    const data = snap.data();
    if (data.purpose !== purpose) return false;
    if (data.code !== code) return false;
    if (Date.now() > data.expireAt) return false;
    await authCodesCol().doc(email).delete(); // 使用済み削除
    return true;
}

// Web Crypto 互換のパスワードハッシュ生成（Node.js側）
async function hashPassword(password) {
    const salt = Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString('hex');
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

// [SEC] パスワード検証共通関数（/auth/login, /auth/change-password, /auth/delete-account で共用）
// passwordHash は { salt: hex, hash: hex } 形式（PBKDF2-SHA256, 100000 iterations）
async function verifyPassword(plainPassword, passwordHash) {
    if (!passwordHash || !plainPassword) return false;
    const { salt, hash: storedHash } = passwordHash;
    const encoder = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey(
        'raw', encoder.encode(plainPassword), { name: 'PBKDF2' }, false, ['deriveBits']
    );
    const derivedBits = await crypto.subtle.deriveBits(
        { name: 'PBKDF2', salt: Buffer.from(salt, 'hex'), iterations: 100000, hash: 'SHA-256' },
        keyMaterial, 256
    );
    const computedHash = Buffer.from(derivedBits).toString('hex');
    return computedHash === storedHash;
}

// ── 確認コード送信
// [SEC] パスワード変更・アカウント削除は「現在のパスワード再入力」方式に変更したため、
//       このエンドポイントは reset-password（パスワードを忘れた場合の再設定）専用とする。
//       reset-password はメール送信（RESEND_API_KEY）が未設定の間は機能しない（保留中の機能）。
app.post('/auth/send-code', async (req, res) => {
    if (!_rateLimit(req, 'auth-send-code', 3)) return res.status(429).json({ error: 'しばらく待ってから再試行してください' });
    const { email, purpose } = req.body;
    if (!isValidEmail(email) || purpose !== 'reset-password') return res.status(400).json({ error: '入力が無効です' });

    // ユーザー存在確認攻撃対策で常に同じレスポンスを返す
    const user = await getUser(email);
    if (user) await sendAuthCode(email, purpose).catch(e => console.error('sendAuthCode error:', e));
    res.json({ success: true });
});

// ── パスワード変更（ログイン済み・現在のパスワード再入力で本人確認）
app.post('/auth/change-password', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'change-password', 5)) return res.status(429).json({ error: 'リクエストが多すぎます。しばらく待ってから再試行してください' });
    try {
        const email = req.user.email;
        const { currentPassword, newPassword } = req.body;
        if (!currentPassword || !newPassword || newPassword.length < 8) {
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

// ── アイコン更新（設定アプリのアイコン変更をRiviftアカウントにも反映）
app.post('/auth/update-icon', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'update-icon', 20)) return res.status(429).json({ error: 'リクエストが多すぎます' });
    try {
        const email = req.user.email;
        const { icon } = req.body;
        if (typeof icon !== 'string' || icon.length > 400000) { // 約300KBのicon想定+base64オーバーヘッド余裕
            return res.status(400).json({ error: 'iconが無効です' });
        }
        await usersCol().doc(email).update({ icon });
        res.json({ success: true, icon });
    } catch (e) { console.error('update-icon error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── パスワードリセット（未ログイン）
app.post('/auth/reset-password', async (req, res) => {
    try {
        const { email, code, newPassword } = req.body;
        if (!isValidEmail(email) || !code || !newPassword || newPassword.length < 8) {
            return res.status(400).json({ error: '入力が無効です' });
        }

        const valid = await verifyAuthCode(email, code, 'reset-password');
        if (!valid) return res.status(400).json({ error: 'コードが無効または期限切れです' });

        const user = await getUser(email);
        if (!user) return res.status(400).json({ error: '入力が無効です' }); // 存在確認攻撃対策

        const passwordHash = await hashPassword(newPassword);
        await usersCol().doc(email).update({ passwordHash });
        res.json({ success: true });
    } catch (e) { console.error('reset-password error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── アカウント削除（現在のパスワード再入力で本人確認）
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

        const batchSize = 400; // Firestoreバッチ上限 500 に余裕を持たせる

        // 1. store_apps（自分の投稿）
        const myAppsSnap = await appsCol().where('authorEmail', '==', email).get();
        for (let i = 0; i < myAppsSnap.docs.length; i += batchSize) {
            const batch = db.batch();
            myAppsSnap.docs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }

        // 2. store_comments
        const myCommentsSnap = await commentsCol().where('email', '==', email).get();
        for (let i = 0; i < myCommentsSnap.docs.length; i += batchSize) {
            const batch = db.batch();
            myCommentsSnap.docs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }

        // 3. mail_messages（from or to）
        const mailFromSnap = await mailCol().where('from', '==', email).get();
        const mailToSnap = await mailCol().where('to', '==', email).get();
        const mailDocs = [...new Map([...mailFromSnap.docs, ...mailToSnap.docs].map(d => [d.id, d])).values()];
        for (let i = 0; i < mailDocs.length; i += batchSize) {
            const batch = db.batch();
            mailDocs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
            await batch.commit();
        }

        // 4. chats（サブコレクション含む）
        if (user && user.friends) {
            for (const friendEmail of user.friends) {
                const chatID = [email, friendEmail].sort().join('__');
                const msgsSnap = await chatsCol().doc(chatID).collection('messages').get();
                for (let i = 0; i < msgsSnap.docs.length; i += batchSize) {
                    const batch = db.batch();
                    msgsSnap.docs.slice(i, i + batchSize).forEach(d => batch.delete(d.ref));
                    await batch.commit();
                }
                await chatsCol().doc(chatID).delete();
                // 相手のフレンドリストからも削除
                await usersCol().doc(friendEmail).update({ friends: admin.firestore.FieldValue.arrayRemove(email) });
            }
        }

        // 5. auth_codes
        await authCodesCol().doc(email).delete().catch(() => {});

        // 6. users
        await usersCol().doc(email).delete();

        res.json({ success: true });
    } catch (e) { console.error('delete-account error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Phase 4: Rivift Mail エンドポイント（全て requireAuth）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ── 受信箱
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
        // deletedBy に含まれるものを除外
        const messages = snap.docs
            .map(d => ({ id: d.id, ...d.data() }))
            .filter(m => !(m.deletedBy || []).includes(email) && !(m.trashBy || []).includes(email));

        res.json({ messages });
    } catch (e) { console.error('mail/inbox error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── 送信済み
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

// ── ゴミ箱
app.get('/mail/trash', requireAuth, async (req, res) => {
    try {
        const email = req.user.email;
        const limit = Math.min(Number(req.query.limit) || 20, 50);

        // trashBy に email が含まれるものを取得（Firestore array-contains）
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

// ── メール送信
app.post('/mail/send', requireAuth, async (req, res) => {
    if (!_rateLimit(req, 'mail-send', 10)) return res.status(429).json({ error: 'メール送信が多すぎます' });
    try {
        const from = req.user.email; // JWTから取得（改ざん不可）
        const { to, subject, body } = req.body;

        if (!isValidEmail(to)) return res.status(400).json({ error: '宛先メールアドレスが無効です' });
        if (from === to) return res.status(400).json({ error: '自分自身には送れません' });
        const safeSubject = sanitizeString(subject, 100);
        const safeBody = sanitizeString(body, 5000);
        if (!safeSubject || !safeBody) return res.status(400).json({ error: '件名と本文は必須です' });

        // 受信者がRiviftアカウントを持っているか確認
        const recipient = await getUser(to);
        if (!recipient) return res.status(404).json({ error: 'Riviftアカウントが見つかりません' });

        const now = Date.now();
        const docRef = await mailCol().add({
            from, to, subject: safeSubject, body: safeBody,
            sentAt: now, readAt: null, deletedBy: [], trashBy: [],
        });

        // リアルタイム通知
        const recipientSocketId = onlineUsers[to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('new_mail', {
                messageId: docRef.id, from, subject: safeSubject, sentAt: now,
            });
        }

        res.json({ success: true, messageId: docRef.id });
    } catch (e) { console.error('mail/send error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── 既読
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

// ── ゴミ箱へ移動
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

// ── 削除（物理削除：両者が deletedBy に入ったら完全削除）
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
        // 両者が削除済みなら物理削除
        const bothDeleted = [data.from, data.to].every(e => newDeletedBy.includes(e));
        if (bothDeleted) {
            await ref.delete();
        } else {
            await ref.update({ deletedBy: newDeletedBy });
        }
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── 未読件数
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
// サーバー起動
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
server.listen(PORT, () => console.log(`Rivift Connect Server v6.0 (Firebase) is running on port ${PORT}`));
