require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const path = require('path');
const axios = require('axios');
const fetch = require('node-fetch');
const twilio = require('twilio');

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
// Express / Socket.io セットアップ
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: '50mb' }));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    maxHttpBufferSize: 50e6
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
// REST API
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v5.1 (Firebase) is Active!</h1>'));

// ユーザー作成
app.post('/createUser', async (req, res) => {
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
        const existing = await getUser(email);
        if (existing) return res.status(400).json({ error: 'User already exists' });
        await usersCol().doc(email).set({
            email, displayName, publicKeyJwk, encryptedPrivateKeyPayload,
            icon: icon || null, friends: [], requests: [], sentRequests: []
        });
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
        const fetchOptions = {
            method, headers: { ...headers }, body: body ? Buffer.from(body, 'base64') : null, redirect: 'follow'
        };
        delete fetchOptions.headers['host'];
        delete fetchOptions.headers['origin'];
        delete fetchOptions.headers['referer'];
        delete fetchOptions.headers['content-length'];
        fetchOptions.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36';
        const response = await fetch(url, fetchOptions);
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
app.post('/store/apps', async (req, res) => {
    try {
        const { email, title, description, category, htmlContent, iconEmoji, iconImage, version } = req.body;
        if (!email || !title || !htmlContent) return res.status(400).json({ error: 'email, title, htmlContent は必須です' });
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
app.put('/store/apps/:id', async (req, res) => {
    try {
        const { email, title, description, category, htmlContent, iconEmoji, iconImage, version } = req.body;
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
app.delete('/store/apps/:id', async (req, res) => {
    try {
        const { email } = req.body;
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
app.post('/store/apps/:id/like', async (req, res) => {
    try {
        if (!_rateLimit(req, 'like')) return res.status(429).json({ error: 'Too many requests' });
        const { email } = req.body;
        if (!email) return res.status(400).json({ error: 'email is required' });

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
app.post('/store/apps/:id/rating', async (req, res) => {
    try {
        if (!_rateLimit(req, 'rating')) return res.status(429).json({ error: 'Too many requests' });
        const { email, score } = req.body;
        if (!email) return res.status(400).json({ error: 'email is required' });
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
app.post('/store/apps/:id/comments', async (req, res) => {
    try {
        if (!_rateLimit(req, 'comments-post', 3)) return res.status(429).json({ error: 'コメントは1分間に3件までです。しばらく待ってから再試行してください。' });

        const { email, body } = req.body;
        if (!email) return res.status(400).json({ error: 'email is required' });
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
app.delete('/store/apps/:id/comments/:commentId', async (req, res) => {
    try {
        const { email } = req.body;
        if (!email) return res.status(400).json({ error: 'email is required' });

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
// サーバー起動
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
server.listen(PORT, () => console.log(`Rivift Connect Server v5.1 (Firebase) is running on port ${PORT}`));
