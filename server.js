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
// Firebase Admin SDK（MongoDBの代わり）
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Firestore ヘルパー（MongoDBと同じ感覚で使えるように）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ユーザー取得（MongoDB: usersCollection.findOne({ _id: email })）
async function getUser(email) {
    const snap = await usersCol().doc(email).get();
    return snap.exists ? snap.data() : null;
}

// チャット取得（MongoDB: chatsCollection.findOne({ _id: chatID })）
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
// REST API
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v5.0 (Firebase) is Active!</h1>'));

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

        // 連絡先のユーザーデータを取得（Firestoreはin queryが30件まで）
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

        // 未読数カウント
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

// Rivift App Store API — Server.js への追記分 (修正版)

const appsCol = () => db.collection('store_apps');

// ── アプリ一覧取得（htmlContentは返さない）─────────────
app.get('/store/apps', async (req, res) => {
    try {
        const { category = 'all', sort = 'newest', limit = 30, offset = 0 } = req.query;
        let q = appsCol();
        if (category && category !== 'all') q = q.where('category', '==', category);
        q = q.orderBy(sort === 'popular' ? 'downloads' : 'createdAt', 'desc')
             .limit(Number(limit)).offset(Number(offset));
        const snap = await q.get();
        const apps = snap.docs.map(d => {
            const { htmlContent, ...rest } = d.data();
            return { id: d.id, ...rest };
        });
        res.json({ apps });
    } catch (e) { console.error('store/apps error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── アプリ詳細（htmlContent含む）──────────────────────
app.get('/store/apps/:id', async (req, res) => {
    try {
        const snap = await appsCol().doc(req.params.id).get();
        if (!snap.exists) return res.status(404).json({ error: 'App not found' });
        res.json({ app: { id: snap.id, ...snap.data() } });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── アプリ投稿 ──────────────────────────────────────────
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
            createdAt: now,
            updatedAt: now,
        });
        res.json({ success: true, id: docRef.id });
    } catch (e) { console.error('store publish error:', e); res.status(500).json({ error: 'Server error' }); }
});

// ── アプリ更新 ──────────────────────────────────────────
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
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── アプリ削除 ──────────────────────────────────────────
app.delete('/store/apps/:id', async (req, res) => {
    try {
        const { email } = req.body;
        const snap = await appsCol().doc(req.params.id).get();
        if (!snap.exists) return res.status(404).json({ error: 'App not found' });
        if (snap.data().authorEmail !== email) return res.status(403).json({ error: '権限がありません' });
        await appsCol().doc(req.params.id).delete();
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── ダウンロード数インクリメント ────────────────────────
app.post('/store/apps/:id/download', async (req, res) => {
    try {
        await appsCol().doc(req.params.id).update({
            downloads: admin.firestore.FieldValue.increment(1)
        });
        res.json({ success: true });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});

// ── 自分の投稿アプリ一覧（htmlContent・iconImageは除外）
app.get('/store/my-apps', async (req, res) => {
    try {
        const { email } = req.query;
        if (!email) return res.status(400).json({ error: 'email is required' });
        const snap = await appsCol()
            .where('authorEmail', '==', email)
            .get();
        const apps = snap.docs.map(d => {
            const { htmlContent, iconImage, ...rest } = d.data();
            return { id: d.id, ...rest };
        });
        .sort((a, b) => b.createdAt - a.createdAt); 
        res.json({ apps });
    } catch (e) { res.status(500).json({ error: 'Server error' }); }
});



// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Socket.io（リアルタイム通信 - 変更なし）
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
            // サブコレクションに保存（1ドキュメントに全メッセージを詰めない→速度改善）
            await chatsCol().doc(chatID).set({ users: [from, to], lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
            await chatsCol().doc(chatID).collection('messages').doc(msgId).set({ ...payload, id: msgId });
            // リアルタイム配信
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
            // サブコレクションの未読メッセージを一括既読
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
            // サブコレクションを全削除
            const msgsSnap = await chatsCol().doc(chatID).collection('messages').get();
            const batch = db.batch();
            msgsSnap.docs.forEach(d => batch.delete(d.ref));
            batch.delete(chatsCol().doc(chatID));
            await batch.commit();
            notifyUsers([user1, user2]);
        } catch (e) { console.error('delete_chat error:', e); }
    });

    // メッセージ編集
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

    // リアクション追加・トグル
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

    // WebRTC（変更なし）
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

    // LiveCanvas（変更なし）
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

    // ── 近距離共有 シグナリング ──────────────────────────
    // 送信側 → 受信側に招待通知
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

    // 受信側の承認/拒否を送信側に通知
    socket.on('proximity_share_response', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (!recipientSocketId) return;
        // 承認・拒否どちらもそのまま送信側に転送する（proximity_ready_ack は廃止）
        io.to(recipientSocketId).emit('proximity_share_response', { from: socket.email, accepted: payload.accepted });
    });

    // 受信側準備完了 → 送信側にOffer送信を促す
    socket.on('proximity_ready', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_do_offer', { from: socket.email });
    });

    // WebRTC Offer中継（送信側→受信側）
    socket.on('proximity_do_answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_do_answer', {
            from: socket.email,
            offer: payload.offer,
        });
    });

    // WebRTC Answer中継（受信側→送信側）
    socket.on('proximity_answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_answer', {
            from: socket.email,
            answer: payload.answer,
        });
    });

    // ICE候補中継（双方向）
    socket.on('proximity_ice', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_ice', {
            from: socket.email,
            candidate: payload.candidate,
        });
    });
    
    // 受信完了通知中継（受信側→送信側）
    socket.on('proximity_received', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_received', {
            from: socket.email,
            fileName: payload.fileName,
        });
    });

    // キャンセル通知中継（送信側→受信側 または 受信側→送信側）
    socket.on('proximity_cancel', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) io.to(recipientSocketId).emit('proximity_cancel', {
            from: socket.email,
        });
    });
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// サーバー起動（MongoDBへの接続不要になったので直接起動）
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
server.listen(PORT, () => console.log(`Rivift Connect Server v5.0 (Firebase) is running on port ${PORT}`));
