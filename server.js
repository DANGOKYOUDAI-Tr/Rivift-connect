require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');
const path = require('path');
const { MongoClient } = require('mongodb');
const axios = require('axios');
const fetch = require('node-fetch');
const twilio = require('twilio');

const app = express();
app.use(cors());
app.use(express.json({limit: '50mb'}));

const server = http.createServer(app);
const io = new Server(server, { 
    cors: { origin: "*", methods: ["GET", "POST"] },
    maxHttpBufferSize: 50e6
});
const PORT = process.env.PORT || 3001;

const MONGO_URI = process.env.MONGO_URI;
let usersCollection;
let chatsCollection;

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;

async function connectToDatabase() {
    if (!MONGO_URI) { console.error("MONGO_URI not found."); process.exit(1); }
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        const db = client.db("rivift-connect-db");
        usersCollection = db.collection("users");
        chatsCollection = db.collection("chats");
        console.log('Successfully connected to MongoDB Atlas');
    } catch (e) {
        console.error("Failed to connect to MongoDB Atlas", e);
        process.exit(1);
    }
}

let onlineUsers = {};

app.get('/', (req, res) => res.send('<h1>Rivift Connect Server v4.1 is Active!</h1>'));

app.post('/createUser', async (req, res) => {
    try {
        const { email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon } = req.body;
        const existingUser = await usersCollection.findOne({ _id: email });
        if (existingUser) {
            return res.status(400).json({ error: 'User already exists' });
        }
        await usersCollection.insertOne({ _id: email, displayName, publicKeyJwk, encryptedPrivateKeyPayload, icon, friends: [], requests: [], sentRequests: [] });
        res.json({ success: true });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/getUserData', async (req, res) => {
    try {
        const { email } = req.body;
        const userData = await usersCollection.findOne({ _id: email });
        res.json({ userData });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/getSidebarData', async (req, res) => {
    try {
        const { email } = req.body;
        const user = await usersCollection.findOne({ _id: email });
        if (!user) return res.status(404).json({ error: 'User not found' });
        
        const contactEmails = [...user.friends, ...user.requests, ...user.sentRequests];
        const uniqueEmails = [...new Set(contactEmails)];
        
        const contactsData = {};
        if (uniqueEmails.length > 0) {
            const users = await usersCollection.find({ _id: { $in: uniqueEmails } }).toArray();
            users.forEach(u => {
                contactsData[u._id] = { displayName: u.displayName, icon: u.icon };
            });
        }
        
        const unreadCounts = {};
        if (user.friends.length > 0) {
            for (const friendEmail of user.friends) {
                const chatID = [email, friendEmail].sort().join('__');
                const chat = await chatsCollection.findOne({ _id: chatID });
                if (chat) {
                    const count = chat.messages.filter(msg => msg.to === email && !msg.read).length;
                    unreadCounts[friendEmail] = count;
                } else {
                    unreadCounts[friendEmail] = 0;
                }
            }
        }
        
        res.json({
            friends: user.friends,
            requests: user.requests,
            sentRequests: user.sentRequests,
            usersData: contactsData,
            unreadCounts
        });
    } catch (e) { 
        console.error("getSidebarData error:", e);
        res.status(500).json({ error: 'Server error' }); 
    }
});

app.post('/getUsersData', async (req, res) => {
    try {
        const { emails } = req.body;
        const usersData = {};
        if (emails && emails.length > 0) {
            const users = await usersCollection.find({ _id: { $in: emails } }).toArray();
            users.forEach(user => {
                usersData[user._id] = { displayName: user.displayName, icon: user.icon, publicKeyJwk: user.publicKeyJwk };
            });
        }
        res.json({ usersData });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.post('/getMessageHistory', async (req, res) => {
    try {
        const { user1, user2, limit = 50, skip = 0 } = req.body; 
        const chatID = [user1, user2].sort().join('__');
        const history = await chatsCollection
            .find({ chatID: chatID })
            .sort({ timestamp: -1 }) 
            .skip(skip)               
            .limit(limit)           
            .toArray();
        res.json({ history: history.reverse() });

    } catch (error) {
        console.error("getMessageHistory error:", error);
        res.status(500).json({ error: 'Server error' });
    }
});

app.post('/saveMessage', async (req, res) => {
    try {
        const { user1, user2, message } = req.body;
        const chatID = [user1, user2].sort().join('__');
        await chatsCollection.updateOne(
            { _id: chatID },
            { $push: { messages: message }, $setOnInsert: { users: [user1, user2] } },
            { upsert: true }
        );
        res.json({ success: true });
    } catch (error) { res.status(500).json({ error: 'Server error' }); }
});

app.get('/search', async (req, res) => {
    const query = req.query.q;
    if (!query) {
        return res.status(400).json({ error: 'Query parameter "q" is required.' });
    }
    try {
        const response = await axios.get('https://api.duckduckgo.com/', {
            params: {
                q: query,
                format: 'json',
                no_html: 1,
                skip_disambig: 1
            },
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });

        const searchResults = (response.data.RelatedTopics || [])
            .filter(topic => topic.Result) 
            .map(topic => ({
                title: topic.Text,
                description: topic.Result.replace(/<[^>]*>?/gm, ''), 
                url: topic.FirstURL
            }));

        res.json({ results: searchResults });


    } catch (error) {
        console.error('DuckDuckGo API Error:', error.message);
        res.status(500).json({ error: 'Failed to fetch search results.' });
    }
});



let totalDataProxied = 0; 

function rewriteHtmlPaths(html, baseUrl) {
    const base = new URL(baseUrl);
    const domain = base.origin;
    const rewrite = (content) => {
        content = content.replace(/(src|href|action|data-src)=(["'])(\/(?!\/)[^"']+)(["'])/g,
            (match, attr, q1, path, q2) => `${attr}=${q1}${domain}${path}${q2}`
        );
        content = content.replace(/(src|href|action|data-src)=(["'])(?!\/|https?:|data:)([^"']+)(["'])/g,
            (match, attr, q1, path, q2) => {
                const newUrl = new URL(path, baseUrl).href;
                return `${attr}=${q1}${newUrl}${q2}`;
            }
        );
        
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

app.post('/proxy', async (req, res) => {
    try {
        const { url, method, headers, body } = req.body;
        if (!url) {
            return res.status(400).send('URL is required.');
        }
        const fetchOptions = {
            method: method,
            headers: { ...headers },
            body: body ? Buffer.from(body, 'base64') : null,
            redirect: 'follow'
        };
        delete fetchOptions.headers['host'];
        delete fetchOptions.headers['origin'];
        delete fetchOptions.headers['referer'];
        delete fetchOptions.headers['content-length'];
        fetchOptions.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36';
        const response = await fetch(url, fetchOptions);
        res.setHeader('x-proxy-headers', JSON.stringify(Object.fromEntries(response.headers)));
        response.body.pipe(res);
    } catch (error) {
        console.error('Proxy error:', error.message);
        res.status(500).send(error.message);
    }
});

const google = require('google-it');

app.get('/youtube-search', async (req, res) => {
    const query = req.query.q;
    if (!query) {
        return res.status(400).json({ error: 'Query is required' });
    }
    try {
        const options = {
            query: `${query} site:youtube.com`, 
            'no-display': true
        };
        const results = await google(options);
        const videoResults = results.map(r => {
            const videoIdMatch = r.link.match(/watch\?v=([a-zA-Z0-9_-]{11})/);
            return {
                title: r.title,
                link: r.link,
                videoId: videoIdMatch ? videoIdMatch[1] : null
            };
        }).filter(r => r.videoId); 

        res.json({ results: videoResults });

    } catch (error) {
        console.error('YouTube search error:', error);
        res.status(500).json({ error: 'Failed to search YouTube' });
    }
});

app.get('/getProxyStats', (req, res) => {
    res.json({
        totalDataProxied,
        totalDataProxiedMB: (totalDataProxied / (1024 * 1024)).toFixed(2)
    });
});

const { JSDOM } = require('jsdom');
const { Readability } = require('@mozilla/readability');

app.get('/extract', async (req, res) => {
    const url = req.query.url;
    if (!url) {
        return res.status(400).json({ error: 'URL parameter is required.' });
    }

    try {
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });

        const dom = new JSDOM(response.data, { url: url });
        const reader = new Readability(dom.window.document);
        const article = reader.parse();

        if (article) {
            res.json({
                title: article.title,
                content: article.content, 
                textContent: article.textContent, 
                author: article.byline,
                length: article.length
            });
        } else {
            res.status(404).json({ error: '記事のコンテンツを抽出できませんでした。' });
        }

    } catch (error) {
        console.error('Extraction error:', error.message);
        res.status(500).json({ error: 'ページの読み込みまたは解析に失敗しました。' });
    }
});

io.on('connection', (socket) => {
    let currentUserEmail = null;

socket.on('get-ice-servers', async (payload, callback) => {
    console.log("--- [1/4] 'get-ice-servers' イベントを受信しました。 ---");
    
    if (!accountSid || !authToken) {
        const errorMsg = "Twilioの認証情報が.envファイルに設定されていません。";
        console.error("--- [ERROR] " + errorMsg + " ---");
        return callback({ error: errorMsg });
    }
    
    try {
        const client = twilio(accountSid, authToken);
        const token = await client.tokens.create({ ttl: 3600 });
        
        console.log("--- [4/4] Twilioからトークンを取得しました！クライアントに返信します。 ---");
        callback({ iceServers: token.iceServers });

    } catch (error) {
        console.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        console.error("--- [FATAL ERROR] Twilioとの通信中にエラーが発生 ---");
        console.error("エラー名:", error.name);
        console.error("エラーメッセージ:", error.message);
        console.error("エラースタック:", error.stack);
        console.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        
        callback({ error: `Twilioとの通信に失敗しました: ${error.message}` });
    }
});

    const notifyFriendsOfStatusChange = async (email, isOnline) => {
        try {
            const user = await usersCollection.findOne({ _id: email });
            if (user && user.friends) {
                user.friends.forEach(friendEmail => {
                    const friendSocketId = onlineUsers[friendEmail];
                    if (friendSocketId) {
                        io.to(friendSocketId).emit('friend_status_changed', {
                            email: email,
                            isOnline: isOnline
                        });
                    }
                });
            }
        } catch (error) {
            console.error('Error notifying friends of status change:', error);
        }
    };

    socket.on('login', ({ email }) => {
        onlineUsers[email] = socket.id;
        currentUserEmail = email;
        socket.email = email;
        notifyFriendsOfStatusChange(email, true);
    });

    socket.on('get_initial_statuses', async (payload, callback) => {
        const userEmail = payload.email;
        const user = await usersCollection.findOne({ _id: userEmail });
        if (user && user.friends) {
            const statuses = {};
            user.friends.forEach(friendEmail => {
                statuses[friendEmail] = !!onlineUsers[friendEmail];
            });
            callback(statuses);
        }
    });

    socket.on('disconnect', () => {
        if (currentUserEmail) {
            delete onlineUsers[currentUserEmail];
            notifyFriendsOfStatusChange(currentUserEmail, false);
        }
    });

    const notifyUsers = (userEmails) => {
        userEmails.forEach(email => {
            const socketId = onlineUsers[email];
            if (socketId) io.to(socketId).emit('db_updated_notification');
        });
    };

    socket.on('send_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: to }, { $addToSet: { requests: from } });
        await usersCollection.updateOne({ _id: from }, { $addToSet: { sentRequests: to } });
        notifyUsers([from, to]);
    });

    socket.on('accept_friend_request', async ({ from, to }) => {
        await usersCollection.updateOne({ _id: from }, { $pull: { requests: to }, $addToSet: { friends: to } });
        await usersCollection.updateOne({ _id: to }, { $pull: { sentRequests: from }, $addToSet: { friends: from } });
        notifyUsers([from, to]);
    });

    socket.on('delete_friend', async ({ from, to }) => {
        try {
            const fromUser = await usersCollection.findOne({ _id: from });
            const toUser = await usersCollection.findOne({ _id: to });
            if (fromUser && toUser) {
                await usersCollection.updateOne({ _id: from }, { $pull: { friends: to } });
                await usersCollection.updateOne({ _id: to }, { $pull: { friends: from } });
                const chatID = [from, to].sort().join('__');
                await chatsCollection.deleteOne({ _id: chatID });
                notifyUsers([from, to]);
            }
        } catch (e) {
            console.error("delete_friend error:", e);
        }
    });
    
    socket.on('private_message', async (payload) => {
        try {
            const { from, to, timestamp } = payload;
            const chatID = [from, to].sort().join('__');
            const messageToStore = {
                chatID: chatID,
                ...payload
            };
            await chatsCollection.insertOne(messageToStore);
            const recipientSocketId = onlineUsers[to];
            if (recipientSocketId) {
                io.to(recipientSocketId).emit('private_message', payload);
            }
            const senderSocketId = onlineUsers[from];
            if (senderSocketId) {
                io.to(senderSocketId).emit('db_updated_notification');
            }
        } catch(e) {
            console.error("private_message error:", e);
        }
    });

socket.on('read_receipt', async (payload) => {
    try { 
        const readerEmail = payload.from; 
        const writerEmail = payload.to;
        const chatID = [readerEmail, writerEmail].sort().join('__');
        await chatsCollection.updateMany(
            { chatID: chatID, "messages.to": readerEmail, "messages.read": { $ne: true } },
            { $set: { "messages.$[elem].read": true } },
            { arrayFilters: [ { "elem.to": readerEmail } ] }
        );
        const readerSocketId = onlineUsers[readerEmail];
        const writerSocketId = onlineUsers[writerEmail];

        if (readerSocketId) {
            io.to(readerSocketId).emit('messages_marked_as_read', { chatPartner: writerEmail });
        }
        if (writerSocketId) {
            io.to(writerSocketId).emit('messages_marked_as_read', { chatPartner: readerEmail });
        }

    } catch (e) {
        console.error("read_receipt error:", e);
    }
});

    socket.on('delete_message', async ({ from, to, messageId }) => {
        const chatID = [from, to].sort().join('__');
        await chatsCollection.updateOne(
            { _id: chatID, "messages.id": messageId, "messages.from": from },
            { $set: { 
                "messages.$.type": "deleted",
                "messages.$.content": "メッセージが削除されました",
            }}
        );
        notifyUsers([from, to]);
    });
    
    socket.on('delete_chat', async ({ user1, user2 }) => {
        const chatID = [user1, user2].sort().join('__');
        await chatsCollection.deleteOne({ _id: chatID });
        notifyUsers([user1, user2]);
    });
    
    socket.on('update_profile', async ({email, newDisplayName, newIcon}) => {
        const updateData = {};
        if (newDisplayName) updateData.displayName = newDisplayName;
        if (newIcon) updateData.icon = newIcon;
        const result = await usersCollection.findOneAndUpdate({ _id: email }, { $set: updateData }, { returnDocument: 'after' });
        if (result.value) {
            notifyUsers([email, ...result.value.friends]);
        }
    });

    socket.on('disconnect', () => {
        if (socket.email) delete onlineUsers[socket.email];
    });

    socket.on('webrtc-offer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-offer', {
                from: socket.email,
                offer: payload.offer
            });
        }
    });

    socket.on('webrtc-answer', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-answer', {
                from: socket.email,
                answer: payload.answer
            });
        }
    });

    socket.on('webrtc-ice-candidate', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-ice-candidate', {
                from: socket.email,
                candidate: payload.candidate
            });
        }
    });

    socket.on('webrtc-end-call', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-end-call', { from: socket.email });
        }
    });

    socket.on('webrtc-reject-call', (payload) => {
        const recipientSocketId = onlineUsers[payload.to];
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('webrtc-reject-call', { from: socket.email });
        }
    });

socket.on('live_canvas_invite', (payload) => {
    const recipientSocketId = onlineUsers[payload.to];
    if (recipientSocketId) {
        io.to(recipientSocketId).emit('live_canvas_invite', {
            from: socket.email
        });
    }
});

socket.on('live_canvas_draw', (payload) => {
    const recipientSocketId = onlineUsers[payload.to];
    if (recipientSocketId) {
        io.to(recipientSocketId).emit('live_canvas_draw', {
            from: socket.email,
            x: payload.x, y: payload.y,
            lastX: payload.lastX, lastY: payload.lastY,
            color: payload.color, size: payload.size
        });
    }
});

socket.on('live_canvas_clear', (payload) => {
    const recipientSocketId = onlineUsers[payload.to];
    if (recipientSocketId) {
        io.to(recipientSocketId).emit('live_canvas_clear', {
            from: socket.email
        });
    }
});

});

connectToDatabase().then(() => {
    server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
}).catch(err => {
    console.error("Failed to start server:", err);
});