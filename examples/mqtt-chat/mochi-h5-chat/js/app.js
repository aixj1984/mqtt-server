/**
 * Mochi H5 聊天 — 固定 Broker: ws://127.0.0.1:1882
 * 话题规则与原版一致：MQTT topic = sha256(房间/频道字符串)，载荷为 OpenPGP 对称加密。
 */

const MQTT_HOST = 'ws://127.0.0.1:1882';

/** 单条消息 JSON 长度上限（含 Base64），避免超过 Broker / 浏览器限制 */
const MAX_JSON_CHARS = 340000;

/** 常用表情（类微信快捷栏） */
const EMOJI_CHARS = Array.from(
    '😀😃😄😁😆🥹😅😂🤣🥲☺️😊😍🥰😘😗😙😚🙂🤔😏😒🙄😮‍💨🤥😌😔😪🤤😴😷🤒🤕🤑🤠😎🤓🧐🥳🥸😲😮😯😦😧😨😰😥😢😭😱😖😣😞😓😩😫🥱😤😡🤬😈👿💀☠️💩👻👽🤖💋💌💘💝💖💗💓💞💕💟❣️💔❤️‍🔥❤️‍🩹🧡💛💚💙💜🤎🖤🤍💯💢💥💫💦💨🕳️💬👁️‍🗨️🗨️🗯️💭💤👋🤚🖐️✋🖖👌🤌🤏✌️🤞🫰🤟🤘🤙👈👉👆🖕👇☝️👍👎✊👊🤛🤜👏🙌🤲🤝🙏✍️💅🤳💪🦾🦿🦵🦶👂🦻👃🧠🫀🫁🦷🦴👀👁️👅👄🫦💋🩸👶🧒👦👧🧑👱👨🧔‍♂️👩🧓👴👵🙍🙎🙅🙆💁🙋🧏🙇🤦🤷👮💂🥷👷🫅🤴👸👳🧕👲🤵👰🤰🫃🫄🤱👼🎅🧑‍🎄🦸🦹🧙🧚🧛🧜🧝🧞🧟🧌💆💇🚶🧍🧎🏃💃🕺🕴️👯🧘🛀🛌👭👫👬💏💑👪🗣️👤👥🫂👣🐵🐶🐱🐭🐹🐰🦊🐻🐼🐻‍❄️🐨🐯🦁🐮🐷🐽🐸🐵🙈🙉🙊🐒🐔🐧🐦🐤🐣🐥🦆🦅🦉🦇🐺🐗🐴🦄🫎🐝🪱🐛🦋🐌🐞🐜🪰🪲🪳🦟🦗🕷️🕸️🦂🐢🐍🦎🦖🦕🐙🦑🦐🦞🦀🐡🐠🐟🐬🐳🐋🦈🐊🐅🐆🦓🦍🦧🦣🐘🦛🦏🐪🐫🦒🦘🦬🐃🐂🐄🐎🐖🐏🐑🦙🐐🦌🐕🐩🦮🐕‍🦺🐈🐈‍⬛🪶🪽🐓🦃🦤🦚🦜🦢🪿🦩🕊️🐇🦝🦨🦡🦫🦦🦥🐁🐀🐿️🦔🌲🌳🌴🌵🌶️🫑🌷🌺🌸🌼🌻🌞🌝🌛🌜🌚🌕🌖🌗🌘🌑🌒🌓🌔🌙⭐🌟✨💫☄️🌈☀️🌤️⛅🌥️☁️🌦️🌧️⛈️🌩️🌨️❄️☃️⛄🌬️💨💧💦☔☂️🌊🫧🔥💥❄️🎄✨🎁🎂🎉🎊🍻🍺🥂🍷🍾🥤☕🍵🧋🍼🥛🍯🍪🍩🍰🧁🍫🍬🍭🍮🍦🍨🍧🍡🥟🍜🍲🍱🍣🍙🍚🥡🥢🍴🍽️🥄🔪🫙🏠🏡🏢🚀✈️🚗🚕🚌🎵🎶🎤🎧📷📱💻⌚',
);

const PUBLIC_CHANNELS = [
    { slug: 'lobby', name: '公共大厅' },
    { slug: 'tech', name: '技术交流' },
    { slug: 'watercooler', name: '茶水间' },
];

const STORAGE = {
    followedPublic: 'mochi_followed_public',
    myRooms: 'mochi_my_rooms',
    friends: 'mochi_friends',
    dmSecrets: 'mochi_dm_secrets',
};

let nickname = '';
let mqttClient = null;
/** @type {Map<string, { type: string, name: string, secret: string, topic: string }>} */
const topicMeta = new Map();
/** @type {{ type: string, topic: string, secret: string, title: string, sub?: string, slug?: string, roomId?: string, pair?: string } | null} */
let activeSession = null;

async function sha256(message) {
    const encoded = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest('SHA-256', encoded);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
}

function roomTopicKey(kind, key) {
    if (kind === 'public') return `public:${key}`;
    if (kind === 'room') return `room:${key}`;
    if (kind === 'dm') return `dm:${key}`;
    throw new Error('unknown kind');
}

async function topicFromKey(kind, key) {
    return sha256(roomTopicKey(kind, key));
}

function loadJson(key, fallback) {
    try {
        const raw = localStorage.getItem(key);
        return raw ? JSON.parse(raw) : fallback;
    } catch {
        return fallback;
    }
}

function saveJson(key, val) {
    localStorage.setItem(key, JSON.stringify(val));
}

function getFollowedPublic() {
    return new Set(loadJson(STORAGE.followedPublic, ['lobby']));
}

function setFollowedPublic(set) {
    saveJson(STORAGE.followedPublic, [...set]);
}

function getMyRooms() {
    return loadJson(STORAGE.myRooms, []);
}

function saveMyRooms(list) {
    saveJson(STORAGE.myRooms, list);
}

function getFriends() {
    return loadJson(STORAGE.friends, []);
}

function saveFriends(list) {
    saveJson(STORAGE.friends, list);
}

function dmPairKey(a, b) {
    const [x, y] = [a, b].sort();
    return `${x}::${y}`;
}

function getDmSecrets() {
    return loadJson(STORAGE.dmSecrets, {});
}

function saveDmSecrets(obj) {
    saveJson(STORAGE.dmSecrets, obj);
}

function toast(msg) {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.classList.remove('hidden');
    clearTimeout(toast._t);
    toast._t = setTimeout(() => el.classList.add('hidden'), 2200);
}

function createPayloadJson(text, sys = false) {
    return JSON.stringify({
        m: text,
        t: Math.floor(Date.now() / 1000),
        u: sys ? 'sys' : nickname,
    });
}

/** k: txt（默认）| img | aud */
function createRichPayload(kind, fields) {
    return JSON.stringify({
        u: nickname,
        t: Math.floor(Date.now() / 1000),
        k: kind,
        ...fields,
    });
}

function safeBase64Fragment(s) {
    return typeof s === 'string' && s.length > 0 && s.length < 8000000 && /^[A-Za-z0-9+/=_-]+$/.test(s);
}

async function sendPayloadJson(jsonStr) {
    if (!activeSession) return false;
    if (jsonStr.length > MAX_JSON_CHARS) {
        toast('消息过大（图片请换小图或缩短语音）');
        return false;
    }
    await publishToSession(activeSession, jsonStr);
    return true;
}

async function compressImageToPayload(file) {
    if (!file.type.startsWith('image/')) throw new Error('not image');
    const maxEdge = 960;
    const maxBytesApprox = 130000;

    const bitmap = await createImageBitmap(file);
    let w = bitmap.width;
    let h = bitmap.height;
    const scale = Math.min(1, maxEdge / Math.max(w, h));
    w = Math.round(w * scale);
    h = Math.round(h * scale);
    const canvas = document.createElement('canvas');
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(bitmap, 0, 0, w, h);
    bitmap.close();

    let q = 0.88;
    let dataUrl = canvas.toDataURL('image/jpeg', q);
    for (let i = 0; i < 6 && dataUrl.length > maxBytesApprox * 1.4; i++) {
        q -= 0.1;
        if (q < 0.35) break;
        dataUrl = canvas.toDataURL('image/jpeg', q);
    }
    const comma = dataUrl.indexOf(',');
    const d = dataUrl.slice(comma + 1);
    return { d, mime: 'image/jpeg' };
}

function pickAudioMimeType() {
    const cands = ['audio/webm;codecs=opus', 'audio/webm', 'audio/mp4'];
    for (const c of cands) {
        if (typeof MediaRecorder !== 'undefined' && MediaRecorder.isTypeSupported(c)) return c;
    }
    return '';
}

let voiceState = {
    stream: null,
    recorder: null,
    chunks: [],
    t0: 0,
    timer: null,
    startY: 0,
    cancel: false,
    maxDurTimer: null,
};

/** 防止「已松手但麦克风尚未就绪」仍开始录音 */
const voiceArm = { generation: 0 };

/** 最长录音时长（与界面倒计时一致） */
const VOICE_MAX_MS = 30000;

function formatRecordCountdown(elapsedMs) {
    const left = Math.max(0, VOICE_MAX_MS - elapsedMs);
    const s = Math.ceil(left / 1000);
    return `${s}″`;
}

function stopVoiceTimer() {
    if (voiceState.timer) {
        clearInterval(voiceState.timer);
        voiceState.timer = null;
    }
    if (voiceState.maxDurTimer) {
        clearTimeout(voiceState.maxDurTimer);
        voiceState.maxDurTimer = null;
    }
}

async function beginVoiceHold(clientY, armGen) {
    if (!activeSession || voiceState.recorder) return;
    voiceState.chunks = [];
    voiceState.cancel = false;
    voiceState.startY = clientY;
    voiceState.t0 = Date.now();

    let stream;
    try {
        stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    } catch (_) {
        toast('无法使用麦克风');
        return;
    }

    if (armGen !== voiceArm.generation) {
        stream.getTracks().forEach((t) => t.stop());
        return;
    }

    const mimePick = pickAudioMimeType();
    const rec = mimePick ? new MediaRecorder(stream, { mimeType: mimePick }) : new MediaRecorder(stream);
    voiceState.stream = stream;
    voiceState.recorder = rec;
    rec.ondataavailable = (e) => {
        if (e.data && e.data.size) voiceState.chunks.push(e.data);
    };
    rec.start(240);

    if (armGen !== voiceArm.generation) {
        voiceState.cancel = true;
        await endVoiceHold();
        return;
    }

    const overlay = document.getElementById('record-overlay');
    const btn = document.getElementById('btn-voice-hold');
    const timeEl = document.getElementById('record-time');
    overlay.classList.remove('hidden', 'cancel-mode');
    btn.classList.add('recording');
    timeEl.textContent = formatRecordCountdown(0);
    voiceState.timer = setInterval(() => {
        timeEl.textContent = formatRecordCountdown(Date.now() - voiceState.t0);
    }, 200);
    voiceState.maxDurTimer = setTimeout(() => {
        voiceState.cancel = false;
        endVoiceHold();
    }, VOICE_MAX_MS);
}

async function endVoiceHold() {
    const overlay = document.getElementById('record-overlay');
    const btn = document.getElementById('btn-voice-hold');
    stopVoiceTimer();
    overlay.classList.add('hidden');
    overlay.classList.remove('cancel-mode');
    btn.classList.remove('recording');

    const rec = voiceState.recorder;
    const stream = voiceState.stream;
    voiceState.recorder = null;
    voiceState.stream = null;

    if (!rec) return;

    const savedMime = rec.mimeType || pickAudioMimeType() || 'audio/webm';
    const durMs = Date.now() - voiceState.t0;
    const cancelled = voiceState.cancel;
    voiceState.cancel = false;

    try {
        await new Promise((resolve) => {
            rec.onstop = resolve;
            rec.stop();
        });
    } catch (_) {}

    if (stream) {
        stream.getTracks().forEach((t) => t.stop());
    }

    if (cancelled || durMs < 450) {
        voiceState.chunks = [];
        if (!cancelled) toast('说话时间太短');
        return;
    }

    const blob = new Blob(voiceState.chunks, { type: savedMime });
    voiceState.chunks = [];
    if (!blob.size) {
        toast('录音失败');
        return;
    }

    let dataUrl;
    try {
        dataUrl = await new Promise((res, rej) => {
            const fr = new FileReader();
            fr.onload = () => res(fr.result);
            fr.onerror = rej;
            fr.readAsDataURL(blob);
        });
    } catch (_) {
        toast('读取录音失败');
        return;
    }
    const comma = dataUrl.indexOf(',');
    const d = dataUrl.slice(comma + 1);
    const mime = blob.type || savedMime;
    const json = createRichPayload('aud', { d, mime, dur: Math.max(1, Math.round(durMs / 1000)) });
    try {
        await sendPayloadJson(json);
    } catch (e) {
        console.error(e);
        toast('语音发送失败');
    }
}

async function encryptPayload(jsonStr, secret) {
    const message = await openpgp.createMessage({
        binary: new TextEncoder().encode(jsonStr),
    });
    const out = await openpgp.encrypt({
        message,
        passwords: [secret],
        format: 'binary',
    });
    return out;
}

async function decryptPayload(binary, secret) {
    const encryptedMessage = await openpgp.readMessage({ binaryMessage: binary });
    const decrypted = await openpgp.decrypt({
        message: encryptedMessage,
        passwords: [secret],
        format: 'binary',
    });
    return new TextDecoder().decode(decrypted.data);
}

function switchScreen(id) {
    document.querySelectorAll('.screen').forEach((s) => s.classList.add('hidden'));
    document.getElementById(id).classList.remove('hidden');
}

function renderPublicList() {
    const ul = document.getElementById('list-public-channels');
    const followed = getFollowedPublic();
    ul.innerHTML = '';
    PUBLIC_CHANNELS.forEach((ch) => {
        const li = document.createElement('li');
        const on = followed.has(ch.slug);
        li.innerHTML = `
      <div class="left">
        <div class="name">${ch.name}</div>
        <div class="meta">${on ? '已关注 · 接收消息' : '未关注'}</div>
      </div>
      <button type="button" class="btn small" data-slug="${ch.slug}">${on ? '取消' : '关注'}</button>
    `;
        const btn = li.querySelector('button');
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            toggleFollow(ch.slug);
        });
        li.addEventListener('click', () => {
            if (on) openChatFromPublic(ch.slug, ch.name);
        });
        ul.appendChild(li);
    });
}

function toggleFollow(slug) {
    const s = getFollowedPublic();
    if (s.has(slug)) s.delete(slug);
    else s.add(slug);
    setFollowedPublic(s);
    renderPublicList();
    resubscribeAll();
}

function renderMyRooms() {
    const ul = document.getElementById('list-my-rooms');
    const rooms = getMyRooms();
    ul.innerHTML = '';
    if (!rooms.length) {
        const li = document.createElement('li');
        li.innerHTML = '<div class="meta">暂无，点击「新建」创建</div>';
        ul.appendChild(li);
        return;
    }
    rooms.forEach((r) => {
        const li = document.createElement('li');
        li.innerHTML = `
      <div class="left">
        <div class="name">${escapeHtml(r.displayName)}</div>
        <div class="meta">房间 ${escapeHtml(r.roomId)}</div>
      </div>
    `;
        li.addEventListener('click', () => openChatFromMyRoom(r));
        ul.appendChild(li);
    });
}

function renderFriends() {
    const ul = document.getElementById('list-friends');
    const friends = getFriends();
    ul.innerHTML = '';
    if (!friends.length) {
        const li = document.createElement('li');
        li.innerHTML = '<div class="meta">添加好友后可发起私聊或拉群</div>';
        ul.appendChild(li);
        return;
    }
    friends.forEach((fid) => {
        const li = document.createElement('li');
        li.innerHTML = `
      <div class="left">
        <div class="name">${escapeHtml(fid)}</div>
        <div class="meta">点按发消息 · 群内可继续邀请</div>
      </div>
    `;
        li.addEventListener('click', () => openChatFromFriend(fid));
        ul.appendChild(li);
    });
}

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

async function ensureMqtt() {
    if (mqttClient?.connected) {
        await resubscribeAll();
        return;
    }
    if (mqttClient) {
        try {
            mqttClient.end(true);
        } catch (_) {}
        mqttClient = null;
    }
    const clientId = 'mqttjs_' + (await sha256(nickname));
    const options = {
        keepalive: 180,
        clientId,
        protocolId: 'MQTT',
        protocolVersion: 4,
        clean: false,
        reconnectPeriod: 1000,
        connectTimeout: 30000,
    };
    return new Promise((resolve, reject) => {
        const c = mqtt.connect(MQTT_HOST, options);
        c.on('error', (err) => {
            console.error(err);
            reject(err);
        });
        c.on('connect', () => {
            mqttClient = c;
            attachMessageHandlerOnce(c);
            resubscribeAll().then(() => resolve()).catch(reject);
        });
    });
}

function attachMessageHandlerOnce(c) {
    if (c._mochiMessageBound) return;
    c._mochiMessageBound = true;
    c.on('message', (topic, payload) => {
        const meta = topicMeta.get(topic);
        if (!meta) return;
        decryptPayload(payload, meta.secret)
            .then((json) => handleIncomingJson(topic, meta, json))
            .catch(() => {
                if (activeSession && activeSession.topic === topic) {
                    appendMessageBubble(topic, { u: 'sys', m: '** 无法解密（密钥不一致？）**', t: Math.floor(Date.now() / 1000) }, true);
                }
            });
    });
}

function handleIncomingJson(topic, meta, jsonStr) {
    let obj;
    try {
        obj = JSON.parse(jsonStr);
    } catch {
        return;
    }
    if (activeSession && activeSession.topic === topic) {
        appendMessageBubble(topic, obj, false);
    }
}

async function resubscribeAll() {
    if (!mqttClient?.connected) return;
    topicMeta.clear();
    const subs = [];

    for (const slug of getFollowedPublic()) {
        const key = roomTopicKey('public', slug);
        const topic = await sha256(key);
        const secret = 'public-' + slug;
        topicMeta.set(topic, { type: 'public', name: slug, secret, topic });
        subs.push({ topic, qos: 1 });
    }

    for (const r of getMyRooms()) {
        const key = roomTopicKey('room', r.roomId);
        const topic = await sha256(key);
        topicMeta.set(topic, { type: 'room', name: r.displayName, secret: r.secret, topic });
        subs.push({ topic, qos: 1 });
    }

    const secrets = getDmSecrets();
    for (const pair of Object.keys(secrets)) {
        const secret = secrets[pair];
        const key = roomTopicKey('dm', pair);
        const topic = await sha256(key);
        topicMeta.set(topic, { type: 'dm', name: pair, secret, topic });
        subs.push({ topic, qos: 1 });
    }

    if (!subs.length) return;

    const topics = subs.map((s) => s.topic);
    await new Promise((resolve) => {
        mqttClient.subscribe(topics, { qos: 1 }, (err) => {
            if (err) console.error(err);
            resolve();
        });
    });
}

async function publishToSession(session, jsonStr) {
    if (!mqttClient?.connected) throw new Error('未连接');
    const bin = await encryptPayload(jsonStr, session.secret);
    await new Promise((resolve, reject) => {
        mqttClient.publish(session.topic, bin, { qos: 1, retain: false }, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

function appendMessageBubble(topic, obj, isError) {
    const box = document.getElementById('chat-messages');
    const div = document.createElement('div');
    const ts = new Date((obj.t || 0) * 1000).toLocaleTimeString();
    if (obj.u === 'sys') {
        div.className = 'msg sys';
        div.textContent = `${ts} ${obj.m || ''}`;
        box.insertBefore(div, box.firstChild);
        return;
    }
    const me = obj.u === nickname;
    div.className = 'msg ' + (me ? 'me' : 'other');
    const kind = obj.k || 'txt';
    let bodyHtml = '';
    if (kind === 'img' && obj.d && safeBase64Fragment(obj.d)) {
        const mime = escapeHtml(obj.mime || 'image/jpeg');
        bodyHtml = `<div class="msg-media"><img src="data:${mime};base64,${obj.d}" alt="图片" loading="lazy"></div>`;
    } else if (kind === 'aud' && obj.d && safeBase64Fragment(obj.d)) {
        const mime = escapeHtml(obj.mime || 'audio/webm');
        const durLabel = obj.dur != null ? `<span class="aud-dur">${escapeHtml(String(obj.dur))}″</span>` : '';
        bodyHtml = `<div class="msg-media"><audio controls preload="metadata" src="data:${mime};base64,${obj.d}"></audio>${durLabel}</div>`;
    } else {
        bodyHtml = `<div class="msg-text">${escapeHtml(obj.m || '')}</div>`;
    }
    div.innerHTML = `<div class="who">${escapeHtml(obj.u)}</div>${bodyHtml}<div class="ts">${ts}</div>`;
    box.insertBefore(div, box.firstChild);
}

async function openChatFromPublic(slug, displayName) {
    const followed = getFollowedPublic();
    if (!followed.has(slug)) {
        followed.add(slug);
        setFollowedPublic(followed);
        renderPublicList();
    }
    await resubscribeAll();
    const secret = 'public-' + slug;
    const topic = await topicFromKey('public', slug);
    activeSession = {
        type: 'public',
        topic,
        secret,
        title: displayName,
        sub: '公共频道 · 所有人使用相同约定密钥',
        slug,
    };
    openChatScreen(true);
    await sendJoinLine();
}

async function openChatFromMyRoom(r) {
    await resubscribeAll();
    const topic = await topicFromKey('room', r.roomId);
    activeSession = {
        type: 'room',
        topic,
        secret: r.secret,
        title: r.displayName,
        sub: `自建房 · ${r.roomId}`,
        roomId: r.roomId,
    };
    openChatScreen(true);
    await sendJoinLine();
}

async function openChatFromFriend(friendId) {
    const pair = dmPairKey(nickname, friendId);
    let secrets = getDmSecrets();
    if (!secrets[pair]) {
        const rand = [...crypto.getRandomValues(new Uint8Array(8))].map((b) => b.toString(16).padStart(2, '0')).join('');
        secrets[pair] = rand;
        saveDmSecrets(secrets);
    }
    const secret = secrets[pair];
    await resubscribeAll();
    const topic = await topicFromKey('dm', pair);
    activeSession = {
        type: 'dm',
        topic,
        secret,
        title: friendId,
        sub: '私聊 · 用右上角邀请链接把密钥同步给对方',
        pair,
    };
    openChatScreen(true);
    await sendJoinLine();
}

function openChatScreen(clear) {
    document.getElementById('chat-title').textContent = activeSession.title;
    document.getElementById('chat-sub').textContent = activeSession.sub || '';
    const box = document.getElementById('chat-messages');
    if (clear) box.innerHTML = '';
    document.getElementById('chat-input').value = '';
    switchScreen('screen-chat');
}

async function sendJoinLine() {
    const line = createPayloadJson(`** ${nickname} 进入聊天 **`, true);
    await publishToSession(activeSession, line);
}

async function sendCurrentMessage() {
    const ta = document.getElementById('chat-input');
    const text = ta.value.trim();
    if (!text || !activeSession) return;
    await sendPayloadJson(createPayloadJson(text));
    ta.value = '';
    const ep = document.getElementById('emoji-panel');
    ep.classList.add('hidden');
    ep.setAttribute('aria-hidden', 'true');
}

function buildInviteHash() {
    if (!activeSession) return '';
    let roomKey = '';
    if (activeSession.type === 'public') {
        roomKey = `public:${activeSession.slug}`;
    } else if (activeSession.type === 'room') {
        roomKey = `room:${activeSession.roomId}`;
    } else {
        roomKey = `dm:${activeSession.pair}`;
    }
    const params = {
        host: MQTT_HOST,
        nick: nickname,
        kind: activeSession.type,
        roomKey,
        secret: activeSession.secret,
    };
    const q = Object.entries(params)
        .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
        .join('&');
    return `#${q}`;
}

function showInviteModal() {
    const base = `${location.origin || ''}${location.pathname || '/'}${location.search || ''}`;
    const url = `${base}${buildInviteHash()}`;
    document.getElementById('invite-url').value = url;
    document.getElementById('modal-invite').classList.remove('hidden');
}

function parseInitialHash() {
    const hash = window.location.hash.replace(/^#/, '');
    if (!hash) return;
    const params = {};
    hash.split('&').forEach((item) => {
        const [k, v] = item.split('=');
        if (k && v !== undefined) params[k] = decodeURIComponent(v);
    });
    if (params.roomKey && (params.secret || String(params.roomKey).startsWith('public:'))) {
        sessionStorage.setItem('mochi_pending_join', JSON.stringify(params));
    }
}

async function applyPendingJoin() {
    const raw = sessionStorage.getItem('mochi_pending_join');
    if (!raw) return;
    sessionStorage.removeItem('mochi_pending_join');
    let p;
    try {
        p = JSON.parse(raw);
    } catch {
        return;
    }
    if (p.nick) document.getElementById('login-nickname').value = p.nick;
    // After login user taps nothing — auto open optional: skip auto for safety
    window.__pendingInvite = p;
}

function randomRoomId() {
    return 'r' + Math.random().toString(36).slice(2, 8);
}

function randomSecret() {
    return [...crypto.getRandomValues(new Uint8Array(6))].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function buildEmojiPanel() {
    const panel = document.getElementById('emoji-panel');
    panel.innerHTML = '';
    EMOJI_CHARS.forEach((ch) => {
        const b = document.createElement('button');
        b.type = 'button';
        b.textContent = ch;
        b.addEventListener('click', () => {
            const ta = document.getElementById('chat-input');
            ta.focus();
            ta.value += ch;
        });
        panel.appendChild(b);
    });
}

function setupChatMedia() {
    const emojiPanel = document.getElementById('emoji-panel');
    const btnEmoji = document.getElementById('btn-emoji');
    const btnImage = document.getElementById('btn-image');
    const btnVoice = document.getElementById('btn-voice-hold');
    const imgInput = document.getElementById('chat-image-input');
    const chatMessages = document.getElementById('chat-messages');
    const imagePreviewOverlay = document.getElementById('image-preview-overlay');
    const imagePreviewImg = document.getElementById('image-preview-img');
    const btnImagePreviewClose = document.getElementById('btn-image-preview-close');

    function closeImagePreview() {
        imagePreviewOverlay.classList.add('hidden');
        imagePreviewImg.removeAttribute('src');
    }

    function openImagePreview(src) {
        if (!src) return;
        imagePreviewImg.src = src;
        imagePreviewOverlay.classList.remove('hidden');
    }

    chatMessages.addEventListener('click', (e) => {
        const thumb = e.target.closest('.msg-media img');
        if (!thumb || !thumb.src) return;
        e.preventDefault();
        openImagePreview(thumb.src);
    });

    imagePreviewOverlay.addEventListener('click', (e) => {
        if (e.target === imagePreviewImg) return;
        if (e.target === imagePreviewOverlay || e.target === btnImagePreviewClose) closeImagePreview();
    });

    document.addEventListener('keydown', (e) => {
        if (e.key !== 'Escape') return;
        if (imagePreviewOverlay.classList.contains('hidden')) return;
        closeImagePreview();
    });

    btnEmoji.addEventListener('click', () => {
        emojiPanel.classList.toggle('hidden');
        emojiPanel.setAttribute('aria-hidden', emojiPanel.classList.contains('hidden') ? 'true' : 'false');
    });

    btnImage.addEventListener('click', () => {
        if (!activeSession) {
            toast('请先进入聊天');
            return;
        }
        imgInput.click();
    });

    imgInput.addEventListener('change', async (e) => {
        const file = e.target.files && e.target.files[0];
        e.target.value = '';
        if (!file || !activeSession) return;
        try {
            const { d, mime } = await compressImageToPayload(file);
            const json = createRichPayload('img', { d, mime });
            await sendPayloadJson(json);
        } catch (err) {
            console.error(err);
            toast('图片发送失败，可换小图重试');
        }
    });

    btnVoice.addEventListener('pointerdown', (e) => {
        if (e.button !== 0) return;
        if (!activeSession) {
            toast('请先进入聊天');
            return;
        }
        try {
            btnVoice.setPointerCapture(e.pointerId);
        } catch (_) {}
        const g = ++voiceArm.generation;
        beginVoiceHold(e.clientY, g);
    });
    btnVoice.addEventListener('pointerup', () => {
        voiceArm.generation++;
        endVoiceHold();
    });
    btnVoice.addEventListener('pointercancel', () => {
        voiceState.cancel = true;
        voiceArm.generation++;
        endVoiceHold();
    });
    btnVoice.addEventListener('pointermove', (e) => {
        if (!voiceState.recorder) return;
        const el = document.getElementById('record-overlay');
        if (e.clientY < voiceState.startY - 64) {
            voiceState.cancel = true;
            el.classList.add('cancel-mode');
        } else {
            voiceState.cancel = false;
            el.classList.remove('cancel-mode');
        }
    });
}

function setupUi() {
    buildEmojiPanel();
    setupChatMedia();

    document.getElementById('btn-login').addEventListener('click', async () => {
        const nick = document.getElementById('login-nickname').value.trim().replace(/:/g, '');
        if (!nick) {
            toast('请输入昵称');
            return;
        }
        nickname = nick;
        try {
            await ensureMqtt();
        } catch {
            toast('连接失败，请确认 ' + MQTT_HOST + ' 已启动');
            return;
        }
        document.getElementById('main-nickname').textContent = nick;
        switchScreen('screen-main');
        renderPublicList();
        renderMyRooms();
        renderFriends();

        const inv = window.__pendingInvite;
        if (inv && inv.roomKey && (inv.secret || String(inv.roomKey).startsWith('public:'))) {
            delete window.__pendingInvite;
            await joinFromInvite(inv);
        }
    });

    document.querySelectorAll('.tab').forEach((tab) => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach((t) => t.classList.remove('active'));
            tab.classList.add('active');
            const id = tab.dataset.tab;
            document.querySelectorAll('.panel').forEach((p) => p.classList.remove('active'));
            document.getElementById(`panel-${id}`).classList.add('active');
        });
    });

    document.getElementById('btn-create-room').addEventListener('click', () => {
        document.getElementById('room-name').value = '';
        document.getElementById('room-id').value = randomRoomId();
        document.getElementById('room-secret').value = randomSecret();
        document.getElementById('modal-room').classList.remove('hidden');
    });

    document.getElementById('btn-room-cancel').addEventListener('click', () => {
        document.getElementById('modal-room').classList.add('hidden');
    });

    document.getElementById('btn-room-save').addEventListener('click', async () => {
        const displayName = document.getElementById('room-name').value.trim() || '未命名房间';
        let roomId = document.getElementById('room-id').value.trim().replace(/\s+/g, '');
        const secret = document.getElementById('room-secret').value.trim();
        if (!roomId || !secret) {
            toast('请填写房间 ID 和密钥');
            return;
        }
        const rooms = getMyRooms();
        if (rooms.some((r) => r.roomId === roomId)) {
            toast('房间 ID 已存在');
            return;
        }
        rooms.push({ displayName, roomId, secret });
        saveMyRooms(rooms);
        document.getElementById('modal-room').classList.add('hidden');
        renderMyRooms();
        await resubscribeAll();
        await openChatFromMyRoom({ displayName, roomId, secret });
        toast('房间已创建');
    });

    document.getElementById('btn-add-friend').addEventListener('click', () => {
        let fid = document.getElementById('friend-add-id').value.trim().replace(/:/g, '');
        if (!fid || fid === nickname) {
            toast('无效的好友昵称');
            return;
        }
        const list = getFriends();
        if (list.includes(fid)) {
            toast('已在好友列表');
            return;
        }
        list.push(fid);
        saveFriends(list);
        document.getElementById('friend-add-id').value = '';
        renderFriends();
        toast('已添加好友');
    });

    document.getElementById('btn-chat-back').addEventListener('click', () => {
        document.getElementById('emoji-panel').classList.add('hidden');
        switchScreen('screen-main');
        activeSession = null;
    });

    document.getElementById('btn-chat-invite').addEventListener('click', () => {
        if (!activeSession) return;
        showInviteModal();
    });

    document.getElementById('btn-invite-close').addEventListener('click', () => {
        document.getElementById('modal-invite').classList.add('hidden');
    });

    document.getElementById('btn-invite-copy').addEventListener('click', async () => {
        const t = document.getElementById('invite-url');
        try {
            await navigator.clipboard.writeText(t.value);
            toast('已复制');
        } catch {
            t.select();
            toast('请手动复制');
        }
    });

    document.getElementById('btn-send').addEventListener('click', () => sendCurrentMessage().catch(() => toast('发送失败')));
    document.getElementById('chat-input').addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter' && ev.ctrlKey) {
            ev.preventDefault();
            sendCurrentMessage().catch(() => toast('发送失败'));
        }
    });
}

async function joinFromInvite(inv) {
    const { roomKey, secret } = inv;
    if (!roomKey) return;
    if (roomKey.startsWith('public:')) {
        const slug = roomKey.slice('public:'.length);
        const pubSecret = 'public-' + slug;
        await openChatFromPublic(slug, PUBLIC_CHANNELS.find((c) => c.slug === slug)?.name || slug);
        toast(secret && secret !== pubSecret ? '已按公共频道规则使用固定密钥' : '已进入公共频道');
        return;
    }
    if (roomKey.startsWith('room:')) {
        const roomId = roomKey.slice('room:'.length);
        if (!secret) {
            toast('邀请链接缺少密钥');
            return;
        }
        const rooms = getMyRooms();
        const idx = rooms.findIndex((r) => r.roomId === roomId);
        if (idx === -1) {
            rooms.push({ displayName: '邀请加入', roomId, secret });
        } else if (secret) {
            rooms[idx] = { ...rooms[idx], secret };
        }
        saveMyRooms(rooms);
        renderMyRooms();
        const r = rooms.find((x) => x.roomId === roomId);
        await openChatFromMyRoom(r);
        toast('已通过邀请进入会话');
        return;
    }
    if (roomKey.startsWith('dm:')) {
        if (!secret) {
            toast('邀请链接缺少密钥');
            return;
        }
        const pair = roomKey.slice('dm:'.length);
        const parts = pair.split('::');
        const other = parts.find((p) => p !== nickname) || parts[0];
        const secrets = getDmSecrets();
        secrets[pair] = secret;
        saveDmSecrets(secrets);
        const fl = getFriends();
        if (other && !fl.includes(other)) {
            fl.push(other);
            saveFriends(fl);
            renderFriends();
        }
        await resubscribeAll();
        const topic = await topicFromKey('dm', pair);
        activeSession = {
            type: 'dm',
            topic,
            secret,
            title: other,
            sub: '私聊（来自邀请）',
            pair,
        };
        openChatScreen(true);
        await sendJoinLine();
        toast('已通过邀请进入会话');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    parseInitialHash();
    setupUi();
    applyPendingJoin();
});
