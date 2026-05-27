// AtomMQTT Broker Dashboard - Main Script

const API_BASE = '/api';
let refreshInterval = null;
let startTime = Date.now();

// ========== 认证管理 ==========

function getAuthHeaders() {
    const username = sessionStorage.getItem('web_username');
    const password = sessionStorage.getItem('web_password');
    if (username && password) {
        const encoded = btoa(username + ':' + password);
        return { 'Authorization': 'Basic ' + encoded };
    }
    return {};
}

function redirectToLogin() {
    sessionStorage.removeItem('web_username');
    sessionStorage.removeItem('web_password');
    window.location.href = '/login.html';
}

function checkAuth() {
    const username = sessionStorage.getItem('web_username');
    const password = sessionStorage.getItem('web_password');
    if (!username || !password) {
        redirectToLogin();
        return false;
    }
    return true;
}

/**
 * 带认证的 fetch 封装。
 * 自动附加 Authorization 头，遇到 401 跳转到登录页。
 */
async function apiFetch(url, options = {}) {
    if (!checkAuth()) {
        // 如果没凭证，checkAuth 已经重定向了
        throw new Error('未登录');
    }

    const headers = {
        ...getAuthHeaders(),
        ...options.headers,
    };

    const resp = await fetch(url, { ...options, headers });

    if (resp.status === 401) {
        redirectToLogin();
        throw new Error('身份验证失败，请重新登录');
    }

    return resp;
}

// Navigation
document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', (e) => {
        e.preventDefault();
        const page = item.dataset.page;
        navigateTo(page);
    });
});

function navigateTo(page) {
    // Update nav
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelector(`.nav-item[data-page="${page}"]`).classList.add('active');
    
    // Show page
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById(`page-${page}`).classList.add('active');
    
    // Refresh data based on page
    switch(page) {
        case 'dashboard': refreshDashboard(); break;
        case 'clients': refreshClients(); break;
        case 'subscriptions': refreshSubscriptions(); break;
        case 'retained': refreshRetained(); break;
        case 'info': refreshInfo(); break;
    }
}

// Form submission
document.getElementById('publishForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const topic = document.getElementById('pubTopic').value;
    const payload = document.getElementById('pubPayload').value;
    const qos = parseInt(document.getElementById('pubQos').value);
    const retain = document.getElementById('pubRetain').checked;

    const resultBox = document.getElementById('publishResult');
    resultBox.classList.remove('hidden', 'success', 'error');

    try {
        const resp = await apiFetch(`${API_BASE}/publish`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ topic, payload, qos, retain })
        });
        const data = await resp.json();
        resultBox.className = `result-box ${data.success ? 'success' : 'error'}`;
        resultBox.textContent = data.success 
            ? `✅ 已发布到 "${topic}"，${data.subscriber_count} 个订阅者`
            : `❌ 发布失败: ${JSON.stringify(data)}`;
    } catch (err) {
        // 401 等场景不显示错误（已跳转登录页）
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        resultBox.className = 'result-box error';
        resultBox.textContent = `❌ 请求失败: ${err.message}`;
    }
});

// Dashboard
async function refreshDashboard() {
    try {
        const resp = await apiFetch(`${API_BASE}/metrics`);
        const metrics = await resp.json();
        
        document.getElementById('clientsConnected').textContent = metrics.clients_connected ?? 0;
        document.getElementById('subscriptionsActive').textContent = metrics.subscriptions_active ?? 0;
        document.getElementById('messagesPublished').textContent = metrics.messages_published ?? 0;
        document.getElementById('messagesReceived').textContent = metrics.messages_received ?? 0;
        document.getElementById('packetsSent').textContent = metrics.packets_sent ?? 0;
        document.getElementById('packetsReceived').textContent = metrics.packets_received ?? 0;
        document.getElementById('bytesSent').textContent = formatBytes(metrics.bytes_sent ?? 0);
        document.getElementById('bytesReceived').textContent = formatBytes(metrics.bytes_received ?? 0);
        
        const uptime = Math.floor((Date.now() - startTime) / 1000);
        document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        console.error('Dashboard refresh failed:', err);
    }
}

// Clients
async function refreshClients() {
    const tbody = document.getElementById('clientsTableBody');
    try {
        const resp = await apiFetch(`${API_BASE}/clients`);
        const clients = await resp.json();
        
        if (clients.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-state">暂无在线客户端</td></tr>';
            return;
        }
        
        tbody.innerHTML = clients.map(client => `
            <tr>
                <td><strong>${escapeHtml(client.client_id)}</strong></td>
                <td>${client.protocol_version}</td>
                <td><span class="status-dot" style="display:inline-block;vertical-align:middle;margin-right:6px;background:${client.connected ? 'var(--success)' : 'var(--danger)'}"></span>${client.connected ? '在线' : '离线'}</td>
                <td>${client.keep_alive}s</td>
                <td>${escapeHtml(client.username) || '-'}</td>
                <td>
                    <button class="btn btn-sm btn-danger" onclick="disconnectClient('${escapeHtml(client.client_id)}')">断开</button>
                </td>
            </tr>
        `).join('');
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        tbody.innerHTML = `<tr><td colspan="6" class="empty-state">加载失败: ${err.message}</td></tr>`;
    }
}

async function disconnectClient(clientId) {
    if (!confirm(`确定要断开客户端 "${clientId}" 吗？`)) return;
    try {
        const resp = await apiFetch(`${API_BASE}/clients/${encodeURIComponent(clientId)}/disconnect`, { method: 'POST' });
        const data = await resp.json();
        if (data.success) refreshClients();
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        alert('操作失败: ' + err.message);
    }
}

// Subscriptions
async function refreshSubscriptions() {
    const tbody = document.getElementById('subscriptionsTableBody');
    try {
        const resp = await apiFetch(`${API_BASE}/subscriptions`);
        const subs = await resp.json();
        
        document.getElementById('subCount').textContent = subs.length;
        
        if (subs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" class="empty-state">暂无订阅</td></tr>';
            return;
        }
        
        tbody.innerHTML = subs.map(sub => `
            <tr>
                <td>${escapeHtml(sub.client_id)}</td>
                <td><code>${escapeHtml(sub.filter)}</code></td>
                <td>${sub.qos}</td>
            </tr>
        `).join('');
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        tbody.innerHTML = `<tr><td colspan="3" class="empty-state">加载失败: ${err.message}</td></tr>`;
    }
}

// Retained Messages
async function refreshRetained() {
    const tbody = document.getElementById('retainedTableBody');
    try {
        const resp = await apiFetch(`${API_BASE}/retained`);
        const messages = await resp.json();
        
        if (messages.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-state">暂无保留消息</td></tr>';
            return;
        }
        
        tbody.innerHTML = messages.map(msg => `
            <tr>
                <td><code>${escapeHtml(msg.topic)}</code></td>
                <td>${msg.qos}</td>
                <td>${formatBytes(msg.payload_size)}</td>
                <td class="payload-preview">${escapeHtml(msg.payload_preview)}</td>
                <td><button class="btn btn-sm btn-danger" onclick="deleteRetained('${escapeHtml(msg.topic)}')">删除</button></td>
            </tr>
        `).join('');
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        tbody.innerHTML = `<tr><td colspan="5" class="empty-state">加载失败: ${err.message}</td></tr>`;
    }
}

async function deleteRetained(topic) {
    if (!confirm('确定要删除保留消息 [' + topic + '] 吗？')) return;
    try {
        const resp = await apiFetch(`${API_BASE}/retained/${encodeURIComponent(topic)}`, { method: 'DELETE' });
        const data = await resp.json();
        if (data.success) {
            showToast('保留消息已删除', 'success');
            refreshRetained();
        } else {
            showToast('删除失败: ' + JSON.stringify(data), 'error');
        }
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        showToast('删除失败: ' + err.message, 'error');
    }
}

// Server Info
async function refreshInfo() {
    try {
        const resp = await apiFetch(`${API_BASE}/broker/info`);
        const info = await resp.json();
        
        document.getElementById('infoName').textContent = info.name;
        document.getElementById('infoVersion').textContent = `v${info.version}`;
        
        const uptime = info.uptime_seconds || Math.floor((Date.now() - startTime) / 1000);
        document.getElementById('infoUptime').textContent = formatDuration(uptime);
        document.getElementById('infoTcpAddr').textContent = `${info.config.tcp_host}:${info.config.tcp_port}`;
        document.getElementById('infoWebAddr').textContent = `${info.config.web_host}:${info.config.web_port}`;
        document.getElementById('infoProtocols').textContent = info.protocol_versions.join(', ');
        document.getElementById('infoAnonymous').textContent = info.config.allow_anonymous ? '✅ 允许' : '❌ 禁止';
        document.getElementById('infoMaxPkt').textContent = formatBytes(info.config.max_packet_size);
    } catch (err) {
        if (err.message === '未登录' || err.message === '身份验证失败，请重新登录') return;
        console.error('Info refresh failed:', err);
    }
}

// Helpers
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDuration(seconds) {
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    const parts = [];
    if (d > 0) parts.push(`${d}天`);
    if (h > 0) parts.push(`${h}时`);
    if (m > 0) parts.push(`${m}分`);
    parts.push(`${s}秒`);
    return parts.join(' ');
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// Auto-refresh
function startAutoRefresh() {
    if (refreshInterval) clearInterval(refreshInterval);
    refreshInterval = setInterval(() => {
        const activePage = document.querySelector('.page.active');
        if (activePage) {
            const pageId = activePage.id.replace('page-', '');
            switch(pageId) {
                case 'dashboard': refreshDashboard(); break;
                case 'clients': refreshClients(); break;
                case 'subscriptions': refreshSubscriptions(); break;
                case 'retained': refreshRetained(); break;
            }
        }
    }, 3000);
}

// ========== WebSocket 订阅管理 ==========

let ws = null;
let wsReconnectTimer = null;
let subscribeMessageCount = 0;

// WebSocket 连接
function wsConnect() {
    if (ws && ws.readyState === WebSocket.OPEN) return;
    
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${location.host}/ws/subscribe`;
    
    updateWsStatus('connecting', '正在连接...');
    
    ws = new WebSocket(wsUrl);
    
    ws.onopen = () => {
        updateWsStatus('connected', '已连接');
        if (wsReconnectTimer) {
            clearTimeout(wsReconnectTimer);
            wsReconnectTimer = null;
        }
    };
    
    ws.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            handleWsMessage(msg);
        } catch (err) {
            console.error('WS 消息解析失败:', err);
        }
    };
    
    ws.onclose = () => {
        updateWsStatus('disconnected', '已断开');
        scheduleReconnect();
    };
    
    ws.onerror = () => {
        updateWsStatus('error', '连接错误');
        ws.close();
    };
}

function wsDisconnect() {
    if (wsReconnectTimer) {
        clearTimeout(wsReconnectTimer);
        wsReconnectTimer = null;
    }
    if (ws) {
        ws.onclose = null; // 阻止触发重连
        ws.close();
        ws = null;
    }
    updateWsStatus('disconnected', '已断开');
}

function scheduleReconnect() {
    if (wsReconnectTimer) return;
    wsReconnectTimer = setTimeout(() => {
        wsReconnectTimer = null;
        if (!ws || ws.readyState === WebSocket.CLOSED) {
            wsConnect();
        }
    }, 3000);
}

function updateWsStatus(status, text) {
    const badge = document.getElementById('wsStatus');
    if (!badge) return;
    const colors = {
        'connected': 'var(--success)',
        'connecting': 'var(--warning)',
        'disconnected': '#888',
        'error': 'var(--danger)'
    };
    badge.style.background = colors[status] || '#888';
    badge.textContent = text;
}

// 发送 JSON 命令到 WebSocket
function wsSend(data) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        alert('WebSocket 未连接');
        return false;
    }
    ws.send(JSON.stringify(data));
    return true;
}

function subscribe() {
    const topicFilter = document.getElementById('subTopicFilter').value.trim();
    const qos = parseInt(document.getElementById('subQos').value);
    
    if (!topicFilter) {
        alert('请输入主题过滤器');
        return;
    }
    
    if (wsSend({ type: 'subscribe', topic_filter: topicFilter, qos })) {
        // Keep the topic filter so user can continue subscribing
        document.getElementById('subTopicFilter').focus();
    }
}

function unsubscribeAll() {
    if (!confirm('确定要取消所有订阅吗？')) return;
    wsSend({ type: 'unsubscribe', topic_filter: '*' });
}

function clearSubscribeMessages() {
    const tbody = document.getElementById('subMessagesBody');
    if (tbody) {
        tbody.innerHTML = '<tr><td colspan="4" class="empty-state">已清空</td></tr>';
    }
    subscribeMessageCount = 0;
    const countEl = document.getElementById('msgCount');
    if (countEl) countEl.textContent = '0';
}

// 处理接收到的 WebSocket 消息
function handleWsMessage(msg) {
    switch (msg.type) {
        case 'subscribed':
            showToast(`✅ 已订阅: ${escapeHtml(msg.topic_filter)} (QoS ${msg.qos})`, 'success');
            break;
        case 'unsubscribed':
            showToast(`已取消订阅: ${escapeHtml(msg.topic_filter)}`, 'info');
            break;
        case 'publish':
            addMessageRow(msg);
            break;
        case 'pong':
            break;
        case 'error':
            showToast(`❌ ${escapeHtml(msg.message)}`, 'error');
            break;
        default:
            showToast(`未知消息: ${msg.type}`, 'info');
    }
}

function addMessageRow(msg) {
    const tbody = document.getElementById('subMessagesBody');
    if (!tbody) return;
    
    // 移除空状态提示
    if (tbody.querySelector('.empty-state')) {
        tbody.innerHTML = '';
    }
    
    subscribeMessageCount++;
    const countEl = document.getElementById('msgCount');
    if (countEl) countEl.textContent = subscribeMessageCount;
    
    const time = new Date().toLocaleTimeString();
    const payload = escapeHtml(msg.payload || '');
    const topic = escapeHtml(msg.topic || '');
    const qos = msg.qos !== undefined ? msg.qos : '-';
    
    const row = document.createElement('tr');
    row.innerHTML = `<td class="msg-time">${time}</td><td><code>${topic}</code></td><td class="msg-payload">${payload}</td><td>${qos}</td>`;
    row.classList.add('msg-new');
    
    tbody.appendChild(row);
    
    // 限制显示最近 200 条消息
    while (tbody.children.length > 200) {
        tbody.removeChild(tbody.firstChild);
    }
    
    // 滚动到底部
    tbody.scrollTop = tbody.scrollHeight;
}

// 简易提示
function showToast(text, type) {
    const container = document.getElementById('toastContainer') || (() => {
        const c = document.createElement('div');
        c.id = 'toastContainer';
        c.style.cssText = 'position:fixed;bottom:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px';
        document.body.appendChild(c);
        return c;
    })();
    const toast = document.createElement('div');
    toast.style.cssText = 'padding:8px 16px;border-radius:6px;font-size:13px;animation:fadeIn 0.3s;max-width:400px;word-break:break-all';
    toast.style.background = type === 'success' ? '#e6ffe6' : type === 'error' ? '#ffe6e6' : '#f0f0f0';
    toast.style.color = type === 'success' ? '#006600' : type === 'error' ? '#cc0000' : '#333';
    toast.style.border = '1px solid ' + (type === 'success' ? '#99cc99' : type === 'error' ? '#cc9999' : '#ccc');
    toast.textContent = text;
    container.appendChild(toast);
    setTimeout(() => { toast.style.opacity = '0'; toast.style.transition = 'opacity 0.5s'; setTimeout(() => toast.remove(), 500); }, 3000);
}

// 订阅页面初始化
function initSubscribePage() {
    document.getElementById('subscribeForm').addEventListener('submit', (e) => {
        e.preventDefault();
        subscribe();
    });
    
    const unsubBtn = document.getElementById('unsubAllBtn');
    if (unsubBtn) unsubBtn.addEventListener('click', unsubscribeAll);
    
    const clearBtn = document.getElementById('clearMsgsBtn');
    if (clearBtn) clearBtn.addEventListener('click', clearSubscribeMessages);
    
    wsConnect();
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', () => {
    // 检查是否有认证凭据，没有则跳转到登录页
    if (!checkAuth()) return;

    // 立即拉取仪表盘数据（HTML 中的 0 会被覆盖）
    refreshDashboard();
    
    // 启动自动刷新定时器（每 3 秒刷新当前页面数据）
    startAutoRefresh();
    
    // 初始化 WebSocket 订阅页面
    if (document.getElementById('subscribeForm')) {
        initSubscribePage();
    }
});
