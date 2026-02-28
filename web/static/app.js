/* ══════════════════════════════════════════════════════════════
   app.js — 良富居地產 v5.0 前端應用
   ══════════════════════════════════════════════════════════════ */

// ── 全域狀態 ──
let map, markerClusterGroup, markerGroup;
let txData = [];
let activeCardIdx = -1;
let currentSort = 'date';
let lastSearchType = '';
let unit = 'ping';
let locationMarker, locationCircle;
let collapsedCommunities = {};
let communitySummaries = {};
let markerSettings = {
  outerMode: 'total_price', innerMode: 'unit_price',
  contentMode: 'recent2yr',
  unitThresholds: [20, 40, 70], totalThresholds: [500, 1500, 3000],
  osmZoom: 16, showLotAddr: false
};

// ── 工具函式 ──
const escHtml = s => s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '';
const escAttr = s => s ? String(s).replace(/'/g, '&#39;').replace(/"/g, '&quot;').replace(/</g, '&lt;') : '';
const cssId = s => s ? s.replace(/[^a-zA-Z0-9\u4e00-\u9fff]/g, '_') : '';
function fmtWan(v) { if (!v || v <= 0) return '-'; const w = v / 10000; return w >= 10000 ? (w / 10000).toFixed(1) + '億' : Math.round(w) + '萬'; }
function fmtArea(sqm, ping) { return unit === 'ping' ? (ping > 0 ? ping.toFixed(1) + '坪' : '-') : (sqm > 0 ? sqm.toFixed(1) + 'm²' : '-'); }
function fmtUnitPrice(ping, sqm) { return unit === 'ping' ? (ping > 0 ? Math.round(ping / 10000) + '萬/坪' : '-') : (sqm > 0 ? Math.round(sqm / 10000) + '萬/m²' : '-'); }
function isLotAddress(addr) { return /^\S*段\S*地號/.test(addr) || /段\d+地號/.test(addr); }
function getLocationMode() { const z = map ? map.getZoom() : 15; return z >= (markerSettings.osmZoom || 16) ? 'osm' : 'db'; }

// ── 深色模式 ──
function toggleTheme() {
  const html = document.documentElement;
  const isDark = html.getAttribute('data-theme') === 'dark';
  html.setAttribute('data-theme', isDark ? 'light' : 'dark');
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = isDark ? '🌙' : '☀️';
  try { localStorage.setItem('theme', isDark ? 'light' : 'dark'); } catch (e) { }
}
function loadTheme() {
  try {
    const t = localStorage.getItem('theme');
    if (t === 'dark') { document.documentElement.setAttribute('data-theme', 'dark'); const b = document.getElementById('themeToggle'); if (b) b.textContent = '☀️'; }
  } catch (e) { }
}

// ── 坪/m² 切換 ──
function setUnit(u) {
  unit = u;
  document.querySelectorAll('.unit-toggle button').forEach(b => { b.classList.toggle('active', b.dataset.unit === u); });
  if (txData.length > 0) { renderResults(); renderSummary(); }
}

// ── 地圖初始化 ──
function initMap() {
  map = L.map('map', { center: [25.033, 121.565], zoom: 13, zoomControl: false });
  L.control.zoom({ position: 'bottomright' }).addTo(map);

  // 圖層切換
  const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '© OpenStreetMap' });
  const carto = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', { maxZoom: 19, attribution: '© CartoDB' });
  const cartoDark = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { maxZoom: 19, attribution: '© CartoDB' });
  carto.addTo(map);
  L.control.layers({ '淺色地圖': carto, '標準地圖': osm, '深色地圖': cartoDark }, {}, { position: 'topright' }).addTo(map);

  markerGroup = L.layerGroup().addTo(map);
  markerClusterGroup = L.markerClusterGroup({
    maxClusterRadius: 45, spiderfyOnMaxZoom: true, showCoverageOnHover: false,
    zoomToBoundsOnClick: false,
    spiderfyDistanceMultiplier: 2.5,
    iconCreateFunction: c => {
      const markers = c.getAllChildMarkers();
      let totalCount = 0, totalPrice = 0, totalUnit = 0, validP = 0, validU = 0;
      markers.forEach(m => {
        const gc = m._groupCount || 1; totalCount += gc;
        if (m._avgPrice && m._avgPrice > 0) { totalPrice += m._avgPrice * gc; validP += gc; }
        if (m._avgUnitPrice && m._avgUnitPrice > 0) { totalUnit += m._avgUnitPrice * gc; validU += gc; }
      });
      const labels = markers.map(m => m._groupLabel).filter(Boolean);
      const uniqueLabels = [...new Set(labels)];
      const sameComm = uniqueLabels.length === 1;
      const commLabel = sameComm ? uniqueLabels[0].substring(0, 6) : '';
      let sz = 44;
      if (totalCount >= 100) sz = 60; else if (totalCount >= 30) sz = 54; else if (totalCount >= 10) sz = 48;
      const avgPriceWan = validP > 0 ? (totalPrice / validP / 10000) : 0;
      const avgUnitWan = validU > 0 ? (totalUnit / validU / 10000) : 0;
      // 中位數計算
      const allUnitPrices = []; markers.forEach(m => { if (m._groupItems) m._groupItems.forEach(({ tx }) => { if (tx.unit_price_ping > 0) allUnitPrices.push(tx.unit_price_ping); }); });
      allUnitPrices.sort((a, b) => a - b);
      const medianUnit = allUnitPrices.length > 0 ? allUnitPrices[Math.floor(allUnitPrices.length / 2)] / 10000 : 0;
      const outerColor = getColorForMode(markerSettings.outerMode, avgPriceWan, avgUnitWan);
      const innerColor = getColorForMode(markerSettings.innerMode, avgPriceWan, avgUnitWan);
      let priceText = '';
      if (avgPriceWan >= 10000) priceText = (avgPriceWan / 10000).toFixed(1) + '億';
      else if (avgPriceWan >= 1) priceText = avgPriceWan.toFixed(0) + '萬';
      const line1 = priceText || totalCount + '筆';
      const line2 = priceText ? totalCount + '筆' : '';
      const svgHtml = makeMarkerSVG({ sz, outerColor, innerColor, line1, line2 });
      const labelHtml = commLabel ? `<div style="margin-top:-2px;padding:1px 4px;background:rgba(255,255,255,.92);border-radius:6px;font-size:8px;font-weight:600;color:#333;white-space:nowrap;max-width:70px;overflow:hidden;text-overflow:ellipsis;box-shadow:0 1px 3px rgba(0,0,0,.15);border:1px solid rgba(0,0,0,.08)">${commLabel}</div>` : '';
      const totalH = commLabel ? sz + 14 : sz;
      return L.divIcon({
        html: `<div style="display:flex;flex-direction:column;align-items:center">${svgHtml}${labelHtml}</div>`,
        className: 'price-marker', iconSize: [sz + 8, totalH], iconAnchor: [(sz + 8) / 2, totalH / 2]
      });
    }
  });
  map.addLayer(markerClusterGroup);
  // 叢集點擊：同建案展開列表、不同建案 spiderfy
  markerClusterGroup.on('clusterclick', function (e) {
    const mkrs = e.layer.getAllChildMarkers();
    const labels = mkrs.map(m => m._groupLabel).filter(Boolean);
    const unique = [...new Set(labels)];
    if (unique.length <= 1) {
      // 同建案 → 展開交易列表，不放大
      const items = []; mkrs.forEach(m => { if (m._groupItems) items.push(...m._groupItems); });
      if (items.length > 0) showClusterList(items);
    } else {
      // 不同建案 → spiderfy 展開圈圈
      e.layer.spiderfy();
    }
  });
  // spiderfy 時加模糊背景
  markerClusterGroup.on('spiderfied', () => { document.getElementById('mapBlur').classList.add('active'); });
  markerClusterGroup.on('unspiderfied', () => { document.getElementById('mapBlur').classList.remove('active'); });
  map.on('click', () => { document.getElementById('mapBlur').classList.remove('active'); });
  addLegend();
}

// ── 篩選面板 ──
function toggleFilters() {
  const p = document.getElementById('filterPanel');
  const a = document.getElementById('filterArrow');
  const show = !p.classList.contains('show');
  p.classList.toggle('show', show);
  a.textContent = show ? '▼' : '▶';
}
function clearFilters() {
  ['fBuildType', 'fRooms', 'fPing', 'fRatio', 'fUnitPrice', 'fPrice', 'fYear'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
  const cb = document.getElementById('fExcludeSpecial'); if (cb) cb.checked = false;
  document.querySelectorAll('.quick-filters button').forEach(b => b.classList.remove('active'));
}
function quickFilter(mode) {
  const nowYear = new Date().getFullYear() - 1911;
  if (mode === '1yr') { document.getElementById('fYear').value = `${nowYear - 1}-${nowYear}`; document.getElementById('qf1yr').classList.add('active'); document.getElementById('qf2yr').classList.remove('active'); }
  else if (mode === '2yr') { document.getElementById('fYear').value = `${nowYear - 2}-${nowYear}`; document.getElementById('qf2yr').classList.add('active'); document.getElementById('qf1yr').classList.remove('active'); }
  else if (mode === 'nospecial') { const cb = document.getElementById('fExcludeSpecial'); cb.checked = !cb.checked; document.getElementById('qfNoSpec').classList.toggle('active', cb.checked); }
  else if (mode === 'clear') { clearFilters(); if (txData.length > 0) rerunSearch(); return; }
  if (txData.length > 0) rerunSearch();
}
function getFilterParams() {
  let p = '';
  const fields = [['fBuildType', 'building_type'], ['fRooms', 'rooms'], ['fPing', 'ping'], ['fRatio', 'public_ratio'], ['fUnitPrice', 'unit_price'], ['fPrice', 'price'], ['fYear', 'year']];
  fields.forEach(([id, param]) => { const v = document.getElementById(id).value.trim(); if (v) p += '&' + param + '=' + encodeURIComponent(v); });
  const exSp = document.getElementById('fExcludeSpecial'); if (exSp && exSp.checked) p += '&exclude_special=1';
  return p;
}
function getHeaderFilterParams() {
  let p = '';
  const hd = document.getElementById('hfDistrict'); if (hd && hd.value) p += '&district=' + encodeURIComponent(hd.value);
  return p;
}

// ── 排序 ──
function sortData(sortType) {
  const sorters = {
    date: (a, b) => (b.date_raw || '').localeCompare(a.date_raw || ''),
    price: (a, b) => (b.price || 0) - (a.price || 0),
    unit_price: (a, b) => (b.unit_price_ping || 0) - (a.unit_price_ping || 0),
    ping: (a, b) => (b.area_ping || 0) - (a.area_ping || 0),
    public_ratio: (a, b) => (a.public_ratio || 999) - (b.public_ratio || 999),
    community: (a, b) => { const ca = a.community_name || '', cb2 = b.community_name || ''; if (ca && !cb2) return -1; if (!ca && cb2) return 1; if (ca !== cb2) return ca.localeCompare(cb2); return (b.date_raw || '').localeCompare(a.date_raw || ''); }
  };
  if (sorters[sortType]) txData.sort(sorters[sortType]);
}
function setSort(btn) {
  document.querySelectorAll('.sort-bar button[data-sort]').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentSort = btn.dataset.sort;
  if (txData.length > 0) { sortData(currentSort); renderResults(); plotMarkers(false); }
}
function rerunSearch() { if (lastSearchType === 'area') doAreaSearch(); else if (lastSearchType === 'keyword') doSearch(); }

// ── 建案自動建議 ──
let _acTimer = null, _acIdx = -1, _acResults = [], _selectedCommunity = null;
function handleSearchKeydown(e) {
  const list = document.getElementById('acList');
  if (list.classList.contains('show') && _acResults.length > 0) {
    if (e.key === 'ArrowDown') { e.preventDefault(); _acIdx = Math.min(_acIdx + 1, _acResults.length - 1); renderAcList(); return; }
    if (e.key === 'ArrowUp') { e.preventDefault(); _acIdx = Math.max(_acIdx - 1, -1); renderAcList(); return; }
    if (e.key === 'Enter' && _acIdx >= 0) { e.preventDefault(); selectCommunity(_acResults[_acIdx].name); return; }
  }
  if (e.key === 'Escape') { hideAcList(); return; }
  if (e.key === 'Enter') doSearch();
}
function onSearchInput() {
  if (_selectedCommunity) clearSelectedCommunity();
  const kw = document.getElementById('searchInput').value.trim();
  if (kw.length < 2) { hideAcList(); return; }
  clearTimeout(_acTimer);
  _acTimer = setTimeout(() => fetchAcResults(kw), 250);
}
async function fetchAcResults(kw) {
  try {
    const resp = await fetch('/api/com_match?keyword=' + encodeURIComponent(kw) + '&top_n=8');
    const data = await resp.json();
    if (data.success && data.results && data.results.length > 0) { _acResults = data.results; _acIdx = -1; positionAcList(); renderAcList(); document.getElementById('acList').classList.add('show'); }
    else hideAcList();
  } catch (e) { hideAcList(); }
}
function renderAcList() {
  const list = document.getElementById('acList');
  list.innerHTML = _acResults.map((r, i) => {
    const tagClass = r.match_type === '精確' ? 'exact' : (r.match_type === '包含' ? 'contains' : 'fuzzy');
    const priceWan = r.avg_price ? Math.round(r.avg_price / 10000) : 0;
    return `<div class="autocomplete-item${i === _acIdx ? ' selected' : ''}" onclick="selectCommunity('${escAttr(r.name)}')">
      <span class="ac-name">${escHtml(r.name)}<span class="ac-tag ${tagClass}">${r.match_type}</span></span>
      <span class="ac-meta">${r.tx_count}筆${priceWan > 0 ? ' · 均' + priceWan + '萬' : ''} · ${escHtml(r.district || '')}</span>
    </div>`;
  }).join('');
}
function selectCommunity(name) { _selectedCommunity = name; document.getElementById('searchInput').value = name; document.getElementById('selComName').textContent = name; document.getElementById('selectedCommunity').style.display = ''; hideAcList(); doSearch(); }
function clearSelectedCommunity() { _selectedCommunity = null; document.getElementById('selectedCommunity').style.display = 'none'; }
function hideAcList() { document.getElementById('acList').classList.remove('show'); _acResults = []; _acIdx = -1; }
function positionAcList() {
  const input = document.getElementById('searchInput'), list = document.getElementById('acList'), rect = input.getBoundingClientRect();
  list.style.left = rect.left + 'px'; list.style.top = (rect.bottom + 2) + 'px'; list.style.width = (rect.right - rect.left + 60) + 'px';
}
function hoverTx(idx) {
  let targetMarker = null;
  markerClusterGroup.eachLayer(layer => { if (!targetMarker && layer._groupItems && layer._groupItems.some(it => it.origIdx === idx)) targetMarker = layer; });
  if (!targetMarker) return;
  // 只在 marker 不在可視範圍時才移動地圖
  const ll = targetMarker.getLatLng();
  if (!map.getBounds().contains(ll)) {
    map.panTo(ll, { animate: true, duration: 0.25 });
  }
  // 無論如何都要持續跳動動畫
  const tryBounce = () => {
    const iconEl = targetMarker._icon;
    if (!iconEl) return;
    const inner = iconEl.firstElementChild || iconEl;
    inner.classList.remove('marker-bounce'); void inner.offsetWidth;
    inner.classList.add('marker-bounce');
  };
  // marker 可能在叢集中，先展開再動畫
  if (targetMarker._icon) tryBounce();
  else markerClusterGroup.zoomToShowLayer(targetMarker, () => setTimeout(tryBounce, 100));
}
function hoverCommunity(name) {
  // 找到所有屬於該建案的 markers 並跳動
  const matchedMarkers = [];
  markerClusterGroup.eachLayer(layer => {
    if (layer._groupLabel === name || (layer._groupItems && layer._groupItems.some(it => it.tx.community_name === name))) {
      matchedMarkers.push(layer);
    }
  });
  if (matchedMarkers.length === 0) return;
  // 檢查是否有任何 marker 在可視範圍
  const bounds = map.getBounds();
  const anyVisible = matchedMarkers.some(m => m._icon && bounds.contains(m.getLatLng()));
  if (!anyVisible) {
    // 都不可見，pan 到第一個
    map.panTo(matchedMarkers[0].getLatLng(), { animate: true, duration: 0.3 });
  }
  // 對所有可見的 marker 跳動
  setTimeout(() => {
    matchedMarkers.forEach(m => {
      if (!m._icon) return;
      const inner = m._icon.firstElementChild || m._icon;
      inner.classList.remove('marker-bounce'); void inner.offsetWidth;
      inner.classList.add('marker-bounce');
    });
  }, anyVisible ? 0 : 350);
}
document.addEventListener('click', e => { if (!e.target.closest('.autocomplete-wrap')) hideAcList(); });

// ── 搜尋 ──
async function doSearch() {
  const kw = document.getElementById('searchInput').value.trim();
  if (!kw) { alert('請輸入搜尋關鍵字'); return; }
  hideAcList(); lastSearchType = 'keyword';
  const results = document.getElementById('results');
  results.innerHTML = '<div class="loading"><div class="skeleton" style="height:60px;margin:16px"></div><div class="skeleton" style="height:60px;margin:16px"></div><div class="skeleton" style="height:60px;margin:16px"></div></div>';
  const limitVal = document.getElementById('limitSelect').value;
  let url = '/api/search?keyword=' + encodeURIComponent(kw) + '&limit=' + limitVal + '&location_mode=' + getLocationMode() + getFilterParams();
  if (_selectedCommunity) url += '&community=' + encodeURIComponent(_selectedCommunity);
  try {
    const resp = await fetch(url);
    const ctype = resp.headers.get('content-type') || '';
    if (!ctype.includes('application/json')) { results.innerHTML = `<div class="empty">❌ 伺服器回應異常 (HTTP ${resp.status})</div>`; return; }
    const data = await resp.json();
    if (!data.success) { results.innerHTML = '<div class="empty">❌ ' + (data.error || '搜尋失敗') + '</div>'; return; }
    handleSearchResult(data);
  } catch (e) { results.innerHTML = '<div class="empty">❌ 網路錯誤: ' + e.message + '</div>'; }
}

async function doAreaSearch() {
  const bounds = map.getBounds(); lastSearchType = 'area';
  const results = document.getElementById('results');
  results.innerHTML = '<div class="loading"><div class="skeleton" style="height:60px;margin:16px"></div><div class="skeleton" style="height:60px;margin:16px"></div></div>';
  const limitVal = document.getElementById('limitSelect').value;
  const url = `/api/search_area?south=${bounds.getSouth()}&north=${bounds.getNorth()}&west=${bounds.getWest()}&east=${bounds.getEast()}&limit=${limitVal}&location_mode=${getLocationMode()}` + getFilterParams() + getHeaderFilterParams();
  try {
    const resp = await fetch(url);
    const ctype = resp.headers.get('content-type') || '';
    if (!ctype.includes('application/json')) { const text = await resp.text(); results.innerHTML = `<div class="empty">❌ 搜此區域失敗 (HTTP ${resp.status})</div>`; return; }
    const data = await resp.json();
    if (!data.success) { results.innerHTML = '<div class="empty">❌ ' + (data.error || '搜尋失敗') + '</div>'; return; }
    if (!data.transactions || data.transactions.length === 0) { results.innerHTML = '<div class="empty">😢 此區域沒有成交紀錄<br><span style="font-size:12px">試試放大地圖或移動到其他區域</span></div>'; document.getElementById('summaryBar').style.display = 'none'; markerGroup.clearLayers(); return; }
    handleSearchResult(data, false);
  } catch (e) { results.innerHTML = '<div class="empty">❌ 搜此區域失敗: ' + e.message + '</div>'; }
}

function handleSearchResult(data, fitBounds = true) {
  txData = data.transactions || [];
  if (!markerSettings.showLotAddr) txData = txData.filter(tx => !isLotAddress(tx.address_raw || tx.address || ''));
  if (txData.length === 0) { document.getElementById('results').innerHTML = '<div class="empty">😢 沒有找到符合條件的資料</div>'; document.getElementById('summaryBar').style.display = 'none'; markerGroup.clearLayers(); return; }
  window._communityName = data.community_name || null;
  window._searchType = data.search_type || 'address';
  window._summary = data.summary || {};
  communitySummaries = data.community_summaries || {};
  if (data.search_type === 'area') { collapsedCommunities = {}; const cNames = [...new Set(txData.map(tx => tx.community_name).filter(Boolean))]; cNames.forEach(cn => { collapsedCommunities[cn] = true; }); document.getElementById('headerFilters').classList.add('show'); }
  else { collapsedCommunities = {}; document.getElementById('headerFilters').classList.remove('show'); }
  sortData(currentSort); renderResults(); renderSummary(); plotMarkers(fitBounds);
  if (window.innerWidth <= 768) document.getElementById('sidebar').classList.remove('open');
}

// ── 渲染結果列表 ──
function renderResults() {
  const container = document.getElementById('results');
  const groups = [], communityMap = {}, noComItems = [];
  txData.forEach((tx, i) => {
    if (!markerSettings.showLotAddr && isLotAddress(tx.address_raw || tx.address || '')) return;
    const cn = tx.community_name || '';
    if (cn) { if (!(cn in communityMap)) { communityMap[cn] = groups.length; groups.push({ name: cn, items: [] }); } groups[communityMap[cn]].items.push({ tx, origIdx: i }); }
    else noComItems.push({ tx, origIdx: i });
  });
  let html = '';
  if (window._communityName && groups.length <= 1 && window._searchType !== 'area') {
    html += `<div style="padding:12px 16px;background:var(--green-bg);border-bottom:1px solid var(--border)">
      <span style="font-weight:800;font-size:15px;color:var(--green)">🏘️ ${escHtml(window._communityName)}</span></div>`;
  }
  groups.forEach(group => {
    const cn = group.name, isCollapsed = collapsedCommunities[cn] === true;
    const stats = communitySummaries[cn] || computeLocalStats(group.items);
    html += `<div class="community-group">`;
    const inlineStats = stats ? [stats.avg_unit_price_ping > 0 ? `均單 ${(stats.avg_unit_price_ping / 10000).toFixed(0)}萬/坪` : '', stats.avg_ping > 0 ? `均坪 ${stats.avg_ping.toFixed(0)}坪` : '', stats.avg_ratio > 0 ? `公設 ${stats.avg_ratio.toFixed(0)}%` : ''
    ].filter(Boolean) : [];
    html += `<div class="community-header" onclick="toggleCommunity(this,'${escAttr(cn)}')" onmouseenter="hoverCommunity('${escAttr(cn)}')">
      <span class="ch-arrow ${isCollapsed ? '' : 'open'}">▶</span>
      <div style="flex:1;min-width:0"><div class="ch-name">${escHtml(cn)}</div>
      ${inlineStats.length ? `<div class="ch-stats-inline">${inlineStats.map(s => `<span>${s}</span>`).join('')}</div>` : ''}</div>
      <span class="ch-count">${group.items.length} 筆</span></div>`;
    if (stats) {
      html += `<div class="community-stats" id="cstats-${cssId(cn)}" style="${isCollapsed ? 'display:none' : ''}">
        <div class="cs-item"><span class="cs-label">📊 筆數</span><span class="cs-value">${group.items.length}</span></div>
        <div class="cs-item"><span class="cs-label">💰 均總</span><span class="cs-value">${fmtWan(stats.avg_price)}</span></div>
        <div class="cs-item"><span class="cs-label">�� 均單</span><span class="cs-value">${stats.avg_unit_price_ping > 0 ? (stats.avg_unit_price_ping / 10000).toFixed(1) + '萬/坪' : '-'}</span></div>
        <div class="cs-item"><span class="cs-label">📐 均坪</span><span class="cs-value">${stats.avg_ping > 0 ? stats.avg_ping.toFixed(1) + '坪' : '-'}</span></div>
        <div class="cs-item"><span class="cs-label">🏗️ 公設</span><span class="cs-value" style="color:${stats.avg_ratio > 35 ? 'var(--red)' : stats.avg_ratio > 30 ? 'var(--orange)' : 'var(--green)'}">${stats.avg_ratio > 0 ? stats.avg_ratio.toFixed(1) + '%' : '-'}</span></div>
      </div>`;
    }
    html += `<div class="community-items ${isCollapsed ? 'collapsed' : ''}" id="citems-${cssId(cn)}" style="${isCollapsed ? 'max-height:0' : 'max-height:999999px'}">`;
    group.items.forEach(item => { html += renderTxCard(item.tx, item.origIdx, true); });
    html += `</div></div>`;
  });
  if (noComItems.length > 0) {
    if (groups.length > 0) html += `<div style="padding:8px 16px;background:var(--bg);border-bottom:1px solid var(--border);font-size:12px;color:var(--text2);font-weight:600">其他交易 (${noComItems.length} 筆)</div>`;
    noComItems.forEach(item => { html += renderTxCard(item.tx, item.origIdx, false); });
  }
  if (!html) html = '<div class="empty">沒有資料</div>';
  container.innerHTML = html;
}

function renderTxCard(tx, idx, inGroup) {
  const isActive = idx === activeCardIdx, cn = tx.community_name || '';
  if (!markerSettings.showLotAddr && isLotAddress(tx.address_raw || tx.address || '')) return '';
  const upWan = (tx.unit_price_ping || 0) / 10000;
  let priceClass = ''; if (upWan > 100) priceClass = ' price-high'; else if (upWan > 50) priceClass = ' price-mid'; else if (upWan > 0) priceClass = ' price-low';
  const avgPriceW = (tx.price || 0) / 10000, avgUnitW = upWan;
  const dotOuter = getColorForMode(markerSettings.outerMode, avgPriceW, avgUnitW);
  const dotInner = getColorForMode(markerSettings.innerMode, avgPriceW, avgUnitW);
  const colorDot = `<svg class="tx-color-dot" width="18" height="18" viewBox="0 0 18 18"><circle cx="9" cy="9" r="8" fill="${dotOuter}" stroke="#fff" stroke-width="1.5"/><circle cx="9" cy="9" r="5" fill="${dotInner}"/></svg>`;
  const cnTag = cn ? `<span class="tx-community-tag" title="${escAttr(cn)}">${escHtml(cn)}</span>` : '';
  const cnRow = cnTag ? `<div class="tx-community-row">${cnTag}</div>` : '';
  const specialCls = tx.is_special ? ' special' : '';
  const specialBadge = tx.is_special ? '<span class="special-badge">特殊</span>' : '';
  const parkingTag = tx.has_parking ? `<span class="tx-parking-tag">🚗 含車位${tx.parking_price > 0 ? ' ' + fmtWan(tx.parking_price) : ''}</span>` : '';
  return `<div class="tx-card${isActive ? ' active' : ''}${priceClass}${specialCls}" onclick="selectTx(${idx})" onmouseenter="hoverTx(${idx})" data-idx="${idx}">
    ${colorDot}
    <div class="tx-addr" title="${escAttr(tx.address)}">${escHtml(tx.address)}${specialBadge}</div>
    ${cnRow}
    <div class="tx-detail-row">
      <span>📅 ${tx.date || '-'}</span><span>📐 ${fmtArea(tx.area_sqm, tx.area_ping)}</span>
      <span>${tx.rooms || 0}房${tx.halls || 0}廳${tx.baths || 0}衛</span>
      ${tx.floor ? `<span>🏢 ${escHtml(String(tx.floor))}F/${escHtml(String(tx.total_floors))}F</span>` : ''}
      ${tx.public_ratio > 0 ? `<span class="tag">公設${tx.public_ratio}%</span>` : ''}
      ${tx.building_type ? `<span class="tag">${escHtml(tx.building_type)}</span>` : ''}
      ${parkingTag}
      ${tx.note ? `<span style="color:var(--text3);font-size:10px">📝 ${escHtml(tx.note.length > 30 ? tx.note.substring(0, 30) + '…' : tx.note)}</span>` : ''}
    </div>
    <div class="tx-price-col">
      <div class="tx-unit">${fmtUnitPrice(tx.unit_price_ping, tx.unit_price_sqm)}</div>
      <div class="tx-price">${fmtWan(tx.price)}</div>
    </div>
  </div>`;
}

function computeLocalStats(items) {
  if (!items || items.length === 0) return null;
  let prices = [], ups = [], pings = [], ratios = [];
  items.forEach(({ tx }) => { if (tx.price > 0) prices.push(tx.price); if (tx.unit_price_ping > 0) ups.push(tx.unit_price_ping); if (tx.area_ping > 0) pings.push(tx.area_ping); if (tx.public_ratio > 0) ratios.push(tx.public_ratio); });
  const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
  return { count: items.length, avg_price: Math.round(avg(prices)), avg_unit_price_ping: avg(ups), avg_ping: avg(pings), avg_ratio: avg(ratios) };
}

function toggleCommunity(headerEl, name) {
  const isNowCollapsed = !collapsedCommunities[name]; collapsedCommunities[name] = isNowCollapsed;
  const itemsEl = document.getElementById('citems-' + cssId(name)), statsEl = document.getElementById('cstats-' + cssId(name)), arrow = headerEl.querySelector('.ch-arrow');
  if (itemsEl) { if (isNowCollapsed) { itemsEl.classList.add('collapsed'); itemsEl.style.maxHeight = '0'; } else { itemsEl.classList.remove('collapsed'); itemsEl.style.maxHeight = '999999px'; } }
  if (statsEl) statsEl.style.display = isNowCollapsed ? 'none' : '';
  if (arrow) arrow.classList.toggle('open', !isNowCollapsed);
}

function renderSummary() {
  const s = window._summary; if (!s || !s.total) { document.getElementById('summaryBar').style.display = 'none'; return; }
  const bar = document.getElementById('summaryBar'); bar.style.display = 'block';
  const avgUp = unit === 'ping' ? fmtUnitPrice(s.avg_unit_price_ping, 0) : fmtUnitPrice(0, s.avg_unit_price_ping / 3.30579);
  const minUp = unit === 'ping' ? fmtUnitPrice(s.min_unit_price_ping, 0) : fmtUnitPrice(0, s.min_unit_price_ping / 3.30579);
  const maxUp = unit === 'ping' ? fmtUnitPrice(s.max_unit_price_ping, 0) : fmtUnitPrice(0, s.max_unit_price_ping / 3.30579);
  const comCount = Object.keys(communitySummaries).length;
  const comInfo = comCount > 0 ? ` ｜ <span class="val">${comCount}</span> 個建案` : '';
  bar.innerHTML = `共 <span class="val">${s.total}</span> 筆${comInfo} ｜ 均價 <span class="val">${fmtWan(s.avg_price)}</span> ｜ 均坪 <span class="val">${s.avg_ping}坪</span> ｜ 單價 <span class="val">${avgUp}</span><br>單價區間 <span class="val">${minUp}</span> ~ <span class="val">${maxUp}</span> ｜ 均公設 <span class="val">${s.avg_ratio || '-'}%</span>`;
}

// ── 地圖 Marker ──
function baseAddress(addr) { if (!addr) return ''; return addr.replace(/\d+樓.*$/, '').replace(/\d+F.*$/i, '').replace(/地下.*$/, ''); }
function stripCityJS(addr) { if (!addr) return ''; let s = addr.replace(/^(?:(?:台|臺)(?:北|中|南|東)市|(?:新北|桃園|高雄|基隆|新竹|嘉義)[市縣]|.{2,3}縣)/, ''); s = s.replace(/^[\u4e00-\u9fff]{1,4}[區鄉鎮市]/, ''); return s; }

function buildGroups() {
  const preciseMode = getLocationMode() === 'osm';
  const raw = {};
  txData.forEach((tx, idx) => {
    if (!tx.lat || !tx.lng) return;
    if (!markerSettings.showLotAddr && isLotAddress(tx.address_raw || tx.address || '')) return;
    let key;
    if (preciseMode) {
      // 放大時：按地址精確分組，但同建案仍用建案名作 key 前綴以便後續合併
      if (tx.community_name) key = 'c:' + tx.community_name + ':' + baseAddress(tx.address_raw || tx.address);
      else key = 'a:' + baseAddress(tx.address_raw || tx.address);
    } else {
      // 縮小時：同建案一律合併
      if (tx.community_name) key = 'c:' + tx.community_name;
      else key = 'a:' + baseAddress(tx.address_raw || tx.address);
    }
    if (!raw[key]) raw[key] = { label: tx.community_name || stripCityJS(baseAddress(tx.address)), communityName: tx.community_name || '', items: [], lats: [], lngs: [], prices: [], unitPrices: [] };
    const g = raw[key]; g.items.push({ tx, origIdx: idx }); g.lats.push(tx.lat); g.lngs.push(tx.lng);
    if (tx.price > 0) g.prices.push(tx.price); if (tx.unit_price_ping > 0) g.unitPrices.push(tx.unit_price_ping);
  });
  const arr = Object.values(raw), len = arr.length;
  arr.forEach(g => { const sLat = g.lats.slice().sort((a, b) => a - b), sLng = g.lngs.slice().sort((a, b) => a - b), m = Math.floor(sLat.length / 2); g._cLat = sLat[m]; g._cLng = sLng[m]; });

  // 合併距離：放大時小（只合併很近的），縮小時大
  const MERGE = preciseMode ? 0.00008 : 0.0003;
  const par = Array.from({ length: len }, (_, i) => i);
  function find(x) { while (par[x] !== x) { par[x] = par[par[x]]; x = par[x]; } return x; }
  for (let i = 0; i < len; i++) {
    for (let j = i + 1; j < len; j++) {
      const dist = Math.abs(arr[i]._cLat - arr[j]._cLat) + Math.abs(arr[i]._cLng - arr[j]._cLng);
      // 同建案且距離足夠近：合併
      const sameCom = arr[i].communityName && arr[i].communityName === arr[j].communityName;
      if (sameCom && dist < MERGE * 3) par[find(i)] = find(j);
      // 不同建案但非常接近：也合併（避免重疊）
      else if (dist < MERGE) par[find(i)] = find(j);
    }
  }
  const buckets = {}; for (let i = 0; i < len; i++) { const r = find(i); if (!buckets[r]) buckets[r] = []; buckets[r].push(arr[i]); }
  const merged = Object.values(buckets).map(gs => {
    if (gs.length === 1) return gs[0];
    const m = { items: [], lats: [], lngs: [], prices: [], unitPrices: [] }; const lbls = [];
    gs.forEach(g => { m.items.push(...g.items); m.lats.push(...g.lats); m.lngs.push(...g.lngs); m.prices.push(...g.prices); m.unitPrices.push(...g.unitPrices); if (g.label) lbls.push(g.label); if (g.communityName) m.communityName = g.communityName; });
    const ul = [...new Set(lbls)]; if (ul.length <= 1) m.label = ul[0] || ''; else if (ul.length === 2) m.label = ul.join('·'); else m.label = ul[0] + '等' + ul.length + '案';
    return m;
  });
  const nowYear = new Date().getFullYear() - 1911, twoYearThreshold = (nowYear - 2) * 10000;
  merged.forEach(g => {
    const recent = g.items.filter(({ tx }) => { if (tx.is_special) return false; const dr = parseInt(String(tx.date_raw || '0').replace(/\D/g, ''), 10); return dr >= twoYearThreshold; });
    const rPrices = recent.map(({ tx }) => tx.price).filter(v => v > 0), rUnits = recent.map(({ tx }) => tx.unit_price_ping).filter(v => v > 0);
    g.recentCount = recent.length; g.recentAvgPrice = rPrices.length ? rPrices.reduce((a, b) => a + b, 0) / rPrices.length : 0; g.recentAvgUnitPrice = rUnits.length ? rUnits.reduce((a, b) => a + b, 0) / rUnits.length : 0;
    // 中位數
    rUnits.sort((a, b) => a - b);
    g.recentMedianUnitPrice = rUnits.length > 0 ? rUnits[Math.floor(rUnits.length / 2)] : 0;
  });
  return merged;
}

function getUnitPriceColor(wan) { if (wan <= 0) return '#888'; const t = markerSettings.unitThresholds; if (wan >= t[2]) return '#b71c1c'; if (wan >= t[1]) return '#e65100'; if (wan >= t[0]) return '#f57f17'; return '#1b5e20'; }
function getTotalPriceColor(wan) { if (wan <= 0) return '#888'; const t = markerSettings.totalThresholds; if (wan >= t[2]) return '#b71c1c'; if (wan >= t[1]) return '#e65100'; if (wan >= t[0]) return '#f57f17'; return '#1b5e20'; }
function getColorForMode(mode, avgPriceWan, avgUnitWan) { if (mode === 'total_price') return getTotalPriceColor(avgPriceWan); return getUnitPriceColor(avgUnitWan); }

function makeMarkerSVG({ sz, outerColor, innerColor, line1, line2, fontSz1, fontSz2 }) {
  const cx = sz / 2, cy = sz / 2, outerR = sz / 2 - 1, ringW = Math.max(4, Math.floor(sz * 0.1)), innerR = outerR - ringW - 1.5;
  const hasTwo = line1 && line2, y1 = hasTwo ? cy - 4 : cy, y2 = cy + 7;
  const fs1 = fontSz1 || (sz >= 54 ? 11 : 10), fs2 = fontSz2 || (sz >= 54 ? 9 : 8);
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${sz}" height="${sz}">
    <circle cx="${cx}" cy="${cy}" r="${outerR}" fill="${outerColor}" stroke="rgba(255,255,255,.9)" stroke-width="2"/>
    <circle cx="${cx}" cy="${cy}" r="${innerR}" fill="${innerColor}" stroke="rgba(255,255,255,.5)" stroke-width="1.5"/>
    ${line1 ? `<text x="${cx}" y="${y1}" text-anchor="middle" dominant-baseline="central" fill="#fff" font-size="${fs1}" font-weight="700" font-family="Arial,sans-serif" style="paint-order:stroke" stroke="rgba(0,0,0,.25)" stroke-width=".8">${line1}</text>` : ''}
    ${line2 ? `<text x="${cx}" y="${y2}" text-anchor="middle" dominant-baseline="central" fill="rgba(255,255,255,.95)" font-size="${fs2}" font-weight="600" font-family="Arial,sans-serif" style="paint-order:stroke" stroke="rgba(0,0,0,.2)" stroke-width=".6">${line2}</text>` : ''}
  </svg>`;
}

function plotMarkers(fitBounds = true) {
  markerClusterGroup.clearLayers(); const boundsArr = [], groups = buildGroups();
  groups.forEach(g => {
    const n = g.items.length, sortedLats = g.lats.slice().sort((a, b) => a - b), sortedLngs = g.lngs.slice().sort((a, b) => a - b), mid = Math.floor(sortedLats.length / 2);
    const lat = sortedLats[mid], lng = sortedLngs[mid];
    const useRecent = markerSettings.contentMode === 'recent2yr' && g.recentCount > 0;
    const avgPrice = useRecent ? g.recentAvgPrice : (g.prices.length ? g.prices.reduce((a, b) => a + b, 0) / g.prices.length : 0);
    const avgUnitPrice = useRecent ? g.recentAvgUnitPrice : (g.unitPrices.length ? g.unitPrices.reduce((a, b) => a + b, 0) / g.unitPrices.length : 0);
    const avgPriceWan = avgPrice / 10000, avgUnitWan = avgUnitPrice / 10000;
    const outerColor = getColorForMode(markerSettings.outerMode, avgPriceWan, avgUnitWan);
    const innerColor = getColorForMode(markerSettings.innerMode, avgPriceWan, avgUnitWan);
    const label = g.label ? g.label.substring(0, 8) : '';
    let priceText = '';
    if (avgPriceWan >= 10000) priceText = (avgPriceWan / 10000).toFixed(1) + '億';
    else if (avgPriceWan >= 1) priceText = Math.round(avgPriceWan) + '萬';
    else priceText = '-';
    let sz = n >= 20 ? 56 : (n >= 5 ? 50 : 44); if (n === 1) sz = 42;
    let line1, line2; if (n === 1) { line1 = priceText; line2 = ''; } else { line1 = priceText; line2 = n + '筆'; }
    const svgHtml = makeMarkerSVG({ sz, outerColor, innerColor, line1, line2 });
    const labelHtml = label ? `<div style="margin-top:-2px;padding:1px 5px;background:rgba(255,255,255,.95);border-radius:6px;font-size:${n > 1 ? 9 : 8}px;font-weight:700;color:#333;white-space:nowrap;max-width:80px;overflow:hidden;text-overflow:ellipsis;box-shadow:0 1px 3px rgba(0,0,0,.15);border:1px solid rgba(0,0,0,.08)">${escHtml(label)}</div>` : '';
    const totalH = label ? sz + 15 : sz;
    const icon = L.divIcon({ html: `<div style="display:flex;flex-direction:column;align-items:center">${svgHtml}${labelHtml}</div>`, iconSize: [sz + 8, totalH], iconAnchor: [(sz + 8) / 2, totalH / 2], className: 'price-marker' });
    const marker = L.marker([lat, lng], { icon });
    marker._groupCount = n; marker._avgPrice = avgPrice; marker._avgUnitPrice = avgUnitPrice; marker._groupLabel = g.label; marker._groupItems = g.items;
    if (n === 1) { const tx = g.items[0].tx, origIdx = g.items[0].origIdx; marker.bindPopup(makePopup(tx), { maxWidth: 320 }); marker.on('click', () => { activeCardIdx = origIdx; renderResults(); const card = document.querySelector(`.tx-card[data-idx="${origIdx}"]`); if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' }); }); }
    else marker.on('click', () => showClusterList(g.items));
    markerClusterGroup.addLayer(marker); boundsArr.push([lat, lng]);
  });
  if (fitBounds && boundsArr.length > 0) map.fitBounds(boundsArr, { padding: [40, 40], maxZoom: 16 });
}

function makePopup(tx) {
  const cn = tx.community_name ? `<div style="background:var(--green-bg);padding:2px 8px;border-radius:8px;display:inline-block;color:var(--green);font-size:12px;font-weight:600;margin-bottom:4px">🏘️ ${escHtml(tx.community_name)}</div><br>` : '';
  return `<div style="font-size:13px;line-height:1.6">${cn}<b>${escHtml(tx.address)}</b><br>📅 ${tx.date || '-'} ｜ 📐 ${fmtArea(tx.area_sqm, tx.area_ping)}<br>💰 <b style="color:var(--red)">${fmtWan(tx.price)}</b> <span style="color:var(--text2)">${fmtUnitPrice(tx.unit_price_ping, tx.unit_price_sqm)}</span><br>🏢 ${tx.rooms || 0}房${tx.halls || 0}廳${tx.baths || 0}衛${tx.floor ? '｜ ' + tx.floor + 'F/' + tx.total_floors + 'F' : ''}${tx.public_ratio > 0 ? '｜ 公設' + tx.public_ratio + '%' : ''}${tx.building_type ? '<br>' + escHtml(tx.building_type) : ''}${tx.has_parking ? '<br>🚗 含車位 ' + fmtWan(tx.parking_price) : ''}${tx.note ? '<br><span style="color:var(--text3);font-size:11px">' + escHtml(tx.note) + '</span>' : ''}</div>`;
}

function selectTx(idx) {
  activeCardIdx = idx; renderResults();
  const tx = txData[idx];
  if (tx && tx.lat && tx.lng) { map.setView([tx.lat, tx.lng], 17); markerClusterGroup.eachLayer(layer => { if (layer._groupItems && layer._groupItems.some(it => it.origIdx === idx)) { markerClusterGroup.zoomToShowLayer(layer, () => { if (layer._groupItems.length === 1) layer.openPopup(); }); } }); }
}

function addLegend() {
  const legend = L.control({ position: 'bottomleft' });
  legend.onAdd = function () {
    const div = L.DomUtil.create('div', '');
    div.innerHTML = `<div style="background:var(--card);padding:10px 12px;border-radius:var(--radius);box-shadow:var(--shadow-md);font-size:11px;line-height:1.8;min-width:170px;border:1px solid var(--border)">
      <div style="font-weight:700;margin-bottom:4px;font-size:12px">🎯 雙圈色彩圖例</div>
      <div style="font-weight:600;font-size:10px;color:var(--primary);margin-bottom:2px">● 外環＝總價 ｜ ● 內圈＝單價/坪</div>
      <div style="display:flex;align-items:center;gap:6px"><svg width="18" height="18"><circle cx="9" cy="9" r="8" fill="#1b5e20" stroke="#fff" stroke-width="1.5"/><circle cx="9" cy="9" r="5" fill="#1b5e20"/></svg><span>低（＜500萬/＜20萬）</span></div>
      <div style="display:flex;align-items:center;gap:6px"><svg width="18" height="18"><circle cx="9" cy="9" r="8" fill="#f57f17" stroke="#fff" stroke-width="1.5"/><circle cx="9" cy="9" r="5" fill="#f57f17"/></svg><span>中（500-1500/20-40萬）</span></div>
      <div style="display:flex;align-items:center;gap:6px"><svg width="18" height="18"><circle cx="9" cy="9" r="8" fill="#e65100" stroke="#fff" stroke-width="1.5"/><circle cx="9" cy="9" r="5" fill="#e65100"/></svg><span>中高（1500-3000/40-70萬）</span></div>
      <div style="display:flex;align-items:center;gap:6px"><svg width="18" height="18"><circle cx="9" cy="9" r="8" fill="#b71c1c" stroke="#fff" stroke-width="1.5"/><circle cx="9" cy="9" r="5" fill="#b71c1c"/></svg><span>高（＞3000萬/＞70萬）</span></div>
      <div style="font-weight:600;margin-top:6px;font-size:10px;color:var(--text2)">📊 圈內＝近2年均價(排除特殊)</div>
    </div>`;
    L.DomEvent.disableScrollPropagation(div); L.DomEvent.disableClickPropagation(div); return div;
  };
  legend.addTo(map);
}

function locateMe() {
  if (!navigator.geolocation) { alert('您的瀏覽器不支援定位功能'); return; }
  const btn = document.querySelector('.map-controls button[title="移動到我的位置"]');
  if (btn) { btn.style.background = 'var(--primary-light)'; btn.innerHTML = '⏳'; }
  navigator.geolocation.getCurrentPosition(pos => {
    const { latitude: lat, longitude: lng, accuracy } = pos.coords; map.setView([lat, lng], 16);
    if (locationMarker) map.removeLayer(locationMarker); if (locationCircle) map.removeLayer(locationCircle);
    locationCircle = L.circle([lat, lng], { radius: accuracy, color: 'var(--primary)', fillColor: 'var(--primary)', fillOpacity: .1, weight: 1 }).addTo(map);
    locationMarker = L.marker([lat, lng], { icon: L.divIcon({ html: '<div class="locate-pulse"></div>', iconSize: [16, 16], className: '' }), zIndexOffset: 1000 }).addTo(map).bindPopup(`📍 您的位置<br><span style="font-size:11px;color:var(--text2)">精確度: ±${Math.round(accuracy)}m</span>`).openPopup();
    setTimeout(() => { if (locationCircle) map.removeLayer(locationCircle); locationCircle = null; }, 5000);
    if (btn) { btn.style.background = ''; btn.innerHTML = '📍'; }
  }, err => { alert('定位失敗: ' + err.message); if (btn) { btn.style.background = ''; btn.innerHTML = '📍'; } }, { enableHighAccuracy: true, timeout: 10000 });
}

function showClusterList(items) {
  const container = document.getElementById('results');
  const comGroups = {}; items.forEach(it => { const cn = it.tx.community_name || '未知建案'; if (!comGroups[cn]) comGroups[cn] = []; comGroups[cn].push(it); });
  const comList = Object.entries(comGroups).sort((a, b) => b[1].length - a[1].length);
  let html = `<div class="cluster-list-header"><span>📍 此位置 ${items.length} 筆重疊資料</span><button onclick="renderResults()">↩ 返回全列表</button></div>`;
  comList.forEach(([cn, comItems]) => {
    html += `<div style="padding:6px 12px;background:var(--card);border-bottom:1px solid var(--border);font-weight:600;font-size:13px;color:var(--text)">🏘️ ${escHtml(cn)}（${comItems.length}筆）</div>`;
    comItems.forEach(({ tx, origIdx }) => { html += renderTxCard(tx, origIdx, false); });
  });
  container.innerHTML = html;
  if (window.innerWidth <= 768) document.getElementById('sidebar').classList.add('open');
}

// ── 設定面板 ──
function toggleSettings() { const panel = document.getElementById('settingsPanel'), overlay = document.getElementById('settingsOverlay'), isOpen = panel.classList.contains('open'); panel.classList.toggle('open', !isOpen); overlay.classList.toggle('show', !isOpen); }
function applySettings() {
  markerSettings.outerMode = document.getElementById('sOuter').value; markerSettings.innerMode = document.getElementById('sInner').value; markerSettings.contentMode = document.getElementById('sContent').value;
  markerSettings.unitThresholds = [parseFloat(document.getElementById('sUnitT1').value) || 20, parseFloat(document.getElementById('sUnitT2').value) || 40, parseFloat(document.getElementById('sUnitT3').value) || 70];
  markerSettings.totalThresholds = [parseFloat(document.getElementById('sTotalT1').value) || 500, parseFloat(document.getElementById('sTotalT2').value) || 1500, parseFloat(document.getElementById('sTotalT3').value) || 3000];
  markerSettings.osmZoom = parseInt(document.getElementById('sOsmZoom').value) || 16; markerSettings.showLotAddr = document.getElementById('sShowLotAddr').checked;
  document.getElementById('sUnitT3r').value = markerSettings.unitThresholds[2]; document.getElementById('sTotalT3r').value = markerSettings.totalThresholds[2];
  try { localStorage.setItem('markerSettings', JSON.stringify(markerSettings)); } catch (e) { }
  if (txData.length > 0) plotMarkers(false);
}
function loadSettings() {
  try {
    const saved = localStorage.getItem('markerSettings'); if (saved) { const s = JSON.parse(saved); markerSettings = { ...markerSettings, ...s }; }
    document.getElementById('sOuter').value = markerSettings.outerMode; document.getElementById('sInner').value = markerSettings.innerMode; document.getElementById('sContent').value = markerSettings.contentMode;
    const ut = markerSettings.unitThresholds || [20, 40, 70], tt = markerSettings.totalThresholds || [500, 1500, 3000];
    document.getElementById('sUnitT1').value = ut[0]; document.getElementById('sUnitT2').value = ut[1]; document.getElementById('sUnitT3').value = ut[2]; document.getElementById('sUnitT3r').value = ut[2];
    document.getElementById('sTotalT1').value = tt[0]; document.getElementById('sTotalT2').value = tt[1]; document.getElementById('sTotalT3').value = tt[2]; document.getElementById('sTotalT3r').value = tt[2];
    document.getElementById('sOsmZoom').value = markerSettings.osmZoom || 16; document.getElementById('sShowLotAddr').checked = !!markerSettings.showLotAddr;
  } catch (e) { }
}

// ── 鍵盤快捷鍵 ──
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
  if (e.key === 'd' || e.key === 'D') { toggleTheme(); e.preventDefault(); }
  if (e.key === '/' || e.key === 's') { document.getElementById('searchInput').focus(); e.preventDefault(); }
});

// ── 初始化 ──
document.addEventListener('DOMContentLoaded', () => {
  loadTheme(); loadSettings(); initMap();
  window.addEventListener('resize', hideAcList);
  if (window.innerWidth <= 768) { map.on('click', () => { document.getElementById('sidebar').classList.remove('open'); }); }
});
