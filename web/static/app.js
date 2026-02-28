/* ══════════════════════════════════════════════════════════════
   app.js — 良富居地產 v5.0 前端應用
   ══════════════════════════════════════════════════════════════ */

// ── 全域狀態 ──
let map, markerClusterGroup, markerGroup;
let txData = [];
let activeCardIdx = -1;
let currentSort = 'date';
let sortDirection = 'desc';
let lastSearchType = '';
let unit = 'ping';
let locationMarker, locationCircle;
let collapsedCommunities = {};
let _lastBouncingEls = [];
let communitySummaries = {};
let markerSettings = {
  outerMode: 'unit_price', innerMode: 'total_price',
  contentMode: 'recent2yr',
  unitThresholds: [20, 40, 70], totalThresholds: [500, 1500, 3000],
  osmZoom: 16, showLotAddr: false, yearFormat: 'roc'
};
let areaAutoSearch = false;
let _areaSearchTimer = null;
let _hoverPanSuppressed = false;  // prevent hover panTo → area reload loop

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
  map = L.map('map', { zoomControl: false }).setView([23.6978, 120.9605], 8);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager_labels_under/{z}/{x}/{y}{r}.png', { maxZoom: 20, attribution: '&copy; OpenStreetMap & CartoDB' }).addTo(map);

  const iconCreateFn = function (cluster) {
    const markers = cluster.getAllChildMarkers();
    let totalPrice = 0, validP = 0, totalUnit = 0, validU = 0, totalCount = 0;
    markers.forEach(m => {
      const gc = m._groupCount || 1; totalCount += gc;
      if (m._avgPrice > 0) { totalPrice += m._avgPrice * gc; validP += gc; }
      if (m._avgUnitPrice > 0) { totalUnit += m._avgUnitPrice * gc; validU += gc; }
    });
    const avgPriceWan = validP > 0 ? (totalPrice / validP / 10000) : 0;
    const avgUnitWan = validU > 0 ? (totalUnit / validU / 10000) : 0;

    const outerColor = getColorForMode(markerSettings.outerMode, avgPriceWan, avgUnitWan);
    const innerColor = getColorForMode(markerSettings.innerMode, avgPriceWan, avgUnitWan);

    let priceText = '';
    if (avgPriceWan >= 10000) priceText = (avgPriceWan / 10000).toFixed(1) + '億';
    else if (avgPriceWan >= 1) priceText = avgPriceWan.toFixed(0) + '萬';

    let sz = 44;
    if (totalCount >= 100) sz = 60; else if (totalCount >= 30) sz = 54; else if (totalCount >= 10) sz = 48;

    const line1 = priceText || totalCount + '筆';
    const line2 = priceText ? totalCount + '筆' : '';
    const svgHtml = makeMarkerSVG({ sz, outerColor, innerColor, line1, line2 });

    const labels = markers.map(m => m._groupLabel).filter(Boolean);
    const uniqueLabels = [...new Set(labels)];
    const commLabel = uniqueLabels.length === 1 ? uniqueLabels[0].substring(0, 6) : '';
    const labelHtml = commLabel ? `<div style="margin-top:-2px;padding:1px 4px;background:rgba(255,255,255,.92);border-radius:6px;font-size:8px;font-weight:600;color:#333;white-space:nowrap;max-width:70px;overflow:hidden;text-overflow:ellipsis;box-shadow:0 1px 3px rgba(0,0,0,.15);border:1px solid rgba(0,0,0,.08)">${commLabel}</div>` : '';

    const totalH = commLabel ? sz + 14 : sz;
    return L.divIcon({
      html: `<div style="display:flex;flex-direction:column;align-items:center">${svgHtml}${labelHtml}</div>`,
      className: 'price-marker custom-cluster-icon',
      iconSize: [sz + 8, totalH],
      iconAnchor: [(sz + 8) / 2, totalH / 2]
    });
  };

  markerClusterGroup = L.markerClusterGroup({
    spiderfyOnMaxZoom: true, showCoverageOnHover: false, zoomToBoundsOnClick: false, maxClusterRadius: 40,
    spiderfyDistanceMultiplier: 2.5,
    iconCreateFunction: iconCreateFn
  });

  communityClusterGroup = L.markerClusterGroup({
    spiderfyOnMaxZoom: true, showCoverageOnHover: false, zoomToBoundsOnClick: false, maxClusterRadius: 1,
    spiderfyDistanceMultiplier: 2.5,
    iconCreateFunction: iconCreateFn
  });

  const spiderfyHandler = (e) => {
    e.cluster._icon.classList.add('spider-focus');
    e.markers.forEach(m => { if (m._icon) m._icon.classList.add('spider-focus'); });
    document.getElementById('map').classList.add('spiderfied-active');
  };
  const unspiderfyHandler = (e) => {
    if (e.cluster._icon) e.cluster._icon.classList.remove('spider-focus');
    e.markers.forEach(m => { if (m._icon) m._icon.classList.remove('spider-focus'); });
    document.getElementById('map').classList.remove('spiderfied-active');
  };

  markerClusterGroup.on('spiderfied', spiderfyHandler);
  markerClusterGroup.on('unspiderfied', unspiderfyHandler);
  communityClusterGroup.on('spiderfied', spiderfyHandler);
  communityClusterGroup.on('unspiderfied', unspiderfyHandler);

  map.addLayer(markerClusterGroup);
  map.addLayer(communityClusterGroup);
  markerGroup = L.featureGroup().addTo(map);
  map.on('moveend', onMapMoveEnd);
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
  const dir = sortDirection === 'asc' ? 1 : -1;
  const sorters = {
    date: (a, b) => dir * (b.date_raw || '').localeCompare(a.date_raw || ''),
    price: (a, b) => dir * ((b.price || 0) - (a.price || 0)),
    unit_price: (a, b) => dir * ((b.unit_price_ping || 0) - (a.unit_price_ping || 0)),
    ping: (a, b) => dir * ((b.area_ping || 0) - (a.area_ping || 0)),
    public_ratio: (a, b) => dir * ((a.public_ratio || 999) - (b.public_ratio || 999)),
    community: (a, b) => { const ca = a.community_name || '', cb2 = b.community_name || ''; if (ca && !cb2) return -1; if (!ca && cb2) return 1; if (ca !== cb2) return dir * ca.localeCompare(cb2); return dir * (b.date_raw || '').localeCompare(a.date_raw || ''); }
  };
  if (sorters[sortType]) txData.sort(sorters[sortType]);
}
function setSort(btn) {
  const newSort = btn.dataset.sort;
  if (currentSort === newSort) {
    sortDirection = sortDirection === 'desc' ? 'asc' : 'desc';
  } else {
    currentSort = newSort;
    sortDirection = 'desc';
  }
  document.querySelectorAll('.sort-bar button[data-sort]').forEach(b => {
    b.classList.remove('active');
    // 移除舊箭頭
    const oldArrow = b.querySelector('.sort-arrow');
    if (oldArrow) oldArrow.remove();
  });
  btn.classList.add('active');
  const arrow = document.createElement('span');
  arrow.className = 'sort-arrow';
  arrow.textContent = sortDirection === 'desc' ? ' ▼' : ' ▲';
  btn.appendChild(arrow);
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
function stopAllBounce() {
  _lastBouncingEls.forEach(el => { if (el) el.classList.remove('marker-bounce'); });
  _lastBouncingEls = [];
}
function bounceElement(el) {
  stopAllBounce();
  el.classList.remove('marker-bounce'); void el.offsetWidth;
  el.classList.add('marker-bounce');
  _lastBouncingEls = [el];
}
function hoverTx(idx) {
  let targetMarker = null;
  markerClusterGroup.eachLayer(layer => { if (!targetMarker && layer._groupItems && layer._groupItems.some(it => it.origIdx === idx)) targetMarker = layer; });
  if (!targetMarker) communityClusterGroup.eachLayer(layer => { if (!targetMarker && layer._groupItems && layer._groupItems.some(it => it.origIdx === idx)) targetMarker = layer; });
  if (!targetMarker) return;
  const ll = targetMarker.getLatLng();
  if (!map.getBounds().contains(ll)) {
    _hoverPanSuppressed = true;
    map.panTo(ll, { animate: true, duration: 0.25 });
    setTimeout(() => { _hoverPanSuppressed = false; }, 600);
  }
  const tryBounce = () => {
    const iconEl = targetMarker._icon;
    if (!iconEl) return;
    const inner = iconEl.firstElementChild || iconEl;
    bounceElement(inner);
  };
  if (targetMarker._icon) tryBounce();
  else markerClusterGroup.zoomToShowLayer(targetMarker, () => setTimeout(tryBounce, 100));
}
function unhoverTx() {
  stopAllBounce();
  hideMarkerTooltip();
}
function hoverCommunity(name) {
  stopAllBounce();
  const matchedMarkers = [];
  markerClusterGroup.eachLayer(layer => {
    if (layer._groupLabel === name || (layer._groupItems && layer._groupItems.some(it => it.tx.community_name === name))) {
      matchedMarkers.push(layer);
    }
  });
  communityClusterGroup.eachLayer(layer => {
    if (layer._groupLabel === name || (layer._groupItems && layer._groupItems.some(it => it.tx.community_name === name))) {
      matchedMarkers.push(layer);
    }
  });
  if (matchedMarkers.length === 0) return;
  const bounds = map.getBounds();
  const anyVisible = matchedMarkers.some(m => m._icon && bounds.contains(m.getLatLng()));
  if (!anyVisible) {
    _hoverPanSuppressed = true;
    map.panTo(matchedMarkers[0].getLatLng(), { animate: true, duration: 0.3 });
    setTimeout(() => { _hoverPanSuppressed = false; }, 600);
  }
  // 只跳第一個可見的 marker
  const firstVisible = matchedMarkers.find(m => m._icon && bounds.contains(m.getLatLng())) || matchedMarkers[0];
  const doBounce = () => {
    if (!firstVisible._icon) return;
    const inner = firstVisible._icon.firstElementChild || firstVisible._icon;
    bounceElement(inner);
  };
  if (firstVisible._icon) doBounce();
  else setTimeout(doBounce, 350);
}
function unhoverCommunity() {
  stopAllBounce();
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
  if (data.search_type === 'area') { collapsedCommunities = {}; const cNames = [...new Set(txData.map(tx => tx.community_name).filter(Boolean))]; cNames.forEach(cn => { collapsedCommunities[cn] = true; }); document.getElementById('headerFilters').classList.add('show'); populateDistrictFilter(); }
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
    html += `<div class="community-header" onclick="toggleCommunity(this,'${escAttr(cn)}')" onmouseenter="hoverCommunity('${escAttr(cn)}')" onmouseleave="unhoverCommunity()">
      <span class="ch-arrow ${isCollapsed ? '' : 'open'}">▶</span>
      <div style="flex:1;min-width:0"><div class="ch-name">${escHtml(cn)}</div>
      ${inlineStats.length ? `<div class="ch-stats-inline">${inlineStats.map(s => `<span>${s}</span>`).join('')}</div>` : ''}</div>
      <span class="ch-count">${group.items.length} 筆</span></div>`;
    if (stats) {
      html += `<div class="community-stats" id="cstats-${cssId(cn)}" style="${isCollapsed ? 'display:none' : ''}">
        <div class="cs-item"><span class="cs-label">📊 筆數</span><span class="cs-value">${group.items.length}</span></div>
        <div class="cs-item"><span class="cs-label">💰 均總</span><span class="cs-value">${fmtWan(stats.avg_price)}</span></div>
        <div class="cs-item"><span class="cs-label"> 均單</span><span class="cs-value">${stats.avg_unit_price_ping > 0 ? (stats.avg_unit_price_ping / 10000).toFixed(1) + '萬/坪' : '-'}</span></div>
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
  return `<div class="tx-card${isActive ? ' active' : ''}${priceClass}${specialCls}" onclick="selectTx(${idx})" onmouseenter="hoverTx(${idx})" onmouseleave="unhoverTx()" data-idx="${idx}">
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

function extractDistrict(tx) {
  return tx.district || '';
}

function buildGroups() {
  const raw = {};
  txData.forEach((tx, idx) => {
    if (!tx.lat || !tx.lng) return;
    if (!markerSettings.showLotAddr && isLotAddress(tx.address_raw || tx.address || '')) return;
    let key;
    // 同建案名 + 同區 才合併（不同區視為不同建案）
    if (tx.community_name) {
      const dist = extractDistrict(tx);
      key = 'c:' + tx.community_name + '@' + dist;
    } else {
      key = 'a:' + baseAddress(tx.address_raw || tx.address);
    }
    if (!raw[key]) raw[key] = { label: tx.community_name || stripCityJS(baseAddress(tx.address)), communityName: tx.community_name || '', hasCommunity: !!tx.community_name, items: [], lats: [], lngs: [], prices: [], unitPrices: [] };
    const g = raw[key]; g.items.push({ tx, origIdx: idx }); g.lats.push(tx.lat); g.lngs.push(tx.lng);
    if (tx.price > 0) g.prices.push(tx.price); if (tx.unit_price_ping > 0) g.unitPrices.push(tx.unit_price_ping);
  });
  const arr = Object.values(raw), len = arr.length;
  arr.forEach(g => { const sLat = g.lats.slice().sort((a, b) => a - b), sLng = g.lngs.slice().sort((a, b) => a - b), m = Math.floor(sLat.length / 2); g._cLat = sLat[m]; g._cLng = sLng[m]; });

  // ── Phase 3: Union-Find 近距離合併 ──
  //    有建案名：≈28m 以內合併
  //    無建案名：≈8m 以內才合併（避免不同交易被誤合）
  //    有建案 vs 無建案：不合併
  const MERGE_COM = 0.00025;    // ≈ 28m for community groups
  const MERGE_NO_COM = 0.00008; // ≈ 8m for non-community (OSM precise)
  const par = Array.from({ length: len }, (_, i) => i);
  function find(x) { while (par[x] !== x) { par[x] = par[par[x]]; x = par[x]; } return x; }
  const grid = {};
  const GRID_SIZE = MERGE_COM; // use larger grid for spatial index
  for (let i = 0; i < len; i++) {
    const cx = Math.floor(arr[i]._cLat / GRID_SIZE);
    const cy = Math.floor(arr[i]._cLng / GRID_SIZE);
    for (let dx = -1; dx <= 1; dx++)
      for (let dy = -1; dy <= 1; dy++) {
        const nk = `${cx + dx},${cy + dy}`;
        if (grid[nk]) grid[nk].forEach(j => {
          const gi = arr[i], gj = arr[j];
          // 有建案 vs 無建案不合併
          if (gi.hasCommunity !== gj.hasCommunity) return;
          // 選擇合併半徑
          const mergeR = gi.hasCommunity ? MERGE_COM : MERGE_NO_COM;
          if (Math.abs(gi._cLat - gj._cLat) < mergeR &&
            Math.abs(gi._cLng - gj._cLng) < mergeR)
            par[find(i)] = find(j);
        });
      }
    const k = `${cx},${cy}`;
    (grid[k] = grid[k] || []).push(i);
  }
  const buckets = {}; for (let i = 0; i < len; i++) { const r = find(i); if (!buckets[r]) buckets[r] = []; buckets[r].push(arr[i]); }
  const merged = Object.values(buckets).map(gs => {
    if (gs.length === 1) return gs[0];
    const m = { items: [], lats: [], lngs: [], prices: [], unitPrices: [], hasCommunity: gs[0].hasCommunity }; const lbls = []; const comNames = [];
    gs.forEach(g => { m.items.push(...g.items); m.lats.push(...g.lats); m.lngs.push(...g.lngs); m.prices.push(...g.prices); m.unitPrices.push(...g.unitPrices); if (g.label) lbls.push(g.label); if (g.communityName) { m.communityName = g.communityName; comNames.push(g.communityName); } });
    // 優先使用建案名作為標籤
    const uniqueComs = [...new Set(comNames)];
    if (uniqueComs.length === 1) m.label = uniqueComs[0];
    else if (uniqueComs.length > 1) m.label = uniqueComs[0] + '·' + uniqueComs.slice(1).join('·');
    else { const ul = [...new Set(lbls)]; if (ul.length <= 1) m.label = ul[0] || ''; else if (ul.length === 2) m.label = ul.join('·'); else m.label = ul[0] + '等' + ul.length + '案'; }
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

// 色彩漸變：綠→黃→紅（HSL 插值）
function priceColorGradient(value, lo, hi) {
  if (value <= 0) return '#888';
  if (value <= lo) return 'hsl(120,70%,35%)'; // 綠
  if (value >= hi) return 'hsl(0,75%,45%)';   // 紅
  // 線性插值 hue: 120(綠) → 60(黃) → 0(紅)
  const ratio = (value - lo) / (hi - lo);
  const hue = 120 - ratio * 120;
  const sat = 70 + ratio * 5;
  const light = 35 + ratio * 10;
  return `hsl(${Math.round(hue)},${Math.round(sat)}%,${Math.round(light)}%)`;
}
function getUnitPriceColor(wan) { const t = markerSettings.unitThresholds; return priceColorGradient(wan, t[0], t[2]); }
function getTotalPriceColor(wan) { const t = markerSettings.totalThresholds; return priceColorGradient(wan, t[0], t[2]); }
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
  markerClusterGroup.clearLayers();
  communityClusterGroup.clearLayers();
  const boundsArr = [], groups = buildGroups();
  groups.forEach(g => {
    const n = g.items.length;
    if (n === 0) return;
    const sortedLats = g.lats.slice().sort((a, b) => a - b), sortedLngs = g.lngs.slice().sort((a, b) => a - b), mid = Math.floor(sortedLats.length / 2);
    const lat = sortedLats[mid], lng = sortedLngs[mid];
    if (lat == null || lng == null || isNaN(lat) || isNaN(lng)) return; // Guard against Invalid LatLng
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
    marker.on('mouseover', () => onMarkerHover(marker, g));
    marker.on('mouseout', () => onMarkerUnhover());
    // Click always shows cluster list (no popup)
    marker.on('click', () => showClusterList(g.items));
    // Community markers go to non-merging group; others to merging group
    if (g.hasCommunity) {
      communityClusterGroup.addLayer(marker);
    } else {
      markerClusterGroup.addLayer(marker);
    }
    boundsArr.push([lat, lng]);
  });
  if (fitBounds && boundsArr.length > 0) map.fitBounds(boundsArr, { padding: [40, 40], maxZoom: 18 });
}

function makePopup(tx) {
  const cn = tx.community_name ? `<div style="background:var(--green-bg);padding:2px 8px;border-radius:8px;display:inline-block;color:var(--green);font-size:12px;font-weight:600;margin-bottom:4px">🏘️ ${escHtml(tx.community_name)}</div><br>` : '';
  return `<div style="font-size:13px;line-height:1.6">${cn}<b>${escHtml(tx.address)}</b><br>📅 ${tx.date || '-'} ｜ 📐 ${fmtArea(tx.area_sqm, tx.area_ping)}<br>💰 <b style="color:var(--red)">${fmtWan(tx.price)}</b> <span style="color:var(--text2)">${fmtUnitPrice(tx.unit_price_ping, tx.unit_price_sqm)}</span><br>🏢 ${tx.rooms || 0}房${tx.halls || 0}廳${tx.baths || 0}衛${tx.floor ? '｜ ' + tx.floor + 'F/' + tx.total_floors + 'F' : ''}${tx.public_ratio > 0 ? '｜ 公設' + tx.public_ratio + '%' : ''}${tx.building_type ? '<br>' + escHtml(tx.building_type) : ''}${tx.has_parking ? '<br>🚗 含車位 ' + fmtWan(tx.parking_price) : ''}${tx.note ? '<br><span style="color:var(--text3);font-size:11px">' + escHtml(tx.note) + '</span>' : ''}</div>`;
}

function selectTx(idx) {
  activeCardIdx = idx; renderResults();
  const tx = txData[idx];
  if (tx && tx.lat && tx.lng) {
    map.setView([tx.lat, tx.lng], 17);
    const findInGroup = (group) => {
      group.eachLayer(layer => {
        if (layer._groupItems && layer._groupItems.some(it => it.origIdx === idx)) {
          group.zoomToShowLayer(layer);
        }
      });
    };
    findInGroup(markerClusterGroup);
    findInGroup(communityClusterGroup);
  }
}

// ── Marker hover → 左側同步 + tooltip ──
let _markerTooltipEl = null;
function onMarkerHover(marker, group) {
  // 1. 泡泡跳動
  if (marker._icon) {
    const inner = marker._icon.firstElementChild || marker._icon;
    bounceElement(inner);
  }
  // 2. 左側列表跳到對應建案/交易（不移動地圖，避免loop）
  const cn = group.communityName || group.label || '';
  if (cn) {
    const allHeaders = document.querySelectorAll('.community-header');
    for (const h of allHeaders) {
      const nameEl = h.querySelector('.ch-name');
      if (nameEl && nameEl.textContent.trim() === cn) {
        h.scrollIntoView({ behavior: 'smooth', block: 'center' });
        h.classList.add('hover-highlight');
        break;
      }
    }
  } else if (group.items.length > 0) {
    const firstIdx = group.items[0].origIdx;
    const card = document.querySelector(`.tx-card[data-idx="${firstIdx}"]`);
    if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
  // 3. 泡泡上方顯示資訊 tooltip
  showMarkerTooltip(marker, group);
}

function onMarkerUnhover() {
  stopAllBounce();
  hideMarkerTooltip();
  document.querySelectorAll('.community-header.hover-highlight').forEach(h => h.classList.remove('hover-highlight'));
}

function formatDateStr(raw) {
  if (!raw) return '-';
  const s = String(raw).trim();
  if (s.length >= 7) {
    const rocY = parseInt(s.substring(0, s.length - 4), 10);
    const mm = s.substring(s.length - 4, s.length - 2);
    const dd = s.substring(s.length - 2);
    if (markerSettings.yearFormat === 'ce') return (rocY + 1911) + '/' + mm + '/' + dd;
    return rocY + '/' + mm + '/' + dd;
  }
  return s;
}

function fmtBuildDate(raw) {
  if (!raw) return '-';
  const s = String(raw).trim();
  if (s.length >= 5) {
    const rocY = parseInt(s.substring(0, 3), 10);
    const y = markerSettings.yearFormat === 'ce' ? rocY + 1911 : rocY;
    return y + '/' + s.substring(3, 5);
  }
  return s || '-';
}

function showMarkerTooltip(marker, group) {
  hideMarkerTooltip();
  if (!marker._icon) return;
  const items = group.items || [];
  if (items.length === 0) return;
  const label = group.communityName || group.label || '';

  // Year & floor details
  const years = items.map(({ tx }) => tx.date_raw ? String(tx.date_raw).substring(0, 3) : '').filter(Boolean);
  const uniqueYears = [...new Set(years)].sort();
  const yearRange = uniqueYears.length > 0 ? (uniqueYears.length <= 2 ? uniqueYears.join('-') : uniqueYears[0] + '-' + uniqueYears[uniqueYears.length - 1]) : '-';
  const floors = items.map(({ tx }) => tx.total_floors).filter(v => v > 0);
  const maxFloor = floors.length > 0 ? Math.max(...floors) : 0;

  // Area & Types
  const types = [...new Set(items.map(({ tx }) => tx.building_type).filter(Boolean))];
  const typeText = types.length > 0 ? types.slice(0, 2).join('/') : '-';
  const pings = items.map(({ tx }) => tx.area_ping).filter(v => v > 0);
  const avgPing = pings.length > 0 ? (pings.reduce((a, b) => a + b, 0) / pings.length).toFixed(0) : '-';

  // Completion date, Materials & Uses
  const completionDates = [...new Set(items.map(({ tx }) => tx.completion_date).filter(Boolean))];
  const buildDateText = completionDates.length > 0 ? fmtBuildDate(completionDates[0]) : '-';
  const materials = [...new Set(items.map(({ tx }) => tx.main_material).filter(Boolean))];
  const materialText = materials.length > 0 ? materials.slice(0, 2).join('/') : '-';
  const uses = [...new Set(items.map(({ tx }) => tx.main_use).filter(Boolean))];
  const useText = uses.length > 0 ? uses.slice(0, 2).join('/') : '-';

  const tip = document.createElement('div');
  tip.className = 'marker-tooltip-info';
  tip.innerHTML = `
    ${label ? `<div class="mti-name">${escHtml(label)}</div>` : ''}
    <div class="mti-row"><span>📅</span> 交易 ${yearRange}年 ｜ 完工 ${buildDateText}</div>
    ${maxFloor > 0 ? `<div class="mti-row"><span>🏢</span> ${maxFloor}樓 ｜ ${escHtml(typeText)} ${escHtml(materialText)}</div>` : `<div class="mti-row"><span>🏠</span> ${escHtml(typeText)} ${escHtml(materialText)}</div>`}
    <div class="mti-row"><span>📐</span> 均${avgPing}坪 ｜ ${escHtml(useText)}</div>
  `;
  const iconRect = marker._icon.getBoundingClientRect();
  tip.style.position = 'fixed';
  tip.style.left = (iconRect.left + iconRect.width / 2) + 'px';
  tip.style.top = (iconRect.top - 8) + 'px';
  tip.style.zIndex = '2000';
  document.body.appendChild(tip);
  _markerTooltipEl = tip;
}

function hideMarkerTooltip() {
  if (_markerTooltipEl) {
    _markerTooltipEl.remove();
    _markerTooltipEl = null;
  }
}

function addLegend() {
  const legend = L.control({ position: 'bottomleft' });
  legend.onAdd = function () {
    const div = L.DomUtil.create('div', '');
    div.innerHTML = `<div style="background:var(--card);padding:10px 12px;border-radius:var(--radius);box-shadow:var(--shadow-md);font-size:11px;line-height:1.8;min-width:170px;border:1px solid var(--border)">
      <div style="font-weight:700;margin-bottom:4px;font-size:12px">🎯 雙圈色彩圖例</div>
      <div style="font-weight:600;font-size:10px;color:var(--primary);margin-bottom:2px">● 外環＝單價/坪 ｜ ● 內圈＝總價</div>
      <div style="display:flex;align-items:center;gap:6px"><div style="width:14px;height:14px;border-radius:50%;background:linear-gradient(135deg,hsl(120,70%,35%),hsl(60,72%,40%))"></div><span>低→中（綠→黃）</span></div>
      <div style="display:flex;align-items:center;gap:6px"><div style="width:14px;height:14px;border-radius:50%;background:linear-gradient(135deg,hsl(60,72%,40%),hsl(0,75%,45%))"></div><span>中→高（黃→紅）</span></div>
      <div style="font-size:10px;color:var(--text3);margin-top:2px">單價: ${markerSettings.unitThresholds[0]}~${markerSettings.unitThresholds[2]}萬/坪<br>總價: ${markerSettings.totalThresholds[0]}~${markerSettings.totalThresholds[2]}萬</div>
      <div style="font-weight:600;margin-top:6px;font-size:10px;color:var(--text2)">📊 圈內＝近2年均價(排除特殊)</div>
    </div>`;
    L.DomEvent.disableScrollPropagation(div); L.DomEvent.disableClickPropagation(div); return div;
  };
  legend.addTo(map);
}

// ── 動態填充行政區篩選 ──
function populateDistrictFilter() {
  const sel = document.getElementById('hfDistrict');
  if (!sel) return;
  const districts = [...new Set(txData.map(tx => tx.district).filter(Boolean))].sort();
  sel.innerHTML = '<option value="">全部區域</option>';
  districts.forEach(d => {
    sel.innerHTML += `<option value="${escAttr(d)}">${escHtml(d)}</option>`;
  });
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

// ── 設定與控制面板 ──
function toggleSettings() { document.getElementById('settingsPanel').classList.toggle('show'); }

function updateThresh() {
  // 處理 unit sliders
  let um1 = parseInt(document.getElementById('unitMin').value, 10);
  let um2 = parseInt(document.getElementById('unitMax').value, 10);
  if (um1 >= um2) { um1 = Math.max(0, um2 - 5); document.getElementById('unitMin').value = um1; }
  document.getElementById('vUnitMin').textContent = um1;
  document.getElementById('vUnitMax').textContent = um2;

  // 處理 total sliders
  let tm1 = parseInt(document.getElementById('totalMin').value, 10);
  let tm2 = parseInt(document.getElementById('totalMax').value, 10);
  if (tm1 >= tm2) { tm1 = Math.max(0, tm2 - 100); document.getElementById('totalMin').value = tm1; }
  document.getElementById('vTotalMin').textContent = tm1;
  document.getElementById('vTotalMax').textContent = tm2;

  markerSettings.unitThresholds = [um1, (um1 + um2) / 2, um2];
  markerSettings.totalThresholds = [tm1, (tm1 + tm2) / 2, tm2];

  if (txData.length > 0) {
    clearTimeout(window._replotTimer);
    window._replotTimer = setTimeout(() => { plotMarkers(false); }, 300);
  }
}

function applySettings() {
  markerSettings.outerMode = document.getElementById('sOuter').value;
  markerSettings.innerMode = document.getElementById('sInner').value;
  markerSettings.showLotAddr = document.getElementById('sShowLotAddr').checked;
  markerSettings.yearFormat = document.getElementById('sYearFormat') ? document.getElementById('sYearFormat').value : 'roc';
  localStorage.setItem('markerSettings', JSON.stringify(markerSettings));
  if (txData.length > 0) plotMarkers(false);
  else doAreaSearch();
}

function loadSettings() {
  try {
    const saved = localStorage.getItem('markerSettings');
    if (saved) {
      const p = JSON.parse(saved);
      Object.assign(markerSettings, p);
    }
  } catch (e) { }
  document.getElementById('sOuter').value = markerSettings.outerMode;
  document.getElementById('sInner').value = markerSettings.innerMode;
  document.getElementById('sShowLotAddr').checked = !!markerSettings.showLotAddr;
  if (document.getElementById('sYearFormat')) document.getElementById('sYearFormat').value = markerSettings.yearFormat || 'roc';

  // Set slider values
  const ut = markerSettings.unitThresholds;
  const tt = markerSettings.totalThresholds;
  document.getElementById('unitMin').value = ut[0] || 20;
  document.getElementById('unitMax').value = ut[2] || 70;
  document.getElementById('totalMin').value = tt[0] || 500;
  document.getElementById('totalMax').value = tt[2] || 3000;
  updateThresh(); // Update labels
}

// ── 鍵盤快捷鍵 ──
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
  if (e.key === 'd' || e.key === 'D') { toggleTheme(); e.preventDefault(); }
  if (e.key === '/' || e.key === 's') { document.getElementById('searchInput').focus(); e.preventDefault(); }
});

// ── 搜此區域 Toggle ──
function toggleAreaAutoSearch(on) {
  areaAutoSearch = on;
  try { localStorage.setItem('areaAutoSearch', on ? '1' : '0'); } catch (e) { }
  updateAreaToggleState();
  if (on && map && map.getZoom() >= (markerSettings.osmZoom || 16)) {
    doAreaSearch();
  }
}

function updateAreaToggleState() {
  const label = document.getElementById('areaToggleLabel');
  if (label) label.textContent = '自動顯示建案';
}

function onMapMoveEnd() {
  if (!areaAutoSearch) return;
  if (_hoverPanSuppressed) return;  // 防止 hover panTo 觸發重新載入
  const z = map.getZoom();
  if (z < (markerSettings.osmZoom || 16)) return;
  clearTimeout(_areaSearchTimer);
  _areaSearchTimer = setTimeout(() => {
    if (_hoverPanSuppressed) return;
    if (areaAutoSearch && map.getZoom() >= (markerSettings.osmZoom || 16)) {
      doAreaSearch();
    }
  }, 800);
}

// ── 初始化 ──
document.addEventListener('DOMContentLoaded', () => {
  loadTheme(); loadSettings(); initMap();
  // 復原 auto-search toggle 狀態
  try {
    const saved = localStorage.getItem('areaAutoSearch');
    if (saved === '1') {
      areaAutoSearch = true;
      const cb = document.getElementById('areaToggle');
      if (cb) cb.checked = true;
    }
  } catch (e) { }
  updateAreaToggleState();
  window.addEventListener('resize', hideAcList);
  if (window.innerWidth <= 768) { map.on('click', () => { document.getElementById('sidebar').classList.remove('open'); }); }
});
