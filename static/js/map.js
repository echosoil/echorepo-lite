(function () {
  const mapDiv = document.getElementById('map');
  if (!mapDiv) return;

  // ---- Config & helpers ----
  const cfg = (window.ECHOREPO_CFG || {});
  const LAT_KEY = cfg.lat_col || 'GPS_lat';
  const LON_KEY = cfg.lon_col || 'GPS_long';
  const SHOULD_DROP = (k) => /_orig$/i.test(k);
  const JITTER_M = Number(cfg.jitter_m) || 1000;

  const map = L.map('map', { boxZoom: true });
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
    updateWhenZooming: false,   // fewer mid-zoom reflows
    updateWhenIdle: true,
    detectRetina: true,         // crisper tiles on HiDPI
    className: 'tiles-no-seams' // so CSS can target only tiles
  }).addTo(map);


  /** ─────────────────────────────────────────────────────────────
   *  Degree rulers (left: latitude, bottom: longitude)
   *  - No grid lines, only ticks + labels on edges
   *  - Pointer-events: none, so it never blocks map interactions
   *  - Updates on move/zoom/resize
   *  ──────────────────────────────────────────────────────────── */
  (function addDegreeRulers() {
    const container = map.getContainer();
    const overlay = document.createElement('div');
    overlay.className = 'deg-rulers';
    Object.assign(overlay.style, {
      position: 'absolute',
      left: '0', top: '0', right: '0', bottom: '0',
      pointerEvents: 'none',
      zIndex: 450   // above tiles, below most controls
    });
    container.appendChild(overlay);

    const svgNS = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(svgNS, 'svg');
    Object.assign(svg.style, {
      position: 'absolute',
      left: 0, top: 0, width: '100%', height: '100%'
    });
    overlay.appendChild(svg);

    function clear() { while (svg.firstChild) svg.removeChild(svg.firstChild); }

    function chooseStep(spanDeg) {
      // Aim for ~4–10 ticks across current span
      const steps = [30, 20, 10, 5, 2, 1, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01];
      for (const s of steps) {
        const ticks = spanDeg / s;
        if (ticks >= 4 && ticks <= 10) return s;
      }
      return steps[steps.length - 1];
    }
    function decimalsFor(step) {
      return step >= 1 ? 0 : Math.min(2, Math.max(1, Math.ceil(-Math.log10(step))));
    }
    function fmtLon(deg, d) {
      const abs = Math.abs(deg).toFixed(d);
      const hemi = deg === 0 ? '' : (deg > 0 ? 'E' : 'W');
      return `${abs}°${hemi}`;
    }
    function fmtLat(deg, d) {
      const abs = Math.abs(deg).toFixed(d);
      const hemi = deg === 0 ? '' : (deg > 0 ? 'N' : 'S');
      return `${abs}°${hemi}`;
    }
    const normLon = (x) => ((((x + 180) % 360) + 360) % 360) - 180;

    function draw() {
      clear();
      const size = map.getSize();
      const pad = 6;              // distance of labels from edges (px)
      const tick = 6;             // tick length (px)
      const font = 11;            // label font size

      const b = map.getBounds();
      const south = b.getSouth();
      const north = b.getNorth();
      let west = b.getWest();
      let east = b.getEast();
      // handle anti-meridian
      if (east < west) east += 360;

      // Steps
      const latSpan = Math.abs(north - south);
      const lonSpan = Math.abs(east - west);
      const latStep = chooseStep(latSpan || 180);
      const lonStep = chooseStep(lonSpan || 360);
      const latDec = decimalsFor(latStep);
      const lonDec = decimalsFor(lonStep);

      // LAT ticks on left edge
      const latStart = Math.ceil(south / latStep) * latStep;
      for (let lat = latStart; lat <= north + 1e-9; lat += latStep) {
        const pt = map.latLngToContainerPoint([lat, (west + east) / 2]);
        const y = Math.round(pt.y);
        // small tick
        const line = document.createElementNS(svgNS, 'line');
        line.setAttribute('x1', '0');
        line.setAttribute('x2', String(tick));
        line.setAttribute('y1', String(y));
        line.setAttribute('y2', String(y));
        line.setAttribute('stroke', 'rgba(0,0,0,0.55)');
        line.setAttribute('stroke-width', '1');
        svg.appendChild(line);
        // label
        const txt = document.createElementNS(svgNS, 'text');
        txt.textContent = fmtLat(lat, latDec);
        txt.setAttribute('x', String(tick + 2));
        txt.setAttribute('y', String(y + 3)); // vertical centering tweak
        txt.setAttribute('font-size', String(font));
        txt.setAttribute('font-family', 'Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif');
        txt.setAttribute('fill', 'rgba(0,0,0,0.65)');
        txt.setAttribute('paint-order', 'stroke');
        txt.setAttribute('stroke', 'white');
        txt.setAttribute('stroke-width', '3'); // halo for legibility
        svg.appendChild(txt);
      }

      // LON ticks on bottom edge
      const lonStart = Math.ceil(west / lonStep) * lonStep;
      for (let lon = lonStart; lon <= east + 1e-9; lon += lonStep) {
        const lonWrapped = normLon(lon);
        const pt = map.latLngToContainerPoint([(south + north) / 2, lonWrapped]);
        const x = Math.round(pt.x);
        // small tick
        const line = document.createElementNS(svgNS, 'line');
        line.setAttribute('x1', String(x));
        line.setAttribute('x2', String(x));
        line.setAttribute('y1', String(size.y - tick));
        line.setAttribute('y2', String(size.y));
        line.setAttribute('stroke', 'rgba(0,0,0,0.55)');
        line.setAttribute('stroke-width', '1');
        svg.appendChild(line);
        // label
        const txt = document.createElementNS(svgNS, 'text');
        txt.textContent = fmtLon(lonWrapped, lonDec);
        txt.setAttribute('text-anchor', 'middle');
        txt.setAttribute('x', String(x));
        txt.setAttribute('y', String(size.y - tick - pad));
        txt.setAttribute('font-size', String(font));
        txt.setAttribute('font-family', 'Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif');
        txt.setAttribute('fill', 'rgba(0,0,0,0.65)');
        txt.setAttribute('paint-order', 'stroke');
        txt.setAttribute('stroke', 'white');
        txt.setAttribute('stroke-width', '3');
        svg.appendChild(txt);
      }
    }

    // keep in sync with map
    map.on('moveend zoomend viewreset resize', draw);
    // draw once when tiles load
    map.whenReady(draw);
  })();
  // ── end rulers ──

  function parsePh(val){
    if (val==null) return NaN;
    const s=String(val).replace(',', '.').toLowerCase();
    const m=s.match(/(\d+(?:\.\d+)?)/);
    if(!m) return NaN;
    let v=parseFloat(m[1]);
    if(!Number.isFinite(v)) return NaN;
    return Math.min(14, Math.max(0, v));
  }
  function getPhFromProps(props){
    if(!props) return NaN;
    for (const k of ["PH_ph","ph","pH","ph_value","PH_value"]) {
      if (k in props) { const v=parsePh(props[k]); if(Number.isFinite(v)) return v; }
    }
    for (const [k,v] of Object.entries(props)) {
      const kl=k.toLowerCase(); if(kl.startsWith("photo")) continue;
      if(/\bph\b/.test(kl)){ const n=parsePh(v); if(Number.isFinite(n)) return n; }
    }
    return NaN;
  }
  function phColor(phLike){
    const v=typeof phLike==="number"?phLike:parsePh(phLike);
    if(isNaN(v)) return "#999";
    if(v<5.5) return "#d73027";
    if(v<6.5) return "#fc8d59";
    if(v<7.5) return "#fee08b";
    if(v<8.5) return "#91bfdb";
    return "#4575b4";
  }
  function fmtInt(v){ const n=Number(v); if(Number.isFinite(n)) return String(Math.trunc(n)); return (v===0||v==="0")?"0":(v??"—"); }
  function formatDate(iso){ if(!iso) return "—"; const d=new Date(iso); if(isNaN(d)) return iso; return d.toLocaleDateString(undefined,{year:'numeric',month:'short',day:'2-digit'}); }
  function formatPopup(f, isOwnerLayer){
    const p=f.properties||{}; const fmt=(v)=>(v==null||(typeof v==="string"&&v.trim()===""))?"—":v;
    const rows=[
      ['<i class="bi bi-calendar"></i> Date',formatDate(p.collectedAt)],
      ['<i class="bi bi-qr-code-scan"></i> QR code',p.QR_qrCode],
      ['<i class="bi bi-droplet-half"></i> pH',p.PH_ph],
      ['<i class="bi bi-palette"></i> Colour',p.SOIL_COLOR_color],
      ['<i class="bi bi-grid-3x3-gap"></i> Texture',p.SOIL_TEXTURE_texture],
      ['<i class="bi bi-diagram-3"></i> Structure',p.SOIL_STRUCTURE_structure],
      ['<i class="bi bi-bug"></i> Earthworms',fmtInt(p.SOIL_DIVER_earthworms)],
      ['<i class="bi bi-bag"></i> Plastic',fmtInt(p.SOIL_CONTAMINATION_plastic)],
      ['<i class="bi bi-bricks"></i> Debris',fmtInt(p.SOIL_CONTAMINATION_debris)],
      ['<i class="bi bi-exclamation-triangle"></i> Contamination',p.SOIL_CONTAMINATION_comments],
      ['<i class="bi bi-nut"></i> Metals',p.METALS_info],
    ].filter(([_,v])=>!(v==null||(typeof v==="string"&&v.trim()==="")||v==="—"));
    let html=`<div class="popup-card"><table class="table table-sm mb-2">${
      rows.map(([k,v])=>`<tr><th>${k}</th><td>${fmt(v)}</td></tr>`).join("")}</table>`;
    if(p.PHOTO_photos_1_path){ const url=String(p.PHOTO_photos_1_path);
      html+=`<a href="${url}" target="_blank" rel="noopener"><img src="${url}" alt="Sample photo"></a>`; }
    if(p.sampleId){
      html+=`<div class="mt-2"><a class="btn btn-sm btn-outline-primary"
              href="/download/sample_csv?sampleId=${encodeURIComponent(p.sampleId)}"
              target="_blank" rel="noopener"><i class="bi bi-filetype-csv"></i> Download CSV</a></div>`;
    }
    html+=`</div>`; return html;
  }

  // ---- State ----
  let ALL_HEADERS=null, userGJ, othersGJ;

  // Cluster groups (will be rebuilt on filter)
  let userCluster   = L.markerClusterGroup();
  let othersCluster = L.markerClusterGroup();

  // Rings & base layers
  const userRings=[], otherRings=[];
  let userLayer, othersLayer;
  let twoToggleControl=null;

  // Selection state
  const drawnItems = new L.FeatureGroup().addTo(map);
  let selectionLayers=[], selectionRows=[];
  let selectionButtonEl=null, clearButtonEl=null;

  // Filter state (UI elements in page)
  const phMinEl = document.getElementById('phMin');
  const phMaxEl = document.getElementById('phMax');
  const btnApplyFilter = document.getElementById('btnApplyFilter');
  const btnExportFiltered = document.getElementById('btnExportFiltered');

  // --- Active filter for use everywhere ---
  let activePhMin = null;
  let activePhMax = null;
  let filteredRows = [];

  function inRangeGiven(ph, min, max){
    if (!Number.isFinite(ph)) return (min == null && max == null); // accept unknown only when no filter
    if (min != null && ph < min) return false;
    if (max != null && ph > max) return false;
    return true;
  }
  function passesCurrentFilter(props){
    const ph = getPhFromProps(props || {});
    return inRangeGiven(ph, activePhMin, activePhMax);
  }

  function computeAllHeaders(){
    const preferred=[
      "sampleId","collectedAt","QR_qrCode","PH_ph",
      "SOIL_COLOR_color","SOIL_TEXTURE_texture","SOIL_STRUCTURE_structure",
      "SOIL_DIVER_earthworms","SOIL_CONTAMINATION_plastic","SOIL_CONTAMINATION_debris",
      "SOIL_CONTAMINATION_comments","METALS_info"
    ];
    const set=new Set(preferred);
    const add=(gj)=>(gj?.features||[]).forEach(f=>{
      const p=f.properties||{};
      Object.keys(p).forEach(k=>{ if(!SHOULD_DROP(k)) set.add(k); });
    });
    add(userGJ); add(othersGJ);
    set.add(LAT_KEY); set.add(LON_KEY);
    const rest=[...set].filter(k=>!preferred.includes(k)).sort(); ALL_HEADERS=[...preferred,...rest];
  }

  // ---- Build layers (rings + base invisible markers for selection) ----
  function buildLayers(){
    const userStyle={radius:1,weight:0,opacity:0,fillOpacity:0,interactive:false};
    const otherStyle={radius:1,weight:0,opacity:0,fillOpacity:0,interactive:false};
    const mkUser=(_f,latlng)=>L.circleMarker(latlng,userStyle);
    const mkOther=(_f,latlng)=>L.circleMarker(latlng,otherStyle);

    function makeLayer(gj, mk, isOwner, bucket){
      return L.geoJSON(gj,{
        pointToLayer:(_f,latlng)=>mk(_f,latlng),
        onEachFeature:(f,marker)=>{
          const props = f.properties || {};
          const ph    = getPhFromProps(props);
          const clr   = phColor(ph);
          const ring  = L.circle(marker.getLatLng(),{
            radius:JITTER_M, color:clr, weight:1, opacity:0.9,
            fillColor:clr, fillOpacity:0.35
          });
          ring.__props = props;           // save for filtering
          ring.bindPopup(formatPopup(f,isOwner));
          ring.addTo(map);
          bucket.push(ring);
        }
      });
    }

    userLayer   = makeLayer(userGJ, mkUser,  true,  userRings);
    othersLayer = makeLayer(othersGJ, mkOther, false, otherRings);

    // Initial clusters (unfiltered = all)
    userCluster.addLayer(userLayer);
    othersCluster.addLayer(othersLayer);
    map.addLayer(userCluster);
    map.addLayer(othersCluster);

    // Fit once
    let b=null;
    const hasUser = Array.isArray(userGJ?.features) && userGJ.features.length>0;
    const hasOthers = Array.isArray(othersGJ?.features) && othersGJ.features.length>0;
    if(hasUser) b=userCluster.getBounds();
    if(hasOthers) b=b?b.extend(othersCluster.getBounds()):othersCluster.getBounds();
    if(b&&b.isValid()) map.fitBounds(b,{padding:[20,20]}); else map.setView([50,10],4);

    addTwoToggleControl();
    addSelectionControl();
    addLegends();

    // Initialize filter counts/UI using default (no filter)
    updateFiltered();
  }

  // ---- Rebuild clusters to reflect current filter ----
  function rebuildClustersForFilter(){
    // Remove old groups from map
    if (map.hasLayer(userCluster))   map.removeLayer(userCluster);
    if (map.hasLayer(othersCluster)) map.removeLayer(othersCluster);

    // New groups
    const newUser   = L.markerClusterGroup();
    const newOthers = L.markerClusterGroup();

    const invisible = { radius:1, weight:0, opacity:0, fillOpacity:0, interactive:false };

    function addFilteredMarkers(gj, targetGroup){
      (gj?.features||[]).forEach(f=>{
        const coords = f?.geometry?.coordinates || [];
        const lon = coords[0], lat = coords[1];
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
        const props = f.properties || {};
        if (!passesCurrentFilter(props)) return;
        const m = L.circleMarker([lat, lon], invisible);
        m.feature = f; // keep reference for selection lookup if needed
        targetGroup.addLayer(m);
      });
    }

    addFilteredMarkers(userGJ, newUser);
    addFilteredMarkers(othersGJ, newOthers);

    // Swap references
    userCluster = newUser;
    othersCluster = newOthers;

    // Respect current toggle states when adding back
    const state = twoToggleControl ? twoToggleControl._getState() : {user:true, others:true};
    if (state.user)   map.addLayer(userCluster);
    if (state.others) map.addLayer(othersCluster);
  }

  // ---- Show/hide rings based on current filter + toggles ----
  function applyFilterToRings(){
    const state = twoToggleControl ? twoToggleControl._getState() : {user:true, others:true};

    function process(rings, includeGroup){
      for (const r of rings){
        const shouldShow = includeGroup && passesCurrentFilter(r.__props || {});
        if (shouldShow){
          if (!map.hasLayer(r)) r.addTo(map);
        } else {
          if (map.hasLayer(r)) map.removeLayer(r);
        }
      }
    }
    process(userRings,  state.user);
    process(otherRings, state.others);
  }

  // ---- Two checkboxes (toggle clusters + rings together) ----
  function addTwoToggleControl(){
    const state={user:true, others:true};

    function sync(){
      if(state.user){ if(!map.hasLayer(userCluster)) map.addLayer(userCluster); }
      else          { if(map.hasLayer(userCluster))  map.removeLayer(userCluster); }
      if(state.others){ if(!map.hasLayer(othersCluster)) map.addLayer(othersCluster); }
      else            { if(map.hasLayer(othersCluster))  map.removeLayer(othersCluster); }

      // rings obey both toggle and current filter
      applyFilterToRings();

      // keep selection & filtered counts in sync
      updateSelectionCount();
      updateFilteredCountsLabelOnly();
    }

    const ctl=L.control({position:'topleft'});
    ctl.onAdd=function(){
      const div=L.DomUtil.create('div','leaflet-control leaflet-bar p-2');
      div.style.background='white'; div.style.borderRadius='8px'; div.style.lineHeight='1.1';
      div.innerHTML=`
        <div class="form-check" style="margin:.1rem 0;">
          <input class="form-check-input" type="checkbox" id="togUser" checked>
          <label class="form-check-label" for="togUser">Your samples</label>
        </div>
        <div class="form-check" style="margin:.1rem 0;">
          <input class="form-check-input" type="checkbox" id="togOther" checked>
          <label class="form-check-label" for="togOther">Other samples</label>
        </div>`;
      L.DomEvent.disableClickPropagation(div);
      const cUser=div.querySelector('#togUser'), cOther=div.querySelector('#togOther');
      cUser.addEventListener('change',()=>{state.user=!!cUser.checked; sync();});
      cOther.addEventListener('change',()=>{state.others=!!cOther.checked; sync();});
      div._getState=()=>({...state}); twoToggleControl=div; return div;
    };
    ctl.addTo(map); sync();
  }

  // ---- Selection (rectangle multi-select) ----
  function addSelectionControl(){
    const ctl=L.control({position:'topright'});
    ctl.onAdd=function(){
      const div=L.DomUtil.create('div','leaflet-control leaflet-bar p-2');
      div.style.background='white'; div.style.borderRadius='8px'; div.style.lineHeight='1';
      div.innerHTML=`
        <div class="d-flex gap-2 align-items-center">
          <button type="button" class="btn btn-sm btn-primary" id="btnExportSel" disabled title="Export selected">
            Export (0)
          </button>
          <button type="button" class="btn btn-sm btn-outline-secondary" id="btnClearSel" disabled title="Clear selections">
            Clear
          </button>
        </div>`;
      L.DomEvent.disableClickPropagation(div);
      selectionButtonEl=div.querySelector('#btnExportSel');
      clearButtonEl=div.querySelector('#btnClearSel');
      selectionButtonEl.addEventListener('click',()=>{
        if(!selectionRows.length) return;
        const csv=toCsv(selectionRows); if(!csv) return;
        downloadCsv('echorepo_selection.csv', csv);
      });
      clearButtonEl.addEventListener('click', clearSelections);
      return div;
    };
    ctl.addTo(map);

    const RECT_STYLE = { color:'#0d6efd', weight:2, opacity:1, fill:true, fillOpacity:0.18 };
    const drawControl = new L.Control.Draw({
      draw: { polygon:false, polyline:false, circle:false, marker:false, circlemarker:false, rectangle:{ shapeOptions: RECT_STYLE } },
      edit: false
    });
    map.addControl(drawControl);

    map.on(L.Draw.Event.CREATED, (e) => {
      const layer = e.layer;
      if (layer.setStyle) layer.setStyle(RECT_STYLE);
      if (layer.bringToFront) layer.bringToFront();
      selectionLayers.push(layer);
      drawnItems.addLayer(layer);
      updateSelectionCount();
    });
  }
  function clearSelections(){ drawnItems.clearLayers(); selectionLayers=[]; selectionRows=[]; updateSelectionCount(); }

  function collectRowsWithinAll(){
    if(!selectionLayers.length) return [];
    const active = twoToggleControl ? twoToggleControl._getState() : {user:true, others:true};
    const rows=[], seen=new Set();
    const inAny=(ll)=>selectionLayers.some(r=>r.getBounds().contains(ll));

    function scan(layer, include){
      if(!include||!layer) return;
      layer.eachLayer(m=>{
        const ll=m.getLatLng(); if(!ll) return;
        if(!inAny(ll)) return;
        const f=m.feature||{}; const props={...(f.properties||{})};

        // Respect active pH filter
        if (!passesCurrentFilter(props)) return;

        Object.keys(props).forEach(k=>{ if(SHOULD_DROP(k)) delete props[k]; });
        props[LAT_KEY]=ll.lat; props[LON_KEY]=ll.lng;
        const key=props.sampleId||props.QR_qrCode||`${ll.lat.toFixed(6)},${ll.lng.toFixed(6)}`;
        if(seen.has(key)) return; seen.add(key); rows.push(props);
      });
    }
    scan(userLayer,   active.user);
    scan(othersLayer, active.others);
    return rows;
  }
  function updateSelectionCount(){
    if(!selectionButtonEl||!clearButtonEl) return;
    selectionRows = collectRowsWithinAll();
    const n = selectionRows.length;
    selectionButtonEl.disabled = n===0;
    selectionButtonEl.textContent = `Export (${n})`;
    clearButtonEl.disabled = selectionLayers.length===0;
  }

  // ---- Filter by pH & export ----
  function collectRowsFiltered(phMin, phMax){
    const active = twoToggleControl ? twoToggleControl._getState() : {user:true, others:true};
    const rows=[], seen=new Set();
    function inRange(ph){
      return inRangeGiven(ph, phMin, phMax);
    }
    function scan(layer, include){
      if(!include||!layer) return;
      layer.eachLayer(m=>{
        const ll=m.getLatLng(); if(!ll) return;
        const f=m.feature||{}; const props={...(f.properties||{})};
        const ph = getPhFromProps(props);
        if(!inRange(ph)) return;
        Object.keys(props).forEach(k=>{ if(SHOULD_DROP(k)) delete props[k]; });
        props[LAT_KEY]=ll.lat; props[LON_KEY]=ll.lng;
        const key=props.sampleId||props.QR_qrCode||`${ll.lat.toFixed(6)},${ll.lng.toFixed(6)}`;
        if(seen.has(key)) return; seen.add(key); rows.push(props);
      });
    }
    scan(userLayer,   active.user);
    scan(othersLayer, active.others);
    return rows;
  }

  function updateFilteredCountsLabelOnly(){
    if(!btnExportFiltered) return;
    const n = filteredRows.length || 0;
    btnExportFiltered.disabled = n===0;
    btnExportFiltered.textContent = `Export filtered (${n})`;
  }

  function updateFiltered(){
    if(!btnExportFiltered) return;

    const minV = phMinEl ? parseFloat(phMinEl.value) : NaN;
    const maxV = phMaxEl ? parseFloat(phMaxEl.value) : NaN;

    activePhMin = Number.isFinite(minV) ? minV : null;
    activePhMax = Number.isFinite(maxV) ? maxV : null;

    // Apply to rings + clusters
    applyFilterToRings();
    rebuildClustersForFilter();

    // Recompute filtered rows for the export button
    filteredRows = collectRowsFiltered(activePhMin, activePhMax);
    updateFilteredCountsLabelOnly();

    // Refresh selection count since selection respects the active filter
    updateSelectionCount();
  }

  btnApplyFilter?.addEventListener('click', updateFiltered);
  btnExportFiltered?.addEventListener('click', ()=>{
    if(!filteredRows.length) return;
    const csv = toCsv(filteredRows);
    if(!csv) return;
    downloadCsv('echorepo_filtered.csv', csv);
  });
  [phMinEl, phMaxEl].forEach(el=> el?.addEventListener('keydown', (e)=>{
    if(e.key==='Enter'){ e.preventDefault(); updateFiltered(); }
  }));

  // ---- CSV helpers ----
  function toCsv(rows){
    if(!rows.length) return "";
    const headers=ALL_HEADERS||Object.keys(rows[0]);
    const esc=(v)=>{ if(v==null) return ""; const s=String(v); return /[",\n]/.test(s)?`"${s.replace(/"/g,'""')}"`:s; };
    const lines=[headers.map(esc).join(",")];
    for(const r of rows) lines.push(headers.map(h=>esc(r[h])).join(","));
    return lines.join("\n");
  }
  function downloadCsv(filename, csv){
    const blob=new Blob([csv],{type:"text/csv;charset=utf-8;"}), url=URL.createObjectURL(blob);
    const a=document.createElement("a"); a.href=url; a.download=filename; document.body.appendChild(a); a.click();
    setTimeout(()=>{ document.body.removeChild(a); URL.revokeObjectURL(url); },0);
  }

  function addLegends(){
    const legend=L.control({position:'bottomleft'});
    legend.onAdd=function(){
      const div=L.DomUtil.create('div','leaflet-control leaflet-bar p-2');
      div.style.background='white'; div.style.borderRadius='8px'; div.style.lineHeight='1.1';
      div.innerHTML=`<div style="display:flex;align-items:center;gap:.4rem;margin:.2rem 0;">
        <svg width="14" height="14" aria-hidden="true"><circle cx="7" cy="7" r="5" stroke="#333" fill="none"/></svg>
        <span>Privacy radius (~±${Math.round(JITTER_M/1000)} km)</span></div>`;
      return div;
    }; legend.addTo(map);

    const phLegend=L.control({position:'bottomright'});
    phLegend.onAdd=function(){
      const div=L.DomUtil.create('div','leaflet-control leaflet-bar p-2');
      div.style.background='white'; div.style.borderRadius='8px'; div.style.lineHeight='1.2';
      div.innerHTML=`
        <div class="fw-semibold mb-1">Soil pH</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#d73027;">●</span> Acidic (≤5.5)</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#fc8d59;">●</span> Slightly acidic (5.5–6.5)</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#fee08b;">●</span> Neutral (6.5–7.5)</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#91bfdb;">●</span> Slightly alkaline (7.5–8.5)</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#4575b4;">●</span> Alkaline (≥8.5)</div>`;
      return div;
    }; phLegend.addTo(map);
  }

  // ---- Boot ----
  Promise.all([
    fetch('/api/user_geojson',   { credentials:'same-origin' }).then(r=>r.json()),
    fetch('/api/others_geojson', { credentials:'same-origin' }).then(r=>r.json())
  ]).then(([u,o])=>{
    userGJ=u; othersGJ=o;
    computeAllHeaders(); buildLayers();
  }).catch(err=>{
    console.warn('GeoJSON fetch failed:', err);
    mapDiv.style.display='none';
  });
})();
