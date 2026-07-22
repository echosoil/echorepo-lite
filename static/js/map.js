(function () {
  const mapDiv = document.getElementById('map');
  if (!mapDiv) return;

  // ---------- i18n helpers ----------
  const UI_LANG = document.documentElement.lang || 'en';

  // ---- Config & helpers ----
  const URL_PARAMS = new URLSearchParams(window.location.search);

  const FORCE_DESKTOP_MODE = [
    '1',
    'true'
  ].includes(
    String(
      URL_PARAMS.get('desktop') || ''
    ).toLowerCase()
  );

  const MOBILE_COMPACT_MODE =
    !FORCE_DESKTOP_MODE &&
    window.matchMedia(
      '(max-width: 767.98px)'
    ).matches;

  const cfg = window.ECHOREPO_CFG || {};
  const LAT_KEY =
    cfg.lat_col || 'GPS_lat';

  const LON_KEY =
    cfg.lon_col || 'GPS_long';

  const SHOULD_DROP =
    key => /_orig$/i.test(key);

  const JITTER_M =
    Number(cfg.jitter_m) || 1000;

  const DISABLE_SELECTION_TOOLS = !!cfg.disable_selection_tools || MOBILE_COMPACT_MODE;

  const DISABLE_SAMPLE_TOGGLES = !!cfg.disable_sample_toggles;

  const SINGLE_SAMPLE_ID = (
    URL_PARAMS.get('sample_id') ||
    URL_PARAMS.get('sampleId') ||
    URL_PARAMS.get('qr') ||
    ''
  ).trim();

  const SINGLE_SAMPLE_MODE =
    URL_PARAMS.get('single') === '1' ||
    URL_PARAMS.get('single') === 'true';

  function getSampleIdFromProps(props) {
    props = props || {};

    return String(
      props.QR_qrCode ||
      props.qr_code ||
      props.qr ||
      props.sample_id ||
      props.sampleId ||
      props.Sample ||
      ''
    ).trim();
  }

  function isRequestedSingleSample(props) {
    if (!SINGLE_SAMPLE_MODE || !SINGLE_SAMPLE_ID) return true;

    return getSampleIdFromProps(props).toUpperCase() === SINGLE_SAMPLE_ID.toUpperCase();
  }
  const SHOW_WRONG_IN_SINGLE =
    SINGLE_SAMPLE_MODE &&
    (
      URL_PARAMS.get('show_wrong') === '1' ||
      URL_PARAMS.get('show_wrong') === 'true'
    );

  // If true, samples flagged by pull_and_enrich as wrong_coordinates
  // are kept in the data but hidden from the map, clusters, selection and export.
  const HIDE_WRONG_COORDINATES = cfg.hide_wrong_coordinates !== false;

  const map = L.map('map', {
    minZoom: 4,
    maxZoom: 15,
    preferCanvas: true
  });

  if (!DISABLE_SELECTION_TOOLS) {
    map.createPane('selectionPane');
    map.getPane('selectionPane').style.zIndex = 650;
  }

  // Highlighting is independent of the drawing/selection tools.
  map.createPane('selectedSamplePane');

  Object.assign(
    map.getPane('selectedSamplePane').style,
    {
      zIndex: 675,
      pointerEvents: 'none'
    }
  );

  // Default fallback view
  let initialView = { lat: 50, lng: 10, z: 5 };

  // If URL has lat/lng/z, use it
  (function initViewFromUrl() {
    const params = new URLSearchParams(window.location.search);

    const lat = parseFloat(params.get('lat'));
    const lng = parseFloat(params.get('lng'));
    const z = parseInt(params.get('z'), 10);

    if (Number.isFinite(lat) && Number.isFinite(lng) && Number.isFinite(z)) {
      initialView = { lat, lng, z };
    }
  })();

  map.setView([initialView.lat, initialView.lng], initialView.z);

  map.on('popupopen', () => {
    popupIsOpen = true;
  });

  map.on('popupclose', () => {
    popupIsOpen = false;
  });

  // ---- Invalidate size after short delay (fixes display issues when map is in a hidden tab or collapsible) ----
  setTimeout(() => map.invalidateSize(true), 0);
  setTimeout(() => map.invalidateSize(true), 300);

  window.addEventListener('resize', () => {
    map.invalidateSize(true);
  });

  // ---- Debounced URL update on map move ----
  let _viewUrlTimer = null;

  function updateURLFromViewDebounced() {
    clearTimeout(_viewUrlTimer);
    _viewUrlTimer = setTimeout(updateURLFromView, 150);
  }

  map.on('moveend zoomend', updateURLFromViewDebounced);

  document.getElementById('btnCopyView')?.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(location.href);
      alert(T('viewLinkCopied', {}, 'Link copied to clipboard'));
    } catch {
      prompt(
        T('copyThisLink', {}, 'Copy this link:'),
        location.href
      );
    }
  });

  // ---- Country name i18n (browser-native) ----
  const countryNames = (() => {
    try {
      const lang =
        document.documentElement.lang ||
        (navigator.language || 'en').split('-')[0];

      return new Intl.DisplayNames([lang], { type: 'region' });
    } catch (e) {
      return null;
    }
  })();

  let activeCountry = null;
  let activeDateFrom = null; // YYYY-MM-DD
  let activeDateTo = null; // YYYY-MM-DD

  const dateFromEl = document.getElementById('dateFrom');
  const dateToEl = document.getElementById('dateTo');

  function inDateRange(ts) {
    // If no date filter is active, do not exclude rows with missing dates
    if (!activeDateFrom && !activeDateTo) return true;

    // If date filter is active but sample has no date, exclude it
    if (!ts) return false;

    const d = String(ts).slice(0, 10); // YYYY-MM-DD

    if (activeDateFrom && d < activeDateFrom) return false;
    if (activeDateTo && d > activeDateTo) return false;

    return true;
  }

  function populateCountryFilter() {
    const sel = document.getElementById('countryFilter');
    if (!sel) return;

    const current = sel.value || activeCountry || '';

    sel.innerHTML = '';

    const allOpt = document.createElement('option');
    allOpt.value = '';
    allOpt.textContent = T('anyCountry', {}, 'Any country');
    sel.appendChild(allOpt);

    const counts = {};

    for (const ring of window.__echomapIndex.values()) {
      const p = ring.__props || {};
      if (HIDE_WRONG_COORDINATES && hasWrongCoordinates(p)) continue;

      const cc = p.country_code ? String(p.country_code).toUpperCase() : null;
      if (!cc) continue;

      counts[cc] = (counts[cc] || 0) + 1;
    }

    Object.keys(counts).sort().forEach(cc => {
      const opt = document.createElement('option');
      opt.value = cc;

      const label = countryNames?.of(cc) || cc;
      opt.textContent = `${label} (${counts[cc]})`;

      if (cc === current) opt.selected = true;

      sel.appendChild(opt);
    });
  }

  function updateURLFromFilters() {
    // START from current URL, not from scratch
    const params = new URLSearchParams(window.location.search);

    // country
    if (activeCountry) params.set('country', activeCountry);
    else params.delete('country');

    // pH range
    if (activePhMin != null) params.set('ph_min', activePhMin);
    else params.delete('ph_min');

    if (activePhMax != null) params.set('ph_max', activePhMax);
    else params.delete('ph_max');

    // date range
    if (activeDateFrom) params.set('date_from', activeDateFrom);
    else params.delete('date_from');

    if (activeDateTo) params.set('date_to', activeDateTo);
    else params.delete('date_to');

    // map view
    const center = map.getCenter();
    params.set('lat', center.lat.toFixed(5));
    params.set('lng', center.lng.toFixed(5));
    params.set('z', map.getZoom());

    // Build new URL
    const newUrl =
      params.toString()
        ? `${location.pathname}?${params.toString()}`
        : location.pathname;

    // Avoid spamming history if nothing changed
    const old = window.location.search.replace(/^\?/, '');
    if (old === params.toString()) return;

    // Update URL without reloading
    history.replaceState({}, '', newUrl);
  }

  function updateURLFromView() {
    const params = new URLSearchParams(location.search);

    const center = map.getCenter();
    params.set('lat', center.lat.toFixed(5));
    params.set('lng', center.lng.toFixed(5));
    params.set('z', map.getZoom());

    history.replaceState({}, '', `${location.pathname}?${params.toString()}`);
  }


  function getBoundsForCountry(countryCode) {
    let bounds = null;

    for (const ring of window.__echomapIndex.values()) {
      const p = ring.__props || {};
      if (HIDE_WRONG_COORDINATES && hasWrongCoordinates(p)) continue;
      if (
        String(p.country_code || '').toUpperCase() !==
        String(countryCode || '').toUpperCase()
      ) {
        continue;
      }

      const ll = ring.getLatLng?.();
      if (!ll) continue;

      if (!bounds) {
        bounds = L.latLngBounds(ll, ll);
      } else {
        bounds.extend(ll);
      }
    }

    return bounds;
  }

  function initFiltersFromUrl() {
    const params = new URLSearchParams(window.location.search);

    // ---- pH ----
    const phMin = params.get('ph_min');
    const phMax = params.get('ph_max');

    activePhMin = Number.isFinite(parseFloat(phMin)) ? parseFloat(phMin) : null;
    activePhMax = Number.isFinite(parseFloat(phMax)) ? parseFloat(phMax) : null;

    // ---- country ----
    activeCountry = params.get('country') || null;

    // ---- date range ----
    activeDateFrom = params.get('date_from') || null;
    activeDateTo = params.get('date_to') || null;
  }

  function syncFiltersToUI() {
    if (phMinEl) phMinEl.value = activePhMin ?? '';
    if (phMaxEl) phMaxEl.value = activePhMax ?? '';

    if (dateFromEl) dateFromEl.value = activeDateFrom ?? '';
    if (dateToEl) dateToEl.value = activeDateTo ?? '';

    const sel = document.getElementById('countryFilter');
    if (sel) sel.value = activeCountry ?? '';
  }

  // 👇 Expose map + global index + "show" helper
  window.__echomap = map;
  window.__echomapIndex = new Map();

  function showIndexedSample(sampleId, opts = {}) {
    const id = String(sampleId || '').trim();
    if (!id) return false;

    const candidates = [
      id,
      id.toUpperCase(),
      id.toLowerCase()
    ];

    let ring = null;

    for (const key of candidates) {
      ring = window.__echomapIndex.get(key);
      if (ring) break;
    }

    if (!ring) {
      // Last-resort case-insensitive scan.
      const wanted = id.toUpperCase();
      for (const [key, value] of window.__echomapIndex.entries()) {
        if (String(key).toUpperCase() === wanted) {
          ring = value;
          break;
        }
      }
    }

    if (!ring) return false;

    const allowWrongCoordinates =
      !!opts?.includeWrong ||
      SHOW_WRONG_IN_SINGLE;

    if (
      HIDE_WRONG_COORDINATES &&
      hasWrongCoordinates(ring.__props || {}) &&
      !allowWrongCoordinates
    ) {
      return false;
    }

    const ll = ring.getLatLng ? ring.getLatLng() : null;
    if (!ll) return false;

    // Any request to focus a sample is placed inside the ring range.
    // For example, a requested zoom of 15 becomes zoom 12.
    const targetZoom = resolveSampleTargetZoom(opts);

    // This setView emits moveend/zoomend. Prevent the automatic
    // bbox reload from immediately replacing the focused sample.
    suppressBboxReloadUntil = Date.now() + 1500;

    let popupOpened = false;

    function finishShowingSample() {
      if (popupOpened) return;

      popupOpened = true;

      refreshRingsVisibilityByZoom();

      activateSelectedSample(
        ring,
        id,
        {
          highlight:
            opts.highlight !== false,

          openPopup:
            opts.openPopup !== false
        }
      );
    }

    map.once('moveend', finishShowingSample);

    map.setView(ll, targetZoom, {
      animate: true
    });

    // Fallback for cases where setView does not emit moveend,
    // for example when the map is already at the target location.
    setTimeout(finishShowingSample, 450);

    return true;
  }

  window.__echomapShow = async function (sampleId, opts = {}) {
    const id = String(sampleId || '').trim();

    opts = {
      highlight: true,
      openPopup: true,
      ...opts
    };

    if (!id) return false;

    // Fast path: sample is already in the current dynamic map window.
    if (showIndexedSample(id, opts)) return true;

    // Dynamic map loading means the sample may not be in the current bbox.
    // Fetch this exact sample, rebuild the map around it, then try again.
    if (opts.fetchIfMissing === false) return false;

    try {
      const loaded = await loadSingleSampleForMap(id, opts);
      if (!loaded) return false;
      return showIndexedSample(id, opts);
    } catch (err) {
      console.warn('Could not dynamically load sample for map:', id, err);
      return false;
    }
  };

  // Inject CSS once for scrollable popups
  (function ensureMapCSS() {
    if (document.getElementById('echoMapCSS')) return;

    const style = document.createElement('style');
    style.id = 'echoMapCSS';
    style.textContent = `
      .leaflet-popup.echo-popup {
        max-width: 420px;
      }

      .leaflet-popup-content {
        margin: 8px 12px;
      }

      .leaflet-popup-content .popup-scroll {
        max-height: 360px;
        overflow: auto;
      }

      .leaflet-popup-content .popup-table th {
        white-space: nowrap;
        vertical-align: top;
        padding-right: .5rem;
      }

      .leaflet-popup-content .popup-table td {
        word-break: break-word;
      }

      .leaflet-popup-content .popup-biodiversity {
        margin-top: 0.75rem;
      }

      .leaflet-popup-content .popup-bio-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.5rem;
      }

      .leaflet-popup-content .popup-bio-item {
        min-width: 0;
      }

      .leaflet-popup-content .popup-bio-item img {
        width: 100%;
        height: 105px;
        object-fit: contain;
        display: block;
        border: 1px solid #ddd;
        border-radius: 6px;
        background: #fff;
      }

      .leaflet-popup-content .popup-bio-item .small {
        line-height: 1.15;
        white-space: normal;
      }

      .leaflet-popup-content .popup-photo img {
        width: 100%;
        height: auto;
        max-height: 180px;
        display: block;
        object-fit: cover;
        border-radius: 6px;
      }
      .leaflet-popup.echo-popup
      .leaflet-popup-tip-container {
        display: none;
      }
      .leaflet-popup.echo-popup
      .leaflet-popup-content-wrapper {
        border: 1px solid rgba(0, 0, 0, 0.12);
        box-shadow:
          0 8px 28px rgba(0, 0, 0, 0.24);
      }
      .echo-map-loader-overlay {
        position: absolute;
        inset: 0;
        z-index: 1200;
        display: none;
        align-items: center;
        justify-content: center;
        pointer-events: none;
        background: rgba(255, 255, 255, 0.35);
        backdrop-filter: blur(2px);
      }

      .echo-map-loader-overlay.is-visible {
        display: flex;
      }

      .echo-loader-box {
        min-width: 280px;
        max-width: 90%;
        padding: 1.35rem 1.6rem;
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.96);
        box-shadow: 0 14px 40px rgba(0, 0, 0, 0.22);
        text-align: center;
        font-size: 1.1rem;
        font-weight: 700;
        color: #263128;
      }

      .echo-spinner {
        width: 52px;
        height: 52px;
        margin: 0 auto 0.85rem auto;
        border: 6px solid rgba(0,0,0,0.13);
        border-top-color: rgba(0,0,0,0.68);
        border-radius: 50%;
        animation: echo-spin 0.8s linear infinite;
      }

      .echo-loader-subtext {
        margin-top: 0.35rem;
        font-size: 0.85rem;
        font-weight: 400;
        color: #6c757d;
      }
      
      .echo-selection-tool-symbol {
        display: inline-block;
        width: 14px;
        height: 14px;
        margin: 0 3px;
        vertical-align: -2px;
        border: 2px solid #111;
        background: #111;
        border-radius: 2px;
      }

      .leaflet-control-layers-expanded {
        border-radius: 10px;
        padding: 8px 10px;
        font-size: 0.9rem;
      }

      .leaflet-control-layers-base label {
        margin-bottom: 3px;
      }

      .leaflet-popup-content .popup-help-btn {
        border: 0;
        background: transparent;
        padding: 0;
        margin-left: .35rem;
        line-height: 1;
        color: #6c757d;
        vertical-align: middle;
      }

      .leaflet-popup-content .popup-help-btn:hover {
        color: #0d6efd;
      }
      .echo-high-zoom-pin-icon {
        background: transparent !important;
        border: 0 !important;
      }

      .echo-high-zoom-pin {
        display: block;
        color: var(--echo-pin-color, #777);
        font-size: 25px;
        line-height: 1;
        text-align: center;
        filter:
          drop-shadow(0 0 1px #fff)
          drop-shadow(0 1px 2px rgba(0, 0, 0, 0.65));
        transform-origin: 50% 100%;
        transition: transform 100ms ease;
      }

      .echo-high-zoom-pin-icon:hover .echo-high-zoom-pin {
        transform: scale(1.18);
      }
      .echo-selected-sample-icon {
        background: transparent !important;
        border: 0 !important;
        overflow: visible !important;
      }

      .echo-selected-sample-highlight {
        position: relative;
        display: block;
        width: 56px;
        height: 60px;
      }

      .echo-selected-sample-halo {
        position: absolute;
        left: 50%;
        bottom: 7px;
        width: 28px;
        height: 28px;

        border: 4px solid #ffb000;
        border-radius: 50%;

        background: rgba(255, 193, 7, 0.25);

        box-shadow:
          0 0 0 3px rgba(255, 255, 255, 0.95),
          0 0 18px 8px rgba(255, 176, 0, 0.8);

        transform: translate(-50%, 50%);

        animation:
          echo-selected-pulse 1.35s ease-out infinite;
      }

      .echo-selected-sample-pin {
        position: absolute;
        left: 50%;
        bottom: 3px;

        display: block;

        color: var(--echo-selected-color, #ff9800);
        font-size: 39px;
        line-height: 1;

        transform: translateX(-50%);
        transform-origin: 50% 100%;

        filter:
          drop-shadow(0 0 3px #fff)
          drop-shadow(0 0 5px #fff)
          drop-shadow(0 3px 4px rgba(0, 0, 0, 0.8));

        animation:
          echo-selected-bob 1.1s ease-in-out infinite;
      }

      @keyframes echo-selected-pulse {
        0% {
          opacity: 1;
          transform:
            translate(-50%, 50%)
            scale(0.55);
        }

        75%,
        100% {
          opacity: 0;
          transform:
            translate(-50%, 50%)
            scale(1.65);
        }
      }

      @keyframes echo-selected-bob {
        0%,
        100% {
          transform:
            translateX(-50%)
            translateY(0);
        }

        50% {
          transform:
            translateX(-50%)
            translateY(-5px);
        }
      }

      @media (prefers-reduced-motion: reduce) {
        .echo-selected-sample-halo,
        .echo-selected-sample-pin {
          animation: none;
        }
      }
      @keyframes echo-spin {
        to {
          transform: rotate(360deg);
        }
      }
    `;
    document.head.appendChild(style);
  })();

  function refreshI18NTexts() {
    // Export selection button
    if (selectionButtonEl) {
      const n = selectionRows?.length || 0;
      selectionButtonEl.textContent =
        `${T('exportSelection', {}, 'Export selection')} (${n})`;
    }

    // Export filtered button
    if (btnExportFiltered) {
      const n = filteredRows?.length || 0;
      btnExportFiltered.textContent =
        T('exportFiltered', { n }, `Export filtered (${n})`);
    }

    // Draw tool hint (rectangle)
    if (window.__echodraw) {
      try {
        const rect =
          window.__echodraw._toolbars.draw._modes.rectangle?.handler;
        if (rect) {
          rect._endLabelText =
            T('releaseToFinish', {}, 'Release mouse to finish drawing.');
        }
      } catch (_) { }
    }
  }


  // --- Metals cleaner: drop oxides + round to 2 sig figs ---
  const OXIDES = new Set(["MN2O3", "AL2O3", "CAO", "FE2O3", "MGO", "SIO2", "P2O5", "TIO2", "K2O", "SO3"]);

  function roundSigStr(n, sig = 2) {
    const v = Number(n);
    if (!Number.isFinite(v) || v === 0) return "0";

    const exp = Math.floor(Math.log10(Math.abs(v)));
    const dec = sig - 1 - exp;

    if (dec >= 0) {
      let s = v.toFixed(dec);
      if (s.includes(".")) {
        // Remove trailing zeros after the decimal point
        s = s.replace(/0+$/, "").replace(/\.$/, "");
      }
      return s;
    } else {
      const f = Math.pow(10, -dec);
      const rounded = Math.round(v / f) * f;
      return String(rounded);
    }
  }

  /** Accepts "PARAM=VAL [UNIT]" separated by ";", "<br>", or newlines */
  function cleanMetalsInfo(raw) {
    if (raw == null) return "";

    const pieces = String(raw)
      .replace(/<br\s*\/?>/gi, "\n")
      .split(/[;\n]+/)
      .map(s => s.trim())
      .filter(Boolean);

    const out = [];

    for (const tok of pieces) {
      const eqPos = tok.indexOf("=");

      // If the token has no "=", keep it instead of silently losing it.
      if (eqPos < 0) {
        out.push(escapeHtml(tok));
        continue;
      }

      const left = tok.slice(0, eqPos).trim();
      const right = tok.slice(eqPos + 1).trim();

      if (!left) continue;

      const nameNorm = left.replace(/\s+/g, "").toUpperCase();

      // Drop oxide rows, but not elemental Ca.
      // Note: CAO is oxide; CA is element and will be kept.
      if (OXIDES.has(nameNorm)) continue;

      const m = right.match(/^([+-]?(?:\d+(?:[.,]\d*)?|[.,]\d+)(?:[eE][+-]?\d+)?)(?:\s+(.*))?$/);

      let valueHtml = "";
      let unitHtml = "";

      if (m) {
        const rawNum = m[1];
        const unit = (m[2] || "").trim();
        const num = Number(String(rawNum).replace(",", "."));

        if (Number.isFinite(num) && num === 0) {
          valueHtml = escapeHtml(T('notAvailable', {}, 'Not available'));
          unitHtml = "";
        } else if (Number.isFinite(num)) {
          valueHtml = escapeHtml(roundSigStr(num, 2));
          unitHtml = unit ? " " + escapeHtml(unit) : "";
        } else {
          valueHtml = escapeHtml(right);
        }
      } else {
        valueHtml = escapeHtml(right);
      }

      out.push(`${escapeHtml(left)} = ${valueHtml}${unitHtml}`);
    }

    return out.join("<br>");
  }

  // ---- Base map layers ----
  const streetLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
    updateWhenZooming: false,
    updateWhenIdle: true,
    detectRetina: true,
    className: 'tiles-no-seams'
  });

  const satelliteLayer = L.tileLayer(
    'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    {
      maxZoom: 19,
      attribution: 'Tiles &copy; Esri',
      updateWhenZooming: false,
      updateWhenIdle: true,
      detectRetina: true
    }
  );

  // Default base layer
  streetLayer.addTo(map);

  // Leaflet base-layer switcher
  L.control.layers(
    {
      [T('streetMap', {}, 'Street map')]: streetLayer,
      [T('satellite', {}, 'Satellite')]: satelliteLayer
    },
    null,
    {
      position: 'topright',
      collapsed: false
    }
  ).addTo(map);

  /** ─────────────────────────────────────────────────────────────
   *  Degree rulers (left: latitude, bottom: longitude) — ticks only
   *  ──────────────────────────────────────────────────────────── */
  (function addDegreeRulers() {
    const container = map.getContainer();
    const overlay = document.createElement('div');
    overlay.className = 'deg-rulers';
    Object.assign(overlay.style, {
      position: 'absolute',
      left: '0', top: '0', right: '0', bottom: '0',
      pointerEvents: 'none',
      zIndex: 450
    });
    container.appendChild(overlay);

    const svgNS = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(svgNS, 'svg');
    Object.assign(svg.style, { position: 'absolute', left: 0, top: 0, width: '100%', height: '100%' });
    overlay.appendChild(svg);

    function clear() { while (svg.firstChild) svg.removeChild(svg.firstChild); }

    function chooseStep(spanDeg) {
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
      if (map.getZoom() < 6) {
        clear();
        return;
      }
      const size = map.getSize();
      const pad = 6, tick = 6, font = 11;

      const b = map.getBounds();
      const south = b.getSouth();
      const north = b.getNorth();
      let west = b.getWest();
      let east = b.getEast();
      if (east < west) east += 360;

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
        const line = document.createElementNS(svgNS, 'line');
        line.setAttribute('x1', '0');
        line.setAttribute('x2', String(tick));
        line.setAttribute('y1', String(y));
        line.setAttribute('y2', String(y));
        line.setAttribute('stroke', 'rgba(0,0,0,0.55)');
        line.setAttribute('stroke-width', '1');
        line.setAttribute('shape-rendering', 'crispEdges');
        svg.appendChild(line);

        const txt = document.createElementNS(svgNS, 'text');
        txt.textContent = fmtLat(lat, latDec);
        txt.setAttribute('x', String(tick + 2));
        txt.setAttribute('y', String(y + 3));
        txt.setAttribute('font-size', String(font));
        txt.setAttribute(
          'font-family',
          'Satoshi, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif'
        );
        txt.setAttribute('fill', 'rgba(0,0,0,0.65)');
        txt.setAttribute('paint-order', 'stroke');
        txt.setAttribute('stroke', 'white');
        txt.setAttribute('stroke-width', '3');
        svg.appendChild(txt);
      }

      // LON ticks on bottom edge
      const lonStart = Math.ceil(west / lonStep) * lonStep;
      for (let lon = lonStart; lon <= east + 1e-9; lon += lonStep) {
        const lonWrapped = normLon(lon);
        const pt = map.latLngToContainerPoint([(south + north) / 2, lonWrapped]);
        const x = Math.round(pt.x);

        const line = document.createElementNS(svgNS, 'line');
        line.setAttribute('x1', String(x));
        line.setAttribute('x2', String(x));
        line.setAttribute('y1', String(size.y - tick));
        line.setAttribute('y2', String(size.y));
        line.setAttribute('stroke', 'rgba(0,0,0,0.55)');
        line.setAttribute('stroke-width', '1');
        line.setAttribute('shape-rendering', 'crispEdges');
        svg.appendChild(line);

        const txt = document.createElementNS(svgNS, 'text');
        txt.textContent = fmtLon(lonWrapped, lonDec);
        txt.setAttribute('text-anchor', 'middle');
        txt.setAttribute('x', String(x));
        txt.setAttribute('y', String(size.y - tick - pad));
        txt.setAttribute('font-size', String(font));
        txt.setAttribute(
          'font-family',
          'Satoshi, system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif'
        );
        txt.setAttribute('fill', 'rgba(0,0,0,0.65)');
        txt.setAttribute('paint-order', 'stroke');
        txt.setAttribute('stroke', 'white');
        txt.setAttribute('stroke-width', '3');
        svg.appendChild(txt);
      }
    }

    map.on('moveend zoomend viewreset resize', draw);
    map.whenReady(draw);
  })();
  // ── end rulers ──

  function parsePh(val) {
    if (val == null) return NaN;
    const s = String(val).replace(',', '.').toLowerCase();
    const m = s.match(/(\d+(?:\.\d+)?)/);
    if (!m) return NaN;
    let v = parseFloat(m[1]);
    if (!Number.isFinite(v)) return NaN;
    return Math.min(14, Math.max(0, v));
  }
  function getPhFromProps(props) {
    if (!props) return NaN;
    for (const k of ["PH_ph", "ph", "pH", "ph_value", "PH_value"]) {
      if (k in props) { const v = parsePh(props[k]); if (Number.isFinite(v)) return v; }
    }
    for (const [k, v] of Object.entries(props)) {
      const kl = k.toLowerCase(); if (kl.startsWith("photo")) continue;
      if (/\bph\b/.test(kl)) { const n = parsePh(v); if (Number.isFinite(n)) return n; }
    }
    return NaN;
  }
  function phColor(phLike) {
    const v = typeof phLike === "number" ? phLike : parsePh(phLike);
    if (isNaN(v)) return "#999";
    if (v < 5.5) return "#d73027";
    if (v < 6.5) return "#fc8d59";
    if (v < 7.5) return "#fee08b";
    if (v < 8.5) return "#91bfdb";
    return "#4575b4";
  }
  function fmtInt(v) { const n = Number(v); if (Number.isFinite(n)) return String(Math.trunc(n)); return (v === 0 || v === "0") ? "0" : (v ?? "—"); }
  function formatDate(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleDateString(UI_LANG, { year: 'numeric', month: 'short', day: '2-digit' });
  }

  function T(key, vars = {}, defaultText) {
    const I = window.I18N || {};
    const labels = I.labels || {};
    const byMsgid = I.by_msgid || {};

    let raw =
      (key != null && Object.prototype.hasOwnProperty.call(labels, key))
        ? labels[key]
        : (defaultText != null && Object.prototype.hasOwnProperty.call(byMsgid, defaultText))
          ? byMsgid[defaultText]
          : (defaultText != null ? defaultText : key);

    let out = String(raw);

    out = out.replace(/\{([A-Za-z0-9_]+)\}/g, (_, k) =>
      Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : `{${k}}`
    );

    out = out.replace(/%\(([A-Za-z0-9_]+)\)s/g, (_, k) =>
      Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : `%(${k})s`
    );

    return out;
  }

  window.T = T;

  function elementalConcentrationsHeaderHtml() {
    const helpText = T(
      'elementalConcentrationsHelp',
      {},
      'Percentage values (%) can be converted to mg/kg by multiplying by 10000.'
    );

    const ariaLabel = T(
      'unitConversionHelp',
      {},
      'Unit conversion help'
    );

    return `
      <i class="bi bi-nut"></i>
      ${T('elementalConcentrations', {}, 'Elemental concentrations')}
      <button
        type="button"
        class="popup-help-btn"
        data-bs-toggle="tooltip"
        data-bs-container="body"
        data-bs-placement="top"
        title="${escapeHtml(helpText)}"
        aria-label="${escapeHtml(ariaLabel)}">
        <i class="bi bi-question-circle-fill"></i>
      </button>
    `;
  }

  function initPopupTooltips(popup) {
    if (!popup || !popup.getElement) return;

    const root = popup.getElement();
    if (!root) return;

    const buttons = root.querySelectorAll('.popup-help-btn');

    buttons.forEach(btn => {
      // Avoid clicks on the help icon interacting with the map/popup.
      if (!btn.dataset.echoHelpReady) {
        btn.addEventListener('click', ev => {
          ev.preventDefault();
          ev.stopPropagation();
        });
        btn.dataset.echoHelpReady = '1';
      }

      if (window.bootstrap && bootstrap.Tooltip) {
        bootstrap.Tooltip.getOrCreateInstance(btn, {
          container: 'body',
          trigger: 'hover focus'
        });
      }
    });
  }

  async function fetchSampleImage(sampleId) {
    if (!sampleId) return null;
    try {
      const r = await fetch(`/public/sample_image/${encodeURIComponent(sampleId)}`, {
        credentials: "same-origin"
      });
      if (!r.ok) return null;
      const j = await r.json();
      if (!j || !j.image_url) return null;
      return { url: j.image_url, desc: j.caption || "" };
    } catch (e) {
      return null;
    }
  }

  async function fetchBacterialGuildplot(sampleId) {
    if (!sampleId) return null;

    const url =
      `/storage/biodiversity/guildplots/bacteria/${encodeURIComponent(sampleId)}.png`;

    try {
      let r = await fetch(url, {
        method: "HEAD",
        credentials: "same-origin"
      });

      if (r.status === 405) {
        r = await fetch(url, {
          method: "GET",
          credentials: "same-origin"
        });
      }

      if (!r.ok) return null;

      return {
        url,
        desc: "Bacterial ecological guilds"
      };
    } catch {
      return null;
    }
  }

  async function fetchFungalGuildplot(sampleId) {
    if (!sampleId) return null;

    const url =
      `/storage/biodiversity/guildplots/fungi/${encodeURIComponent(sampleId)}.png`;

    try {
      // Prefer HEAD so we do not download the image just to check existence.
      let r = await fetch(url, {
        method: "HEAD",
        credentials: "same-origin"
      });

      // Some Flask/static routes may not support HEAD, so fallback to GET.
      if (r.status === 405) {
        r = await fetch(url, {
          method: "GET",
          credentials: "same-origin"
        });
      }

      if (!r.ok) return null;

      return {
        url,
        desc: "Fungal ecological guilds"
      };
    } catch {
      return null;
    }
  }


  async function fetchSamplePiechart(sampleId, marker = "16S", level = "Genus") {
    if (!sampleId) return null;
    try {
      const r = await fetch(
        `/public/sample_piechart/${encodeURIComponent(sampleId)}?marker=${encodeURIComponent(marker)}&level=${encodeURIComponent(level)}`,
        { credentials: "same-origin" }
      );
      if (!r.ok) return null;
      const j = await r.json();
      if (!j || !j.image_url) return null;
      return { url: j.image_url, desc: j.caption || "" };
    } catch {
      return null;
    }
  }
  function pickPhotoFromProps(props) {
    props = props || {};

    // ------------------------------------------------------------
    // 0) New/canonical/public support (single image)
    // ------------------------------------------------------------
    // If your public popup fetch puts these into properties:
    //   { image_url, image_description_en, image_description_orig }
    if (props.image_url) {
      const url = String(props.image_url).trim();
      if (url) {
        const desc = String(
          props.image_description_en || props.image_description_orig || ""
        ).trim();
        return { idx: 0, url, opt: "photo", desc };
      }
    }

    // ------------------------------------------------------------
    // 1) New/canonical/public support (array of images)
    // ------------------------------------------------------------
    // If you ever decide to ship:
    //   props.images = [{ image_url, image_description_en, ... }, ...]
    if (Array.isArray(props.images) && props.images.length) {
      const first = props.images.find(x => x && x.image_url) || props.images[0];
      if (first && first.image_url) {
        const url = String(first.image_url).trim();
        if (url) {
          const desc = String(
            first.image_description_en || first.image_description_orig || first.caption || ""
          ).trim();
          return { idx: 0, url, opt: "photo", desc };
        }
      }
    }

    // ------------------------------------------------------------
    // 2) Legacy support: PHOTO_photos_<n>_path
    // ------------------------------------------------------------
    const items = [];
    for (const [k, v] of Object.entries(props)) {
      const m = /^PHOTO_photos_(\d+)_path$/.exec(k);
      if (!m || !v) continue;
      const idx = Number(m[1]);
      const url = String(v).trim();
      if (!url) continue;

      const opt = String(props[`PHOTO_photos_${idx}_option`] || "").toLowerCase();
      const desc = String(props[`PHOTO_photos_${idx}_description`] || "").trim();
      items.push({ idx, url, opt, desc });
    }
    if (!items.length) return null;

    items.sort((a, b) => a.idx - b.idx);

    const prefer = [
      (x) => /landscape/.test(x.opt),
      (x) => /cover|banner/.test(x.opt),
      (x) => /default|main|principal/.test(x.opt),
    ];
    for (const rule of prefer) {
      const hit = items.find(rule);
      if (hit) return hit;
    }
    return items[0];
  }



  function formatPopup(f, isOwnerLayer) {
    const p = f.properties || {};
    const fmt = (v) => (v == null || (typeof v === "string" && v.trim() === "")) ? "—" : v;

    // ---- helpers: pick first non-empty ----
    const pick = (...vals) => {
      for (const v of vals) {
        if (v == null) continue;
        if (typeof v === "string") {
          const s = v.trim();
          if (s !== "") return s;
        } else {
          return v;
        }
      }
      return null;
    };

    // ---- normalize common fields (old UI schema OR canonical schema) ----
    const sampleId = pick(p.sampleId, p.sample_id, p.Sample, p.QR_qrCode);
    const dateIso = pick(p.collectedAt, p.timestamp_utc, p.date, p.collected_at);
    const qrLike = pick(p.QR_qrCode, p.qr_code, p.qr, sampleId);

    const phVal = pick(p.PH_ph, p.ph, p.pH, p.PH_value, p.ph_value);

    const soilColor = pick(p.SOIL_COLOR_color, p.soil_color, p.color);

    const texture = pick(
      p.SOIL_TEXTURE_texture,
      p.soil_texture_en, p.soil_texture_orig,
      p.texture_en, p.texture_orig,
      p.texture
    );

    const structure = pick(
      p.SOIL_STRUCTURE_structure,
      p.soil_structure_en, p.soil_structure_orig,
      p.structure_en, p.structure_orig,
      p.structure
    );

    const earthworms = pick(p.SOIL_DIVER_earthworms, p.earthworms_count, p.earthworms);
    const plastic = pick(p.SOIL_CONTAMINATION_plastic, p.contamination_plastic, p.plastic);
    const debris = pick(p.SOIL_CONTAMINATION_debris, p.contamination_debris, p.debris);

    const contaminationNotes = pick(
      p.SOIL_CONTAMINATION_comments,
      p.contamination_other_en, p.contamination_other_orig,
      p.observations_en, p.observations_orig,
      p.notes
    );

    // metals: old blob OR canonical fields
    const metalsRaw = pick(
      p.METALS_info,
      p.metals_info_en, p.metals_info_orig,
      p.elemental_concentrations_en, p.elemental_concentrations_orig
    );

    // Clean & format metals (oxide-free, 2 sig figs, <br> separators)
    const metals = cleanMetalsInfo(metalsRaw);

    const rows = [
      ['<i class="bi bi-calendar"></i> ' + T('date', {}, 'Date'), formatDate(dateIso)],
      ['<i class="bi bi-qr-code-scan"></i> ' + T('qr', {}, 'QR code'), qrLike],
      ['<i class="bi bi-droplet-half"></i> ' + T('ph', {}, 'pH'), phVal],
      ['<i class="bi bi-palette"></i> ' + T('soilOrganicMatter', {}, 'Soil organic matter'), soilColor],
      ['<i class="bi bi-grid-3x3-gap"></i> ' + T('texture', {}, 'Texture'), texture],
      ['<i class="bi bi-diagram-3"></i> ' + T('structure', {}, 'Structure'), structure],
      ['<i class="bi bi-bug"></i> ' + T('earthworms', {}, 'Earthworms'), fmtInt(earthworms)],
      ['<i class="bi bi-bag"></i> ' + T('plastic', {}, 'Plastic'), fmtInt(plastic)],
      ['<i class="bi bi-bricks"></i> ' + T('debris', {}, 'Debris'), fmtInt(debris)],
      ['<i class="bi bi-exclamation-triangle"></i> ' + T('contamination', {}, 'Contamination'), contaminationNotes],
      [
        elementalConcentrationsHeaderHtml(),
        metals,
        true
      ],
    ].filter(([_, v]) => !(v == null || (typeof v === "string" && v.trim() === "") || v === "—"));

    const tableHtml = `<table class="table table-sm popup-table mb-2">${rows.map(([k, v, trustedHtml]) => {
      const value = fmt(v);

      return `<tr>
          <th>${k}</th>
          <td>${value === "—" ? "—" : (trustedHtml ? value : escapeHtml(value))}</td>
        </tr>`;
    }).join("")
      }</table>`;

    const PUBLIC_MODE = !!(window.ECHOREPO_CFG || {}).public_mode;

    const bestPhoto = pickPhotoFromProps(p);
    let photoHtml = "";
    if (bestPhoto) {
      const caption = bestPhoto.desc || (bestPhoto.opt ? (bestPhoto.opt[0].toUpperCase() + bestPhoto.opt.slice(1)) : "");
      photoHtml = `
        <div class="popup-photo mt-2">
          <a href="${bestPhoto.url}" target="_blank" rel="noopener">
            <img
              src="${bestPhoto.url}"
              alt="${caption || 'Sample photo'}"
              style="max-width:100%;height:auto;max-height:180px;display:block;object-fit:cover;">
          </a>
          ${caption ? `<div class="small text-muted mt-1">${caption}</div>` : ""}
        </div>`;
    }


    let biodiversityHtml = "";

    const bioItems = [
      p.piechart_16s_url
        ? {
          url: p.piechart_16s_url,
          caption: p.piechart_16s_caption || "16S · Family",
          alt: "16S taxonomic pie chart"
        }
        : null,

      p.piechart_its_url
        ? {
          url: p.piechart_its_url,
          caption: p.piechart_its_caption || "ITS · Family",
          alt: "ITS taxonomic pie chart"
        }
        : null,

      p.fungal_guildplot_url
        ? {
          url: p.fungal_guildplot_url,
          caption: p.fungal_guildplot_caption || "Fungal ecological guilds",
          alt: "Fungal ecological guilds"
        }
        : null,

      p.bacterial_guildplot_url
        ? {
          url: p.bacterial_guildplot_url,
          caption: p.bacterial_guildplot_caption || "Bacterial ecological guilds",
          alt: "Bacterial ecological guilds"
        }
        : null
    ].filter(Boolean);

    if (bioItems.length) {
      biodiversityHtml = `
        <div class="popup-biodiversity mt-3">
          <div class="small fw-semibold mb-2">
            ${T('biodiversityCharts', {}, 'Biodiversity charts')}
          </div>

          <div class="popup-bio-grid">
            ${bioItems.map(item => `
              <div class="popup-bio-item">
                <a href="${item.url}" target="_blank" rel="noopener">
                  <img
                    src="${item.url}"
                    alt="${item.alt}"
                    loading="lazy"
                    decoding="async">
                </a>
                <div class="small text-muted mt-1">
                  ${item.caption}
                </div>
              </div>
            `).join("")}
          </div>
        </div>`;
    }
    let exportHtml = "";
    if (!PUBLIC_MODE && sampleId && !MOBILE_COMPACT_MODE) {
      exportHtml = `<div class="mt-2">
        <a class="btn btn-sm btn-outline-primary"
          href="/download/sample_csv?sampleId=${encodeURIComponent(sampleId)}"
          target="_blank" rel="noopener">
          <i class="bi bi-filetype-csv"></i> ${T('export', {}, 'Export')}
        </a>
      </div>`;
    }

    return `<div class="popup-card">
              <div class="popup-scroll">
                ${tableHtml}
                ${photoHtml}
                ${biodiversityHtml}
              </div>
              ${exportHtml}
            </div>`;
  }

  // ---- State ----
  let ALL_HEADERS = null, userGJ, othersGJ;

  // Ticks are shown outside the middle zoom range.
  // Privacy-radius rings are shown only from zoom 9 through 12.
  const RINGS_MIN_ZOOM = 9;
  const RINGS_MAX_ZOOM = 12;
  const SAMPLE_FOCUS_ZOOM = 11;

  function isRingZoom(zoom) {
    return zoom >= RINGS_MIN_ZOOM &&
      zoom <= RINGS_MAX_ZOOM;
  }

  function clampToRingZoom(requestedZoom) {
    const parsed = Number(requestedZoom);

    const zoom = Number.isFinite(parsed)
      ? parsed
      : SAMPLE_FOCUS_ZOOM;

    return Math.min(
      RINGS_MAX_ZOOM,
      Math.max(RINGS_MIN_ZOOM, Math.round(zoom))
    );
  }

  function resolveSampleTargetZoom(
    opts = {}
  ) {
    const requested =
      Number(opts.zoom);

    if (opts.keepZoom) {
      if (Number.isFinite(requested)) {
        return Math.min(
          map.getMaxZoom(),
          Math.max(
            map.getMinZoom(),
            Math.round(requested)
          )
        );
      }

      return map.getZoom();
    }

    return clampToRingZoom(requested);
  }

  // Keep the existing variable names to minimise changes elsewhere,
  // but these are now ordinary tick layers, not cluster groups.
  // Low-zoom canvas dots
  let userLowDots = L.layerGroup();
  let otherLowDots = L.layerGroup();

  // High-zoom location pins
  let userHighPins = L.layerGroup();
  let otherHighPins = L.layerGroup();

  // Rings & base layers
  const userRings = [], otherRings = [];
  let userLayer, othersLayer;
  let twoToggleControl = null;

  // Selection state
  const drawnItems = DISABLE_SELECTION_TOOLS
    ? null
    : new L.FeatureGroup([], { pane: 'selectionPane' }).addTo(map);
  let selectionLayers = [], selectionRows = [];
  let selectionButtonEl = null, clearButtonEl = null;

  let selectedSampleMarker = null;

  function clearSelectedSampleHighlight() {
    if (
      selectedSampleMarker &&
      map.hasLayer(selectedSampleMarker)
    ) {
      map.removeLayer(selectedSampleMarker);
    }

    selectedSampleMarker = null;
  }

  function highlightSelectedSample(ring, sampleId) {
    const latLng = ring?.getLatLng?.();

    if (!latLng) {
      return false;
    }

    clearSelectedSampleHighlight();

    const props = ring.__props || {};
    const color = phColor(getPhFromProps(props));

    const icon = L.divIcon({
      className: 'echo-selected-sample-icon',
      html: `
        <span
          class="echo-selected-sample-highlight"
          style="--echo-selected-color:${color}">
          <span class="echo-selected-sample-halo"></span>

          <i
            class="bi bi-geo-alt-fill echo-selected-sample-pin">
          </i>
        </span>
      `,
      iconSize: [56, 60],
      iconAnchor: [28, 54]
    });

    selectedSampleMarker = L.marker(latLng, {
      icon,
      pane: 'selectedSamplePane',
      interactive: false,
      keyboard: false,
      zIndexOffset: 10000
    }).addTo(map);

    return true;
  }

  function activateSelectedSample(
    ring,
    sampleId,
    opts = {}
  ) {
    if (!ring) return false;

    const latLng = ring.getLatLng?.();
    if (!latLng) return false;

    // Highlight by default.
    if (opts.highlight !== false) {
      highlightSelectedSample(
        ring,
        sampleId
      );
    }

    // Open the popup by default.
    if (opts.openPopup !== false) {
      const popup = ring.getPopup?.();

      if (!popup) {
        console.warn(
          'No popup found for sample:',
          sampleId
        );

        return true;
      }

      configureSamplePopupPosition(popup);

      if (isRingZoom(map.getZoom())) {
        // Middle zoom: the ring itself is visible.
        if (!map.hasLayer(ring)) {
          ring.addTo(map);
        }

        ring.openPopup();

      } else {
        // High/low zoom: do not display the 1 km ring.
        // Open its existing popup at the point location.
        popup.setLatLng(latLng);
        popup.openOn(map);
      }
    }

    return true;
  }

  function configureSamplePopupPosition(popup) {
    if (!popup) return;

    const mapWidth = map.getSize().x;
    const useLeftPosition = mapWidth >= 760;

    if (useLeftPosition) {
      /*
      * Keep the popup reasonably wide.
      *
      * Leaflet normally centres the popup over its anchor:
      *
      *   half popup width + desired gap
      *
      * Moving it left by that amount places its right edge
      * approximately 28 px before the selected pin.
      */
      const popupWidth = Math.min(
        420,
        Math.max(
          340,
          Math.round(mapWidth * 0.38)
        )
      );

      const gapFromPin = 28;

      popup.options.minWidth = 340;
      popup.options.maxWidth = popupWidth;

      popup.options.offset = L.point(
        -Math.round(
          popupWidth / 2 + gapFromPin
        ),
        0
      );
    } else {
      /*
      * On smaller screens, a popup beside the point would
      * frequently leave the viewport. Use the normal,
      * centred mobile position instead.
      */
      popup.options.minWidth = 50;
      popup.options.maxWidth = Math.max(
        240,
        mapWidth - 40
      );

      popup.options.offset = L.point(
        0,
        12
      );
    }

    popup.options.autoPan = true;
    popup.options.keepInView = true;

    popup.options.autoPanPaddingTopLeft =
      L.point(24, 24);

    popup.options.autoPanPaddingBottomRight =
      L.point(24, 24);
  }

  window.__echomapClearHighlight = clearSelectedSampleHighlight;

  // Filter state (UI elements in page)
  const phMinEl = document.getElementById('phMin');
  const phMaxEl = document.getElementById('phMax');
  const btnApplyFilter = document.getElementById('btnApplyFilter');
  const btnExportFiltered = document.getElementById('btnExportFiltered');

  // --- Active filter for use everywhere ---
  let activePhMin = null;
  let activePhMax = null;
  let filteredRows = [];

  function shouldShowRingsAtCurrentZoom() {
    return isRingZoom(map.getZoom());
  }

  function escapeHtml(v) {
    return String(v ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function refreshRingsVisibilityByZoom() {
    const state = twoToggleControl
      ? twoToggleControl._getState()
      : { user: true, others: true };

    const zoom = map.getZoom();

    const showLowDots = zoom < RINGS_MIN_ZOOM;
    const showRings = isRingZoom(zoom);
    const showHighPins = zoom > RINGS_MAX_ZOOM;

    function setGroupVisibility(group, visible) {
      if (!group) return;

      if (visible) {
        if (!map.hasLayer(group)) {
          map.addLayer(group);
        }
      } else if (map.hasLayer(group)) {
        map.removeLayer(group);
      }
    }

    function processRings(rings, includeGroup) {
      for (const ring of rings) {
        const visible =
          showRings &&
          includeGroup &&
          passesCurrentFilter(ring.__props || {});

        if (visible) {
          if (!map.hasLayer(ring)) {
            ring.addTo(map);
          }
        } else if (map.hasLayer(ring)) {
          map.removeLayer(ring);
        }
      }
    }

    setGroupVisibility(
      userLowDots,
      showLowDots && state.user
    );

    setGroupVisibility(
      otherLowDots,
      showLowDots && state.others
    );

    setGroupVisibility(
      userHighPins,
      showHighPins && state.user
    );

    setGroupVisibility(
      otherHighPins,
      showHighPins && state.others
    );

    processRings(userRings, state.user);
    processRings(otherRings, state.others);
  }

  // ---- Restore filters from URL (Optional D) ----
  (function restoreFiltersFromURL() {
    const qs = new URLSearchParams(window.location.search);

    if (qs.has('country')) {
      activeCountry = qs.get('country');
    }

    if (qs.has('ph_min')) {
      const v = parseFloat(qs.get('ph_min'));
      if (Number.isFinite(v)) activePhMin = v;
    }

    if (qs.has('ph_max')) {
      const v = parseFloat(qs.get('ph_max'));
      if (Number.isFinite(v)) activePhMax = v;
    }
  })();

  const countryEl = document.getElementById('countryFilter');
  countryEl?.addEventListener('change', () => {
    activeCountry = countryEl.value || null;
    updateFiltered();
    updateURLFromFilters();

    if (activeCountry) {
      const b = getBoundsForCountry(activeCountry);
      if (b) {
        map.fitBounds(b, {
          padding: [40, 40],
          maxZoom: 6
        });
      }
    }
    if (!activeCountry) {
      map.setView([50, 10], 5);
    }
  });

  function inRangeGiven(ph, min, max) {
    if (!Number.isFinite(ph)) return (min == null && max == null);
    if (min != null && ph < min) return false;
    if (max != null && ph > max) return false;
    return true;
  }
  function isTruthyFlag(v) {
    if (v === true) return true;
    if (v === 1) return true;

    const s = String(v ?? '').trim().toLowerCase();
    return ['true', '1', 'yes', 'y'].includes(s);
  }

  function hasWrongCoordinates(props) {
    props = props || {};

    if (isTruthyFlag(props.wrong_coordinates)) return true;

    const qa = String(props.qa_status || '').trim().toLowerCase();

    return (
      qa === 'wrong_coordinates' ||
      qa.startsWith('wrong_coordinates:') ||
      qa.includes('wrong coordinate') ||
      qa.includes('invalid coordinate') ||
      qa.includes('bad coordinate') ||
      qa.includes('default_coordinates') ||
      qa.includes('default coordinates')
    );
  }

  function passesCurrentFilter(props) {
    props = props || {};

    // Single-sample mode from /my?sample_id=XXXX&single=1
    if (!isRequestedSingleSample(props)) return false;

    // Hide bad-coordinate samples globally when configured.
    if (HIDE_WRONG_COORDINATES && hasWrongCoordinates(props) && !SHOW_WRONG_IN_SINGLE) return false;

    const ts = props.timestamp_utc || props.collectedAt;
    if (!inDateRange(ts)) return false;

    const ph = getPhFromProps(props || {});

    if (
      activeCountry &&
      String(props.country_code || '').toUpperCase() !== String(activeCountry).toUpperCase()
    ) {
      return false;
    }

    return inRangeGiven(ph, activePhMin, activePhMax);
  }

  function computeAllHeaders() {
    const preferred = [
      "sampleId", "collectedAt", "QR_qrCode", "PH_ph",
      "SOIL_COLOR_color", "SOIL_TEXTURE_texture", "SOIL_STRUCTURE_structure",
      "SOIL_DIVER_earthworms", "SOIL_CONTAMINATION_plastic", "SOIL_CONTAMINATION_debris",
      "SOIL_CONTAMINATION_comments", "METALS_info"
    ];

    const set = new Set(preferred);

    const add = (gj) => {
      (gj?.features || []).forEach(f => {
        const p = f.properties || {};
        Object.keys(p).forEach(k => {
          if (!SHOULD_DROP(k)) set.add(k);
        });
      });
    };

    add(userGJ);
    add(othersGJ);

    set.add(LAT_KEY);
    set.add(LON_KEY);

    const rest = [...set]
      .filter(k => !preferred.includes(k))
      .sort();

    ALL_HEADERS = [...preferred, ...rest];
  }

  let mapLoaderEl = null;
  let mapLoaderTextEl = null;
  let mapLoaderSubtextEl = null;
  let mapLoadingCount = 0;
  let mapLoaderStartedAt = null;

  function forceHideMapLoader() {
    mapLoadingCount = 0;

    if (mapLoaderEl) {
      mapLoaderEl.classList.remove('is-visible');
    }
  }

  function addMapLoaderControl() {
    if (mapLoaderEl) return;

    const container = map.getContainer();

    if (getComputedStyle(container).position === 'static') {
      container.style.position = 'relative';
    }

    const div = document.createElement('div');
    div.className = 'echo-map-loader-overlay';
    div.innerHTML = `
      <div class="echo-loader-box">
        <div class="echo-spinner"></div>
        <div class="echo-loader-text">${T('loadingMapData', {}, 'Loading map data...')}</div>
        <div class="echo-loader-subtext">${T('pleaseWait', {}, 'Please wait')}</div>
      </div>
    `;

    mapLoaderEl = div;
    mapLoaderTextEl = div.querySelector('.echo-loader-text');
    mapLoaderSubtextEl = div.querySelector('.echo-loader-subtext');

    container.appendChild(div);
  }

  function showMapLoader(text, subtext) {
    mapLoadingCount += 1;
    addMapLoaderControl();

    if (!mapLoaderEl) return;

    mapLoaderStartedAt = Date.now();

    if (mapLoaderTextEl) {
      mapLoaderTextEl.textContent = text || T('loadingMapData', {}, 'Loading map data...');
    }

    if (mapLoaderSubtextEl) {
      mapLoaderSubtextEl.textContent = subtext || T('pleaseWait', {}, 'Please wait');
    }

    mapLoaderEl.classList.add('is-visible');
  }

  function updateMapLoader(text, subtext) {
    addMapLoaderControl();

    if (mapLoaderTextEl && text) {
      mapLoaderTextEl.textContent = text;
    }

    if (mapLoaderSubtextEl && subtext) {
      mapLoaderSubtextEl.textContent = subtext;
    }
  }

  function hideMapLoader() {
    mapLoadingCount = Math.max(0, mapLoadingCount - 1);

    if (mapLoadingCount !== 0 || !mapLoaderEl) return;

    const elapsed = mapLoaderStartedAt ? Date.now() - mapLoaderStartedAt : 0;
    const minVisibleMs = 350;
    const delay = Math.max(0, minVisibleMs - elapsed);

    setTimeout(() => {
      if (mapLoadingCount === 0 && mapLoaderEl) {
        mapLoaderEl.classList.remove('is-visible');
      }
    }, delay);
  }

  let currentMapLoadAbort = null;
  let currentMapLoadSeq = 0;
  let bboxLoadTimer = null;
  let dynamicMapReady = false;
  let popupIsOpen = false;
  let suppressBboxReloadUntil = 0;
  let requestedUrlSampleHandled = false;

  function focusRequestedUrlSampleOnce() {
    if (
      requestedUrlSampleHandled ||
      !SINGLE_SAMPLE_ID
    ) {
      return;
    }

    requestedUrlSampleHandled = true;

    setTimeout(async () => {
      if (
        typeof window.__echomapShow !==
        'function'
      ) {
        console.warn(
          'Map sample-focus helper is unavailable.'
        );

        return;
      }

      const focusOptions = SINGLE_SAMPLE_MODE
        ? {
          // Deliberate one-sample mode:
          // use the privacy-ring range.
          zoom: SAMPLE_FOCUS_ZOOM,
          keepZoom: false,

          highlight: true,
          openPopup: true,
          fetchIfMissing: true,
          includeWrong: SHOW_WRONG_IN_SINGLE
        }
        : {
          // Normal link from /search:
          // remain at the requested high zoom.
          zoom: map.getZoom(),
          keepZoom: true,

          highlight: true,
          openPopup: true,
          fetchIfMissing: true,
          includeWrong: false
        };

      const ok = await window.__echomapShow(
        SINGLE_SAMPLE_ID,
        focusOptions
      );

      if (!ok) {
        console.warn(
          'Requested URL sample was not found:',
          SINGLE_SAMPLE_ID
        );
      }
    }, 250);
  }

  function getCurrentBboxParam() {
    const b = map.getBounds();

    return [
      b.getWest(),
      b.getSouth(),
      b.getEast(),
      b.getNorth()
    ].map(x => x.toFixed(6)).join(',');
  }

  function getMapDataUrl() {
    const params = new URLSearchParams();

    params.set('limit', '10000');

    if (SINGLE_SAMPLE_MODE && SINGLE_SAMPLE_ID) {
      params.set('sample_id', SINGLE_SAMPLE_ID);

      // Only use this if you want to allow debug display of bad coords:
      // params.set('include_wrong', '1');
    } else {
      params.set('bbox', getCurrentBboxParam());
    }

    if (activeCountry) {
      params.set('country_code', activeCountry);
    }

    if (activeDateFrom) {
      params.set('from', activeDateFrom);
    }

    if (activeDateTo) {
      params.set('to', activeDateTo);
    }

    return `/api/v1/canonical/map.geojson?${params.toString()}`;
  }

  function clearMapDataLayers() {
    const pointGroups = [
      userLowDots,
      otherLowDots,
      userHighPins,
      otherHighPins
    ];

    clearSelectedSampleHighlight();

    for (const group of pointGroups) {
      try {
        if (group && map.hasLayer(group)) {
          map.removeLayer(group);
        }

        group?.clearLayers();
      } catch (_) { }
    }

    for (const r of userRings) {
      try {
        if (map.hasLayer(r)) map.removeLayer(r);
      } catch (_) { }
    }

    for (const r of otherRings) {
      try {
        if (map.hasLayer(r)) map.removeLayer(r);
      } catch (_) { }
    }

    userRings.length = 0;
    otherRings.length = 0;

    if (window.__echomapIndex) {
      window.__echomapIndex.clear();
    }

    userLayer = null;
    othersLayer = null;

    userLowDots = L.layerGroup();
    otherLowDots = L.layerGroup();
    userHighPins = L.layerGroup();
    otherHighPins = L.layerGroup();
  }

  // Build low-zoom dots, middle-zoom rings and high-zoom pins
  function buildLayers() {

    function makeLowZoomDot(feature, latlng) {
      const props = feature?.properties || {};
      const color = phColor(getPhFromProps(props));

      return L.circleMarker(latlng, {
        radius: 4,
        color: '#ffffff',
        weight: 1,
        opacity: 1,
        fill: true,
        fillColor: color,
        fillOpacity: 0.95,
        interactive: true,
        bubblingMouseEvents: false
      });
    }

    function makeHighZoomPin(feature, latlng) {
      const props = feature?.properties || {};
      const color = phColor(getPhFromProps(props));

      const icon = L.divIcon({
        className: 'echo-high-zoom-pin-icon',
        html: `
          <span
            class="echo-high-zoom-pin"
            style="--echo-pin-color:${color}">
            <i class="bi bi-geo-alt-fill"></i>
          </span>
        `,
        iconSize: [26, 30],
        iconAnchor: [13, 28],
        tooltipAnchor: [0, -25]
      });

      return L.marker(latlng, {
        icon,
        interactive: true,
        keyboard: true,
        riseOnHover: true
      });
    }
    const mkUser = makeLowZoomDot;
    const mkOther = makeLowZoomDot;
    const cfg = window.ECHOREPO_CFG || {};
    const PUBLIC_MODE = !!cfg.public_mode;

    function makeLayer(gj, mk, isOwner, bucket) {
      return L.geoJSON(gj, {
        pointToLayer: (_f, latlng) => mk(_f, latlng),
        onEachFeature: (f, marker) => {
          const props = f.properties || {};
          const ph = getPhFromProps(props);
          const clr = phColor(ph);
          const highPin = makeHighZoomPin(
            f,
            marker.getLatLng()
          );

          highPin.__props = props;
          highPin.__owner = !!isOwner;
          highPin.feature = f;

          // Keep a reference from the low dot to its corresponding pin.
          marker.__highPin = highPin;
          const ring = L.circle(marker.getLatLng(), {
            radius: JITTER_M,
            color: clr,
            weight: 2,
            opacity: 0.95,
            fill: true,
            fillColor: clr,
            fillOpacity: 0.18
          });

          ring.__props = props;
          ring.__owner = !!isOwner;
          ring.feature = f;

          // popup goes on the ring
          ring.bindPopup(
            T('loading', {}, 'Loading...'),
            {
              className: 'echo-popup',
              minWidth: 340,
              maxWidth: 420,
              autoPan: true,
              keepInView: true,
              autoPanPaddingTopLeft: [24, 24],
              autoPanPaddingBottomRight: [24, 24]
            }
          );

          ring.on("popupopen", async (e) => {
            const p = f.properties || {};
            const photoId = p.sampleId || p.sample_id || p.QR_qrCode;
            const chartId = p.QR_qrCode || p.qr_code || p.qr || p.sampleId || p.sample_id;

            // Render immediately with already-known fields.
            e.popup.setContent(formatPopup(f, isOwner));
            setTimeout(() => initPopupTooltips(e.popup), 0);

            if (!photoId && !chartId) return;

            let changed = false;

            if (!p.__img_loaded && photoId) {
              p.__img_loaded = true;
              const img = await fetchSampleImage(photoId);
              if (img) {
                p.image_url = img.url;
                p.image_description_en = img.desc || "";
                changed = true;
              }
            }

            if (!p.__pie16_loaded && chartId) {
              p.__pie16_loaded = true;
              const pie16 = await fetchSamplePiechart(chartId, "16S", "Family");
              if (pie16) {
                p.piechart_16s_url = pie16.url;
                p.piechart_16s_caption = pie16.desc || "16S · Family";
                changed = true;
              }
            }

            if (!p.__pieITS_loaded && chartId) {
              p.__pieITS_loaded = true;
              const pieITS = await fetchSamplePiechart(chartId, "ITS", "Family");
              if (pieITS) {
                p.piechart_its_url = pieITS.url;
                p.piechart_its_caption = pieITS.desc || "ITS · Family";
                changed = true;
              }
            }

            if (!p.__guild_loaded && chartId) {
              p.__guild_loaded = true;

              const [fungalGuild, bacterialGuild] = await Promise.all([
                fetchFungalGuildplot(chartId),
                fetchBacterialGuildplot(chartId)
              ]);

              if (fungalGuild) {
                p.fungal_guildplot_url = fungalGuild.url;
                p.fungal_guildplot_caption = fungalGuild.desc || "Fungal ecological guilds";
                changed = true;
              }

              if (bacterialGuild) {
                p.bacterial_guildplot_url = bacterialGuild.url;
                p.bacterial_guildplot_caption = bacterialGuild.desc || "Bacterial ecological guilds";
                changed = true;
              }
            }

            if (changed) {
              e.popup.setContent(formatPopup(f, isOwner));
              setTimeout(() => initPopupTooltips(e.popup), 0);
            }
          });
          bucket.push(ring);

          if (shouldShowRingsAtCurrentZoom() && passesCurrentFilter(props)) {
            ring.addTo(map);
          }

          // The compact point marker is used below RINGS_MIN_ZOOM.
          marker.__props = props;
          marker.__owner = !!isOwner;
          marker.feature = f;

          const sid = getSampleIdFromProps(props);

          if (sid) {
            ring.on('click', () => {
              highlightSelectedSample(ring, sid);
            });
          }

          function addSampleTooltip(pointMarker) {
            if (!sid || !pointMarker) return;

            pointMarker.bindTooltip(sid, {
              direction: 'top',
              offset: [0, -7],
              opacity: 0.9
            });
          }

          function configureLowZoomDot(dot) {
            if (!sid || !dot) return;

            addSampleTooltip(dot);

            dot.on('click', event => {
              if (event.originalEvent) {
                L.DomEvent.stopPropagation(event.originalEvent);
              }

              // Low zoom: move into the ring range.
              showIndexedSample(sid, {
                zoom: SAMPLE_FOCUS_ZOOM
              });
            });
          }

          function configureHighZoomPin(pin) {
            if (!sid || !pin) return;

            addSampleTooltip(pin);

            pin.on('click', event => {
              if (event.originalEvent) {
                L.DomEvent.stopPropagation(
                  event.originalEvent
                );
              }

              activateSelectedSample(
                ring,
                sid,
                {
                  highlight: true,
                  openPopup: true
                }
              );
            });
          }

          configureLowZoomDot(marker);
          configureHighZoomPin(highPin);

          if (sid) {
            window.__echomapIndex.set(String(sid), ring);
          }

          if (passesCurrentFilter(props)) {
            if (isOwner) {
              userLowDots.addLayer(marker);
              userHighPins.addLayer(highPin);
            } else {
              otherLowDots.addLayer(marker);
              otherHighPins.addLayer(highPin);
            }
          }
        }
      });
    }

    userLayer = makeLayer(userGJ, mkUser, true, userRings);
    othersLayer = makeLayer(othersGJ, mkOther, false, otherRings);

    if (!dynamicMapReady) {
      if (!PUBLIC_MODE && !DISABLE_SAMPLE_TOGGLES) {
        addTwoToggleControl();
      }

      if (!DISABLE_SELECTION_TOOLS) {
        addSelectionControl();
      }
      addLegends();

      dynamicMapReady = true;
    }
    // Display either compact ticks/clusters or privacy rings,
    // depending on the current zoom.
    refreshRingsVisibilityByZoom();

  }

  map.on('zoomend', refreshRingsVisibilityByZoom);

  // Rebuild point layers to reflect the current filter
  function rebuildPointLayersForFilter() {
    const oldGroups = [
      userLowDots,
      otherLowDots,
      userHighPins,
      otherHighPins
    ];

    for (const group of oldGroups) {
      if (map.hasLayer(group)) {
        map.removeLayer(group);
      }
    }

    const newUserLowDots = L.layerGroup();
    const newOtherLowDots = L.layerGroup();
    const newUserHighPins = L.layerGroup();
    const newOtherHighPins = L.layerGroup();

    function addFilteredMarkers(
      layer,
      include,
      lowTarget,
      highTarget
    ) {
      if (!include || !layer) return;

      layer.eachLayer(lowMarker => {
        const props = lowMarker.__props || {};

        if (!passesCurrentFilter(props)) return;

        lowTarget.addLayer(lowMarker);

        if (lowMarker.__highPin) {
          highTarget.addLayer(lowMarker.__highPin);
        }
      });
    }

    const state = twoToggleControl
      ? twoToggleControl._getState()
      : { user: true, others: true };

    addFilteredMarkers(
      userLayer,
      state.user,
      newUserLowDots,
      newUserHighPins
    );

    addFilteredMarkers(
      othersLayer,
      state.others,
      newOtherLowDots,
      newOtherHighPins
    );

    userLowDots = newUserLowDots;
    otherLowDots = newOtherLowDots;
    userHighPins = newUserHighPins;
    otherHighPins = newOtherHighPins;

    refreshRingsVisibilityByZoom();
  }
  // ---- Show/hide rings based on current filter + toggles ----
  function applyFilterToRings() {
    refreshRingsVisibilityByZoom();
  }
  // ---- Two checkboxes (toggle clusters + rings together) ----
  function addTwoToggleControl() {
    const state = { user: true, others: true };

    function sync() {
      refreshRingsVisibilityByZoom();
      updateSelectionCount();
      updateFilteredCountsLabelOnly();
    }

    const ctl = L.control({ position: 'topleft' });
    ctl.onAdd = function () {
      const div = L.DomUtil.create('div', 'leaflet-control leaflet-bar p-2');
      div.style.background = 'white'; div.style.borderRadius = '8px'; div.style.lineHeight = '1.1';
      div.innerHTML = `
        <div class="form-check" style="margin:.1rem 0;">
          <input class="form-check-input" type="checkbox" id="togUser" checked>
          <label class="form-check-label" for="togUser">${T('yourSamples', {}, 'Your samples')}</label>
        </div>
        <div class="form-check" style="margin:.1rem 0%;">
          <input class="form-check-input" type="checkbox" id="togOther" checked>
          <label class="form-check-label" for="togOther">${T('otherSamples', {}, 'Other samples')}</label>
        </div>`;
      L.DomEvent.disableClickPropagation(div);
      const cUser = div.querySelector('#togUser'), cOther = div.querySelector('#togOther');
      cUser.addEventListener('change', () => { state.user = !!cUser.checked; sync(); });
      cOther.addEventListener('change', () => { state.others = !!cOther.checked; sync(); });
      div._getState = () => ({ ...state }); twoToggleControl = div; return div;
    };
    ctl.addTo(map); sync();
  }

  // ---- Localize Leaflet.Draw built-in strings ----
  function applyLeafletDrawTranslations() {
    if (DISABLE_SELECTION_TOOLS || !window.L || !L.drawLocal) return;

    if (L.drawLocal.draw && L.drawLocal.draw.toolbar) {
      const tb = L.drawLocal.draw.toolbar;
      if (tb.buttons) {
        tb.buttons.rectangle = T(
          'drawSelectionRectangle',
          {},
          'Draw selection rectangle'
        );
      }
      if (tb.actions) {
        tb.actions.title = T('cancelDrawing', {}, 'Cancel drawing');
        tb.actions.text = T('cancel', {}, 'Cancel');
      }
      if (tb.undo) {
        tb.undo.title = T('deleteLastPoint', {}, 'Delete last point drawn');
        tb.undo.text = T('deleteLastPoint', {}, 'Delete last point');
      }
    }

    if (L.drawLocal.draw && L.drawLocal.draw.handlers) {
      const h = L.drawLocal.draw.handlers;

      const startText = T(
        'drawRectangleHint',
        {},
        'Click and drag to draw a selection rectangle. You can draw more than one.'
      );

      const endText = T(
        'releaseToFinish',
        {},
        'Release mouse to add this rectangle to the selection.'
      );

      if (h.rectangle && h.rectangle.tooltip) {
        h.rectangle.tooltip.start = startText;
        h.rectangle.tooltip.end = endText;
      }
      if (h.simpleshape && h.simpleshape.tooltip) {
        h.simpleshape.tooltip.start = startText;
        h.simpleshape.tooltip.end = endText;
      }
    }
  }

  // ---- Selection (rectangle multi-select) ----
  function addSelectionControl() {
    if (DISABLE_SELECTION_TOOLS || !window.L || !L.Control || !L.Control.Draw) return;

    const ctl = L.control({ position: 'topright' });
    ctl.onAdd = function () {
      const div = L.DomUtil.create('div', 'leaflet-control leaflet-bar p-2');
      div.style.background = 'white'; div.style.borderRadius = '8px'; div.style.lineHeight = '1';
      div.innerHTML = `
        <div style="min-width: 260px;">
          <div class="fw-semibold mb-1">
            ${T('selectionExport', {}, 'Selection export')}
          </div>

          <div class="small text-muted mb-2" style="line-height:1.2;">
            ${T('selectionExportHintBefore', {}, 'Use selection tool')}
            <span class="echo-selection-tool-symbol" aria-hidden="true"></span>
            ${T(
        'selectionExportHintAfter',
        {},
        'to draw one or more areas'
      )}
          </div>

          <div class="d-flex gap-2 align-items-center">
            <button
              type="button"
              class="btn btn-sm btn-primary"
              id="btnExportSel"
              disabled
              title="${T('exportSelectionTitle', {}, 'Export selected samples')}">
              ${T('exportSelection', {}, 'Export selection')} (0)
            </button>

            <button
              type="button"
              class="btn btn-sm btn-outline-secondary"
              id="btnClearSel"
              disabled
              title="${T('clearSelectionTitle', {}, 'Clear all selection rectangles')}">
              ${T('clearSelection', {}, 'Clear selection')}
            </button>
          </div>
        </div>`;
      L.DomEvent.disableClickPropagation(div);
      selectionButtonEl = div.querySelector('#btnExportSel');
      clearButtonEl = div.querySelector('#btnClearSel');
      selectionButtonEl.addEventListener('click', () => {
        if (!selectionRows.length) return;

        const ids = selectionRows
          .map(r => (
            r.sample_id ||
            r.sampleId ||
            r.QR_qrCode ||
            r.qr_code ||
            r.qr ||
            r.Sample ||
            ''
          ))
          .map(x => String(x).trim())
          .filter(Boolean);

        if (!ids.length) {
          alert(T('noSamplesSelected', {}, 'No selected samples to export.'));
          return;
        }

        const form = document.createElement('form');
        form.method = 'POST';
        form.action = '/search';
        form.style.display = 'none';

        const formatInput = document.createElement('input');
        formatInput.type = 'hidden';
        formatInput.name = 'format';
        formatInput.value = 'zip';
        form.appendChild(formatInput);

        const idsInput = document.createElement('textarea');
        idsInput.name = 'sample_ids';
        idsInput.value = ids.join(',');
        form.appendChild(idsInput);

        document.body.appendChild(form);
        form.submit();

        setTimeout(() => {
          form.remove();
        }, 1000);

      });

      clearButtonEl.addEventListener('click', clearSelections);
      return div;
    };
    ctl.addTo(map);

    applyLeafletDrawTranslations();

    const RECT_STYLE = {
      pane: 'selectionPane',
      color: '#0d6efd',
      weight: 2,
      opacity: 1,
      fill: true,
      fillColor: '#0d6efd',
      fillOpacity: 0.12,
      interactive: false
    };
    const drawControl = new L.Control.Draw({
      draw: {
        polygon: false,
        polyline: false,
        circle: false,
        marker: false,
        circlemarker: false,
        rectangle: {
          shapeOptions: RECT_STYLE
        }
      },
      edit: false
    });

    map.addControl(drawControl);
    window.__echodraw = drawControl;

    const rectHandler = drawControl._toolbars.draw._modes.rectangle.handler;
    const endText = T('releaseToFinish', {}, 'Release mouse to finish drawing.');
    rectHandler._endLabelText = endText;

    map.on(L.Draw.Event.CREATED, (e) => {
      const layer = e.layer;

      if (layer.setStyle) {
        layer.setStyle(RECT_STYLE);
      }

      // The rectangle is only a selection area.
      // It must not block clicks on sample circles underneath.
      layer.options.interactive = false;

      drawnItems.addLayer(layer);

      // Disable pointer events on the actual SVG element after Leaflet creates it.
      setTimeout(() => {
        const el = layer.getElement && layer.getElement();
        if (el) {
          el.style.pointerEvents = 'none';
          el.style.cursor = 'default';
        }
      }, 0);

      selectionLayers.push(layer);
      updateSelectionCount();
    });
  }
  function clearSelections() {
    if (drawnItems) drawnItems.clearLayers();
    selectionLayers = [];
    selectionRows = [];
    updateSelectionCount();
  }

  function collectRowsWithinAll() {
    if (DISABLE_SELECTION_TOOLS || !selectionLayers.length) return [];
    const active = twoToggleControl ? twoToggleControl._getState() : { user: true, others: true };
    const rows = [], seen = new Set();
    const inAny = (ll) => selectionLayers.some(r => r.getBounds().contains(ll));

    function scan(layer, include) {
      if (!include || !layer) return;
      layer.eachLayer(m => {
        const ll = m.getLatLng(); if (!ll) return;
        if (!inAny(ll)) return;
        const f = m.feature || {}; const props = { ...(f.properties || {}) };
        if (!passesCurrentFilter(props)) return;
        Object.keys(props).forEach(k => { if (SHOULD_DROP(k)) delete props[k]; });
        props[LAT_KEY] = ll.lat; props[LON_KEY] = ll.lng;
        const key = props.sampleId || props.QR_qrCode || `${ll.lat.toFixed(6)},${ll.lng.toFixed(6)}`;
        if (seen.has(key)) return; seen.add(key); rows.push(props);
      });
    }
    scan(userLayer, active.user);
    scan(othersLayer, active.others);
    return rows;
  }
  function updateSelectionCount() {
    if (DISABLE_SELECTION_TOOLS || !selectionButtonEl || !clearButtonEl) return;
    selectionRows = collectRowsWithinAll();
    const n = selectionRows.length;
    selectionButtonEl.disabled = n === 0;
    selectionButtonEl.textContent = `${T('exportSelection', {}, 'Export selection')} (${n})`;
    clearButtonEl.disabled = selectionLayers.length === 0;
  }

  // ---- Filter by pH & export ----
  function collectRowsFiltered(phMin, phMax, dateFrom, dateTo) {
    const active = twoToggleControl
      ? twoToggleControl._getState()
      : { user: true, others: true };

    const rows = [];
    const seen = new Set();

    function scan(layer, include) {
      if (!include || !layer) return;

      layer.eachLayer(m => {
        const ll = m.getLatLng();
        if (!ll) return;

        const f = m.feature || {};
        const props = { ...(f.properties || {}) };

        // ---- SINGLE source of truth for filtering ----
        if (!passesCurrentFilter(props)) return;

        // ---- Clean props ----
        Object.keys(props).forEach(k => {
          if (SHOULD_DROP(k)) delete props[k];
        });

        // ---- Ensure coordinates ----
        props[LAT_KEY] = ll.lat;
        props[LON_KEY] = ll.lng;

        const key =
          props.sampleId ||
          props.QR_qrCode ||
          `${ll.lat.toFixed(6)},${ll.lng.toFixed(6)}`;

        if (seen.has(key)) return;
        seen.add(key);

        rows.push(props);
      });
    }

    scan(userLayer, active.user);
    scan(othersLayer, active.others);

    return rows;
  }

  let lastFilterSignature = null;
  let globalFilteredCount = 0;
  let countLoadSeq = 0;
  let countLoadTimer = null;

  function getGlobalCountUrl() {
    const params = new URLSearchParams();

    if (activeCountry) {
      params.set('country_code', activeCountry);
    }

    if (activeDateFrom) {
      params.set('from', activeDateFrom);
    }

    if (activeDateTo) {
      params.set('to', activeDateTo);
    }

    if (activePhMin != null) {
      params.set('ph_min', activePhMin);
    }

    if (activePhMax != null) {
      params.set('ph_max', activePhMax);
    }

    return `/api/v1/canonical/map.count?${params.toString()}`;
  }

  async function refreshGlobalFilteredCount() {
    const seq = ++countLoadSeq;

    try {
      const r = await fetch(getGlobalCountUrl(), {
        credentials: 'same-origin'
      });

      if (!r.ok) {
        throw new Error(`Count API failed: ${r.status}`);
      }

      const j = await r.json();

      if (seq !== countLoadSeq) return;

      globalFilteredCount = Number(j.count) || 0;
      updateFilteredCountsLabelOnly();

    } catch (err) {
      console.warn('Could not refresh global filtered count:', err);
    }
  }

  function scheduleGlobalFilteredCountRefresh() {
    clearTimeout(countLoadTimer);
    countLoadTimer = setTimeout(refreshGlobalFilteredCount, 250);
  }

  function getFilterSignature() {
    const state = twoToggleControl
      ? twoToggleControl._getState()
      : { user: true, others: true };

    return JSON.stringify({
      country: activeCountry || '',
      dateFrom: activeDateFrom || '',
      dateTo: activeDateTo || '',
      phMin: activePhMin,
      phMax: activePhMax,
      user: !!state.user,
      others: !!state.others,
      hideWrong: !!HIDE_WRONG_COORDINATES
    });
  }

  function updateFilteredCountsLabelOnly() {
    if (
      MOBILE_COMPACT_MODE ||
      !btnExportFiltered
    ) {
      return;
    }

    const n = globalFilteredCount || 0;

    btnExportFiltered.disabled = n === 0;
    btnExportFiltered.textContent =
      T('exportFiltered', { n }, `Export filtered (${n})`);
  }

  function updateFiltered() {
    const minV = phMinEl ? parseFloat(phMinEl.value) : NaN;
    const maxV = phMaxEl ? parseFloat(phMaxEl.value) : NaN;

    activePhMin = Number.isFinite(minV) ? minV : null;
    activePhMax = Number.isFinite(maxV) ? maxV : null;

    activeDateFrom = dateFromEl?.value || null;
    activeDateTo = dateToEl?.value || null;

    const sig = getFilterSignature();

    applyFilterToRings();

    if (sig !== lastFilterSignature) {
      rebuildPointLayersForFilter();

      filteredRows = collectRowsFiltered(
        activePhMin,
        activePhMax,
        activeDateFrom,
        activeDateTo
      );

      lastFilterSignature = sig;
    }

    updateFilteredCountsLabelOnly();
    updateSelectionCount();
    if (!MOBILE_COMPACT_MODE) {
      scheduleGlobalFilteredCountRefresh();
    }
  }

  btnApplyFilter?.addEventListener('click', () => {
    updateFiltered();
    updateURLFromFilters();
  });
  btnExportFiltered?.addEventListener('click', () => {
    const cfg = window.ECHOREPO_CFG || {};
    const PUBLIC_MODE = !!cfg.public_mode;

    if (PUBLIC_MODE) {
      alert(T('signInToExport', {}, 'Please sign in to export data.'));
      return;
    }

    if (!filteredRows.length) return;

    const params = new URLSearchParams();
    params.set('format', 'zip');

    if (activePhMin != null) params.set('ph_min', activePhMin);
    if (activePhMax != null) params.set('ph_max', activePhMax);
    if (activeCountry) params.set('country', activeCountry);
    if (activeDateFrom) params.set('date_from', activeDateFrom);
    if (activeDateTo) params.set('date_to', activeDateTo);

    window.location.href = `/search?${params.toString()}`;
  });

  [phMinEl, phMaxEl].forEach(el => el?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); updateFiltered(); }
  }));

  // ---- CSV helpers ----
  function addLegends() {
    const legend = L.control({ position: 'bottomleft' });
    legend.onAdd = function () {
      const div = L.DomUtil.create('div', 'leaflet-control leaflet-bar p-2');
      div.style.background = 'white'; div.style.borderRadius = '8px'; div.style.lineHeight = '1.1';
      div.innerHTML = `<div style="display:flex;align-items:center;gap:.4rem;margin:.2rem 0;">
        <svg width="14" height="14" aria-hidden="true"><circle cx="7" cy="7" r="5" stroke="#333" fill="none"/></svg>
        <span>${T('privacyRadius', { km: Math.round(JITTER_M / 1000) }, 'Privacy radius (~±{km} km)')}</span></div>`;
      return div;
    }; legend.addTo(map);

    const phLegend = L.control({ position: 'bottomright' });
    phLegend.onAdd = function () {
      const div = L.DomUtil.create('div', 'leaflet-control leaflet-bar p-2');
      div.style.background = 'white'; div.style.borderRadius = '8px'; div.style.lineHeight = '1.2';
      div.innerHTML = `
        <div class="fw-semibold mb-1">${T('soilPh', {}, 'Soil pH')}</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#d73027;">●</span> ${T('acid', {}, 'Acidic (≤5.5)')}</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#fc8d59;">●</span> ${T('slightlyAcid', {}, 'Slightly acidic (5.5–6.5)')}</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#fee08b;">●</span> ${T('neutral', {}, 'Neutral (6.5–7.5)')}</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#91bfdb;">●</span> ${T('slightlyAlkaline', {}, 'Slightly alkaline (7.5–8.5)')}</div>
        <div style="display:flex;align-items:center;gap:.4rem;"><span style="color:#4575b4;">●</span> ${T('alkaline', {}, 'Alkaline (≥8.5)')}</div>`;
      return div;
    }; phLegend.addTo(map);
  }

  async function loadSingleSampleForMap(
    sampleId,
    opts = {}
  ) {
    const id = String(sampleId || '').trim();

    if (!id) return false;

    const params = new URLSearchParams();

    params.set('limit', '1');
    params.set('sample_id', id);

    if (opts.includeWrong || SHOW_WRONG_IN_SINGLE) {
      params.set('include_wrong', '1');
    }

    showMapLoader(
      T('loadingSample', {}, 'Loading sample...'),
      id
    );

    try {
      // Fetch the exact sample only to discover its position.
      const response = await fetch(
        `/api/v1/canonical/map.geojson?${params.toString()}`,
        {
          credentials: 'same-origin'
        }
      );

      if (!response.ok) {
        throw new Error(
          `Single-sample map API failed: ${response.status}`
        );
      }

      const geojson = await response.json();
      const feature = geojson?.features?.[0];

      if (!feature) {
        return false;
      }

      const coordinates =
        feature.geometry?.coordinates;

      const lon = Number(coordinates?.[0]);
      const lat = Number(coordinates?.[1]);

      if (
        !Number.isFinite(lat) ||
        !Number.isFinite(lon)
      ) {
        return false;
      }

      const targetZoom =
        resolveSampleTargetZoom(opts);

      suppressBboxReloadUntil =
        Date.now() + 2000;

      // Move first so the normal bbox request is built
      // around the selected sample.
      map.setView(
        [lat, lon],
        targetZoom,
        {
          animate: false
        }
      );

      // Load all samples in that visible map area.
      await loadSamplesForCurrentView();

      return true;

    } catch (error) {
      console.warn(
        'Could not locate sample on map:',
        id,
        error
      );

      return false;

    } finally {
      hideMapLoader();
    }
  }

  async function loadSamplesForCurrentView() {
    const seq = ++currentMapLoadSeq;

    if (currentMapLoadAbort) {
      currentMapLoadAbort.abort();
    }

    currentMapLoadAbort = new AbortController();

    showMapLoader(
      T('loadingSamples', {}, 'Loading samples...'),
      T('fetchingMapPoints', {}, 'Fetching visible map area')
    );

    try {
      const url = getMapDataUrl();

      const r = await fetch(url, {
        credentials: 'same-origin',
        signal: currentMapLoadAbort.signal
      });

      if (!r.ok) {
        throw new Error(`Map API failed: ${r.status}`);
      }

      const gj = await r.json();

      // Ignore late responses from previous requests.
      if (seq !== currentMapLoadSeq) return;

      userGJ = {
        type: "FeatureCollection",
        features: [],
      };

      othersGJ = gj || {
        type: "FeatureCollection",
        features: [],
      };

      updateMapLoader(
        T('renderingMarkers', {}, 'Rendering markers...'),
        T('buildingClusters', {}, 'Building clusters and popups')
      );

      clearMapDataLayers();

      computeAllHeaders();
      buildLayers();

      populateCountryFilter();
      syncFiltersToUI();

      rebuildPointLayersForFilter();
      refreshI18NTexts();
      if (!MOBILE_COMPACT_MODE) {
        scheduleGlobalFilteredCountRefresh();
      }
      focusRequestedUrlSampleOnce();

    } catch (err) {
      if (err.name !== 'AbortError') {
        console.warn('Could not load map samples:', err);
        forceHideMapLoader();
      }
    } finally {
      hideMapLoader();

      if (seq === currentMapLoadSeq) {
        currentMapLoadAbort = null;
      }
    }
  }

  function scheduleBboxReload() {
    if (SINGLE_SAMPLE_MODE && SINGLE_SAMPLE_ID) return;

    if (Date.now() < suppressBboxReloadUntil) return;

    if (popupIsOpen) return;

    clearTimeout(bboxLoadTimer);
    bboxLoadTimer = setTimeout(() => {
      if (!popupIsOpen) {
        loadSamplesForCurrentView();
      }
    }, 350);
  }

  // ---- Boot ----
  (function boot() {
    const safeJson = (url) =>
      fetch(url, { credentials: 'same-origin' })
        .then(r => r.ok ? r.json() : null)
        .catch(() => null);

    addMapLoaderControl();
    showMapLoader(
      T('loadingMapData', {}, 'Loading map data...'),
      T('preparingMap', {}, 'Preparing map')
    );

    safeJson('/i18n/labels?ts=' + Date.now())
      .then((i18n) => {
        const payload = (i18n && (i18n.labels || i18n.by_msgid))
          ? i18n
          : { labels: (i18n || {}), by_msgid: {} };

        window.I18N = window.I18N || { labels: {}, by_msgid: {} };

        if (payload.labels && Object.keys(payload.labels).length) {
          Object.assign(window.I18N.labels, payload.labels);
        }

        if (payload.by_msgid && Object.keys(payload.by_msgid).length) {
          Object.assign(window.I18N.by_msgid, payload.by_msgid);
        }

        userGJ = { type: "FeatureCollection", features: [] };
        othersGJ = { type: "FeatureCollection", features: [] };

        initFiltersFromUrl();
        syncFiltersToUI();

        return loadSamplesForCurrentView();
      })
      .then(() => {
        map.on('dragend zoomend', scheduleBboxReload);
      })
      .catch(err => {
        console.warn('Init failed:', err);
      })
      .finally(() => {
        hideMapLoader();
      });
  })();

})();