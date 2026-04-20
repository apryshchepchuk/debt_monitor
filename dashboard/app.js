const API_URL = "https://script.google.com/macros/s/AKfycbzqaHxa3N-G0Da9MMxgzjUqT6p9lIWgY4x7R4WQ_iKAF7xv_hUFgNGMcenJVT-nPAFN/exec";

let state = {
  debtors: [],
  tab: "active",
  query: "",
  token: "",
  expandedDebtors: new Set(),
};

const els = {
  updatedAt: document.getElementById("updatedAt"),
  statusBox: document.getElementById("statusBox"),
  content: document.getElementById("content"),
  searchInput: document.getElementById("searchInput"),
  tokenBox: document.getElementById("tokenBox"),
  tokenInput: document.getElementById("tokenInput"),
  saveTokenBtn: document.getElementById("saveTokenBtn"),
  tabs: document.querySelectorAll(".tab"),
};

init();

function init() {
  const params = new URLSearchParams(window.location.search);
  const tokenFromUrl = params.get("token");
  const tokenFromStorage = localStorage.getItem("erb_dashboard_token");

  state.token = tokenFromUrl || tokenFromStorage || "";

  if (tokenFromUrl) {
    localStorage.setItem("erb_dashboard_token", tokenFromUrl);
  }

  bindEvents();

  if (!state.token) {
    showTokenBox();
    return;
  }

  loadDashboardData();
}

function bindEvents() {
  els.searchInput.addEventListener("input", (e) => {
    state.query = e.target.value.trim().toLowerCase();
    render();
  });

  els.tabs.forEach((btn) => {
    btn.addEventListener("click", () => {
      state.tab = btn.dataset.tab;
      els.tabs.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      render();
    });
  });

  els.saveTokenBtn.addEventListener("click", () => {
    const token = els.tokenInput.value.trim();
    if (!token) return;

    localStorage.setItem("erb_dashboard_token", token);
    state.token = token;
    hideTokenBox();
    loadDashboardData();
  });

    els.content.addEventListener("click", (e) => {
    if (e.target.closest("details, summary, a, button, input, .record")) return;

    const card = e.target.closest(".debtor-card");
    if (!card) return;

    const key = card.dataset.debtorKey;
    if (!key) return;

    if (state.expandedDebtors.has(key)) {
      state.expandedDebtors.delete(key);
    } else {
      state.expandedDebtors.add(key);
    }

    render();
  });
}

function showTokenBox() {
  els.tokenBox.classList.remove("hidden");
  setStatus("Очікується token доступу.");
}

function hideTokenBox() {
  els.tokenBox.classList.add("hidden");
}

function setStatus(text) {
  els.statusBox.textContent = text;
  els.statusBox.classList.remove("hidden");
}

function clearStatus() {
  els.statusBox.classList.add("hidden");
}

function loadDashboardData() {
  setStatus("Завантаження даних…");

  const callbackName = "__erbDashboardCallback_" + Date.now();
  const script = document.createElement("script");

  window[callbackName] = function (data) {
    delete window[callbackName];
    script.remove();

    if (!data || !data.ok) {
      setStatus(data && data.error ? data.error : "Не вдалося завантажити дані.");
      return;
    }

    state.debtors = Array.isArray(data.debtors) ? data.debtors : [];
    els.updatedAt.textContent = "Оновлення: " + formatDate(data.updated_at);
    clearStatus();
    render();
  };

  const url = new URL(API_URL);
  url.searchParams.set("action", "get_dashboard_data");
  url.searchParams.set("token", state.token);
  url.searchParams.set("callback", callbackName);

  script.onerror = function () {
    delete window[callbackName];
    script.remove();
    setStatus("Помилка завантаження API.");
  };

  script.src = url.toString();
  document.body.appendChild(script);
}

function render() {
  const filtered = filterDebtors(state.debtors);

  if (!filtered.length) {
    els.content.innerHTML = `<div class="empty">Записів не знайдено.</div>`;
    return;
  }

  els.content.innerHTML = filtered.map(renderDebtorCard).join("");
}

function filterDebtors(debtors) {
  return debtors
    .map((debtor) => {
      const records = getRecordsForCurrentTab(debtor.records || []);
      return { ...debtor, records };
    })
    .filter((debtor) => debtor.records.length > 0)
    .filter(matchesSearch);
}

function getRecordsForCurrentTab(records) {
  if (state.tab === "active") {
    return records.filter((r) => r.status === "active");
  }

  return records;
}

function matchesSearch(debtor) {
  if (!state.query) return true;

  const haystack = [
    debtor.debtor_name,
    debtor.debtor_code,
    debtor.debtor_birthdate,
    ...(debtor.records || []).flatMap((r) => [
      r.vp_ordernum,
      r.org_name,
      r.emp_full_fio,
      r.publisher,
      r.vd_cat,
    ]),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  return haystack.includes(state.query);
}

function buildDebtorKey(debtor) {
  if (debtor.debtor_code) return `code:${debtor.debtor_code}`;
  if (debtor.debtor_name && debtor.debtor_birthdate) {
    return `person:${debtor.debtor_name}|${debtor.debtor_birthdate}`;
  }
  return `name:${debtor.debtor_name || "unknown"}`;
}

function renderDebtorCard(debtor) {
  const debtorKey = buildDebtorKey(debtor);
  const isExpanded = state.expandedDebtors.has(debtorKey);

  const activeCount = debtor.records.filter((r) => r.status === "active").length;
  const totalCount = debtor.records.length;

  const badge =
    activeCount > 0
      ? `<span class="badge active">Активний</span>`
      : `<span class="badge inactive">Зник з ЄРБ</span>`;

  const idLine = debtor.debtor_code
    ? `Код: ${escapeHtml(debtor.debtor_code)}`
    : debtor.debtor_birthdate
      ? `Дата нар.: ${escapeHtml(debtor.debtor_birthdate)}`
      : "";

  const countText =
    state.tab === "active"
      ? `${pluralizeRecords(totalCount)} · востаннє підтверджено ${formatDate(debtor.last_seen)}`
      : `${pluralizeRecords(totalCount)}`;

  const recordsHtml = isExpanded
    ? `
      <div class="records">
        ${(debtor.records || []).map(renderRecord).join("")}
      </div>
    `
    : "";

  return `
    <article class="debtor-card ${isExpanded ? "expanded" : "collapsed"}" data-debtor-key="${escapeHtml(debtorKey)}">
      <div class="debtor-head">
        <div>
          <h2 class="debtor-title">${escapeHtml(debtor.debtor_name || "Без назви")}</h2>
          <div class="meta">${escapeHtml(idLine)}${idLine ? "<br>" : ""}${escapeHtml(countText)}</div>
        </div>
        ${badge}
      </div>

      <div class="expand-hint">
        <span>${isExpanded ? "Сховати записи" : "Показати записи"}</span>
        <span class="chevron">${isExpanded ? "⌃" : "⌄"}</span>
      </div>

      ${recordsHtml}
    </article>
  `;
}

function renderRecord(record) {
  const statusBadge =
    record.status === "active"
      ? `<span class="badge active">Активний</span>`
      : `<span class="badge inactive">Зник з ЄРБ</span>`;

  const removed = record.removed_at
    ? `<div>Зник: ${escapeHtml(formatDate(record.removed_at))}</div>`
    : "";

  return `
    <div class="record">
      <div class="record-main">
        <div>
          <div class="record-title">ВП ${escapeHtml(record.vp_ordernum || "—")} · ${statusBadge}</div>
          <div class="record-org">${escapeHtml(record.org_name || "Орган / виконавець не вказано")}</div>
        </div>
        <div class="record-dates">
          <div>Вперше: ${escapeHtml(formatDate(record.first_seen))}</div>
          <div>Востаннє: ${escapeHtml(formatDate(record.last_seen))}</div>
          ${removed}
        </div>
      </div>

      <details class="details">
        <summary>Деталі</summary>
        <div class="details-grid">
          ${detailRow("Категорія", record.vd_cat)}
          ${detailRow("Видавник", record.publisher)}
          ${detailRow("Виконавець", record.emp_full_fio)}
          ${detailRow("Телефон органу", record.org_phone_num)}
          ${detailRow("Телефон виконавця", record.emp_phone_num)}
          ${detailRow("Email", record.email_addr)}
        </div>
      </details>
    </div>
  `;
}

function detailRow(label, value) {
  if (!value) return "";
  return `
    <div class="label">${escapeHtml(label)}</div>
    <div>${escapeHtml(value)}</div>
  `;
}

function pluralizeRecords(n) {
  const last = n % 10;
  const lastTwo = n % 100;

  if (last === 1 && lastTwo !== 11) return `${n} запис`;
  if ([2, 3, 4].includes(last) && ![12, 13, 14].includes(lastTwo)) return `${n} записи`;
  return `${n} записів`;
}

function formatDate(value) {
  if (!value) return "—";

  const s = String(value).trim();

  // Основний формат з Apps Script: 2026-04-20 07:20:11
  const isoLike = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2}))?/);
  if (isoLike) {
    const [, y, mo, d, hh, mm] = isoLike;
    return `${d}.${mo}.${y}${hh && mm ? ` ${hh}:${mm}` : ""}`;
  }

  // Старий Google/JS формат: Mon Apr 20 2026 07:20:11 GMT+0300 ...
  const jsDateLike = s.match(/^[A-Za-z]{3}\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})\s+(\d{2}):(\d{2})/);
  if (jsDateLike) {
    const months = {
      Jan: "01", Feb: "02", Mar: "03", Apr: "04",
      May: "05", Jun: "06", Jul: "07", Aug: "08",
      Sep: "09", Oct: "10", Nov: "11", Dec: "12",
    };

    const [, mon, day, year, hh, mm] = jsDateLike;
    const mo = months[mon] || mon;
    const d = String(day).padStart(2, "0");

    return `${d}.${mo}.${year} ${hh}:${mm}`;
  }

  return s;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
