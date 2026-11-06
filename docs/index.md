---
layout: null
---
{::nomarkdown}
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>JetDatabaseWriter — Pure Managed .NET Reader &amp; Writer for Microsoft Access (.mdb / .accdb)</title>
  <meta name="description" content="Fully managed .NET library for reading, querying, creating, and writing Microsoft Access (Jet3, Jet4, and ACE) .mdb and .accdb databases without OleDB, ODBC, or native drivers." />
  <meta name="theme-color" content="#0b0f19" />
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Cdefs%3E%3ClinearGradient id='g1' x1='0%25' y1='0%25' x2='100%25' y2='100%25'%3E%3Cstop offset='0%25' stop-color='%23e14b5a'/%3E%3Cstop offset='52%25' stop-color='%23a4373a'/%3E%3Cstop offset='100%25' stop-color='%236d4aff'/%3E%3C/linearGradient%3E%3ClinearGradient id='g2' x1='0%25' y1='0%25' x2='100%25' y2='0%25'%3E%3Cstop offset='0%25' stop-color='%2338bdf8'/%3E%3Cstop offset='100%25' stop-color='%23818cf8'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='64' height='64' rx='14' fill='%230b0f19'/%3E%3Cpath d='M12 40c0 4.4 9 8 20 8s20-3.6 20-8V31c0 4.4-9 8-20 8s-20-3.6-20-8v9z' fill='url(%23g1)' opacity='0.82'/%3E%3Cpath d='M12 29c0 4.4 9 8 20 8s20-3.6 20-8V20c0 4.4-9 8-20 8s-20-3.6-20-8v9z' fill='url(%23g1)' opacity='0.92'/%3E%3Cellipse cx='32' cy='20' rx='20' ry='8' fill='url(%23g1)'/%3E%3Cpath d='M24 30l24-18-9 28-6-7-9-3z' fill='url(%23g2)' stroke='%230b0f19' stroke-width='2' stroke-linejoin='round'/%3E%3C/svg%3E" />
  <style>
    :root {
      --bg-0: #070a12;
      --bg-1: #0b0f19;
      --bg-2: #111827;
      --bg-card: rgba(17, 24, 39, 0.78);
      --bg-code: #090d16;
      --border: rgba(148, 163, 184, 0.16);
      --border-hi: rgba(225, 75, 90, 0.45);
      --text-0: #f8fafc;
      --text-1: #cbd5e1;
      --text-2: #94a3b8;
      --jet-red: #e14b5a;
      --jet-crimson: #a4373a;
      --dotnet-purple: #7c5cff;
      --cyan: #38bdf8;
      --emerald: #34d399;
      --amber: #fbbf24;
      --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, "Helvetica Neue", Arial, sans-serif;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html { scroll-behavior: smooth; background: var(--bg-0); color: var(--text-0); font-family: var(--sans); line-height: 1.6; }
    body {
      background:
        radial-gradient(1100px 520px at 15% -5%, rgba(225, 75, 90, 0.16), transparent 65%),
        radial-gradient(950px 500px at 88% 5%, rgba(124, 92, 255, 0.16), transparent 65%),
        radial-gradient(800px 420px at 50% 45%, rgba(56, 189, 248, 0.06), transparent 70%),
        var(--bg-0);
      min-height: 100vh;
      overflow-x: hidden;
    }
    a { color: var(--cyan); text-decoration: none; transition: color 0.15s ease; }
    a:hover { color: #7dd3fc; }
    .container { max-width: 1220px; margin: 0 auto; padding: 0 24px; }
    header.navbar {
      position: sticky;
      top: 0;
      z-index: 100;
      backdrop-filter: blur(14px);
      -webkit-backdrop-filter: blur(14px);
      background: rgba(7, 10, 18, 0.82);
      border-bottom: 1px solid var(--border);
    }
    .nav-inner {
      display: flex;
      align-items: center;
      justify-content: space-between;
      height: 68px;
      gap: 16px;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      color: var(--text-0);
      font-weight: 700;
      font-size: 1.08rem;
      letter-spacing: -0.02em;
    }
    .brand:hover { color: var(--text-0); }
    .brand-badge {
      font-family: var(--mono);
      font-size: 0.72rem;
      font-weight: 600;
      padding: 2px 8px;
      border-radius: 999px;
      background: rgba(225, 75, 90, 0.16);
      border: 1px solid rgba(225, 75, 90, 0.4);
      color: #fda4af;
    }
    .nav-links {
      display: flex;
      align-items: center;
      gap: 24px;
      list-style: none;
      font-size: 0.92rem;
    }
    .nav-links a { color: var(--text-1); font-weight: 500; }
    .nav-links a:hover { color: var(--text-0); }
    .nav-cta {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .btn {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 9px 16px;
      border-radius: 9px;
      font-size: 0.9rem;
      font-weight: 600;
      border: 1px solid var(--border);
      background: rgba(30, 41, 59, 0.65);
      color: var(--text-0);
      cursor: pointer;
      transition: all 0.16s ease;
    }
    .btn:hover {
      border-color: rgba(148, 163, 184, 0.4);
      background: rgba(51, 65, 85, 0.75);
      color: var(--text-0);
      transform: translateY(-1px);
    }
    .btn-primary {
      background: linear-gradient(135deg, var(--jet-red), #b91c47 55%, var(--dotnet-purple));
      border-color: rgba(255, 255, 255, 0.18);
      box-shadow: 0 8px 24px -6px rgba(225, 75, 90, 0.45);
    }
    .btn-primary:hover {
      box-shadow: 0 12px 28px -4px rgba(225, 75, 90, 0.6);
      border-color: rgba(255, 255, 255, 0.32);
    }
    .hero {
      padding: 68px 0 56px;
      position: relative;
    }
    .hero-grid {
      display: grid;
      grid-template-columns: 1.12fr 0.88fr;
      gap: 44px;
      align-items: center;
    }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 5px 13px;
      border-radius: 999px;
      font-size: 0.8rem;
      font-weight: 600;
      letter-spacing: 0.02em;
      background: rgba(56, 189, 248, 0.1);
      border: 1px solid rgba(56, 189, 248, 0.28);
      color: var(--cyan);
      margin-bottom: 18px;
    }
    .hero h1 {
      font-size: clamp(2.25rem, 4.2vw, 3.45rem);
      line-height: 1.1;
      letter-spacing: -0.03em;
      font-weight: 800;
      margin-bottom: 18px;
    }
    .gradient-text {
      background: linear-gradient(120deg, #ff7582 0%, #e14b5a 38%, #a78bfa 78%, #38bdf8 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }
    .hero p.lead {
      font-size: 1.12rem;
      color: var(--text-1);
      margin-bottom: 26px;
      max-width: 630px;
    }
    .install-box {
      background: var(--bg-code);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      margin-bottom: 22px;
      max-width: 580px;
      box-shadow: 0 14px 34px -12px rgba(0, 0, 0, 0.65);
    }
    .install-tabs {
      display: flex;
      gap: 6px;
      margin-bottom: 10px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.12);
      padding-bottom: 8px;
    }
    .install-tab {
      background: transparent;
      border: none;
      color: var(--text-2);
      font-size: 0.76rem;
      font-weight: 600;
      font-family: var(--mono);
      padding: 4px 10px;
      border-radius: 6px;
      cursor: pointer;
    }
    .install-tab.active {
      background: rgba(225, 75, 90, 0.18);
      color: #fda4af;
    }
    .install-cmd-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      font-family: var(--mono);
      font-size: 0.92rem;
      color: #e2e8f0;
      overflow-x: auto;
    }
    .install-cmd-row .prompt { color: var(--jet-red); user-select: none; margin-right: 8px; }
    .copy-btn {
      background: rgba(30, 41, 59, 0.8);
      border: 1px solid var(--border);
      color: var(--text-1);
      font-family: var(--mono);
      font-size: 0.75rem;
      padding: 5px 10px;
      border-radius: 6px;
      cursor: pointer;
      white-space: nowrap;
      transition: all 0.15s ease;
    }
    .copy-btn:hover { border-color: var(--cyan); color: var(--text-0); }
    .hero-pills {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 18px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.78rem;
      font-family: var(--mono);
      padding: 4px 10px;
      border-radius: 6px;
      background: rgba(17, 24, 39, 0.9);
      border: 1px solid var(--border);
      color: var(--text-1);
    }
    .pill-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--emerald); }
    .hero-art-card {
      background: linear-gradient(160deg, rgba(17, 24, 39, 0.92), rgba(9, 13, 22, 0.96));
      border: 1px solid rgba(225, 75, 90, 0.28);
      border-radius: 18px;
      padding: 22px;
      box-shadow:
        0 24px 60px -15px rgba(0, 0, 0, 0.75),
        0 0 40px -12px rgba(225, 75, 90, 0.22);
      position: relative;
    }
    .hero-art-card svg {
      width: 100%;
      height: auto;
      display: block;
    }
    .art-caption {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px solid var(--border);
      font-size: 0.78rem;
      color: var(--text-2);
      font-family: var(--mono);
    }
    section { padding: 56px 0; border-top: 1px solid rgba(148, 163, 184, 0.1); }
    .section-header {
      max-width: 740px;
      margin-bottom: 36px;
    }
    .section-tag {
      font-family: var(--mono);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--jet-red);
      font-weight: 700;
      margin-bottom: 8px;
    }
    .section-header h2 {
      font-size: clamp(1.65rem, 2.8vw, 2.25rem);
      letter-spacing: -0.025em;
      margin-bottom: 10px;
      color: var(--text-0);
    }
    .section-header p { color: var(--text-1); font-size: 1.02rem; }
    .features-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 20px;
    }
    .feature-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 24px;
      transition: transform 0.18s ease, border-color 0.18s ease;
    }
    .feature-card:hover {
      transform: translateY(-2px);
      border-color: var(--border-hi);
    }
    .feature-icon {
      width: 42px;
      height: 42px;
      border-radius: 10px;
      display: flex;
      align-items: center;
      justify-content: center;
      margin-bottom: 16px;
      background: rgba(225, 75, 90, 0.12);
      border: 1px solid rgba(225, 75, 90, 0.3);
      color: #fda4af;
    }
    .feature-card h3 {
      font-size: 1.08rem;
      margin-bottom: 8px;
      color: var(--text-0);
    }
    .feature-card p {
      font-size: 0.92rem;
      color: var(--text-1);
    }
    .feature-card code, td code, p code {
      font-family: var(--mono);
      font-size: 0.84em;
      background: rgba(15, 23, 42, 0.85);
      border: 1px solid rgba(148, 163, 184, 0.18);
      padding: 1px 6px;
      border-radius: 5px;
      color: #f1f5f9;
    }
    .code-showcase {
      background: var(--bg-code);
      border: 1px solid var(--border);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 20px 50px -15px rgba(0, 0, 0, 0.7);
    }
    .code-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      padding: 10px 14px;
      background: rgba(17, 24, 39, 0.9);
      border-bottom: 1px solid var(--border);
    }
    .code-tab {
      background: transparent;
      border: 1px solid transparent;
      color: var(--text-2);
      font-family: var(--sans);
      font-size: 0.86rem;
      font-weight: 600;
      padding: 7px 14px;
      border-radius: 8px;
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .code-tab:hover { color: var(--text-0); }
    .code-tab.active {
      background: rgba(225, 75, 90, 0.16);
      border-color: rgba(225, 75, 90, 0.4);
      color: #fecdd3;
    }
    .code-panel { display: none; padding: 24px; }
    .code-panel.active { display: grid; grid-template-columns: 1.35fr 0.65fr; gap: 28px; align-items: start; }
    .code-panel pre {
      background: #05080f;
      border: 1px solid rgba(148, 163, 184, 0.14);
      border-radius: 10px;
      padding: 18px;
      overflow-x: auto;
      font-family: var(--mono);
      font-size: 0.86rem;
      line-height: 1.65;
      color: #e2e8f0;
    }
    .code-notes h3 { font-size: 1.14rem; margin-bottom: 10px; color: var(--text-0); }
    .code-notes p { font-size: 0.92rem; color: var(--text-1); margin-bottom: 14px; }
    .code-notes ul { list-style: none; display: grid; gap: 8px; }
    .code-notes li {
      font-size: 0.88rem;
      color: var(--text-1);
      padding-left: 18px;
      position: relative;
    }
    .code-notes li::before {
      content: "▸";
      position: absolute;
      left: 0;
      color: var(--jet-red);
    }
    .kw { color: #f472b6; }
    .ty { color: #38bdf8; }
    .st { color: #a7f3d0; }
    .cm { color: #64748b; font-style: italic; }
    .nm { color: #fcd34d; }
    .matrix-wrap {
      overflow-x: auto;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 14px;
    }
    table.matrix {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
      text-align: left;
    }
    table.matrix th, table.matrix td {
      padding: 14px 18px;
      border-bottom: 1px solid var(--border);
    }
    table.matrix th {
      background: rgba(15, 23, 42, 0.85);
      color: var(--text-0);
      font-weight: 700;
      font-size: 0.84rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    table.matrix tr:last-child td { border-bottom: none; }
    table.matrix td { color: var(--text-1); }
    .badge-ok {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-family: var(--mono);
      background: rgba(52, 211, 153, 0.14);
      border: 1px solid rgba(52, 211, 153, 0.35);
      color: #6ee7b7;
    }
    .rigor-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 20px;
    }
    .rigor-card {
      background: linear-gradient(145deg, rgba(17, 24, 39, 0.85), rgba(11, 15, 25, 0.92));
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 24px;
    }
    .rigor-card h3 {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 1.06rem;
      margin-bottom: 10px;
      color: var(--text-0);
    }
    .rigor-card p { font-size: 0.92rem; color: var(--text-1); }
    .docs-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
    }
    .doc-link-card {
      display: block;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      transition: all 0.16s ease;
    }
    .doc-link-card:hover {
      border-color: var(--cyan);
      transform: translateY(-2px);
      background: rgba(30, 41, 59, 0.65);
    }
    .doc-link-card h4 {
      color: var(--text-0);
      font-size: 1rem;
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .doc-link-card p {
      color: var(--text-2);
      font-size: 0.86rem;
    }
    footer {
      border-top: 1px solid var(--border);
      padding: 40px 0;
      margin-top: 40px;
      color: var(--text-2);
      font-size: 0.88rem;
    }
    .footer-inner {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
    }
    .footer-links {
      display: flex;
      flex-wrap: wrap;
      gap: 20px;
      align-items: center;
    }
    @media (max-width: 980px) {
      .hero-grid, .code-panel.active { grid-template-columns: 1fr; }
      .features-grid, .docs-grid { grid-template-columns: repeat(2, 1fr); }
      .rigor-grid { grid-template-columns: 1fr; }
      .nav-links { display: none; }
    }
    @media (max-width: 640px) {
      .features-grid, .docs-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header class="navbar">
    <div class="container nav-inner">
      <a href="#" class="brand">
        <svg width="36" height="36" viewBox="0 0 64 64" aria-label="JetDatabaseWriter Logo" role="img">
          <defs>
            <linearGradient id="nav-cyl" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#ff6b7a" />
              <stop offset="50%" stop-color="#c93b4b" />
              <stop offset="100%" stop-color="#6d4aff" />
            </linearGradient>
            <linearGradient id="nav-jet" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#38bdf8" />
              <stop offset="100%" stop-color="#a78bfa" />
            </linearGradient>
          </defs>
          <rect width="64" height="64" rx="14" fill="#111827" stroke="rgba(225,75,90,0.45)" stroke-width="2" />
          <path d="M12 43c0 4.4 9 8 20 8s20-3.6 20-8v-8c0 4.4-9 8-20 8s-20-3.6-20-8v8z" fill="url(#nav-cyl)" opacity="0.78" />
          <path d="M12 32c0 4.4 9 8 20 8s20-3.6 20-8v-8c0 4.4-9 8-20 8s-20-3.6-20-8v8z" fill="url(#nav-cyl)" opacity="0.9" />
          <ellipse cx="32" cy="22" rx="20" ry="8" fill="url(#nav-cyl)" />
          <ellipse cx="32" cy="22" rx="13" ry="4.2" fill="#0b0f19" opacity="0.55" />
          <path d="M23 32L51 12L40 44L33 35L23 32Z" fill="url(#nav-jet)" stroke="#0b0f19" stroke-width="2" stroke-linejoin="round" />
        </svg>
        <span>JetDatabaseWriter</span>
        <span class="brand-badge">v1.0</span>
      </a>
      <ul class="nav-links">
        <li><a href="#features">Features</a></li>
        <li><a href="#examples">Examples</a></li>
        <li><a href="#formats">Formats &amp; Encryption</a></li>
        <li><a href="#verification">Verification</a></li>
        <li><a href="#docs">Documentation</a></li>
      </ul>
      <div class="nav-cta">
        <a class="btn" href="https://www.nuget.org/packages/JetDatabaseWriter/" target="_blank" rel="noopener">NuGet</a>
        <a class="btn btn-primary" href="https://github.com/symbiogenesis/JetDatabaseWriter" target="_blank" rel="noopener">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>
          <span>GitHub</span>
        </a>
      </div>
    </div>
  </header>
  <main>
    <section class="hero" style="border-top:none;">
      <div class="container hero-grid">
        <div>
          <div class="eyebrow">
            <span>PURE MANAGED .NET STORAGE ENGINE FOR ACCESS</span>
          </div>
          <h1>
            Read &amp; Write <span class="gradient-text">.mdb and .accdb</span> Databases Anywhere.
          </h1>
          <p class="lead">
            <strong>JetDatabaseWriter</strong> is a 100% managed C# library for creating, querying, and updating Microsoft Access (Jet3, Jet4, and ACE) databases directly from files or streams &mdash; with zero OleDB, ODBC, COM, or native driver dependencies.
          </p>
          <div class="install-box">
            <div class="install-tabs" role="tablist">
              <button class="install-tab active" type="button" onclick="setInstallCmd('dotnet add package JetDatabaseWriter', this)">.NET CLI</button>
              <button class="install-tab" type="button" onclick="setInstallCmd('Install-Package JetDatabaseWriter', this)">Package Manager</button>
              <button class="install-tab" type="button" onclick="setInstallCmd('&lt;PackageReference Include=&quot;JetDatabaseWriter&quot; Version=&quot;1.0.*&quot; /&gt;', this)">PackageReference</button>
            </div>
            <div class="install-cmd-row">
              <div><span class="prompt">$</span><span id="install-cmd-text">dotnet add package JetDatabaseWriter</span></div>
              <button class="copy-btn" type="button" id="copy-install-btn" onclick="copyInstallCmd()">Copy</button>
            </div>
          </div>
          <div style="display:flex;flex-wrap:wrap;gap:12px;">
            <a class="btn btn-primary" href="#examples">Explore C# Examples</a>
            <a class="btn" href="reading.html">Reading Guide</a>
            <a class="btn" href="writing.html">Writing Guide</a>
            <a class="btn" href="support-contract.html">Support Contract</a>
          </div>
          <div class="hero-pills">
            <span class="pill"><span class="pill-dot"></span>net11.0 &amp; netstandard2.1</span>
            <span class="pill"><span class="pill-dot"></span>Windows · Linux · macOS</span>
            <span class="pill"><span class="pill-dot"></span>Trimmed &amp; NativeAOT</span>
            <span class="pill"><span class="pill-dot"></span>Dafny-Verified Recovery &amp; Codec Kernels</span>
          </div>
        </div>
        <div class="hero-art-card">
          <svg id="jdw-hero-svg" viewBox="0 0 540 420" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="JetDatabaseWriter Emblem and On-Disk Page Architecture">
            <defs>
              <linearGradient id="hero-bg-grad" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#131c31" />
                <stop offset="100%" stop-color="#090d16" />
              </linearGradient>
              <linearGradient id="cyl-top" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#ff7582" />
                <stop offset="48%" stop-color="#e14b5a" />
                <stop offset="100%" stop-color="#8b5cf6" />
              </linearGradient>
              <linearGradient id="cyl-mid" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stop-color="#be123c" />
                <stop offset="50%" stop-color="#e14b5a" />
                <stop offset="100%" stop-color="#6d28d9" />
              </linearGradient>
              <linearGradient id="cyl-bot" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stop-color="#881337" />
                <stop offset="50%" stop-color="#a4373a" />
                <stop offset="100%" stop-color="#4c1d95" />
              </linearGradient>
              <linearGradient id="jet-wing" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#7dd3fc" />
                <stop offset="45%" stop-color="#38bdf8" />
                <stop offset="100%" stop-color="#818cf8" />
              </linearGradient>
              <filter id="soft-glow" x="-20%" y="-20%" width="140%" height="140%">
                <feGaussianBlur stdDeviation="8" result="blur" />
                <feComposite in="SourceGraphic" in2="blur" operator="over" />
              </filter>
            </defs>
            <rect width="540" height="420" rx="14" fill="url(#hero-bg-grad)" />
            <!-- Subtle technical grid -->
            <g stroke="rgba(148,163,184,0.07)" stroke-width="1">
              <line x1="0" y1="60" x2="540" y2="60" />
              <line x1="0" y1="120" x2="540" y2="120" />
              <line x1="0" y1="180" x2="540" y2="180" />
              <line x1="0" y1="240" x2="540" y2="240" />
              <line x1="0" y1="300" x2="540" y2="300" />
              <line x1="0" y1="360" x2="540" y2="360" />
              <line x1="90" y1="0" x2="90" y2="420" />
              <line x1="180" y1="0" x2="180" y2="420" />
              <line x1="270" y1="0" x2="270" y2="420" />
              <line x1="360" y1="0" x2="360" y2="420" />
              <line x1="450" y1="0" x2="450" y2="420" />
            </g>
            <!-- Left: Isometric JET/ACE 3-tier Database Stack + Supersonic Jet Cursor -->
            <g transform="translate(22, 18)">
              <!-- Bottom tier: Jet3 2KB / LVAL & Usage Maps -->
              <path d="M36 226c0 19 46 34 102 34s102-15 102-34v-36c0 19-46 34-102 34s-102-15-102-34v36z" fill="url(#cyl-bot)" />
              <ellipse cx="138" cy="190" rx="102" ry="34" fill="#4c1d95" opacity="0.45" stroke="rgba(255,255,255,0.18)" />
              <!-- Middle tier: Jet4 4KB / B+Tree Indexes & TDEF -->
              <path d="M36 172c0 19 46 34 102 34s102-15 102-34v-36c0 19-46 34-102 34s-102-15-102-34v36z" fill="url(#cyl-mid)" />
              <ellipse cx="138" cy="136" rx="102" ry="34" fill="#881337" opacity="0.45" stroke="rgba(255,255,255,0.22)" />
              <!-- Top tier: ACE .accdb / Slotted Data Pages -->
              <path d="M36 118c0 19 46 34 102 34s102-15 102-34V82c0 19-46 34-102 34S36 101 36 82v36z" fill="url(#cyl-top)" />
              <ellipse cx="138" cy="82" rx="102" ry="34" fill="url(#cyl-top)" stroke="rgba(255,255,255,0.35)" stroke-width="1.5" />
              <!-- Inner concentric page-ring tracks on top cylinder -->
              <ellipse cx="138" cy="82" rx="72" ry="22" fill="#0b0f19" opacity="0.65" stroke="rgba(56,189,248,0.45)" stroke-dasharray="6 4" />
              <ellipse cx="138" cy="82" rx="36" ry="11" fill="#0b0f19" opacity="0.85" stroke="#ff7582" stroke-width="1.5" />
              <!-- Tier labels -->
              <text x="64" y="132" fill="#fff1f2" font-family="monospace" font-size="11" font-weight="bold">ACE .ACCDB · 4096B</text>
              <text x="64" y="186" fill="#fecdd3" font-family="monospace" font-size="11" font-weight="bold">JET4 .MDB  · UTF-16</text>
              <text x="64" y="240" fill="#e9d5ff" font-family="monospace" font-size="11" font-weight="bold">JET3 .MDB  · 2048B</text>
              <!-- Supersonic Jet Delta Write-Head slicing into the page stack -->
              <path d="M118 106 L252 18 L202 156 L168 119 L118 106 Z" fill="url(#jet-wing)" filter="url(#soft-glow)" stroke="#090d16" stroke-width="2.5" stroke-linejoin="round" />
              <path d="M168 119 L252 18 L184 134 Z" fill="#e0f2fe" opacity="0.82" />
            </g>
            <!-- Right: On-Disk Page & Verification Telemetry Cards -->
            <g transform="translate(300, 32)">
              <rect x="0" y="0" width="214" height="72" rx="9" fill="#0f172a" stroke="rgba(56,189,248,0.4)" />
              <text x="14" y="22" fill="#38bdf8" font-family="monospace" font-size="10.5" font-weight="bold">0x01 · DATA &amp; TDEF PAGES</text>
              <text x="14" y="41" fill="#e2e8f0" font-family="monospace" font-size="11">Slotted rows · OMAP bitmaps</text>
              <text x="14" y="58" fill="#94a3b8" font-family="monospace" font-size="10">LVAL inline &amp; multi-page chains</text>
              <rect x="0" y="88" width="214" height="72" rx="9" fill="#0f172a" stroke="rgba(225,75,90,0.45)" />
              <text x="14" y="110" fill="#fb7185" font-family="monospace" font-size="10.5" font-weight="bold">0x04 · B+TREE INDEX NODES</text>
              <text x="14" y="129" fill="#e2e8f0" font-family="monospace" font-size="11">Gen97 · GenLegacy · General</text>
              <text x="14" y="146" fill="#94a3b8" font-family="monospace" font-size="10">Prefix-compressed branch &amp; leaf</text>
              <rect x="0" y="176" width="214" height="72" rx="9" fill="#0f172a" stroke="rgba(167,139,250,0.45)" />
              <text x="14" y="198" fill="#c4b5fd" font-family="monospace" font-size="10.5" font-weight="bold">CRYPTO · RC4 &amp; AGILE AES</text>
              <text x="14" y="217" fill="#e2e8f0" font-family="monospace" font-size="11">AES-128/192/256 · CBC &amp; CFB8</text>
              <text x="14" y="234" fill="#94a3b8" font-family="monospace" font-size="10">CryptoAPI &amp; Standard AES</text>
            </g>
            <!-- Bottom Span: Verified Recovery & LINQ Pipeline Banner -->
            <g transform="translate(26, 304)">
              <rect x="0" y="0" width="488" height="88" rx="11" fill="#0b1120" stroke="rgba(52,211,153,0.38)" />
              <circle cx="24" cy="26" r="5" fill="#34d399" />
              <text x="38" y="30" fill="#6ee7b7" font-family="monospace" font-size="11" font-weight="bold">DAFNY-VERIFIED KERNELS + ASYNC-FIRST .NET API</text>
              <text x="20" y="53" fill="#e2e8f0" font-family="monospace" font-size="11.5">Stream &lt;-&gt; CLOCK Cache &lt;-&gt; .jdw-journal Undo &lt;-&gt; IQueryable&lt;T&gt;</text>
              <text x="20" y="73" fill="#94a3b8" font-family="monospace" font-size="10.5">Zero native DLLs · Checked arithmetic · Native DAO 3.6 &amp; 12.0 verified</text>
            </g>
          </svg>
          <div class="art-caption">
            <span>DIRECT PAGE-LEVEL STORAGE ENGINE</span>
            <button class="copy-btn" type="button" onclick="downloadLogoSvg()">Download Logo SVG</button>
          </div>
        </div>
      </div>
    </section>
    <section id="features">
      <div class="container">
        <div class="section-header">
          <div class="section-tag">CAPABILITIES</div>
          <h2>Everything Needed to Read, Query, and Author Access Databases</h2>
          <p>Built from the ground up in pure C# to manipulate JET and ACE page frames, catalogs, B+Tree indexes, and complex columns directly.</p>
        </div>
        <div class="features-grid">
          <div class="feature-card">
            <div class="feature-icon">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
            </div>
            <h3>Pure Managed .NET</h3>
            <p>Targets <code>net11.0</code> and <code>netstandard2.1</code> (.NET Core 3.x and .NET 5&ndash;11). Runs on Windows, Linux, and macOS with zero P/Invoke, COM, OleDB, or ACE redistributable drivers.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
            </div>
            <h3>Jet3, Jet4 &amp; ACE Formats</h3>
            <p>Read, write, and create Access 97 (<code>Jet3Mdb</code>), Access 2000&ndash;2003 (<code>Jet4Mdb</code>), and Access 2007+ (<code>AceAccdb</code>) files from disk paths or any seekable <code>Stream</code>.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
            </div>
            <h3>POCO Mapping &amp; Async LINQ</h3>
            <p>Stream rows with <code>Rows&lt;T&gt;</code>, seek B+Tree indexes via <code>FromIndex&lt;T&gt;</code>, or compose <code>Query&lt;T&gt;</code> (<code>IQueryable&lt;T&gt;</code>) with automatic index selection and <code>Include</code> / <code>ThenInclude</code>.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            </div>
            <h3>Full Schema &amp; Row Writer</h3>
            <p>Create, rename, and drop tables, columns, and indexes; enforce primary and foreign keys with cascading updates and deletes; evaluate defaults, validation rules, and calculated fields.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
            </div>
            <h3>Built-In Encryption &amp; Maintenance</h3>
            <p>Read and update Jet3/Jet4 RC4, ACE CryptoAPI, Standard AES, and Agile AES-128/192/256 databases. Encrypt, decrypt, and rotate passwords with atomic file replacement.</p>
          </div>
          <div class="feature-card">
            <div class="feature-icon">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
            </div>
            <h3>Transactions &amp; Crash Recovery</h3>
            <p>Group or nest mutations with <code>CommitAsync</code> and <code>RollbackAsync</code>. Adjacent <code>.jdw-journal</code> rollback journals and Dafny-verified kernels recover interrupted file transactions on reopen.</p>
          </div>
        </div>
      </div>
    </section>
    <section id="examples">
      <div class="container">
        <div class="section-header">
          <div class="section-tag">CODE EXAMPLES</div>
          <h2>Clean, Async-First C# APIs</h2>
          <p>Switch between reading, LINQ querying, schema authoring, transactions, encryption, and CLI model scaffolding.</p>
        </div>
        <div class="code-showcase">
          <div class="code-tabs" role="tablist">
            <button class="code-tab active" type="button" onclick="showTab('tab-read', this)">1. Read &amp; Stream</button>
            <button class="code-tab" type="button" onclick="showTab('tab-linq', this)">2. IQueryable &amp; Include</button>
            <button class="code-tab" type="button" onclick="showTab('tab-write', this)">3. Create &amp; Write</button>
            <button class="code-tab" type="button" onclick="showTab('tab-tx', this)">4. Transactions</button>
            <button class="code-tab" type="button" onclick="showTab('tab-crypto', this)">5. Encryption</button>
            <button class="code-tab" type="button" onclick="showTab('tab-scaffold', this)">6. CLI Scaffolding</button>
          </div>
          <div id="tab-read" class="code-panel active">
<pre><code><span class="kw">using</span> JetDatabaseWriter;
<span></span>
<span class="kw">await using var</span> reader = <span class="kw">await</span> <span class="ty">AccessReader</span>.OpenAsync(<span class="st">"Northwind.mdb"</span>);
<span></span>
<span class="cm">// List user tables in the catalog</span>
<span class="kw">var</span> tables = <span class="kw">await</span> reader.ListTablesAsync();
<span></span>
<span class="cm">// Stream strongly-typed POCOs lazily with automatic index inference</span>
<span class="kw">await foreach</span> (<span class="kw">var</span> order <span class="kw">in</span> reader.Rows&lt;<span class="ty">Order</span>&gt;(<span class="st">"Orders"</span>, o =&gt; o.CustomerId == <span class="nm">42</span>))
{
    Console.WriteLine(<span class="st">$"Order #{order.OrderID}: {order.Freight:C}"</span>);
}
<span></span>
<span class="kw">public sealed class</span> <span class="ty">Order</span>
{
    <span class="kw">public int</span> OrderID { <span class="kw">get</span>; <span class="kw">set</span>; }
    <span class="kw">public int</span> CustomerId { <span class="kw">get</span>; <span class="kw">set</span>; }
    <span class="kw">public decimal</span> Freight { <span class="kw">get</span>; <span class="kw">set</span>; }
}</code></pre>
            <div class="code-notes">
              <h3>Streaming &amp; POCO Projection</h3>
              <p>Properties match column names case-insensitively or via <code>[Column("...")]</code> attributes.</p>
              <ul>
                <li>Narrow projections decode only bound columns and skip unbound long-value chains.</li>
                <li><code>Rows&lt;T&gt;</code> streams over <code>IAsyncEnumerable&lt;T&gt;</code> with bounded memory.</li>
                <li><code>FromIndex&lt;T&gt;</code> offers explicit B+Tree range and equality seeks.</li>
              </ul>
            </div>
          </div>
          <div id="tab-linq" class="code-panel">
<pre><code><span class="kw">using</span> JetDatabaseWriter;
<span class="kw">using</span> JetDatabaseWriter.Linq;
<span></span>
<span class="kw">await using var</span> reader = <span class="kw">await</span> <span class="ty">AccessReader</span>.OpenAsync(<span class="st">"Northwind.accdb"</span>);
<span></span>
<span class="kw">var</span> recentOrders = <span class="kw">await</span> reader.Query&lt;<span class="ty">Order</span>&gt;(<span class="st">"Orders"</span>)
    .Include(o =&gt; o.Customer)
    .ThenInclude(c =&gt; c!.Region)
    .Where(o =&gt; o.Freight &gt;= <span class="nm">50m</span>)
    .OrderByDescending(o =&gt; o.OrderID)
    .Take(<span class="nm">25</span>)
    .ToListAsync();</code></pre>
            <div class="code-notes">
              <h3>Async <code>IQueryable&lt;T&gt;</code> Provider</h3>
              <p>Compose queries using familiar LINQ operators with relationship-inferred eager loading.</p>
              <ul>
                <li>Automatically selects covering unique/non-null B+Tree indexes for <code>Where</code>, <code>OrderBy</code>, <code>MinAsync</code>, and <code>MaxAsync</code>.</li>
                <li><code>Include</code> and <code>ThenInclude</code> resolve foreign keys declared in <code>MSysRelationships</code>.</li>
                <li>Async terminals: <code>ToListAsync</code>, <code>FirstOrDefaultAsync</code>, <code>CountAsync</code>, <code>AnyAsync</code>.</li>
              </ul>
            </div>
          </div>
          <div id="tab-write" class="code-panel">
<pre><code><span class="kw">using</span> JetDatabaseWriter;
<span class="kw">using</span> JetDatabaseWriter.Enums;
<span class="kw">using</span> JetDatabaseWriter.Models;
<span></span>
<span class="kw">await using var</span> writer = <span class="kw">await</span> <span class="ty">AccessWriter</span>.CreateDatabaseAsync(
    <span class="st">"contacts.accdb"</span>, <span class="ty">DatabaseFormat</span>.AceAccdb);
<span></span>
<span class="kw">await</span> writer.CreateTableAsync(<span class="st">"Contacts"</span>, <span class="kw">new</span>[]
{
    <span class="kw">new</span> <span class="ty">ColumnDefinition</span>(<span class="st">"Id"</span>, <span class="kw">typeof</span>(<span class="kw">int</span>)) { IsPrimaryKey = <span class="kw">true</span>, IsAutoNumber = <span class="kw">true</span> },
    <span class="kw">new</span> <span class="ty">ColumnDefinition</span>(<span class="st">"Email"</span>, <span class="kw">typeof</span>(<span class="kw">string</span>), maxLength: <span class="nm">120</span>) { IsRequired = <span class="kw">true</span> },
    <span class="kw">new</span> <span class="ty">ColumnDefinition</span>(<span class="st">"CreatedUtc"</span>, <span class="kw">typeof</span>(<span class="ty">DateTime</span>)) { DefaultValue = <span class="st">"=Now()"</span> },
}, <span class="kw">new</span>[]
{
    <span class="kw">new</span> <span class="ty">IndexDefinition</span>(<span class="st">"IX_Contacts_Email"</span>, <span class="st">"Email"</span>) { IsUnique = <span class="kw">true</span> },
});
<span></span>
<span class="kw">await</span> writer.InsertRowAsync(<span class="st">"Contacts"</span>, <span class="kw">new</span> <span class="ty">RowValues</span>
{
    [<span class="st">"Email"</span>] = <span class="st">"alice@example.com"</span>,
});</code></pre>
            <div class="code-notes">
              <h3>Authoring &amp; Schema Evolution</h3>
              <p>Create databases from scratch or open existing files with <code>AccessWriter.OpenAsync</code>.</p>
              <ul>
                <li>Maintains B+Tree indexes, usage maps, AutoNumber seeds, and foreign-key cascades.</li>
                <li>Supports complex columns (Attachments, Multi-Value fields, Memo version history) and ACCDB calculated columns.</li>
                <li>Oversized values refuse cleanly before mutating storage.</li>
              </ul>
            </div>
          </div>
          <div id="tab-tx" class="code-panel">
<pre><code><span class="kw">using</span> JetDatabaseWriter;
<span class="kw">using</span> JetDatabaseWriter.Models;
<span></span>
<span class="kw">await using var</span> writer = <span class="kw">await</span> <span class="ty">AccessWriter</span>.OpenAsync(<span class="st">"ledger.accdb"</span>);
<span class="kw">await using var</span> tx = <span class="kw">await</span> writer.BeginTransactionAsync();
<span></span>
<span class="kw">await</span> writer.UpdateRowsAsync(
    <span class="st">"Accounts"</span>,
    <span class="ty">RowCriteria</span>.Where(<span class="st">"AccountId"</span>, <span class="nm">101</span>),
    <span class="kw">new</span> <span class="ty">RowValues</span> { [<span class="st">"Balance"</span>] = <span class="nm">950.00m</span> });
<span></span>
<span class="kw">await</span> writer.InsertRowAsync(<span class="st">"AuditLog"</span>, <span class="kw">new</span> <span class="ty">RowValues</span>
{
    [<span class="st">"AccountId"</span>] = <span class="nm">101</span>,
    [<span class="st">"Delta"</span>] = <span class="nm">-50.00m</span>,
});
<span></span>
<span class="kw">await</span> tx.CommitAsync();</code></pre>
            <div class="code-notes">
              <h3>Atomic Statements &amp; Crash Recovery</h3>
              <p>Group multiple row and schema changes into atomic transactions with nested savepoints.</p>
              <ul>
                <li>File-backed writers record pre-images in an adjacent <code>.jdw-journal</code> undo journal.</li>
                <li>If a process crashes mid-transaction, reopening the file with <code>AccessWriter</code> replays verified undo records automatically.</li>
                <li>Caller streams support full in-process live rollback.</li>
              </ul>
            </div>
          </div>
          <div id="tab-crypto" class="code-panel">
<pre><code><span class="kw">using</span> JetDatabaseWriter;
<span></span>
<span class="cm">// Open an encrypted database</span>
<span class="kw">await using var</span> reader = <span class="kw">await</span> <span class="ty">AccessReader</span>.OpenAsync(
    <span class="st">"protected.accdb"</span>,
    <span class="kw">new</span> <span class="ty">AccessReaderOptions</span>(<span class="st">"correct-horse-battery-staple"</span>));
<span></span>
<span class="cm">// Encrypt, decrypt, or rotate passwords in-place via atomic staging</span>
<span class="kw">await</span> <span class="ty">AccessDatabaseEncryption</span>.ChangePasswordAsync(
    <span class="st">"protected.accdb"</span>,
    oldPassword: <span class="st">"correct-horse-battery-staple"</span>,
    newPassword: <span class="st">"new-strong-passphrase"</span>);</code></pre>
            <div class="code-notes">
              <h3>Multi-Generation Cryptography</h3>
              <p>Read, update, and maintain encrypted Access databases using managed .NET cryptography.</p>
              <ul>
                <li><strong>ACE Agile AES</strong>: AES-128/192/256 (CBC &amp; CFB8) with SHA-1/256/384/512; creates &amp; re-encrypts with AES-256-CBC / SHA-512.</li>
                <li><strong>ACE Standard &amp; CryptoAPI</strong>: Read and update native pages.</li>
                <li><strong>Jet3 &amp; Jet4 RC4</strong>: Full password &amp; page encryption, including security SID/Owner remasking.</li>
              </ul>
            </div>
          </div>
          <div id="tab-scaffold" class="code-panel">
<pre><code><span class="cm"># Generate C# entity classes and relationship navigations from an existing file</span>
dotnet run --project src/JetDatabaseWriter.Scaffold -- \
  --database Northwind.accdb \
  --output ./Models \
  --namespace MyApp.Data \
  --records \
  --nullable</code></pre>
            <div class="code-notes">
              <h3>Entity Model Scaffolding CLI</h3>
              <p>Inspect any <code>.mdb</code> or <code>.accdb</code> catalog and emit ready-to-compile C# models.</p>
              <ul>
                <li>Generates classes or records with XML comments showing native JET column types and byte sizes.</li>
                <li>Emits <code>[Table]</code> and <code>[Column]</code> attributes automatically when identifiers contain spaces or symbols.</li>
                <li>Infers parent/child navigation properties from <code>MSysRelationships</code> for <code>Query&lt;T&gt;.Include(...)</code>.</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </section>
    <section id="formats">
      <div class="container">
        <div class="section-header">
          <div class="section-tag">COMPATIBILITY MATRIX</div>
          <h2>Supported Access Generations &amp; On-Disk Formats</h2>
          <p>Interoperability is continuously verified against Microsoft-authored fixtures, DAO 3.6, and pinned ACEDAO 12.0.</p>
        </div>
        <div class="matrix-wrap">
          <table class="matrix">
            <thead>
              <tr>
                <th>Capability</th>
                <th>Jet3 (<code>.mdb</code> · Access 97)</th>
                <th>Jet4 (<code>.mdb</code> · Access 2000&ndash;2003)</th>
                <th>ACE (<code>.accdb</code> · Access 2007+)</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>Page Size &amp; Text Encoding</strong></td>
                <td>2,048 bytes · Database Code Page</td>
                <td>4,096 bytes · UTF-16LE + Compression</td>
                <td>4,096 bytes · UTF-16LE + Compression</td>
              </tr>
              <tr>
                <td><strong>Create / Read / Write / Schema</strong></td>
                <td><span class="badge-ok">Supported</span></td>
                <td><span class="badge-ok">Supported</span></td>
                <td><span class="badge-ok">Supported</span></td>
              </tr>
              <tr>
                <td><strong>B+Tree Text Collations</strong></td>
                <td><code>General97</code></td>
                <td><code>GeneralLegacy</code> · <code>General</code></td>
                <td><code>General</code> · <code>GeneralLegacy</code></td>
              </tr>
              <tr>
                <td><strong>Encryption &amp; Passwords</strong></td>
                <td>Jet3 RC4 &amp; Header Password</td>
                <td>Jet4 RC4, Header Password &amp; SID Remasking</td>
                <td>Agile AES-128/192/256, Standard AES &amp; CryptoAPI RC4</td>
              </tr>
              <tr>
                <td><strong>Complex Columns (Attachments / MVF)</strong></td>
                <td>N/A in format</td>
                <td>N/A in format</td>
                <td><span class="badge-ok">Read, Add, Replace, Delete &amp; Memo History</span></td>
              </tr>
              <tr>
                <td><strong>Calculated Columns</strong></td>
                <td>N/A in format</td>
                <td>N/A in format</td>
                <td><span class="badge-ok">Native Expressions, Recompute &amp; Regional Profiles</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
    <section id="verification">
      <div class="container">
        <div class="section-header">
          <div class="section-tag">ENGINEERING RIGOR</div>
          <h2>Formal Verification, Native DAO Gate &amp; Fuzzing</h2>
          <p>Storage engines demand strict invariants. JetDatabaseWriter combines mathematical proof models, native Microsoft Access runtime validation, and adversarial fuzzing.</p>
        </div>
        <div class="rigor-grid">
          <div class="rigor-card">
            <h3>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#34d399" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
              <span>Dafny-Verified Recovery &amp; Codec Kernels</span>
            </h3>
            <p>Production recovery and value parsing execute reviewed C# generated from verified <a href="https://github.com/symbiogenesis/JetDatabaseWriter/tree/main/verification">Dafny specifications</a> covering bounded journal framing, replay identity, replacement field validation, byte authorization, long-value descriptors, data-page row mutations, index-entry ordering/leaf-chain admission, and usage-map bitmaps. No solver or verifier runtime ships with the package.</p>
          </div>
          <div class="rigor-card">
            <h3>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>
              <span>Zero-Skip Hosted ACEDAO 12.0 &amp; DAO 3.6 Validation</span>
            </h3>
            <p>Every CI run provisions the pinned Microsoft Access Runtime (<code>ACEDAO.DLL</code>) and executes a zero-skip native compatibility suite across <code>net11.0</code> and <code>netstandard2.1</code>, verifying that databases created or mutated by JetDatabaseWriter open, query, update, and pass native Compact &amp; Repair in Microsoft Access.</p>
          </div>
          <div class="rigor-card">
            <h3>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#f472b6" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
              <span>Hostile-File Fuzzing &amp; CVE Traceability</span>
            </h3>
            <p>Parser bounds are hardened against cyclic page chains, out-of-range offsets, and malformed descriptors. Every historical JET/ACE vulnerability in the <a href="cve-vulnerability-analysis.html">CVE vulnerability analysis</a> is tracked against canonical <code>[Trait("CveAnalogue", ...)]</code> xUnit regressions and continuous mutation/truncation fuzzing.</p>
          </div>
          <div class="rigor-card">
            <h3>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#fbbf24" stroke-width="2"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
              <span>Strict Static Analysis, Reproducible Builds &amp; NativeAOT</span>
            </h3>
            <p>Compiled with <code>TreatWarningsAsErrors</code>, <code>AnalysisLevel latest-all</code>, <code>WarningLevel 9999</code>, checked arithmetic, Roslynator, StyleCop, and InferSharp v1.5 memory-safety analysis. Trimmed and NativeAOT smoke consumers verify every build.</p>
          </div>
        </div>
      </div>
    </section>
    <section id="docs">
      <div class="container">
        <div class="section-header">
          <div class="section-tag">DOCUMENTATION</div>
          <h2>Guides, Contracts &amp; Format References</h2>
          <p>Deep-dive into the API guides, operating envelope, and on-disk binary format documentation.</p>
        </div>
        <div class="docs-grid">
          <a class="doc-link-card" href="reading.html">
            <h4><span>Reading &amp; Querying Data</span><span>&rarr;</span></h4>
            <p>POCO mapping, <code>Rows&lt;T&gt;</code> streaming, <code>Query&lt;T&gt;</code> LINQ, <code>Include</code> relationships, B+Tree index seeks, and attachments.</p>
          </a>
          <a class="doc-link-card" href="writing.html">
            <h4><span>Writing Data &amp; Schemas</span><span>&rarr;</span></h4>
            <p>Creating databases, tables, columns, indexes, constraints, foreign keys, calculated columns, and storage maintenance.</p>
          </a>
          <a class="doc-link-card" href="operations.html">
            <h4><span>Operations &amp; Transactions</span><span>&rarr;</span></h4>
            <p>Stream I/O, reader/writer options, page cache tuning, lockfiles, transactions, crash recovery, and encryption maintenance.</p>
          </a>
          <a class="doc-link-card" href="scaffolding.html">
            <h4><span>Generating C# Models</span><span>&rarr;</span></h4>
            <p>Using <code>JetDatabaseWriter.Scaffold</code> to generate classes, records, column attributes, and relationship navigations.</p>
          </a>
          <a class="doc-link-card" href="support-contract.html">
            <h4><span>Supported Operating Envelope</span><span>&rarr;</span></h4>
            <p>Release scope, resource bounds, recovery contracts, caller-stream guarantees, and compatibility boundaries.</p>
          </a>
          <a class="doc-link-card" href="glossary.html">
            <h4><span>Access &amp; JET Glossary</span><span>&rarr;</span></h4>
            <p>Definitions of on-disk structures: <code>TDEF</code>, <code>OMAP</code>, <code>LVAL</code>, <code>MSysObjects</code>, collations, and page tags.</p>
          </a>
          <a class="doc-link-card" href="cve-vulnerability-analysis.html">
            <h4><span>CVE Vulnerability Review</span><span>&rarr;</span></h4>
            <p>Threat-model inventory and canonical test mappings for historical JET/ACE parsing vulnerabilities.</p>
          </a>
          <a class="doc-link-card" href="ci.html">
            <h4><span>CI &amp; Validation Workflows</span><span>&rarr;</span></h4>
            <p>Content-selected CI lanes, Dafny proof checks, native DAO120 gates, fuzzing, portability, and benchmark workflows.</p>
          </a>
          <a class="doc-link-card" href="https://github.com/symbiogenesis/JetDatabaseWriter/tree/main/docs/design" target="_blank" rel="noopener">
            <h4><span>On-Disk Format Design Notes</span><span>&#8599;</span></h4>
            <p>Detailed architectural notes on data pages, B+Tree indexes, encryption, complex columns, and formal verification.</p>
          </a>
        </div>
      </div>
    </section>
  </main>
  <footer>
    <div class="container footer-inner">
      <div>
        <strong>JetDatabaseWriter</strong> &middot; MIT License &middot; Pure Managed .NET Reader &amp; Writer for Microsoft Access Databases
      </div>
      <div class="footer-links">
        <a href="https://github.com/symbiogenesis/JetDatabaseWriter">GitHub Repository</a>
        <a href="https://www.nuget.org/packages/JetDatabaseWriter/">NuGet Package</a>
        <a href="support-contract.html">Support Contract</a>
        <a href="#logo" onclick="downloadLogoSvg(); return false;">Download SVG Logo</a>
      </div>
    </div>
  </footer>
  <script>
    function setInstallCmd(cmdHtml, btn) {
      document.getElementById('install-cmd-text').innerHTML = cmdHtml;
      var tabs = document.querySelectorAll('.install-tab');
      for (var i = 0; i < tabs.length; i++) tabs[i].classList.remove('active');
      btn.classList.add('active');
    }
    function copyInstallCmd() {
      var text = document.getElementById('install-cmd-text').textContent;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text);
      }
      var btn = document.getElementById('copy-install-btn');
      btn.textContent = 'Copied!';
      setTimeout(function () { btn.textContent = 'Copy'; }, 1600);
    }
    function showTab(panelId, btn) {
      var panels = document.querySelectorAll('.code-panel');
      for (var i = 0; i < panels.length; i++) panels[i].classList.remove('active');
      var tabs = document.querySelectorAll('.code-tab');
      for (var j = 0; j < tabs.length; j++) tabs[j].classList.remove('active');
      document.getElementById(panelId).classList.add('active');
      btn.classList.add('active');
    }
    function downloadLogoSvg() {
      var svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256" width="512" height="512">' +
        '<defs>' +
        '<linearGradient id="cyl" x1="0%" y1="0%" x2="100%" y2="100%">' +
        '<stop offset="0%" stop-color="#ff6b7a"/>' +
        '<stop offset="50%" stop-color="#c93b4b"/>' +
        '<stop offset="100%" stop-color="#6d4aff"/>' +
        '</linearGradient>' +
        '<linearGradient id="jet" x1="0%" y1="0%" x2="100%" y2="100%">' +
        '<stop offset="0%" stop-color="#7dd3fc"/>' +
        '<stop offset="45%" stop-color="#38bdf8"/>' +
        '<stop offset="100%" stop-color="#a78bfa"/>' +
        '</linearGradient>' +
        '</defs>' +
        '<rect width="256" height="256" rx="56" fill="#0b0f19" stroke="#e14b5a" stroke-opacity="0.45" stroke-width="6"/>' +
        '<path d="M48 172c0 17.6 36 32 80 32s80-14.4 80-32v-32c0 17.6-36 32-80 32s-80-14.4-80-32v32z" fill="url(#cyl)" opacity="0.78"/>' +
        '<path d="M48 128c0 17.6 36 32 80 32s80-14.4 80-32V96c0 17.6-36 32-80 32s-80-14.4-80-32v32z" fill="url(#cyl)" opacity="0.9"/>' +
        '<ellipse cx="128" cy="88" rx="80" ry="32" fill="url(#cyl)"/>' +
        '<ellipse cx="128" cy="88" rx="52" ry="17" fill="#0b0f19" opacity="0.58"/>' +
        '<path d="M92 128L204 48L160 176L132 140L92 128Z" fill="url(#jet)" stroke="#0b0f19" stroke-width="8" stroke-linejoin="round"/>' +
        '</svg>';
      var blob = new Blob([svg], { type: 'image/svg+xml;charset=utf-8' });
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = 'jetdatabasewriter-logo.svg';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }
  </script>
</body>
</html>
{:/nomarkdown}
