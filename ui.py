from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/upload-ui", response_class=HTMLResponse)
def upload_ui():
    # Minimal UI to allow folder uploads (Swagger UI cannot select directories).
    # Uses `webkitdirectory` to capture relative paths via `file.webkitRelativePath`.
    html = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Upload dataset</title>
    <style>
      body { font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; max-width: 820px; margin: 32px auto; padding: 0 16px; }
      .card { border: 1px solid #ddd; border-radius: 10px; padding: 18px; }
      label { display:block; margin-top: 12px; font-weight: 600; }
      input[type="text"] { width: 100%; padding: 10px 12px; border: 1px solid #ccc; border-radius: 8px; }
      input[type="file"] { margin-top: 6px; }
      button { margin-top: 16px; padding: 10px 14px; border: 0; border-radius: 10px; background: #1f6feb; color: white; font-weight: 700; cursor: pointer; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      pre { background: #0b1020; color: #d6deeb; padding: 12px; border-radius: 10px; overflow-x: auto; }
      .muted { color: #555; font-size: 14px; margin-top: 8px; }
    </style>
  </head>
  <body>
    <h2>Upload dataset (files or folders)</h2>
    <div class="card">
      <label>partner_id</label>
      <input id="partner_id" type="text" placeholder="" />

      <label>dataset_name</label>
      <input id="dataset_name" type="text" placeholder="" />

      <label>Pick a folder (recommended)</label>
      <input id="folder" type="file" webkitdirectory directory multiple />
      <div class="muted">Uses relative paths (e.g. <code>images/1.png</code>) so the server can recreate subfolders.</div>

      <label>Or pick files</label>
      <input id="files" type="file" multiple />

      <button id="upload_btn">Upload</button>
      <pre id="out" aria-live="polite"></pre>
    </div>

    <script>
      const out = document.getElementById('out');
      const btn = document.getElementById('upload_btn');

      function log(obj) {
        out.textContent = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
      }

      btn.addEventListener('click', async () => {
        const partnerId = document.getElementById('partner_id').value.trim();
        const datasetName = document.getElementById('dataset_name').value.trim();
        const folderFiles = Array.from(document.getElementById('folder').files || []);
        const plainFiles = Array.from(document.getElementById('files').files || []);
        const chosenRaw = folderFiles.length ? folderFiles : plainFiles;

        if (!partnerId || !datasetName) return log('partner_id and dataset_name are required');
        if (!chosenRaw.length) return log('Please select a folder or some files');

        btn.disabled = true;
        log(`Preparing ${chosenRaw.length} file(s)...`);

        const form = new FormData();

        let skipped = 0;
        for (const f of chosenRaw) {
          // Preserve relative path for folder uploads (if present).
          const rel = f.webkitRelativePath && f.webkitRelativePath.length ? f.webkitRelativePath : f.name;

          // Skip macOS / hidden metadata files that users typically don't want:
          // - .DS_Store
          // - AppleDouble resource forks like ._foo.jpg
          // - any path segment starting with "."
          const parts = rel.split('/').filter(Boolean);
          const base = parts.length ? parts[parts.length - 1] : rel;
          const hasHiddenSegment = parts.some(p => p.startsWith('.'));
          const isDsStore = base === '.DS_Store';
          const isAppleDouble = base.startsWith('._');
          if (hasHiddenSegment || isDsStore || isAppleDouble) {
            skipped += 1;
            continue;
          }

          form.append('files', f, rel);
        }

        const uploadCount = (chosenRaw.length - skipped);
        if (!uploadCount) {
          btn.disabled = false;
          return log(`No uploadable files found (skipped ${skipped} hidden/metadata file(s)).`);
        }

        log(`Uploading ${uploadCount} file(s)${skipped ? ` (skipped ${skipped})` : ''}...`);

        const url = `/upload?partner_id=${encodeURIComponent(partnerId)}&dataset_name=${encodeURIComponent(datasetName)}`;
        try {
          const resp = await fetch(url, { method: 'POST', body: form });
          const text = await resp.text();
          let body;
          try { body = JSON.parse(text); } catch { body = text; }
          if (!resp.ok) return log({ error: true, status: resp.status, body });
          log(body);
        } catch (e) {
          log({ error: true, message: String(e) });
        } finally {
          btn.disabled = false;
        }
      });
    </script>
  </body>
</html>"""
    return HTMLResponse(html)

