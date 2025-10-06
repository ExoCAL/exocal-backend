
# app.py
# Exoplanet Analyzer API — accepts KOI/TOI/K2 CSVs, runs ExoCAL/exo_analysis_code/exo_analysis.py,
# tracks job status, and provides a downloadable ZIP of the outputs.

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, status, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import Optional, Dict
from pathlib import Path
from uuid import uuid4
import subprocess
import json
import os
import shutil
import sys
import datetime as dt
import re
import time
from collections import defaultdict
from fastapi import Response
from urllib.parse import quote, unquote
import csv, io
import threading
from datetime import datetime, timedelta

PROG_RE = re.compile(r"^\[(KOI|TOI|K2)\]\s+Working on\s+(\d+)/(\d+):\s*(.+)$")
# Put this near your other globals
DEMO_DATA_ROOT = Path("./Exoplanet-Candidate-Assessment-and-Labeling/NASA_archive_data").resolve()
DEMO_PATHS = {
    "koi": DEMO_DATA_ROOT / "koi.csv",
    "toi": DEMO_DATA_ROOT / "toi.csv",
    "k2":  DEMO_DATA_ROOT / "k2.csv",
}

# ======================
# CONFIG
# ======================
OUTPUT_ROOT = Path(os.getenv("OUTPUT_ROOT", "./runs")).resolve()
REACT_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")  # e.g. "https://your-swa.azurestaticapps.net"
MAX_BYTES = int(os.getenv("MAX_UPLOAD_MB", "100")) * 1024 * 1024  # default 100MB/file
ALLOWED_EXTS = {".csv"}

DEFAULT_SEED = int(os.getenv("DEFAULT_SEED") or "42")  # e.g. "42" or None
DEFAULT_LIMIT = os.getenv("DEFAULT_LIMIT")  # e.g. "50" or None
DEFAULT_QUIET = False

# Job expiry configuration
JOB_EXPIRY_HOURS = int(os.getenv("JOB_EXPIRY_HOURS", "1"))  # Default 1 hour
CLEANUP_INTERVAL_MINUTES = int(os.getenv("CLEANUP_INTERVAL_MINUTES", "10"))  # Check every 10 minutes

SCRIPT_PATH = (Path(__file__).parent / "Exoplanet-Candidate-Assessment-and-Labeling" / "exo_cal_code" / "exo_analysis.py").resolve()
PYTHON_BIN = os.getenv("PYTHON_BIN") or sys.executable  # use current venv's python by default

# ======================
# APP
# ======================
app = FastAPI(title="Exoplanet Analyzer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=REACT_ORIGINS if REACT_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# ======================
# CLEANUP FUNCTIONS
# ======================

def cleanup_expired_jobs():
    """Remove job directories that are older than JOB_EXPIRY_HOURS"""
    try:
        current_time = datetime.now()
        expiry_time = current_time - timedelta(hours=JOB_EXPIRY_HOURS)
        
        if not OUTPUT_ROOT.exists():
            return
            
        for job_dir in OUTPUT_ROOT.iterdir():
            if job_dir.is_dir():
                try:
                    # Check if directory is older than expiry time
                    dir_mtime = datetime.fromtimestamp(job_dir.stat().st_mtime)
                    if dir_mtime < expiry_time:
                        print(f"Cleaning up expired job: {job_dir.name}")
                        shutil.rmtree(job_dir, ignore_errors=True)
                except (OSError, PermissionError) as e:
                    print(f"Error cleaning up job {job_dir.name}: {e}")
    except Exception as e:
        print(f"Error during cleanup: {e}")

def cleanup_scheduler():
    """Background thread that runs cleanup periodically"""
    while True:
        try:
            cleanup_expired_jobs()
            time.sleep(CLEANUP_INTERVAL_MINUTES * 60)  # Convert minutes to seconds
        except Exception as e:
            print(f"Error in cleanup scheduler: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_scheduler, daemon=True)
cleanup_thread.start()

@app.post("/api/admin/cleanup")
def manual_cleanup():
    """Manual cleanup endpoint for testing and monitoring"""
    try:
        cleanup_expired_jobs()
        return {"message": "Cleanup completed successfully", "expiry_hours": JOB_EXPIRY_HOURS}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

@app.get("/api/admin/cleanup/status")
def cleanup_status():
    """Get cleanup configuration and status"""
    return {
        "expiry_hours": JOB_EXPIRY_HOURS,
        "cleanup_interval_minutes": CLEANUP_INTERVAL_MINUTES,
        "output_root": str(OUTPUT_ROOT),
        "cleanup_thread_alive": cleanup_thread.is_alive()
    }

# ======================
# UTILITIES
# ======================

def _use_demo_if_requested(kind: str, use_demo: bool, job_dir: Path) -> Optional[Path]:
    """
    If use_demo is True, copy the demo CSV for 'kind' into job_dir (as koi.csv/toi.csv/k2.csv)
    and return its path. If the demo file doesn't exist, raise 500.
    """
    if not use_demo:
        return None
    src = DEMO_PATHS[kind]
    if not src.exists():
        raise HTTPException(status_code=500, detail=f"Demo data missing for {kind}: {src}")
    dest = job_dir / f"{kind}.csv"
    shutil.copyfile(src, dest)
    return dest

def _collect_top_candidate_paths(out_root: Path) -> list[Path]:
    """
    Find all top_candidates.csv below out_root.
    Typical layout:
      out/KOI/top_candidates.csv
      out/TOI/top_candidates.csv
      out/K2/top_candidates.csv
    (works for deeper layouts too)
    """
    return sorted(out_root.rglob("top_candidates.csv"))

def _guess_dataset(out_root: Path, file_path: Path) -> str:
    """
    Dataset = first path segment under out_root (e.g., KOI/TOI/K2).
    If none, return 'DEFAULT'.
    """
    rel = file_path.relative_to(out_root)
    return rel.parts[0] if len(rel.parts) > 1 else "DEFAULT"

def _safe_join(root: Path, *parts: str) -> Path:
    p = root.joinpath(*parts).resolve()
    root = root.resolve()
    if not str(p).startswith(str(root)):
        raise HTTPException(status_code=400, detail="Invalid path.")
    return p

def _now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def _init_status(job_dir: Path) -> Path:
    status_doc = {
        "job_id": job_dir.name,
        "created_at": _now_iso(),
        "started_at": None,
        "finished_at": None,
        "state": "queued",  # queued | running | done | error
        "return_code": None,
        "error": None,
        "out_dir": str(job_dir / "out"),
        "script": str(SCRIPT_PATH),
        "params": {},
        "artifacts": [],
        "zip": None,
        "log": str(job_dir / "run.log"),
        "expires_at": None,  # optional TTL if you add a janitor
    }
    status_path = job_dir / "status.json"
    _write_json(status_path, status_doc)
    return status_path

def _update_status(status_path: Path, **kwargs):
    data = json.loads(status_path.read_text(encoding="utf-8"))
    data.update(kwargs)
    _write_json(status_path, data)

def _safe_save(upload: UploadFile, dest: Path):
    # Simple extension guard
    if not any(str(upload.filename).lower().endswith(ext) for ext in ALLOWED_EXTS):
        raise HTTPException(status_code=415, detail=f"Unsupported file type for {upload.filename}")
    size = 0
    with dest.open("wb") as f:
        while True:
            chunk = upload.file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_BYTES:
                raise HTTPException(status_code=413, detail=f"File too large: {upload.filename}")
            f.write(chunk)
    upload.file.close()

def _list_artifacts(out_dir: Path):
    if not out_dir.exists():
        return []
    files = []
    for p in out_dir.rglob("*"):
        if p.is_file():
            files.append(str(p.relative_to(out_dir)))
    return sorted(files)

# ======================
# BACKGROUND TASK
# ======================
def _zip_out_dir(out_dir: Path, zip_path: Path):
    if not out_dir.exists():
        raise FileNotFoundError(f"{out_dir} does not exist")
    # Rebuild zip atomically
    tmp = zip_path.with_suffix(".zip.tmp")
    if tmp.exists():
        tmp.unlink()
    shutil.make_archive(tmp.with_suffix("").as_posix(), "zip", out_dir.as_posix())
    # shutil.make_archive writes "tmp.zip"; ensure final filename is exactly zip_path
    built = tmp.with_suffix("")  # path without .tmp
    built_zip = Path(str(built) + ".zip")
    if zip_path.exists():
        zip_path.unlink()
    built_zip.replace(zip_path)

def run_exo_analysis(job_dir: Path, saved_files: Dict[str, Optional[Path]], limit_targets, seed, quiet):
    status_path = job_dir / "status.json"
    log_path = job_dir / "run.log"
    out_dir = job_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [PYTHON_BIN, str(SCRIPT_PATH)]
    if saved_files.get("toi"): cmd += ["--toi", str(saved_files["toi"])]
    if saved_files.get("koi"): cmd += ["--koi", str(saved_files["koi"])]
    if saved_files.get("k2"):  cmd += ["--k2",  str(saved_files["k2"])]
    cmd += ["--out", str(out_dir)]
    if limit_targets is not None: cmd += ["--limit-targets", str(limit_targets)]
    if seed is not None:          cmd += ["--seed", str(seed)]
    if quiet:                     cmd += ["--quiet"]

    env = os.environ.copy()
    env.setdefault("MPLBACKEND", "Agg")
    env.setdefault("PYTHONUNBUFFERED", "1")

    _update_status_atomic(
        status_path,
        state="running",
        started_at=_now_iso(),
        last_heartbeat=_now_iso(),
        progress=None,  # we'll fill this as we parse
    )

    creationflags = 0
    preexec = None
    if os.name == "nt":
        creationflags = 0x00000200
    else:
        preexec = os.setsid  # type: ignore

    rc = None
    try:
        with log_path.open("w", encoding="utf-8") as logf:
            # write header
            logf.write(f"[{_now_iso()}] CWD: {SCRIPT_PATH.parent}\n")
            logf.write(f"[{_now_iso()}] PYTHON: {PYTHON_BIN}\n")
            logf.write(f"[{_now_iso()}] SCRIPT: {SCRIPT_PATH}\n")
            logf.write(f"[{_now_iso()}] CMD: {' '.join(cmd)}\n")
            logf.write(f"[{_now_iso()}] MPLBACKEND={env.get('MPLBACKEND')}\n")
            logf.flush()

            proc = subprocess.Popen(
                cmd,
                cwd=str(SCRIPT_PATH.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                creationflags=creationflags,
                preexec_fn=preexec,
                bufsize=1,  # line-buffered
            )

            # live-read loop
            while True:
                line = proc.stdout.readline()
                if not line:
                    # process may be done; break after poll confirms
                    if proc.poll() is not None:
                        break
                    time.sleep(0.2)
                    continue

                # always tee to log
                logf.write(line)
                if not line.endswith("\n"):
                    logf.write("\n")
                logf.flush()

                # heartbeat
                _update_status_atomic(status_path, last_heartbeat=_now_iso())

                # progress parse: "[KOI] Working on 12/50: K00002.01"
                m = PROG_RE.match(line.strip())
                if m:
                    dataset, cur, total, target = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
                    percent = round(100.0 * cur / max(total, 1), 2)
                    _update_status_atomic(
                        status_path,
                        progress={
                            "dataset": dataset,         # "KOI" / "TOI" / "K2"
                            "current": cur,             # 12
                            "total": total,             # 50
                            "percent": percent,         # 24.0
                            "last_target": target,      # "K00002.01"
                            "message": f"{dataset}: {cur}/{total} ({percent}%)",
                            "updated_at": _now_iso(),
                        },
                    )

            rc = proc.wait()

        # after finish, zip if success
        zip_rel = None
        if rc == 0 and any(out_dir.rglob("*")):
            zip_path = job_dir / "out.zip"
            _zip_out_dir(out_dir, zip_path)
            zip_rel = "out.zip"

        _update_status_atomic(
            status_path,
            state="done" if rc == 0 else "error",
            finished_at=_now_iso(),
            return_code=rc,
            zip=zip_rel,
        )

    except Exception as e:
        _update_status_atomic(
            status_path,
            state="error",
            finished_at=_now_iso(),
            error=f"{type(e).__name__}: {e}",
        )


def _write_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)  # atomic on POSIX; on Windows it's still safe enough here

def _update_status_atomic(status_path: Path, **kwargs):
    data = json.loads(status_path.read_text(encoding="utf-8"))
    # don't regress state from done/error back to running
    new_state = kwargs.get("state")
    if new_state == "running" and data.get("state") in ("done", "error"):
        kwargs.pop("state", None)
    data.update(kwargs)
    _write_json_atomic(status_path, data)


IMG_EXTS = {".png", ".jpg", ".jpeg", ".svg", ".webp"}

def _url(job_id: str, rel: Path) -> str:
    # rel is relative to out/; we URL-encode for safety
    return f"/api/jobs/{job_id}/media/{quote(rel.as_posix())}"

def _list_figs_for_dataset(dataset_dir: Path, job_id: str):
    result = {"summary": [], "targets": {}}

    # Summary figs (out/<DS>/fig/*)
    fig_dir = dataset_dir / "fig"
    if fig_dir.exists():
        for p in sorted(fig_dir.iterdir()):
            if p.suffix.lower() in IMG_EXTS and p.is_file():
                rel = p.relative_to(dataset_dir.parent)  # relative to out/
                result["summary"].append(_url(job_id, rel))

    # Target figs (out/<DS>/targets/<name>/*)
    t_dir = dataset_dir / "targets"
    if t_dir.exists():
        for tgt in sorted(t_dir.iterdir()):
            if tgt.is_dir():
                imgs = []
                for p in sorted(tgt.iterdir()):
                    if p.suffix.lower() in IMG_EXTS and p.is_file():
                        rel = p.relative_to(dataset_dir.parent)
                        imgs.append(_url(job_id, rel))
                if imgs:
                    result["targets"][tgt.name] = imgs

    return result


# ======================
# ROUTES
# ======================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "script_exists": SCRIPT_PATH.exists(),
        "script_path": str(SCRIPT_PATH),
        "output_root": str(OUTPUT_ROOT),
    }
@app.post("/api/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload_files(
    background_tasks: BackgroundTasks,
    # File uploads (optional)
    koi: Optional[UploadFile] = File(None),
    toi: Optional[UploadFile] = File(None),
    k2:  Optional[UploadFile] = File(None),
    # Demo toggles in *form-data*
    use_demo_koi: bool = Form(False),
    use_demo_toi: bool = Form(False),
    use_demo_k2:  bool = Form(False),
    # Other params (query or form both work here)
    limit_targets: Optional[int] = None,
    seed: Optional[int] = None,
    quiet: Optional[bool] = None,
):
    """
    Accepts uploaded CSVs and/or demo toggles. If a file is uploaded for a dataset, it wins.
    Otherwise, if the corresponding demo toggle is true, the demo CSV is used.
    """

    if not SCRIPT_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Analysis script not found at {SCRIPT_PATH}")

    # Validate we have at least one source (file or demo)
    any_upload = any([koi, toi, k2])
    any_demo = any([use_demo_koi, use_demo_toi, use_demo_k2])
    if not (any_upload or any_demo):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one dataset: upload a file (koi/toi/k2) or enable a demo toggle (use_demo_koi/use_demo_toi/use_demo_k2)."
        )

    job_id = str(uuid4())
    job_dir = OUTPUT_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    _init_status(job_dir)

    saved: Dict[str, Optional[Path]] = {"koi": None, "toi": None, "k2": None}
    try:
        # 1) Prefer uploaded files
        if koi:
            dest = job_dir / "koi.csv"
            _safe_save(koi, dest)
            saved["koi"] = dest
        if toi:
            dest = job_dir / "toi.csv"
            _safe_save(toi, dest)
            saved["toi"] = dest
        if k2:
            dest = job_dir / "k2.csv"
            _safe_save(k2, dest)
            saved["k2"] = dest

        # 2) If no upload for a dataset, but demo requested → copy demo
        if saved["koi"] is None and use_demo_koi:
            saved["koi"] = _use_demo_if_requested("koi", True, job_dir)
        if saved["toi"] is None and use_demo_toi:
            saved["toi"] = _use_demo_if_requested("toi", True, job_dir)
        if saved["k2"]  is None and use_demo_k2:
            saved["k2"]  = _use_demo_if_requested("k2",  True, job_dir)

        # Re-check that something is now saved
        if not any(saved.values()):
            raise HTTPException(status_code=400, detail="No dataset selected after processing uploads and demo toggles.")

    except HTTPException:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Upload/demo preparation failed: {e}")

    # Resolve defaults
    lt = int(DEFAULT_LIMIT) if (DEFAULT_LIMIT and limit_targets is None) else limit_targets
    sd = seed if seed is not None else DEFAULT_SEED
    qt = quiet if quiet is not None else DEFAULT_QUIET

    # Kick off background job
    background_tasks.add_task(run_exo_analysis, job_dir, saved, lt, sd, qt)

    # Optional: echo sources used (handy for the frontend)
    sources_used = {k: (str(v) if v else None) for k, v in saved.items()}

    return {
        "job_id": job_id,
        "message": "Files received. Analysis started.",
        "sources": sources_used,
        "status_url": f"/api/jobs/{job_id}/status",
        "artifacts_url": f"/api/jobs/{job_id}/artifacts",
        "download_url": f"/api/jobs/{job_id}/download"
    }


@app.get("/api/jobs/{job_id}/status")
def job_status(job_id: str):
    # Clean up expired jobs before checking status
    cleanup_expired_jobs()
    
    status_path = OUTPUT_ROOT / job_id / "status.json"
    if not status_path.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    return json.loads(status_path.read_text(encoding="utf-8"))

@app.get("/api/jobs/{job_id}/artifacts")
def job_artifacts(job_id: str):
    # Clean up expired jobs before accessing artifacts
    cleanup_expired_jobs()
    
    out_dir = OUTPUT_ROOT / job_id / "out"
    if not out_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found or no artifacts yet.")
    return {"job_id": job_id, "out_dir": str(out_dir), "artifacts": _list_artifacts(out_dir)}

@app.get("/api/jobs/{job_id}/download")
def job_download(job_id: str):
    # Clean up expired jobs before downloading
    cleanup_expired_jobs()
    
    job_dir = OUTPUT_ROOT / job_id
    status_path = job_dir / "status.json"
    if not status_path.exists():
        raise HTTPException(status_code=404, detail="Job not found.")

    out_dir = job_dir / "out"
    if not out_dir.exists():
        raise HTTPException(status_code=404, detail="No output directory yet.")

    zip_path = job_dir / "out.zip"
    try:
        _zip_out_dir(out_dir, zip_path)
        # reflect presence in status.json (best-effort)
        data = json.loads(status_path.read_text(encoding="utf-8"))
        data["zip"] = "out.zip"
        _write_json(status_path, data)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="No artifacts to zip yet.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create zip: {e}")

    return FileResponse(
        path=str(zip_path),
        media_type="application/zip",
        filename=f"{job_id}_out.zip",
    )


@app.get("/api/jobs/{job_id}/media/{subpath:path}")
def job_media(job_id: str, subpath: str, inline: bool = True):
    out_root = (OUTPUT_ROOT / job_id / "out").resolve()
    if not out_root.exists():
        raise HTTPException(status_code=404, detail="Output not found.")

    # 1) Strip accidental double prefix like "/api/jobs/<id>/media/..."
    double = f"/api/jobs/{job_id}/media/"
    if subpath.startswith(double):
        subpath = subpath[len(double):]

    # 2) Be tolerant to double-encoding (decode up to twice)
    for _ in range(2):
        new = unquote(subpath)
        if new == subpath:
            break
        subpath = new

    # 3) Prevent traversal and resolve
    file_path = (out_root / subpath).resolve()
    if not str(file_path).startswith(str(out_root)):
        raise HTTPException(status_code=400, detail="Invalid path.")
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found.")

    headers = {
        "Content-Disposition": f'inline; filename="{file_path.name}"' if inline
                               else f'attachment; filename="{file_path.name}"'
    }
    return FileResponse(str(file_path), headers=headers)

@app.get("/api/jobs/{job_id}/figs")
def job_figs(job_id: str):
    out_root = OUTPUT_ROOT / job_id / "out"
    if not out_root.exists():
        raise HTTPException(status_code=404, detail="Output not found.")

    payload = {"job_id": job_id, "datasets": {}}
    # Detect datasets by subdirectories (KOI/TOI/K2)
    for ds_dir in sorted([p for p in out_root.iterdir() if p.is_dir()]):
        ds_name = ds_dir.name
        payload["datasets"][ds_name] = _list_figs_for_dataset(ds_dir, job_id)

    # Fallback: if figs live directly under out/ (no dataset subfolders)
    if not payload["datasets"]:
        summary = []
        targets = defaultdict(list)
        # Collect top-level figs
        for p in sorted(out_root.rglob("*")):
            if p.is_file() and p.suffix.lower() in IMG_EXTS:
                rel = p.relative_to(out_root)
                parts = rel.parts
                # Heuristic: .../targets/<designation>/*.png → bucket by designation
                try:
                    i = parts.index("targets")
                    if i + 2 <= len(parts):
                        designation = parts[i+1]
                        targets[designation].append(_url(job_id, rel))
                        continue
                except ValueError:
                    pass
                # Otherwise treat as summary fig
                summary.append(_url(job_id, rel))
        payload["datasets"]["DEFAULT"] = {
            "summary": summary,
            "targets": dict(targets),
        }

    return payload

@app.get("/api/jobs/{job_id}/artifacts")
def job_artifacts(job_id: str):
    job_dir = OUTPUT_ROOT / job_id
    out_dir = job_dir / "out"
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    if not out_dir.exists():
        return {"job_id": job_id, "artifacts": [], "figs": {}}

    # Existing artifact listing (keep yours)...
    artifacts = []
    for p in sorted(out_dir.rglob("*")):
        if p.is_file() and p.suffix.lower() not in IMG_EXTS:
            rel = p.relative_to(out_dir)
            artifacts.append(rel.as_posix())

    # Reuse the figs builder
    figs = job_figs(job_id)["datasets"]
    return {"job_id": job_id, "artifacts": artifacts, "figs": figs}


@app.get("/api/jobs/{job_id}/top-candidates.csv")
def job_top_candidates_csv(job_id: str):
    # Clean up expired jobs before accessing CSV data
    cleanup_expired_jobs()
    
    job_dir = OUTPUT_ROOT / job_id
    out_root = job_dir / "out"
    if not out_root.exists():
        raise HTTPException(status_code=404, detail="Output not found.")

    files = _collect_top_candidate_paths(out_root)
    if not files:
        # No files found — return empty CSV with a friendly header
        content = "dataset,message\n,No top_candidates.csv files found\n"
        return Response(
            content=content,
            media_type="text/csv",
            headers={"Content-Disposition": f'inline; filename="{job_id}_top_candidates_all.csv"'},
        )

    # 1) Read all rows; track union of headers
    all_rows: list[dict] = []
    fieldnames_union: set[str] = set()
    for fpath in files:
        dataset = _guess_dataset(out_root, fpath)
        try:
            with fpath.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    # normalize keys -> strip BOM/whitespace if any
                    clean = { (k.strip() if k else k): (v.strip() if isinstance(v, str) else v)
                              for k, v in row.items() }
                    clean["dataset"] = dataset
                    all_rows.append(clean)
                    fieldnames_union.update(clean.keys())
        except Exception as e:
            # Skip unreadable files but keep going
            all_rows.append({"dataset": dataset, "error": f"Failed to read {fpath.name}: {e}"})
            fieldnames_union.update({"dataset", "error"})

    if not all_rows:
        content = "dataset,message\n,No rows in any top_candidates.csv\n"
        return Response(
            content=content,
            media_type="text/csv",
            headers={"Content-Disposition": f'inline; filename="{job_id}_top_candidates_all.csv"'},
        )

    # 2) Column order: put helpful columns first if present
    preferred = ["dataset", "designation", "target", "prob", "score"]
    ordered = [c for c in preferred if c in fieldnames_union]
    # include the rest deterministically
    rest = sorted(c for c in fieldnames_union if c not in ordered)
    fieldnames = ordered + rest

    # 3) Optional: sort by 'prob' desc if present
    if "prob" in fieldnames:
        def prob_key(r):
            try:
                return float(r.get("prob", 0.0))
            except Exception:
                return -1e9
        all_rows.sort(key=prob_key, reverse=True)

    # 4) Write combined CSV to memory and return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in all_rows:
        # ensure all keys exist
        for k in fieldnames:
            r.setdefault(k, "")
        writer.writerow(r)

    csv_bytes = buf.getvalue()
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'inline; filename="{job_id}_top_candidates_all.csv"'},
    )

# ======================
# MAIN (for local dev)
# ======================
if __name__ == "__main__":
    # Local dev: uvicorn app:app --reload --port 8080
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")), reload=True)
