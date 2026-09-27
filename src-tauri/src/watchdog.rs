// 看门狗：盯 ZCode 桌面端日志（~/.zcode/v2/logs/YYYY-MM-DD.log），
// 检测回合失败（两类信号）并把「继续」发到出错的那个会话：
//   1) cua-pip-session 行 kind=turn-ended 且 outcome=failed/error（带 sessionId）
//   2) captcha-diagnostics 行 request.respond + headersApplied=false（发送前验证码失败，
//      回合直接中止、不会有 turn-ended 事件；requestId 以 sess_ 开头可提取会话）
// 发送方式：优先 zcode CLI --resume <sessionId> --surface desktop（精准路由到出错会话，
// 不抢焦点）；CLI 不可用时回退为把 ZCode 窗口调到前台键入文本+回车。
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tauri::Emitter;

static ENABLED: AtomicBool = AtomicBool::new(false);
static CONTINUE_TEXT: Mutex<Option<String>> = Mutex::new(None);

const TICK_MS: u64 = 5_000;
const RECENCY_MS: i64 = 120_000;
const SEND_COOLDOWN_MS: i64 = 30_000;
const SENT_CAP: usize = 512;
const RATE_WINDOW_MS: i64 = 600_000;
const RATE_MAX_SENDS: usize = 6;
const CLI_TIMEOUT_MS: u64 = 25_000;

pub fn set_enabled(v: bool) {
    ENABLED.store(v, Ordering::Relaxed);
}

pub fn set_text(t: &str) {
    let trimmed = t.trim();
    let mut guard = match CONTINUE_TEXT.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(if trimmed.is_empty() { "继续".to_string() } else { trimmed.to_string() });
}

fn continue_text() -> String {
    match CONTINUE_TEXT.lock() {
        Ok(g) => g.clone().unwrap_or_else(|| "继续".to_string()),
        Err(poisoned) => poisoned.into_inner().clone().unwrap_or_else(|| "继续".to_string()),
    }
}

fn now_ms() -> i64 {
    chrono::Local::now().timestamp_millis()
}

fn log_path_for(day: &str) -> std::path::PathBuf {
    crate::store::Paths::detect().zcode_dir().join("v2").join("logs").join(format!("{day}.log"))
}

#[derive(Clone)]
struct FailEvent {
    ts_ms: i64,
    key: String,               // 去重键（turnId 或 requestId）
    session: Option<String>,   // sess_xxx
}

fn extract_str(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)? + key.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_ts_ms(line: &str) -> Option<i64> {
    let open = line.find('[')?;
    let close = line[open..].find(']')? + open;
    let raw = &line[open + 1..close];
    let naive = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f").ok()?;
    use chrono::TimeZone;
    chrono::Local
        .from_local_datetime(&naive)
        .single()
        .map(|t| t.timestamp_millis())
}

fn sess_id_of(request_id: &str) -> Option<String> {
    let rid = request_id.strip_prefix("sess_").unwrap_or(request_id);
    let s = format!("sess_{rid}");
    let head = s.split(':').next()?.to_string();
    head.starts_with("sess_").then_some(head)
}

fn parse_fail_line(line: &str) -> Option<FailEvent> {
    let ts_ms = parse_ts_ms(line)?;
    if line.contains("\"kind\":\"turn-ended\"") {
        let outcome = extract_str(line, "\"outcome\":\"").unwrap_or_default();
        if outcome != "failed" && outcome != "error" {
            return None;
        }
        let turn_id = extract_str(line, "\"turnId\":\"")?;
        let session = extract_str(line, "\"sessionId\":\"");
        return Some(FailEvent { ts_ms, key: turn_id, session });
    }
    if line.contains("[captcha-diagnostics]")
        && line.contains("\"event\":\"request.respond\"")
        && line.contains("\"headersApplied\":false")
    {
        let rid = extract_str(line, "\"requestId\":\"").unwrap_or_default();
        let key = if rid.is_empty() { format!("captcha-{ts_ms}") } else { format!("captcha-{rid}") };
        let session = sess_id_of(&rid);
        return Some(FailEvent { ts_ms, key, session });
    }
    None
}

// 会话 → 工作区（日志里同时含 sessionId 与 workspaceKey 的行）
fn parse_session_ws(line: &str, map: &mut HashMap<String, String>) {
    if !line.contains("\"workspaceKey\":\"") || !line.contains("\"sessionId\":\"sess_") {
        return;
    }
    if let (Some(sess), Some(ws)) = (
        extract_str(line, "\"sessionId\":\""),
        extract_str(line, "\"workspaceKey\":\""),
    ) {
        let ws = ws.replace("\\\\", "\\");
        if sess.starts_with("sess_") && !ws.is_empty() {
            map.entry(sess).or_insert(ws);
        }
    }
}

fn is_turn_activity(line: &str) -> bool {
    line.contains("\"kind\":\"turn-started\"") || line.contains("\"kind\":\"turn-ended\"")
}

pub fn spawn(app: tauri::AppHandle) {
    std::thread::spawn(move || {
        let mut offset: u64 = 0;
        let mut day = String::new();
        let mut last_failed: Option<FailEvent> = None;
        let mut sent: HashSet<String> = HashSet::new();
        let mut last_send_ms: i64 = 0;
        let mut send_windows: Vec<i64> = Vec::new();
        let mut session_ws: HashMap<String, String> = HashMap::new();
        eprintln!("[zsw-watchdog] started");
        loop {
            std::thread::sleep(Duration::from_millis(TICK_MS));
            let today = chrono::Local::now().format("%Y-%m-%d").to_string();
            if today != day {
                day = today;
                offset = 0;
                last_failed = None;
            }
            let path = log_path_for(&day);
            let len = match std::fs::metadata(&path) {
                Ok(m) => m.len(),
                Err(_) => {
                    offset = 0;
                    continue;
                }
            };
            if len < offset {
                offset = 0;
            }
            if len > offset {
                let consumed = read_and_parse(&path, offset, &mut last_failed, &mut session_ws);
                offset = consumed.unwrap_or(len);
            }
            if !ENABLED.load(Ordering::Relaxed) {
                continue;
            }
            let Some(fail) = last_failed.clone() else { continue };
            if sent.contains(&fail.key) {
                continue;
            }
            let n = now_ms();
            if n - fail.ts_ms > RECENCY_MS || n - last_send_ms < SEND_COOLDOWN_MS {
                continue;
            }
            send_windows.retain(|t| n - t < RATE_WINDOW_MS);
            if send_windows.len() >= RATE_MAX_SENDS {
                eprintln!("[zsw-watchdog] 10 分钟内已连续发送 {RATE_MAX_SENDS} 次，暂停触发");
                continue;
            }
            let text = continue_text();
            let (ok, via, err) = match fail.session.as_deref() {
                Some(sess) => match send_via_cli(sess, fail.session_ws(&session_ws), &text) {
                    Ok(()) => (true, "cli", None),
                    Err(e) => {
                        eprintln!("[zsw-watchdog] CLI 注入失败，回退键入: {e}");
                        match send_to_zcode_window(&text) {
                            Ok(()) => (true, "keys", None),
                            Err(e2) => (false, "keys", Some(e2)),
                        }
                    }
                },
                None => match send_to_zcode_window(&text) {
                    Ok(()) => (true, "keys", None),
                    Err(e) => (false, "keys", Some(e)),
                },
            };
            if ok {
                sent.insert(fail.key.clone());
                if sent.len() > SENT_CAP {
                    sent.clear();
                }
                last_send_ms = n;
                send_windows.push(n);
                eprintln!("[zsw-watchdog] 已发送继续 (via={via} sess={:?} key={})", fail.session, fail.key);
            } else {
                eprintln!("[zsw-watchdog] 发送失败: {:?}", err);
            }
            let _ = app.emit(
                "watchdog://continue",
                serde_json::json!({ "ok": ok, "turnId": fail.key, "text": text, "via": via, "error": err }),
            );
        }
    });
}

impl FailEvent {
    fn session_ws(&self, map: &HashMap<String, String>) -> Option<String> {
        self.session.as_deref().and_then(|s| map.get(s).cloned())
    }
}

fn read_and_parse(
    path: &std::path::Path,
    offset: u64,
    last_failed: &mut Option<FailEvent>,
    session_ws: &mut HashMap<String, String>,
) -> Option<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).ok()?;
    f.seek(SeekFrom::Start(offset)).ok()?;
    let mut data = Vec::new();
    f.read_to_end(&mut data).ok()?;
    // 只吃完整行，半行留给下一轮
    let cut = data.iter().rposition(|&b| b == b'\n').map(|p| p as u64 + 1).unwrap_or(0);
    let text = String::from_utf8_lossy(&data[..cut as usize]);
    for line in text.lines() {
        if line.contains("\"workspaceKey\":\"") {
            parse_session_ws(line, session_ws);
        }
        if let Some(ev) = parse_fail_line(line) {
            *last_failed = Some(ev);
        } else if is_turn_activity(line) {
            if let Some(ts) = parse_ts_ms(line) {
                if ts >= last_failed.as_ref().map(|f| f.ts_ms).unwrap_or(i64::MAX) {
                    // 失败之后有任何新回合活动（started 或 ended）即认为已恢复/已被接管
                    *last_failed = None;
                }
            }
        }
    }
    Some(offset + cut)
}

fn cli_script_path() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("ZSW_ZCODE_CJS") {
        let pb = std::path::PathBuf::from(p);
        if pb.exists() {
            return Some(pb);
        }
    }
    for cand in [
        r"D:\zcode\resources\glm\zcode.cjs",
        r"C:\Program Files\ZCode\resources\glm\zcode.cjs",
    ] {
        let pb = std::path::PathBuf::from(cand);
        if pb.exists() {
            return Some(pb);
        }
    }
    let local = std::env::var("LOCALAPPDATA").ok().map(|l| {
        std::path::PathBuf::from(l).join(r"Programs\ZCode\resources\glm\zcode.cjs")
    });
    if let Some(pb) = local {
        if pb.exists() {
            return Some(pb);
        }
    }
    None
}

// 通过 zcode CLI 把「继续」注入到指定会话（desktop surface 会在桌面端原会话里呈现）
fn send_via_cli(session: &str, workspace: Option<String>, text: &str) -> Result<(), String> {
    let cjs = cli_script_path().ok_or_else(|| "未找到 zcode.cjs（可用 ZSW_ZCODE_CJS 指定）".to_string())?;
    let mut cmd = std::process::Command::new("node");
    cmd.arg(&cjs)
        .args(["--resume", session, "-p", text, "--surface", "desktop"]);
    if let Some(ws) = workspace {
        cmd.args(["--cwd", &ws]);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("启动 node 失败: {e}"))?;
    let deadline = Instant::now() + Duration::from_millis(CLI_TIMEOUT_MS);
    let mut timed_out = false;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut out = String::new();
                if let Some(mut o) = child.stdout.take() {
                    use std::io::Read;
                    let _ = o.read_to_string(&mut out);
                }
                let tail: String = out.chars().rev().take(300).collect::<String>().chars().rev().collect();
                if status.success() {
                    eprintln!("[zsw-watchdog] CLI resume 完成: {tail}");
                    return Ok(());
                }
                return Err(format!("CLI 退出码 {:?}: {tail}", status.code()));
            }
            Ok(None) => {}
            Err(e) => return Err(format!("wait 失败: {e}")),
        }
        if Instant::now() > deadline {
            timed_out = true;
            let _ = child.kill();
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let _ = timed_out;
    Err("CLI 超时（25s）".into())
}

#[cfg(windows)]
fn send_to_zcode_window(text: &str) -> Result<(), String> {
    use windows_sys::Win32::Foundation::HWND;
    use windows_sys::Win32::System::Threading::{AttachThreadInput, GetCurrentThreadId};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetForegroundWindow, GetTopWindow, GetWindow, GetWindowThreadProcessId, GetWindowTextLengthW,
        IsIconic, IsWindowVisible, SetForegroundWindow, ShowWindow, GW_HWNDNEXT, SW_RESTORE,
    };

    let pids = zcode_pids()?;
    if pids.is_empty() {
        return Err("未找到 ZCode 进程".into());
    }
    let mut target: HWND = 0;
    let mut hwnd = unsafe { GetTopWindow(0) };
    while hwnd != 0 {
        if unsafe { IsWindowVisible(hwnd) } != 0 && unsafe { GetWindowTextLengthW(hwnd) } > 0 {
            let mut pid: u32 = 0;
            unsafe { GetWindowThreadProcessId(hwnd, &mut pid) };
            if pids.contains(&pid) {
                target = hwnd;
                break;
            }
        }
        hwnd = unsafe { GetWindow(hwnd, GW_HWNDNEXT) };
    }
    if target == 0 {
        return Err("未找到 ZCode 主窗口".into());
    }

    let prev = unsafe { GetForegroundWindow() };
    if prev != target {
        let fore_thread = unsafe { GetWindowThreadProcessId(prev, std::ptr::null_mut()) };
        let cur_thread = unsafe { GetCurrentThreadId() };
        let attached = fore_thread != 0 && fore_thread != cur_thread;
        if attached {
            unsafe { AttachThreadInput(cur_thread, fore_thread, 1) };
        }
        if unsafe { IsIconic(target) } != 0 {
            unsafe { ShowWindow(target, SW_RESTORE) };
        }
        unsafe { SetForegroundWindow(target) };
        std::thread::sleep(Duration::from_millis(150));
        let ok = send_text_and_enter(text);
        std::thread::sleep(Duration::from_millis(100));
        if prev != 0 {
            unsafe { SetForegroundWindow(prev) };
        }
        if attached {
            unsafe { AttachThreadInput(cur_thread, fore_thread, 0) };
        }
        if !ok {
            return Err("SendInput 注入失败".into());
        }
    } else {
        if !send_text_and_enter(text) {
            return Err("SendInput 注入失败".into());
        }
    }
    Ok(())
}

#[cfg(windows)]
fn zcode_pids() -> Result<Vec<u32>, String> {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W, TH32CS_SNAPPROCESS,
    };
    let snap = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snap == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
        return Err("进程快照失败".into());
    }
    let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
    let mut pids = Vec::new();
    if unsafe { Process32FirstW(snap, &mut entry) } != 0 {
        loop {
            let name = String::from_utf16_lossy(
                &entry.szExeFile[..entry.szExeFile.iter().position(|&c| c == 0).unwrap_or(0)],
            );
            if name.eq_ignore_ascii_case("zcode.exe") {
                pids.push(entry.th32ProcessID);
            }
            if unsafe { Process32NextW(snap, &mut entry) } == 0 {
                break;
            }
        }
    }
    unsafe { CloseHandle(snap) };
    Ok(pids)
}

#[cfg(windows)]
fn kb_unicode(scan: u16, flags: u32) -> windows_sys::Win32::UI::Input::KeyboardAndMouse::INPUT {
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{INPUT, INPUT_KEYBOARD, KEYBDINPUT};
    let mut i: INPUT = unsafe { std::mem::zeroed() };
    i.r#type = INPUT_KEYBOARD;
    i.Anonymous.ki = KEYBDINPUT { wVk: 0, wScan: scan, dwFlags: flags, time: 0, dwExtraInfo: 0 };
    i
}

#[cfg(windows)]
fn kb_vk(vk: u16, flags: u32) -> windows_sys::Win32::UI::Input::KeyboardAndMouse::INPUT {
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{INPUT, INPUT_KEYBOARD, KEYBDINPUT};
    let mut i: INPUT = unsafe { std::mem::zeroed() };
    i.r#type = INPUT_KEYBOARD;
    i.Anonymous.ki = KEYBDINPUT { wVk: vk, wScan: 0, dwFlags: flags, time: 0, dwExtraInfo: 0 };
    i
}

#[cfg(windows)]
fn send_text_and_enter(text: &str) -> bool {
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
        SendInput, INPUT, KEYEVENTF_KEYUP, KEYEVENTF_UNICODE, VK_RETURN,
    };
    let mut inputs: Vec<INPUT> = Vec::new();
    for ch in text.encode_utf16() {
        inputs.push(kb_unicode(ch, KEYEVENTF_UNICODE));
        inputs.push(kb_unicode(ch, KEYEVENTF_UNICODE | KEYEVENTF_KEYUP));
    }
    inputs.push(kb_vk(VK_RETURN, 0));
    inputs.push(kb_vk(VK_RETURN, KEYEVENTF_KEYUP));
    let n = unsafe { SendInput(inputs.len() as u32, inputs.as_ptr(), std::mem::size_of::<INPUT>() as i32) };
    n == inputs.len() as u32
}

#[cfg(not(windows))]
fn send_to_zcode_window(_text: &str) -> Result<(), String> {
    Err("看门狗键入仅支持 Windows".into())
}
