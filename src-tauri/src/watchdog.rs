// 看门狗：盯 ZCode 桌面端日志（~/.zcode/v2/logs/YYYY-MM-DD.log），
// 检测回合失败（两类信号）并把「继续」发到出错的那个会话：
//   1) cua-pip-session 行 kind=turn-ended 且 outcome=failed/error（带 sessionId）
//   2) captcha-diagnostics 行 request.respond + headersApplied=false（发送前验证码失败，
//      回合直接中止、不会有 turn-ended 事件；requestId 以 sess_ 开头可提取会话）
// 发送方式：只用 zcode CLI --resume <sessionId> --surface desktop 精准路由到出错会话
// （不抢焦点）。已知会话但 CLI 不可用时直接报错——不再回退键盘注入，因为那只会
// 把「继续」打进当前打开的对话。仅当日志里拿不到 sessionId 时才退回键盘注入。
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
            // 已知会话时只走 CLI 精准路由：键盘注入只会落到「当前打开的对话」，
            // 那就是用户反馈的「继续发错会话」。宁可不发，也不发错。
            let (ok, via, err) = match fail.session.as_deref() {
                Some(sess) => match send_via_cli(sess, fail.session_ws(&session_ws), &text) {
                    Ok(()) => (true, "cli", None),
                    Err(e) => {
                        eprintln!("[zsw-watchdog] 精准注入失败（不回退键盘，避免发错会话）: {e}");
                        (false, "cli", Some(e))
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
                serde_json::json!({ "ok": ok, "turnId": fail.key, "sess": fail.session, "text": text, "via": via, "error": err }),
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

// ---------- CLI 定位 ----------
// 修复：原来写死 D:\zcode、C:\Program Files\ZCode、%LOCALAPPDATA%\Programs\ZCode，
// 装到非默认盘符/自定义目录（例如把安装目录改名或换盘）就找不到 zcode.cjs，于是退化成本地键盘注入，
// 「继续」就落进了当前打开的对话。
const ENV_BUILTIN_CFG: &str = "ZCODE_BUILTIN_PROVIDER_CONFIG_FILE";
const ENV_PERSONAL_CFG: &str = "ZCODE_PERSONAL_PROVIDER_CONFIG_FILE";

#[derive(Clone, Debug)]
struct CliTarget {
    exe: std::path::PathBuf,
    cli: std::path::PathBuf,
    as_node: bool,
    envs: Vec<(String, String)>,
    runtime: &'static str,
}

fn push_unique(list: &mut Vec<std::path::PathBuf>, p: std::path::PathBuf) {
    if !p.as_os_str().is_empty() && !list.iter().any(|x| x == &p) {
        list.push(p);
    }
}

// <root>/resources/glm/zcode.cjs → <root>
fn install_root_of_cli(cli: &std::path::Path) -> Option<std::path::PathBuf> {
    let glm = cli.parent()?;
    if !glm.file_name()?.to_string_lossy().eq_ignore_ascii_case("glm") {
        return None;
    }
    let resources = glm.parent()?;
    resources.parent().map(|p| p.to_path_buf())
}

fn install_roots() -> Vec<std::path::PathBuf> {
    let mut roots: Vec<std::path::PathBuf> = Vec::new();
    let paths = crate::store::Paths::detect();
    if let Some(raw) = crate::store::load_settings(&paths).zcode_path.clone() {
        let pb = std::path::PathBuf::from(raw);
        if pb.is_file() {
            if let Some(dir) = pb.parent() {
                push_unique(&mut roots, dir.to_path_buf());
            }
        } else {
            push_unique(&mut roots, pb);
        }
    }
    // 正在运行的 ZCode 进程所在目录最可靠（支持任意盘符 / 自定义目录）
    #[cfg(windows)]
    for pid in zcode_pids().unwrap_or_default() {
        if let Some(img) = process_image_path(pid) {
            if let Some(dir) = img.parent() {
                push_unique(&mut roots, dir.to_path_buf());
            }
        }
    }
    // 其余固定盘符由下面的 C:–Z: 扫描覆盖，这里只保留最常见的几个
    for fixed in [
        r"D:\zcode",
        r"C:\Program Files\ZCode",
        r"C:\Program Files (x86)\ZCode",
    ] {
        push_unique(&mut roots, std::path::PathBuf::from(fixed));
    }
    if let Some(lad) = std::env::var_os("LOCALAPPDATA") {
        push_unique(&mut roots, std::path::PathBuf::from(lad).join(r"Programs\ZCode"));
    }
    for drive in b'C'..=b'Z' {
        let c = drive as char;
        for sub in [r"\zcode", r"\Program Files\ZCode"] {
            push_unique(&mut roots, std::path::PathBuf::from(format!("{c}:{sub}")));
        }
    }
    roots.retain(|r| r.is_dir());
    roots
}

fn node_in_path() -> Option<std::path::PathBuf> {
    let raw = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&raw) {
        for name in ["node.exe", "node"] {
            let pb = dir.join(name);
            if pb.is_file() {
                return Some(pb);
            }
        }
    }
    None
}

fn resolve_cli_target() -> Result<CliTarget, String> {
    let mut roots: Vec<std::path::PathBuf> = Vec::new();
    let mut cli: Option<std::path::PathBuf> = None;
    if let Some(raw) = std::env::var_os("ZSW_ZCODE_CJS") {
        let pb = std::path::PathBuf::from(raw);
        if pb.is_file() {
            if let Some(root) = install_root_of_cli(&pb) {
                push_unique(&mut roots, root);
            }
            cli = Some(pb);
        }
    }
    for root in install_roots() {
        push_unique(&mut roots, root);
    }
    if cli.is_none() {
        for root in &roots {
            let pb = root.join("resources").join("glm").join("zcode.cjs");
            if pb.is_file() {
                cli = Some(pb);
                break;
            }
        }
    }
    let cli = cli.ok_or_else(|| "未找到 zcode.cjs（可设置环境变量 ZSW_ZCODE_CJS 指定）".to_string())?;
    let root_of_cli = install_root_of_cli(&cli);
    if let Some(root) = root_of_cli.as_ref() {
        push_unique(&mut roots, root.clone());
    }

    // CLI 只按自身入口路径推导内置 provider 配置；非默认布局下会直接报
    // 「无法定位 CLI ZCode Built-in Provider Config」退出，必须显式指给它。
    let mut envs: Vec<(String, String)> = Vec::new();
    if let Some(root) = root_of_cli.as_ref() {
        let builtin = root
            .join("resources")
            .join("config")
            .join("provider")
            .join("zcode-builtin.json");
        if builtin.is_file() {
            envs.push((ENV_BUILTIN_CFG.to_string(), builtin.display().to_string()));
        }
    }
    let personal = crate::store::Paths::detect()
        .zcode_dir()
        .join("v2")
        .join("provider_config.json");
    if personal.is_file() {
        envs.push((ENV_PERSONAL_CFG.to_string(), personal.display().to_string()));
    }
    if envs.len() == 1 {
        // ZCode 要求两份配置同时提供，缺一份会直接报错；宁可都不给，让 CLI 走自身推导。
        eprintln!("[zsw-watchdog] provider 配置只找到一份，已跳过以免 CLI 报错");
        envs.clear();
    }

    // 运行时：优先 PATH 里的 node；否则用 ZCode 自带 Electron（造一个 Node 运行时）
    if let Some(node) = node_in_path() {
        return Ok(CliTarget { exe: node, cli, as_node: false, envs, runtime: "node" });
    }
    for root in &roots {
        for name in ["ZCode.exe", "zcode.exe"] {
            let exe = root.join(name);
            if exe.is_file() {
                return Ok(CliTarget { exe, cli, as_node: true, envs, runtime: "electron-node" });
            }
        }
    }
    Err("未找到 Node 运行时：PATH 里没有 node，ZCode 安装目录里也没有 ZCode.exe".into())
}

fn cli_command(target: &CliTarget) -> std::process::Command {
    let mut cmd = std::process::Command::new(&target.exe);
    if target.as_node {
        // ZCode 自带 Electron：ELECTRON_RUN_AS_NODE=1 时它就是 Node 运行时
        cmd.env("ELECTRON_RUN_AS_NODE", "1");
    }
    for (k, v) in &target.envs {
        cmd.env(k, v);
    }
    cmd.arg(&target.cli);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd
}

fn tail_chars(s: &str, n: usize) -> String {
    let mut v: Vec<char> = s.trim().chars().rev().take(n).collect();
    v.reverse();
    v.into_iter().collect()
}

// 通过 zcode CLI 把「继续」注入到指定会话（desktop surface 会在桌面端原会话里呈现）
fn send_via_cli(session: &str, workspace: Option<String>, text: &str) -> Result<(), String> {
    let target = resolve_cli_target()?;
    let mut cmd = cli_command(&target);
    cmd.args(["--resume", session, "-p", text, "--surface", "desktop"]);
    if let Some(ws) = workspace.as_deref().map(str::trim).filter(|w| !w.is_empty()) {
        cmd.args(["--cwd", ws]);
    }
    // stdout 丢给 null，stderr 单独读线程：CLI 的报错全在 stderr，原来只读 stdout
    // 导致失败原因永远是空字符串，无法排查。
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("启动 {} 失败: {e}", target.runtime))?;
    let err_thread = child.stderr.take().map(|mut pipe| {
        std::thread::spawn(move || {
            use std::io::Read;
            let mut buf = String::new();
            let _ = pipe.read_to_string(&mut buf);
            buf
        })
    });
    let deadline = Instant::now() + Duration::from_millis(CLI_TIMEOUT_MS);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let err_text = err_thread.and_then(|h| h.join().ok()).unwrap_or_default();
                let tail = tail_chars(&err_text, 400);
                if status.success() {
                    eprintln!("[zsw-watchdog] CLI resume 完成（{}）sess={session}", target.runtime);
                    return Ok(());
                }
                // 账号型（coding plan）provider 只由桌面端运行时的协议注入，独立 CLI 绑不上，
                // 这句会是用户最常见的失败原因，直接给出可执行的下一步。
                let hint = if tail.contains("Select a model before sending") {
                    "；该会话用的是账号型（coding plan）provider，只有 ZCode 桌面端自身能绑定，独立 CLI 绑不上 —— 请在 ZCode 里手动打开这条会话后发送「继续」"
                } else {
                    ""
                };
                return Err(if tail.is_empty() {
                    format!("CLI 退出码 {:?}（{}）{hint}", status.code(), target.runtime)
                } else {
                    format!("CLI 退出码 {:?}（{}）: {tail}{hint}", status.code(), target.runtime)
                });
            }
            Ok(None) => {}
            Err(e) => return Err(format!("wait 失败: {e}")),
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("CLI 超时（{CLI_TIMEOUT_MS}ms，{}）", target.runtime));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// 自检：CLI 定位结果（供排障 / 设置页使用）
pub fn diagnostics() -> serde_json::Value {
    match resolve_cli_target() {
        Ok(t) => {
            let version = cli_command(&t)
                .arg("--version")
                .output()
                .map(|o| tail_chars(&String::from_utf8_lossy(&o.stdout), 80))
                .unwrap_or_default();
            serde_json::json!({
                "ok": true,
                "runtime": t.runtime,
                "exe": t.exe.display().to_string(),
                "cli": t.cli.display().to_string(),
                "envs": t.envs.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
                "version": version,
            })
        }
        Err(e) => serde_json::json!({ "ok": false, "error": e }),
    }
}

/// ZCode 主窗口（按进程归属查找，兼容任意标题）
#[cfg(windows)]
fn zcode_main_window() -> Result<windows_sys::Win32::Foundation::HWND, String> {
    use windows_sys::Win32::Foundation::HWND;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetTopWindow, GetWindow, GetWindowTextLengthW, GetWindowThreadProcessId, IsWindowVisible,
        GW_HWNDNEXT,
    };
    let pids = zcode_pids()?;
    if pids.is_empty() {
        return Err("未找到 ZCode 进程".into());
    }
    let mut hwnd: HWND = unsafe { GetTopWindow(0) };
    while hwnd != 0 {
        if unsafe { IsWindowVisible(hwnd) } != 0 && unsafe { GetWindowTextLengthW(hwnd) } > 0 {
            let mut pid: u32 = 0;
            unsafe { GetWindowThreadProcessId(hwnd, &mut pid) };
            if pids.contains(&pid) {
                return Ok(hwnd);
            }
        }
        hwnd = unsafe { GetWindow(hwnd, GW_HWNDNEXT) };
    }
    Err("未找到 ZCode 主窗口".into())
}

/// 把窗口调到前台，返回恢复现场所需的信息
#[cfg(windows)]
fn bring_foreground(target: windows_sys::Win32::Foundation::HWND) -> (windows_sys::Win32::Foundation::HWND, u32, u32, bool) {
    use windows_sys::Win32::System::Threading::{AttachThreadInput, GetCurrentThreadId};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetForegroundWindow, GetWindowThreadProcessId, IsIconic, SetForegroundWindow, ShowWindow, SW_RESTORE,
    };
    let prev = unsafe { GetForegroundWindow() };
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
    (prev, fore_thread, cur_thread, attached)
}

#[cfg(windows)]
fn restore_foreground(
    prev: windows_sys::Win32::Foundation::HWND,
    fore_thread: u32,
    cur_thread: u32,
    attached: bool,
) {
    use windows_sys::Win32::System::Threading::AttachThreadInput;
    use windows_sys::Win32::UI::WindowsAndMessaging::SetForegroundWindow;
    if prev != 0 {
        unsafe { SetForegroundWindow(prev) };
    }
    if attached {
        unsafe { AttachThreadInput(cur_thread, fore_thread, 0) };
    }
}

/// 键盘注入兜底：仅在拿不到会话 id 时使用（无法精准路由时才会走到这里）
#[cfg(windows)]
fn send_to_zcode_window(text: &str) -> Result<(), String> {
    let target = zcode_main_window()?;
    let (prev, fore_thread, cur_thread, attached) = bring_foreground(target);
    let ok = send_text_and_enter(text);
    std::thread::sleep(Duration::from_millis(100));
    restore_foreground(prev, fore_thread, cur_thread, attached);
    if ok {
        Ok(())
    } else {
        Err("SendInput 注入失败".into())
    }
}

/// 热切换后让运行中的 ZCode 重新加载登录态（不退出应用）：聚焦窗口并发送 Ctrl+R
#[cfg(windows)]
pub fn reload_zcode_ui() -> Result<(), String> {
    let target = zcode_main_window()?;
    let (prev, fore_thread, cur_thread, attached) = bring_foreground(target);
    let ok = send_ctrl_r();
    std::thread::sleep(Duration::from_millis(120));
    restore_foreground(prev, fore_thread, cur_thread, attached);
    if ok {
        Ok(())
    } else {
        Err("发送 Ctrl+R 失败".into())
    }
}

#[cfg(not(windows))]
pub fn reload_zcode_ui() -> Result<(), String> {
    Err("热切换刷新界面仅支持 Windows".into())
}

#[cfg(windows)]
fn send_ctrl_r() -> bool {
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
        SendInput, INPUT, KEYEVENTF_KEYUP, VK_CONTROL, VK_R,
    };
    let inputs = [
        kb_vk(VK_CONTROL, 0),
        kb_vk(VK_R, 0),
        kb_vk(VK_R, KEYEVENTF_KEYUP),
        kb_vk(VK_CONTROL, KEYEVENTF_KEYUP),
    ];
    let n = unsafe { SendInput(inputs.len() as u32, inputs.as_ptr(), std::mem::size_of::<INPUT>() as i32) };
    n == inputs.len() as u32
}

/// 进程映像路径（定位 ZCode 安装目录，支持任意盘符/自定义目录）
#[cfg(windows)]
fn process_image_path(pid: u32) -> Option<std::path::PathBuf> {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, QueryFullProcessImageNameW, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle == 0 {
        return None;
    }
    let mut buf = [0u16; 1024];
    let mut size = buf.len() as u32;
    let ok = unsafe { QueryFullProcessImageNameW(handle, 0, buf.as_mut_ptr(), &mut size) };
    unsafe { CloseHandle(handle) };
    if ok == 0 {
        return None;
    }
    Some(std::path::PathBuf::from(String::from_utf16_lossy(&buf[..size as usize])))
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
