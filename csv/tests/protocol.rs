//! The built `loadsmith-parser-csv` binary, driven over the wire protocol
//! like the core does: stdin/stdout control lines, the Arrow stream on fd3,
//! events on fd4.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("loadsmith-parser-csv-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Dir(p)
    }
    fn file(&self, name: &str, body: &str) -> String {
        let p = self.0.join(name);
        std::fs::write(&p, body).unwrap();
        p.to_string_lossy().into_owned()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Run {
    code: i32,
    control: Vec<String>,
    events: String,
    /// (rows, ended with the end-of-stream marker)
    stream: (usize, bool),
}

fn run(dir: &Path, config: &str, paths: &[String]) -> Run {
    let data_path = dir.join("fd3.arrow");
    let events_path = dir.join("fd4.jsonl");
    let data = std::fs::File::create(&data_path).unwrap();
    let events = std::fs::File::create(&events_path).unwrap();
    let (data_fd, events_fd) = (data.as_raw_fd(), events.as_raw_fd());

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_loadsmith-parser-csv"));
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null());
    // SAFETY: fcntl and dup2 are async-signal-safe; this runs in the child
    // before exec. Tests run in parallel, so either file may already sit at
    // fd 3 or 4: move both above them first, then onto 3 and 4 (dup2 to
    // another number also clears close-on-exec).
    unsafe {
        cmd.pre_exec(move || {
            let high_data = libc::fcntl(data_fd, libc::F_DUPFD, 10);
            let high_events = libc::fcntl(events_fd, libc::F_DUPFD, 10);
            if high_data < 0
                || high_events < 0
                || libc::dup2(high_data, 3) < 0
                || libc::dup2(high_events, 4) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd.spawn().unwrap();
    drop((data, events));
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut recv = || {
        let mut s = String::new();
        stdout.read_line(&mut s).unwrap();
        s.trim_end().to_string()
    };
    let mut send = |line: &str| {
        // The parser may exit (a failure) before it read everything.
        let _ = writeln!(stdin, "{line}");
    };

    send(r#"{"type":"handshake"}"#);
    assert!(recv().contains(r#""kind":"parser""#));
    send(r#"{"type":"set_protocol_version","protocol_version":1}"#);
    send(r#"{"type":"capabilities_request"}"#);
    assert_eq!(recv(), r#"{"type":"capabilities_response","supports":["parse"]}"#);
    send(&format!(r#"{{"type":"configure","config":{config}}}"#));
    assert_eq!(recv(), r#"{"type":"configure_ack","status":"ok"}"#);
    send(r#"{"type":"start"}"#);
    for p in paths {
        send(&serde_json::json!({"type": "parse_object", "path": p}).to_string());
    }
    drop(send);
    drop(stdin); // EOF: no more files

    let mut control = Vec::new();
    loop {
        let line = recv();
        if line.is_empty() {
            break;
        }
        control.push(line);
    }
    let code = child.wait().unwrap().code().unwrap_or(-1);

    let bytes = std::fs::read(&data_path).unwrap();
    let stream = if bytes.is_empty() {
        (0, false)
    } else {
        let mut r = loadsmith_arrow::IpcReader::new(bytes.as_slice()).unwrap();
        let mut rows = 0;
        while let Ok(Some(b)) = r.read_batch() {
            rows += b.num_rows();
        }
        (rows, r.ended_cleanly())
    };
    Run { code, control, events: std::fs::read_to_string(&events_path).unwrap(), stream }
}

#[test]
fn files_with_one_schema_make_one_stream() {
    let d = Dir::new("ok");
    let a = d.file("a.csv", "id,name\n1,alice\n2,bob\n");
    let b = d.file("b.csv", "id,name\n3,\n");
    let r = run(&d.0, r#"{"batch_size":1}"#, &[a.clone(), b]);
    assert_eq!(r.code, 0, "{:?}", r.control);
    assert!(r.control[0].starts_with(r#"{"type":"schema","fields":[{"name":"id","type":"int64","nullable":true}"#), "{:?}", r.control);
    assert_eq!(r.control.last().unwrap(), r#"{"type":"finished","status":"success","rows_read":3,"batches_read":3}"#);
    assert_eq!(r.stream, (3, true));
    assert!(r.events.contains(&format!(r#""path":"{a}","rows":2,"batches":2,"last_batch_seq":2"#)), "{}", r.events);
}

#[test]
fn a_later_file_with_another_schema_is_a_schema_mismatch() {
    let d = Dir::new("mismatch");
    let a = d.file("a.csv", "id,amount\n1,10\n");
    let b = d.file("b.csv", "id,amount\n2,ten\n");
    let r = run(&d.0, "{}", &[a, b]);
    assert_eq!(r.code, 1);
    let finished = r.control.last().unwrap();
    assert!(finished.contains(r#""code":"SCHEMA_MISMATCH""#), "{finished}");
    assert!(finished.contains("b.csv: column 2 (`amount`) is utf8, the stream has int64"), "{finished}");
    assert!(!r.stream.1, "no end-of-stream marker after a failure");
}

#[test]
fn a_malformed_file_is_a_parse_failure() {
    let d = Dir::new("malformed");
    let a = d.file("a.csv", "id,name\n1,alice\n2\n");
    let r = run(&d.0, "{}", &[a]);
    assert_eq!(r.code, 1);
    let finished = r.control.last().unwrap();
    assert!(finished.contains(r#""code":"OPEN_FAILED""#), "{finished}");
    assert!(finished.contains("line 3: 1 fields, expected 2"), "{finished}");
}

#[test]
fn declared_columns_allow_zero_files() {
    let d = Dir::new("zero");
    let r = run(&d.0, r#"{"columns":[{"name":"id","type":"int64"}]}"#, &[]);
    assert_eq!(r.code, 0, "{:?}", r.control);
    assert_eq!(r.stream, (0, true));
}
