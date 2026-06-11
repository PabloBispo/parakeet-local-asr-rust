//! Renders the bundled integration docs (Markdown) to a styled HTML page,
//! served at `/docs`. The Markdown is embedded in the binary at build time.

use std::sync::OnceLock;

const DOCS_MD: &str = include_str!("../docs/integrations.md");

/// Rendered docs page (computed once, then reused).
pub fn html() -> &'static str {
    static RENDERED: OnceLock<String> = OnceLock::new();
    RENDERED.get_or_init(|| {
        let mut body = String::new();
        let parser =
            pulldown_cmark::Parser::new_ext(DOCS_MD, pulldown_cmark::Options::all());
        pulldown_cmark::html::push_html(&mut body, parser);
        format!("{HEAD}{body}{TAIL}")
    })
}

const HEAD: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>parakeet-asr // docs</title>
<style>
  :root {
    --bg:#0c0a09; --panel:#16120f; --panel2:#1f1916; --border:#2c241f; --border2:#3a2f28;
    --fg:#ece4dc; --muted:#a08c7d; --rust:#f74c00; --sand:#dea584;
    --mono: ui-monospace,"SF Mono","JetBrains Mono","Cascadia Code",Menlo,Consolas,monospace;
    --sans: ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:15px/1.65 var(--sans); -webkit-font-smoothing:antialiased; }
  ::selection { background:rgba(247,76,0,.3); }
  header {
    position:sticky; top:0; z-index:10; background:rgba(12,10,9,.9); backdrop-filter:blur(10px);
    border-bottom:1px solid var(--border); padding:12px 22px; display:flex; align-items:center; gap:14px;
  }
  header .crab { font-size:18px; }
  header h1 { font:600 14px/1 var(--mono); margin:0; }
  header h1 .slash { color:var(--border2); margin:0 2px; } header h1 .rust { color:var(--rust); }
  header a { margin-left:auto; font:600 12px/1 var(--mono); color:var(--muted); text-decoration:none;
    border:1px solid var(--border2); border-radius:4px; padding:7px 12px; transition:.15s; }
  header a:hover { color:var(--fg); border-color:var(--rust); }
  main { max-width:860px; margin:0 auto; padding:28px 22px 90px; }
  main h1 { font:700 26px/1.2 var(--sans); margin:.4em 0 .3em; }
  main h2 { font:650 20px/1.25 var(--sans); margin:1.6em 0 .5em; padding-top:.5em; border-top:1px solid var(--border); }
  main h3 { font:600 16px/1.3 var(--sans); margin:1.3em 0 .4em; color:var(--sand); }
  a { color:var(--rust); }
  code { font:13px var(--mono); background:var(--panel2); border:1px solid var(--border); border-radius:3px; padding:1px 5px; }
  pre { background:var(--panel2); border:1px solid var(--border); border-radius:6px; padding:14px 16px; overflow:auto; }
  pre code { background:none; border:none; padding:0; font-size:13px; line-height:1.55; }
  table { border-collapse:collapse; width:100%; margin:1em 0; font-size:14px; }
  th,td { border:1px solid var(--border); padding:7px 10px; text-align:left; }
  th { background:var(--panel2); font:600 13px var(--mono); }
  blockquote { border-left:3px solid var(--rust); margin:1em 0; padding:.2em 14px; color:var(--muted); background:var(--panel); }
  hr { border:none; border-top:1px solid var(--border); margin:2em 0; }
  ul,ol { padding-left:1.4em; }
</style>
</head>
<body>
<header>
  <span class="crab">&#129408;</span>
  <h1>parakeet-asr<span class="slash">//</span><span class="rust">docs</span></h1>
  <a href="/ui">&larr; back to app</a>
</header>
<main>
"#;

const TAIL: &str = "\n</main>\n</body>\n</html>\n";
