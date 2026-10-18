// عاداتي — واجهة الحفظ والمزامنة على Vercel.
// تخزّن البيانات في Redis (Upstash) الذي يضيفه Vercel من تبويب Storage.
// الترتيب يحدده السيرفر (آخر طلب يصل يكسب)، ولا يعتمد على ساعة أي جهاز.
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const PASS = process.env.APP_PASSCODE || "";
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

async function redis(cmds) {
  const r = await fetch(URL_ + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error("redis_http_" + r.status);
  const out = await r.json();
  return out.map((o) => {
    if (o && o.error) throw new Error(o.error);
    return o.result;
  });
}

function parse(raw) {
  try { const v = JSON.parse(raw); return v && typeof v === "object" ? v : null; } catch (e) { return null; }
}

module.exports = async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  if (!URL_ || !TOKEN) return res.status(500).json({ error: "storage_not_configured" });
  if (PASS && req.headers["x-passcode"] !== encodeURIComponent(PASS)) {
    return res.status(401).json({ error: "bad_passcode" });
  }
  try {
    if (req.method === "GET") {
      const known = Number(req.query && req.query.rev);
      const [revRaw] = await redis([["GET", "habits:rev"]]);
      const rev = Number(revRaw || 0);
      if (known === rev) return res.status(200).json({ rev, same: true });
      const [flat] = await redis([["HGETALL", "habits:data"]]);
      let config = null;
      const entries = {};
      for (let i = 0; i < (flat || []).length; i += 2) {
        const field = flat[i];
        const val = parse(flat[i + 1]);
        if (!val) continue;
        if (field === "cfg") config = val;
        else if (field.startsWith("e:")) entries[field.slice(2)] = val;
      }
      return res.status(200).json({ rev, config, entries });
    }

    if (req.method === "POST") {
      const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
      const ops = Array.isArray(body.ops) ? body.ops : [];
      const cmds = [];

      if (body.config && typeof body.config === "object") {
        cmds.push(["HSET", "habits:data", "cfg", JSON.stringify({ catColors: body.config.catColors || {}, tasks: body.config.tasks || [] })]);
      }

      // تفريغ كل المدخلات (زر «تفريغ الملف»): نمسح كل أيام التسجيل أولاً ثم نطبّق أي تعديلات وصلت بعده.
      const cleared = body.reset === true;
      if (cleared) {
        const [keys] = await redis([["HKEYS", "habits:data"]]);
        const del = (keys || []).filter((k) => typeof k === "string" && k.startsWith("e:"));
        if (del.length) cmds.push(["HDEL", "habits:data", ...del]);
      }

      // نطبّق كل تعديل على المهمة المعنية فقط داخل يومها، بالترتيب الذي وصل به.
      const dates = [...new Set(ops.map((o) => o && o.d).filter((d) => DATE_RE.test(d)))];
      if (dates.length) {
        const [existing] = cleared ? [[]] : await redis([["HMGET", "habits:data", ...dates.map((d) => "e:" + d)]]);
        const docs = {};
        dates.forEach((d, i) => {
          const cur = existing && existing[i] ? parse(existing[i]) : null;
          docs[d] = { ramadan: !!(cur && cur.ramadan), values: (cur && cur.values) || {} };
        });
        for (const o of ops) {
          if (!o || !docs[o.d]) continue;
          if (o.k === "r") docs[o.d].ramadan = !!o.r;
          else if (o.k === "v" && typeof o.id === "string" && o.id) {
            if (o.v && typeof o.v === "object" && Object.keys(o.v).length) docs[o.d].values[o.id] = o.v;
            else delete docs[o.d].values[o.id];
          }
        }
        for (const d of dates) {
          const doc = docs[d];
          if (!doc.ramadan && !Object.keys(doc.values).length) cmds.push(["HDEL", "habits:data", "e:" + d]);
          else cmds.push(["HSET", "habits:data", "e:" + d, JSON.stringify(doc)]);
        }
      }

      if (!cmds.length) return res.status(200).json({ ok: true });
      cmds.push(["INCR", "habits:rev"]);
      const out = await redis(cmds);
      return res.status(200).json({ ok: true, rev: out[out.length - 1] });
    }
    return res.status(405).json({ error: "method_not_allowed" });
  } catch (e) {
    return res.status(500).json({ error: "server_error", detail: String(e.message || e) });
  }
};
