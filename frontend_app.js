const NAYIN_WUXING = {
  "甲子": "金", "乙丑": "金", "丙寅": "火", "丁卯": "火", "戊辰": "木", "己巳": "木",
  "庚午": "土", "辛未": "土", "壬申": "金", "癸酉": "金", "甲戌": "火", "乙亥": "火",
  "丙子": "水", "丁丑": "水", "戊寅": "土", "己卯": "土", "庚辰": "金", "辛巳": "金",
  "壬午": "木", "癸未": "木", "甲申": "水", "乙酉": "水", "丙戌": "土", "丁亥": "土",
  "戊子": "火", "己丑": "火", "庚寅": "木", "辛卯": "木", "壬辰": "水", "癸巳": "水",
  "甲午": "金", "乙未": "金", "丙申": "火", "丁酉": "火", "戊戌": "木", "己亥": "木",
  "庚子": "土", "辛丑": "土", "壬寅": "金", "癸卯": "金", "甲辰": "火", "乙巳": "火",
  "丙午": "水", "丁未": "水", "戊申": "土", "己酉": "土", "庚戌": "金", "辛亥": "金",
  "壬子": "木", "癸丑": "木", "甲寅": "水", "乙卯": "水", "丙辰": "土", "丁巳": "土",
  "戊午": "火", "己未": "火", "庚申": "木", "辛酉": "木", "壬戌": "水", "癸亥": "水",
};

const TIANGAN = ["甲", "乙", "丙", "丁", "戊", "己", "庚", "辛", "壬", "癸"];
const DIZHI = ["子", "丑", "寅", "卯", "辰", "巳", "午", "未", "申", "酉", "戌", "亥"];

function cleanKey(val) {
  if (val === null || val === undefined) return "";
  const s = String(val).replace(/\ufeff/g, "").trim();
  return s === "nan" ? "" : s;
}

function parseOffsets(raw) {
  return String(raw ?? "")
    .replace(/，/g, "|")
    .replace(/\n/g, "|")
    .split("|")
    .map((x) => x.trim())
    .filter((x) => /^\d+$/.test(x))
    .map((x) => Number(x));
}

function getGanGroup(gan) {
  if (!TIANGAN.includes(gan)) return "甲己";
  return ["甲己", "乙庚", "丙辛", "丁壬", "戊癸"][TIANGAN.indexOf(gan) % 5];
}

function isYangYear(yearGan) {
  return ["甲", "丙", "戊", "庚", "壬"].includes(yearGan);
}

function getLiunianGroups(yearGan, yearZhi) {
  let bGroup = "未知";
  if ("寅午戌".includes(yearZhi)) bGroup = "寅午戌";
  else if ("申子辰".includes(yearZhi)) bGroup = "申子辰";
  else if ("巳酉丑".includes(yearZhi)) bGroup = "巳酉丑";
  else if ("亥卯未".includes(yearZhi)) bGroup = "亥卯未";

  let sGroup = "未知";
  if ("甲乙丙丁".includes(yearGan)) sGroup = "甲乙丙丁";
  else if ("戊己".includes(yearGan)) sGroup = "戊己";
  else if ("庚辛".includes(yearGan)) sGroup = "庚辛";
  else if ("壬癸".includes(yearGan)) sGroup = "壬癸";
  return { bGroup, sGroup };
}

function calculateCorrection(originalCorrection, age) {
  if (!originalCorrection) return 0;
  if ((1 <= age && age <= 10) || (81 <= age && age <= 108)) {
    let v = originalCorrection + 2;
    if (v > 6) v -= 6;
    return v;
  }
  let v = originalCorrection + 3;
  if (v > 20) v -= 20;
  return v;
}

function decodeCsvBytes(buf) {
  const u8 = new Uint8Array(buf);
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(u8);
  } catch {
    try {
      return new TextDecoder("gb18030").decode(u8);
    } catch {
      return new TextDecoder("utf-8").decode(u8);
    }
  }
}

async function readCsv(filename, withHeader = true) {
  let resp = null;
  for (const base of ["./data", "./数据库"]) {
    const tryResp = await fetch(`${base}/${filename}`);
    if (tryResp.ok) {
      resp = tryResp;
      break;
    }
  }
  if (!resp) throw new Error(`读取数据库失败: ${filename}`);

  const buf = await resp.arrayBuffer();
  const text = decodeCsvBytes(buf);
  const parsed = Papa.parse(text, { header: withHeader, skipEmptyLines: true });
  return parsed.data;
}

class TiebanData {
  constructor() {
    this.tables = {};
    this.ruleTables = [];
    this.hexDetailMap = new Map();
    this.hexMap = new Map();
    this.destinyData = new Map();
    this.liunianStart = new Map();
    this.liunianSeq = new Map();
    this.markerTable = new Map();
    this.letterTable = new Map();
    this.dataByLetter = new Map();
    this.dataByCorrection = new Map();
    this.correctionToLetter = new Map();
    this.fortuneDuanyuMap = new Map();
  }

  async load() {
    const d141 = await readCsv("14-1.csv");
    this.tables["14-1"] = Object.fromEntries(
      d141.map((r) => [cleanKey(r["农历月份"]), Number(r["数值"] || 0)])
    );

    const d142 = await readCsv("14-2.csv");
    this.tables["14-2"] = Object.fromEntries(
      d142.map((r) => [cleanKey(r["时支"]), Number(r["数值"] || 0)])
    );

    const d143 = await readCsv("14-3.csv");
    this.tables["14-3"] = {};
    for (const r of d143) {
      const raw = cleanKey(r["先天命数"]);
      if (!raw) continue;
      for (const n of raw.split("|")) {
        if (/^\d+$/.test(n.trim())) {
          this.tables["14-3"][Number(n)] = Object.fromEntries(
            Object.entries(r).map(([k, v]) => [cleanKey(k), cleanKey(v)])
          );
        }
      }
    }

    const d144 = await readCsv("14-4.csv");
    this.tables["14-4"] = Object.fromEntries(
      d144.map((r) => [cleanKey(r["五音"]), Number(r["数值"] || 0)])
    );

    const d145 = await readCsv("14-5.csv");
    this.tables["14-5"] = {};
    for (const r of d145) {
      const nayin = cleanKey(r["日柱纳音"]);
      if (!nayin) continue;
      const rowObj = {};
      for (const [k, v] of Object.entries(r)) {
        const key = cleanKey(k);
        if (!key || key === "日柱纳音") continue;
        rowObj[key] = Number(v || 0);
      }
      this.tables["14-5"][nayin] = rowObj;
    }

    const d146 = await readCsv("14-6.csv");
    this.tables["14-6"] = Object.fromEntries(
      d146.map((r) => [cleanKey(r["时柱纳音"]), Number(r["数值"] || 0)])
    );

    this.ruleTables = await readCsv("14-7.csv");

    const d149 = await readCsv("14-9.csv", false);
    for (const row of d149) {
      const kebie = cleanKey(row[0]);
      const mainNum = Number(row[1]);
      const gua = cleanKey(row[2]);
      if (!["初刻", "正刻"].includes(kebie) || !gua || Number.isNaN(mainNum)) continue;
      this.hexDetailMap.set(`${kebie}|${mainNum}`, gua);
      if (!this.hexMap.has(mainNum)) this.hexMap.set(mainNum, gua);
    }

    const d1410 = await readCsv("14-10.csv");
    for (const r of d1410) {
      const gua = cleanKey(r["十二辟卦"] || r["卦名"]);
      if (!gua) continue;
      const base = Number(r["基数"] || 0);
      const seq = Number(r["序数"] || 0);
      const offsets = {
        性格: parseOffsets(r["性格"]),
        才能前程: parseOffsets(r["才能前程"]),
        财运: parseOffsets(r["财运"]),
        兄弟个数: parseOffsets(r["兄弟个数"]),
      };
      const pack = { base, seq, offsets };
      const initialRaw = cleanKey(r["初刻生人先天命数"] || r["初刻先天"]);
      for (const n of initialRaw.split("|")) {
        if (/^\d+$/.test(n.trim())) this.destinyData.set(`${gua}|Initial|${Number(n)}`, pack);
      }
      const mainRaw = cleanKey(r["正刻生人先天命数"] || r["正刻先天"] || r["正刻"]);
      for (const n of mainRaw.split("|")) {
        if (/^\d+$/.test(n.trim())) this.destinyData.set(`${gua}|Main|${Number(n)}`, pack);
      }
    }

    const d14111 = await readCsv("14-11-1.csv");
    for (const r of d14111) {
      const bg = cleanKey(r["年支组"]);
      const gender = cleanKey(r["性别"]);
      const start = Number(r["起始数"] || 0);
      if (bg && gender) this.liunianStart.set(`generic|${bg}|${gender}`, start);
      const numRaw = cleanKey(r["先天命数"]);
      if (numRaw && /^\d+$/.test(numRaw)) this.liunianStart.set(`${Number(numRaw)}|${bg}|${gender}`, start);
    }

    const d14112 = await readCsv("14-11-2.csv");
    for (const r of d14112) {
      if (!r || typeof r !== "object") continue;
      const num = Number(r["先天命数"] || 0);
      const gan = cleanKey(r["天干"] || r["年干组"]);
      if (!num || !gan) continue;
      const seq = [];
      for (let i = 1; i <= 12; i += 1) seq.push(cleanKey(r[String(i)]));
      if (seq.some(Boolean)) this.liunianSeq.set(`${num}|${gan}`, seq);
    }

    const d1412 = await readCsv("14-12.csv");
    for (const r of d1412) {
      const zhi = cleanKey(r["流年地支"]);
      const pnNum = Number(r["后天命数"] || 0);
      const marker = cleanKey(r["流年标记"]);
      if (zhi && pnNum && marker) this.markerTable.set(`${zhi}|${pnNum}`, marker);
    }

    const d1413 = await readCsv("14-13.csv");
    for (const r of d1413) {
      const moment = cleanKey(r["考刻"]);
      const parity = cleanKey(r["日命数加时运数的奇偶性"]);
      const sound = cleanKey(r["流年天四声"]);
      const marker = cleanKey(r["流年标记"]);
      const letter = cleanKey(r["流年字母"]);
      if (moment && parity && sound && marker && letter) {
        this.letterTable.set(`${moment}|${parity}|${sound}|${marker}`, letter);
      }
    }

    const d1414 = await readCsv("14-14.csv");
    for (const r of d1414) {
      const letter = cleanKey(r["流年字母"]);
      const age = Number(r["流年岁数"] || 0);
      const base = Number(r["基数"] || 0);
      const add = Number(r["加数"] || 0);
      const correction = Number(r["条文校正数"] || 0);
      if (!letter || !age) continue;
      this.dataByLetter.set(`${letter}|${age}`, { base, add, correction });
      this.dataByCorrection.set(`${correction}|${age}`, { base, add });
      this.correctionToLetter.set(`${correction}|${age}`, letter);
    }

    let duanyuRows = [];
    try {
      duanyuRows = await readCsv("fortune_duanyu.csv");
    } catch {
      duanyuRows = await readCsv("铁板神数-条文断词.csv");
    }
    for (const r of duanyuRows) {
      const num = Number(r["条文数"] || r["条文数字"] || 0);
      if (!num) continue;
      this.fortuneDuanyuMap.set(num, {
        duanyu: cleanKey(r["吉凶断词"] || r["断语"] || r["断词"]),
        age: cleanKey(r["年龄"] || r["对应年龄"] || r["岁数"]),
      });
    }
  }
}

function parseInputDatetime(dtStr) {
  const parts = dtStr.trim().split(" ");
  if (parts.length !== 2) throw new Error("时间格式错误，应为 YYYY-MM-DD HH:MM");
  const [d, t] = parts;
  const [year, month, day] = d.split("-").map(Number);
  const [hour, minute] = t.split(":").map(Number);
  if ([year, month, day, hour, minute].some((n) => Number.isNaN(n))) {
    throw new Error("时间格式错误，应为 YYYY-MM-DD HH:MM");
  }
  if (hour >= 23) return new Date(year, month - 1, day + 1, 0, minute, 0);
  return new Date(year, month - 1, day, hour, minute, 0);
}

function pillarStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  if (typeof v === "object" && typeof v.toString === "function") return String(v.toString());
  return String(v);
}

function convertToBaziInfo(dt) {
  const solarLib = window.Solar || (window.lunar && window.lunar.Solar);
  if (!solarLib) throw new Error("lunar-javascript 加载失败");
  const s = solarLib.fromYmdHms(
    dt.getFullYear(),
    dt.getMonth() + 1,
    dt.getDate(),
    dt.getHours(),
    dt.getMinutes(),
    dt.getSeconds()
  );
  const lunar = s.getLunar();
  const ec = lunar.getEightChar();
  const lm = lunar.getMonth();
  return {
    lunar_month: Math.abs(lm),
    lunar_day: lunar.getDay(),
    is_leap: lm < 0,
    bazi: {
      year: pillarStr(ec.getYear()),
      month: pillarStr(ec.getMonth()),
      day: pillarStr(ec.getDay()),
      time: pillarStr(ec.getTime()),
    },
    date_str: `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")} ${String(dt.getHours()).padStart(2, "0")}:${String(dt.getMinutes()).padStart(2, "0")}`,
    lunar_str: lunar.toString(),
  };
}

function getFortuneDuanyu(db, fortuneNum) {
  if (!fortuneNum) return { duanyu: "", age: "" };
  const n = Number(fortuneNum);
  if (Number.isNaN(n)) return { duanyu: "", age: "" };
  return db.fortuneDuanyuMap.get(n) || { duanyu: "未找到断语", age: "未知" };
}

function calculateAll(db, gender, birth, query) {
  const yGan = birth.bazi.year[0];
  const yZhi = birth.bazi.year[1];
  const tZhi = birth.bazi.time[1];
  const dDay = birth.bazi.day;
  const tGan = query.bazi.time[0];
  const tTime = query.bazi.time;

  let calcMonth = String(birth.lunar_month + (birth.is_leap ? 1 : 0));
  if (Number(calcMonth) > 12) calcMonth = "1";

  const monthVal = db.tables["14-1"][calcMonth] ?? Number(calcMonth);
  const timeVal = db.tables["14-2"][tZhi] ?? 0;
  let congNum = monthVal + 3 - timeVal;
  if (congNum <= 0) congNum += 12;

  const ganGroup = getGanGroup(yGan);
  const tone = (db.tables["14-3"]?.[congNum] || {})[ganGroup] || "宫";
  const toneNum = db.tables["14-4"][tone] ?? 5;

  const dayN = NAYIN_WUXING[dDay] || "金";
  const dayLife = ((db.tables["14-5"][dayN] || {})[tGan]) ?? 0;
  const timeN = NAYIN_WUXING[tTime] || "金";
  const timeLuck = db.tables["14-6"][timeN] ?? 0;

  const sumVal = dayLife + timeLuck;
  const grp = ((gender === "男" && isYangYear(yGan)) || (gender === "女" && !isYangYear(yGan))) ? "阳男阴女" : "阴男阳女";
  const cond = sumVal > 6 ? ">6" : "<=6";
  let moment = "Main";
  for (const r of db.ruleTables) {
    if (cleanKey(r["组别"]) === grp && cleanKey(r["和值条件"]) === cond) {
      moment = cleanKey(r["刻别"]) === "初刻" ? "Initial" : "Main";
      break;
    }
  }
  const momentCn = moment === "Initial" ? "初刻" : "正刻";

  const baseVal = toneNum * 5 + dayLife + timeLuck;
  const fact = sumVal <= 6 ? (baseVal - 1) : (baseVal - 6);
  const mainNum = fact * 30 + birth.lunar_day;
  const hexName = db.hexDetailMap.get(`${momentCn}|${mainNum}`) || db.hexMap.get(mainNum) || `未知(刻别:${momentCn},本命数:${mainNum}未匹配)`;
  const tblData = db.destinyData.get(`${hexName}|${moment}|${congNum}`) || null;

  const pnSum = congNum + mainNum;
  let pnNum = pnSum % 8;
  if (pnNum === 0) pnNum = 8;

  const { bGroup, sGroup } = getLiunianGroups(yGan, yZhi);
  let start = 0;
  for (const k of [`${congNum}|${bGroup}|${gender}`, `generic|${bGroup}|${gender}`]) {
    if (db.liunianStart.has(k)) {
      start = db.liunianStart.get(k) || 0;
      break;
    }
  }

  let rawSeq = [];
  for (const k of [`${congNum}|${yGan}`, `${congNum}|${sGroup}`]) {
    if (db.liunianSeq.has(k)) {
      rawSeq = db.liunianSeq.get(k) || [];
      break;
    }
  }
  let finalSeq = Array(12).fill("?");
  if (start !== 0 && rawSeq.length >= 12) {
    const off = (13 - start) % 12;
    finalSeq = Array.from({ length: 12 }, (_, i) => rawSeq[(i + off) % 12] || "?");
  }

  const stTg = TIANGAN.indexOf(yGan);
  const stDz = DIZHI.indexOf(yZhi);
  const liunian = [];
  if (stTg >= 0 && stDz >= 0) {
    for (let age = 1; age <= 108; age += 1) {
      const curTg = TIANGAN[(stTg + age - 1) % 10];
      const curDz = DIZHI[(stDz + age - 1) % 12];
      const sound = finalSeq[0] !== "?" ? finalSeq[(age - 1) % 12] : "?";
      const marker = db.markerTable.get(`${curDz}|${pnNum}`) || "?";
      const parity = age % 2 === 1 ? "奇数" : "偶数";
      const letter = db.letterTable.get(`${momentCn}|${parity}|${sound}|${marker}`) || "?";

      let base = 0;
      let add = 0;
      let originalCorrection = 0;
      let correctedCorrection = 0;
      let originalFortune = "";
      let correctedFortune = "";
      let formula = "";
      let correctedLetter = "";

      if (letter !== "?" && db.dataByLetter.has(`${letter}|${age}`)) {
        const row = db.dataByLetter.get(`${letter}|${age}`);
        base = row.base;
        add = row.add;
        originalCorrection = row.correction;
        formula = `${base}+${add}`;
        originalFortune = String(base + add);

        correctedCorrection = calculateCorrection(originalCorrection, age);
        if (correctedCorrection > 0 && db.dataByCorrection.has(`${correctedCorrection}|${age}`)) {
          const corr = db.dataByCorrection.get(`${correctedCorrection}|${age}`);
          correctedFortune = String(corr.base + corr.add);
          correctedLetter = db.correctionToLetter.get(`${correctedCorrection}|${age}`) || "?";
        }
      }

      const od = getFortuneDuanyu(db, originalFortune);
      const cd = getFortuneDuanyu(db, correctedFortune);
      liunian.push({
        age,
        year: `${curTg}${curDz}`,
        sound,
        marker,
        letter,
        corrected_letter: correctedLetter,
        original_correction: String(originalCorrection),
        corrected_correction: String(correctedCorrection),
        formula,
        original_fortune: originalFortune,
        corrected_fortune: correctedFortune,
        original_duanyu: od.duanyu,
        original_duanyu_age: od.age,
        corrected_duanyu: cd.duanyu,
        corrected_duanyu_age: cd.age,
      });
    }
  }

  return {
    congCalc: `先天命数 = ${congNum}`,
    congNum,
    toneNum,
    dayLifeCalc: `日命:${dayLife}, 时运:${timeLuck}`,
    momentCalc: `考刻: ${momentCn} (${grp})`,
    mainCalc: `本命数: ${mainNum}`,
    mainNum,
    pnNum,
    hexName,
    tblData,
    liunian,
  };
}

function formatDestinyNumbers(db, title, offsetList, base, seq) {
  const lines = [];
  for (const off of offsetList || []) {
    const n = base + seq + off;
    const { duanyu, age } = getFortuneDuanyu(db, String(n));
    const ageHint = age && age !== "未知" ? `（条文年龄提示：${age}）` : "";
    lines.push(`  ${base}+${seq}+${off} = ${n}  —— ${duanyu}${ageHint}`);
  }
  if (!lines.length) lines.push("  （无数值）");
  return `${title}:\n${lines.join("\n")}`;
}

function formatDestiny(db, tblData) {
  if (!tblData) return "未找到匹配的本命条文数据";
  const { base, seq, offsets } = tblData;
  return [
    `基数+序数: ${base}+${seq}=${base + seq}`,
    "",
    formatDestinyNumbers(db, "性格", offsets["性格"], base, seq),
    "",
    formatDestinyNumbers(db, "才能前程", offsets["才能前程"], base, seq),
    "",
    formatDestinyNumbers(db, "财运", offsets["财运"], base, seq),
    "",
    formatDestinyNumbers(db, "兄弟个数", offsets["兄弟个数"], base, seq),
  ].join("\n");
}

function renderLiunian(body, liunian) {
  const rows = (liunian || []).filter((x) => 1 <= x.age && x.age <= 100);
  body.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.age}</td>
      <td>${r.year}</td>
      <td>${r.sound}</td>
      <td>${r.marker}</td>
      <td>${r.letter}</td>
      <td>${r.original_fortune || ""}</td>
      <td>${r.original_duanyu || ""}</td>
      <td>${r.corrected_fortune || ""}</td>
      <td>${r.corrected_duanyu || ""}</td>
    `;
    body.appendChild(tr);
  }
}

const ui = {
  runBtn: document.getElementById("runBtn"),
  statusEl: document.getElementById("status"),
  errorEl: document.getElementById("error"),
  coreEl: document.getElementById("coreResult"),
  destinyEl: document.getElementById("destinyResult"),
  liunianBody: document.getElementById("liunianBody"),
  genderEl: document.getElementById("gender"),
  birthEl: document.getElementById("birthTime"),
  queryEl: document.getElementById("queryTime"),
};

let db = null;

function setLoading(loading, msg = "") {
  ui.runBtn.disabled = loading;
  ui.runBtn.textContent = loading ? "处理中..." : "开始测算";
  ui.statusEl.textContent = msg;
  ui.statusEl.className = "small";
}

async function ensureDbLoaded() {
  if (db) return db;
  setLoading(true, "正在加载本地数据库，请稍候...");
  db = new TiebanData();
  await db.load();
  setLoading(false, "数据库加载完成");
  ui.statusEl.className = "ok small";
  return db;
}

ui.runBtn.addEventListener("click", async () => {
  ui.errorEl.textContent = "";
  try {
    setLoading(true, "准备计算...");
    const loadedDb = await ensureDbLoaded();
    const gender = ui.genderEl.value.trim();
    const birthInfo = convertToBaziInfo(parseInputDatetime(ui.birthEl.value));
    const queryInfo = convertToBaziInfo(parseInputDatetime(ui.queryEl.value));
    const result = calculateAll(loadedDb, gender, birthInfo, queryInfo);

    ui.coreEl.value = [
      result.congCalc,
      `五音命数 = ${result.toneNum}`,
      result.dayLifeCalc,
      result.momentCalc,
      result.mainCalc,
      `后天命数 = ${result.pnNum}`,
      `十二辟卦: ${result.hexName}`,
      "",
      `出生八字: ${birthInfo.bazi.year} ${birthInfo.bazi.month} ${birthInfo.bazi.day} ${birthInfo.bazi.time}`,
      `求测八字: ${queryInfo.bazi.year} ${queryInfo.bazi.month} ${queryInfo.bazi.day} ${queryInfo.bazi.time}`,
    ].join("\n");
    ui.destinyEl.value = formatDestiny(loadedDb, result.tblData);
    renderLiunian(ui.liunianBody, result.liunian);
    ui.statusEl.textContent = "测算完成（纯前端完整版：核心 + 本命 + 流年1-100）";
    ui.statusEl.className = "ok small";
  } catch (e) {
    ui.errorEl.textContent = String(e);
    ui.statusEl.textContent = "";
  } finally {
    setLoading(false, ui.statusEl.textContent);
  }
});

ensureDbLoaded().catch((e) => {
  ui.errorEl.textContent = `初始化失败: ${e}`;
  setLoading(false, "");
});
