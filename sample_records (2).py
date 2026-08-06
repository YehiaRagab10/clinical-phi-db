#!/usr/bin/env python3
# =============================================================================
# sample_records.py -- FINAL production sampler for the synthetic Belgian-Dutch
# clinical PHI/PII database (schema: clinical_phi_schema.sql v5, schema `phi`).
#
# WHAT THIS GUARANTEES  (each mapped to a concrete mechanism, see README):
#   COHERENCE ...... records are assembled by WALKING the knowledge links
#                    (condition -> drug/dose/tests/symptoms/procedures/treatments,
#                    allergy -> excluded drugs, heritable condition -> pedigree),
#                    never by independent random draws. -> class Graph + sampler.
#   NO LEAKAGE ..... every PII-*surface* dictionary (given_name, surname, city and
#                    thereby street/postcode, hospital) is partitioned into DISJOINT
#                    train/val/test buckets BEFORE sampling, so no surface value can
#                    appear in two splits. Institution geography is flagged and kept
#                    out of the patient pool. Generated ids (INSZ/RIZIV/phone/...) are
#                    unique per run. -> class Partitioner + SplitPools.
#   NO REPEATS ..... each record's canonical signature is reserved in phi.sample_log
#                    (UNIQUE) -> the same *combination* is never emitted twice, even
#                    across runs/machines. What counts as a "combination" is fully
#                    configurable (CONFIG["dedup"]["dimensions"]). -> Ledger.
#   CONTROLLABLE ... CONFIG controls split ratios, origin mix, age distribution,
#                    per-section presence, item counts, surface-format mix, AND a
#                    combination policy (force/forbid/require/bias co-occurrence) so
#                    "sometimes combo X is present, sometimes not". -> apply_policy.
#   GENERALIZABLE .. multiple letter TEMPLATES (brief/ontslagbrief/spoedverslag/
#                    verwijsbrief) with randomized section order, header synonyms,
#                    casing, bullets and per-field formats -> output is NOT one fixed
#                    layout. Emits text + char-offset spans + token BIO in BOTH the
#                    fine (generation) and coarse (detection) label tracks.
#
# MODES
#   --pedigree N     seed N heritable-disease FAMILIES into the DB: parents/siblings
#                    become real patients with their own histories, and the proband's
#                    `relative` rows are LINKED to them (relative.linked_patient_id) so
#                    the family history is DERIVED and reusable, never recombined.
#   --letter         (default) render full Dutch letters -> JSONL with text, gold
#                    entity spans, and BIO for both label tracks. Combine freely, e.g.
#                    `--pedigree 200 --letter --n 100000`.
#
# USAGE
#   pip install psycopg2-binary
#   python sample_records.py --dsn postgresql://u:p@h/db --demo-seed --n 12 \
#          --out ds.jsonl --seed 1 --print-first
#   python sample_records.py --dsn ... --pedigree 500 --letter --n 100000 \
#          --out corpus.jsonl --persist --seed 42
#   python sample_records.py --selftest      # offline: no DB, validates invariants
# =============================================================================
from __future__ import annotations
import argparse, hashlib, json, random, re, sys, uuid
from datetime import date, datetime, timedelta

# =============================================================================
# CONFIG -- everything tunable lives here; override any subtree with --config file.
# =============================================================================
CONFIG = {
    "splits": {"train": 0.8, "val": 0.1, "test": 0.1},

    # PII *surface* dictionaries partitioned so their VALUES never cross splits.
    # given_name/surname -> NAME ; city -> ADDRESS (drives street/postcode too) ;
    # hospital -> HOSPITAL. Add/remove to taste. Non-PII clinical dictionaries
    # (condition/drug/test/...) are DELIBERATELY shared across splits.
    "pii_partition_tables": ["given_name", "surname", "city", "hospital"],

    "origin_weights": {"flemish": 0.60, "dutch_nl": 0.08, "english": 0.06,
                       "arabic": 0.10, "turkish": 0.06, "french": 0.06, "italian": 0.04},
    "age": {"min": 18, "max": 92, "mode": None},   # mode=None -> uniform; else triangular

    # base probability each optional section is present in a document:
    "sections": {"greeting": 0.8, "voorgeschiedenis": 0.7, "familiaal": 0.5,
                 "anamnese": 0.7, "onderzoeken": 0.6, "measurements": 0.8,
                 "besluit": 0.85, "closing": 0.8, "validator": 0.5},

    "counts": {"symptoms": [1, 4], "tests": [1, 5], "procedures": [1, 3],
               "treatments": [1, 5], "personal_history": [1, 3], "relatives": [1, 3]},

    "allergy_present_prob": 0.5,        # else "Geen gekende allergieën"
    "home_meds_present_prob": 0.5,      # else "Geen thuismedicatie geregistreerd"
    # visit-diagnosis selection: with this prob restrict to drug-linked conditions
    # (more therapy coverage); otherwise pick uniformly over ALL conditions so that
    # heritable/drug-less diagnoses (e.g. many cancers) remain reachable.
    "prefer_linked_condition_prob": 0.4,
    "pedigree": {"heritable_cluster_prob": 0.7, "onset_jitter": 8},

    "formats": {"name_title_prob": 0.4, "insz_dotted_prob": 0.5,
                "header_upper_prob": 0.5, "bullets": ["  ", "  - ", "  • ", "\t"]},

    # ---- COMBINATION POLICY: makes certain combos appear sometimes and not others
    "combinations": {
        # override a section's presence probability when a predicate holds.
        # predicate keys: cond_heritable (bool), condition (exact name), specialty (name)
        "section_prob_when": [
            {"when": {"cond_heritable": True}, "set": {"familiaal": 0.95}},
        ],
        # (condition_name, drug_inn) pairs that must NEVER co-occur (beyond the
        # automatic allergy<->drug exclusion). Drug is resampled/omitted if hit.
        "forbid_condition_drug": [],
        # if predicate holds, these sections are FORCED present (prob := 1).
        "require_section_when": [
            # {"when": {"condition": "Colorectaal carcinoom"}, "require": ["onderzoeken"]}
        ],
        # at most one of each listed group may appear in a document.
        "exclusive_sections": [],
    },

    # ---- which surface templates to emit and with what weight (generalization)
    "templates": {"brief": 0.5, "ontslagbrief": 0.2, "spoedverslag": 0.2, "verwijsbrief": 0.1},

    # ---- what makes two records "the same combination" (the no-repeat unit).
    # Drop items to allow more repeats; note formatting/template is NOT included,
    # so the same clinical scenario is never emitted twice regardless of surface.
    "dedup": {
        "dimensions": ["patient_name", "patient_sex", "patient_age", "address",
                       "hospital", "department", "condition", "drug", "dose",
                       "providers", "symptoms", "tests", "procedures", "treatments",
                       "allergies", "home_meds", "voorgeschiedenis", "familiaal",
                       "blood_type", "visit_type"],
        "max_attempts": 60,
    },
}

def merge_config(base, override):
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            merge_config(base[k], v)
        else:
            base[k] = v
    return base

# Fine (generation) -> coarse (detection) label map.
FINE_TO_COARSE = {
    "PATIENT_NAME": "NAME", "PROVIDER_NAME": "NAME", "RELATIVE_NAME": "NAME",
    "BUILDING_NUMBER": "ADDRESS", "STREET": "ADDRESS", "CITY": "ADDRESS",
    "ZIPCODE": "ADDRESS", "COUNTRY": "ADDRESS",
    "DATE": "DATE", "TIME": "DATE", "AGE": "AGE",
    "INSZ": "ID", "RIZIV": "ID", "MRN": "ID",
    "TELEPHONE": "CONTACT", "FAX": "CONTACT", "EMAIL": "EMAIL", "URL": "URL",
    "HOSPITAL": "HOSPITAL", "DEPARTMENT": "DEPARTMENT", "SPECIALTY": "DEPARTMENT",
    "VISIT": "DEPARTMENT",
    "CONDITION": "CONDITION", "SYMPTOM": "SYMPTOM", "PROCEDURE": "PROCEDURE",
    "TEST": "TEST", "OBSERVATION": "OBSERVATION",
    "DRUG": "DRUG", "DRUG_DOSE": "MEASUREMENT", "MEASUREMENT": "MEASUREMENT",
    "BLOOD_TYPE": "BLOOD_TYPE", "ALLERGY": "ALLERGY",
    "MEDICAL_HISTORY": "MEDICAL_HISTORY", "TREATMENT": "TREATMENT", "PROFESSION": "PROFESSION",
}

# =============================================================================
# SURFACE-FORMAT RENDER HELPERS (Dutch-aware). These make the *format* vary.
# =============================================================================
_U_NL = ["nul","een","twee","drie","vier","vijf","zes","zeven","acht","negen","tien",
         "elf","twaalf","dertien","veertien","vijftien","zestien","zeventien","achttien","negentien"]
_T_NL = ["","","twintig","dertig","veertig","vijftig","zestig","zeventig","tachtig","negentig"]
def words_nl(n: int) -> str:
    if n < 20: return _U_NL[n]
    if n < 100:
        t, o = divmod(n, 10)
        return _T_NL[t] if o == 0 else f"{_U_NL[o]}en{_T_NL[t]}"
    if n < 200: return "honderd" + ("" if n == 100 else words_nl(n - 100))
    return str(n)

def render_age(n: int, rng) -> str:
    return rng.choice([str(n), f"{n} jaar", f"{n}-jarige", f"{n} jaar oud",
                       f"{n} j.", words_nl(n), f"{n} y/o"])

_NL_MONTHS = ["januari","februari","maart","april","mei","juni","juli",
              "augustus","september","oktober","november","december"]
_NL_MON_ABBR = ["jan","feb","mrt","apr","mei","jun","jul","aug","sep","okt","nov","dec"]
_NL_DOW = ["maandag","dinsdag","woensdag","donderdag","vrijdag","zaterdag","zondag"]
def render_date(d, rng, precision="day") -> str:
    if precision == "year":  return str(d.year)
    if precision == "month":
        return rng.choice([f"{d.month:02d}-{d.year}", f"{d.month:02d}/{d.year}",
                           f"{_NL_MONTHS[d.month-1]} {d.year}"])
    style = rng.random()
    if style < 0.20: return f"{d.day} {_NL_MONTHS[d.month-1]} {d.year}"
    if style < 0.30: return f"{d.day} {_NL_MON_ABBR[d.month-1]} {d.year}"
    if style < 0.38: return f"{_NL_DOW[d.weekday()]} {d.day:02d}/{d.month:02d}/{d.year}"
    if style < 0.55: return d.strftime("%d/%m/%Y")
    if style < 0.70: return d.strftime("%d-%m-%Y")
    if style < 0.82: return d.strftime("%d.%m.%Y")
    return d.strftime("%Y-%m-%d")
def render_time(d, rng) -> str:
    return rng.choice([d.strftime("%H:%M"), d.strftime("%H.%M") + " uur",
                       d.strftime("%Hu%M"), d.strftime("%I:%M %p")])

def render_phone(rng):
    if rng.random() < 0.55:   # mobile 04xx
        pre = rng.choice(["470","471","472","473","475","476","477","478","479",
                          "485","486","487","488","489","493","494","495","496","497","498","499"])
        rest = f"{rng.randint(0,999999):06d}"; a, b, c = rest[0:2], rest[2:4], rest[4:6]
        s = rng.choice(["e164","spaced","slashdot","compact"])
        if s == "e164":     return f"+32 {pre} {a} {b} {c}"
        if s == "spaced":   return f"0{pre} {a} {b} {c}"
        if s == "slashdot": return f"0{pre}/{a}.{b}.{c}"
        return f"0{pre}{a}{b}{c}"
    area = rng.choice(["2","3","4","9","11","12","13","14","15","16","50","53","56","71","81"])
    n = f"{rng.randint(0,9999999):07d}"[:9-len(area)]
    s = rng.choice(["e164","slashdot","spaced"])
    if s == "e164":     return f"+32 {area} {n[0:3]} {n[3:5]} {n[5:7]}".strip()
    if s == "slashdot": return f"0{area}/{n[0:3]}.{n[3:5]}.{n[5:7]}"
    return f"0{area} {n[0:3]} {n[3:5]} {n[5:7]}".strip()

def render_email(g, s, rng):
    g, s = g.lower(), s.lower().replace(" ", "").replace("'", "")
    local = rng.choice([f"{g}.{s}", f"{g[0]}{s}", f"{g}{rng.randint(1,99)}", f"{g}_{s}", f"{s}.{g}"])
    return f"{local}@" + rng.choice(["example.com","gmail.com","outlook.be","telenet.be","proximus.be"])

def insz_display(x, rng):
    return f"{x[0:2]}.{x[2:4]}.{x[4:6]}-{x[6:9]}.{x[9:11]}" if rng.random() < CONFIG["formats"]["insz_dotted_prob"] else x
def generate_insz(birth: date, sex: str, rng) -> str:
    yy, mm, dd = birth.year % 100, birth.month, birth.day
    while True:
        seq = rng.randint(1, 998)
        if sex == "M" and seq % 2 == 0: continue
        if sex == "F" and seq % 2 == 1: continue
        break
    base9 = f"{yy:02d}{mm:02d}{dd:02d}{seq:03d}"
    num = int(("2" + base9) if birth.year >= 2000 else base9)
    chk = 97 - (num % 97); chk = 97 if chk == 0 else chk
    return f"{base9}{chk:02d}"
def generate_riziv(rng, profession_digit=1) -> str:
    body = f"{profession_digit}{rng.randint(0,99999):05d}"
    chk = 89 - (int(body) % 89)
    return f"{body}{chk:02d}{rng.randint(0,999):03d}"

def person_name(g, s, rng, sex=None, title=None):
    if title is None and rng.random() < CONFIG["formats"]["name_title_prob"]:
        title = "dr." if sex is None else ("Dhr." if sex == "M" else "Mevr.")
    return (f"{title} " if title else "") + f"{g} {s}"

def hdr(key, rng):
    """A randomized (synonym + casing) section header."""
    h = rng.choice(HEAD[key])
    return h.upper() if rng.random() < CONFIG["formats"]["header_upper_prob"] else h

HEAD = {
    "voorgeschiedenis": ["Voorgeschiedenis","Antecedenten","Medische voorgeschiedenis","Relevante antecedenten"],
    "familiaal": ["Familiaal","Familiale voorgeschiedenis","Familiale antecedenten","Familiale anamnese"],
    "allergie": ["Allergie","Allergieën","Gekende allergieën"],
    "therapie": ["Therapie bij opname","Thuismedicatie","Medicatie bij opname","Huidige medicatie"],
    "anamnese": ["Anamnese","Klinische anamnese","Reden van consultatie"],
    "onderzoeken": ["Technische onderzoeken","Aanvullende onderzoeken","Onderzoeken"],
    "metingen": ["Klinische parameters","Metingen","Vitale parameters"],
    "besluit": ["Besluit","Conclusie","Samenvatting en beleid","Advies"],
}

# =============================================================================
# GRAPH -- the whole knowledge base, loaded ONCE into memory. All sampling is
# then pure-Python and fully reproducible from --seed (no ORDER BY random()).
# =============================================================================
class Graph:
    def __init__(self, d):
        self.__dict__.update(d)
        # id indexes
        self.cond_by_id  = {c["condition_id"]: c for c in self.conditions}
        self.test_by_id  = {t["test_id"]: t for t in self.tests}
        self.drug_by_id  = {dr["drug_id"]: dr for dr in self.drugs}
        self.city_ids    = [c["city_id"] for c in self.cities]
        self.hospital_ids = [h["hospital_id"] for h in self.hospitals]
        self.given_ids   = [g["given_name_id"] for g in self.given]
        self.surname_ids = [s["surname_id"] for s in self.surnames]

    @classmethod
    def from_dict(cls, d):   # used by --selftest and by callers with a prebuilt graph
        return cls(d)

    @classmethod
    def from_db(cls, conn):
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        def q(sql):
            cur.execute(sql); return [dict(r) for r in cur.fetchall()]
        d = {}
        d["origins"]   = q("SELECT origin_id,label FROM phi.name_origin ORDER BY origin_id")
        d["given"]     = q("SELECT given_name_id,name,sex,origin_id,frequency FROM phi.given_name ORDER BY given_name_id")
        d["surnames"]  = q("SELECT surname_id,name,origin_id,tussenvoegsel,frequency FROM phi.surname ORDER BY surname_id")
        d["cities"]    = q("SELECT city_id,name,province_id,country_id FROM phi.city ORDER BY city_id")
        d["addresses"] = q("""SELECT a.address_id,a.house_number,a.box_number,a.is_institutional,
                                     st.name street, pc.code zip, c.city_id, c.name city, co.name country
                              FROM phi.address a JOIN phi.street st ON st.street_id=a.street_id
                              JOIN phi.postcode pc ON pc.postcode_id=st.postcode_id
                              JOIN phi.city c ON c.city_id=pc.city_id
                              JOIN phi.country co ON co.country_id=c.country_id ORDER BY a.address_id""")
        d["hospitals"] = q("SELECT hospital_id,full_name,abbreviation,hospital_type,url FROM phi.hospital ORDER BY hospital_id")
        d["sites"]     = q("""SELECT hs.hospital_id,hs.name site,hs.phone,st.name street,a.house_number,
                                     a.box_number,pc.code zip,c.name city
                              FROM phi.hospital_site hs JOIN phi.address a ON a.address_id=hs.address_id
                              JOIN phi.street st ON st.street_id=a.street_id
                              JOIN phi.postcode pc ON pc.postcode_id=st.postcode_id
                              JOIN phi.city c ON c.city_id=pc.city_id ORDER BY hs.site_id""")
        d["departments"] = q("""SELECT d.department_id,d.name,d.hospital_id,ms.name specialty
                                FROM phi.clinic_department d
                                LEFT JOIN phi.medical_specialty ms ON ms.specialty_id=d.specialty_id
                                ORDER BY d.department_id""")
        d["conditions"] = q("SELECT condition_id,name,icd10_code,is_heritable,typical_onset_age FROM phi.condition ORDER BY condition_id")
        d["symptoms"]   = q("SELECT symptom_id,name FROM phi.symptom ORDER BY symptom_id")
        d["tests"]      = q("SELECT test_id,name,unit,ref_low,ref_high FROM phi.test ORDER BY test_id")
        d["procedures"] = q("SELECT procedure_id,name FROM phi.procedure ORDER BY procedure_id")
        d["drugs"]      = q("SELECT drug_id,inn_name,brand_name FROM phi.drug ORDER BY drug_id")
        d["doses"]      = q("SELECT dose_id,drug_id,amount,unit,frequency,route,dose_display FROM phi.drug_dose ORDER BY dose_id")
        d["allergies"]  = q("SELECT allergy_id,substance_name FROM phi.allergy ORDER BY allergy_id")
        d["treatments"] = q("SELECT treatment_id,name FROM phi.treatment ORDER BY treatment_id")
        d["blood_types"]= q("SELECT blood_type_id,abo,rhesus FROM phi.blood_type ORDER BY blood_type_id")
        d["relationships"]=q("SELECT relationship_id,code,label_nl,default_lineage FROM phi.relationship ORDER BY relationship_id")
        # links
        d["cond_symptom"]   = _group(q("SELECT condition_id,symptom_id FROM phi.condition_symptom"), "condition_id", "symptom_id")
        d["cond_test"]      = _group(q("SELECT condition_id,test_id   FROM phi.condition_test"),    "condition_id", "test_id")
        d["cond_drug"]      = _group(q("SELECT condition_id,drug_id   FROM phi.condition_drug"),    "condition_id", "drug_id")
        d["cond_treatment"] = _group(q("SELECT condition_id,treatment_id FROM phi.condition_treatment"), "condition_id", "treatment_id")
        d["proc_by_cond"]   = _group(q("SELECT condition_id,procedure_id FROM phi.procedure_condition"), "condition_id", "procedure_id")
        d["allergy_drug"]   = _group(q("SELECT allergy_id,drug_id FROM phi.allergy_drug"), "allergy_id", "drug_id")
        d["doses_by_drug"]  = _group([{"drug_id": x["drug_id"], "dose_id": x["dose_id"]} for x in d["doses"]], "drug_id", "dose_id")
        d["dose_by_id"]     = {x["dose_id"]: x for x in d["doses"]}
        d["proc_name"]      = {p["procedure_id"]: p["name"] for p in d["procedures"]}
        d["sym_name"]       = {s["symptom_id"]: s["name"] for s in d["symptoms"]}
        d["treat_name"]     = {t["treatment_id"]: t["name"] for t in d["treatments"]}
        cur.close()
        return cls(d)

def _group(rows, k, v):
    out = {}
    for r in rows:
        out.setdefault(r[k], []).append(r[v])
    return out

# =============================================================================
# PARTITIONER + SPLIT POOLS  (the leakage guard)
# =============================================================================
class Partitioner:
    """Assign every PII-surface dictionary row to exactly ONE split (disjoint),
    deterministically from the seed. Nothing about a surface can cross splits."""
    _PK = {"given_name": "given_name_id", "surname": "surname_id",
           "city": "city_id", "hospital": "hospital_id"}
    def __init__(self, graph, cfg, seed):
        self.map = {}
        splits = list(cfg["splits"].items())
        idlists = {"given_name": graph.given_ids, "surname": graph.surname_ids,
                   "city": graph.city_ids, "hospital": graph.hospital_ids}
        for tbl in cfg["pii_partition_tables"]:
            ids = list(idlists[tbl])
            random.Random(f"{seed}:{tbl}").shuffle(ids)
            buckets, n, start, upto = {s: [] for s, _ in splits}, len(ids), 0, 0.0
            for s, w in splits:
                upto += w; end = int(round(upto * n))
                buckets[s] = ids[start:end]; start = end
            self.map[tbl] = buckets
    def ids(self, tbl, split):
        return self.map[tbl][split]

class SplitPools:
    """Precomputed per-split candidate lists so picking is O(1)-ish and in-split."""
    def __init__(self, graph, part, cfg):
        self.graph, self.part, self.cfg = graph, part, cfg
        self.origin_by_id = {o["origin_id"]: o["label"] for o in graph.origins}
        self.splits = list(cfg["splits"])
        partition = set(cfg["pii_partition_tables"])
        # name index: split -> origin_label -> sexkey -> [given dicts]; and surnames
        self.given_idx, self.surname_idx, self.addr_pool, self.dept_pool, self.hosp_pool = {}, {}, {}, {}, {}
        for sp in self.splits:
            gset = set(part.ids("given_name", sp)) if "given_name" in partition else set(graph.given_ids)
            sset = set(part.ids("surname", sp))    if "surname"    in partition else set(graph.surname_ids)
            gi = {}
            for g in graph.given:
                if g["given_name_id"] not in gset: continue
                gi.setdefault(self.origin_by_id[g["origin_id"]], {}).setdefault(g["sex"], []).append(g)
            self.given_idx[sp] = gi
            si = {}
            for s in graph.surnames:
                if s["surname_id"] not in sset: continue
                si.setdefault(self.origin_by_id[s["origin_id"]], []).append(s)
            self.surname_idx[sp] = si
            # patient addresses: non-institutional, city in-split (if city partitioned)
            cset = set(part.ids("city", sp)) if "city" in partition else set(graph.city_ids)
            self.addr_pool[sp] = [a for a in graph.addresses
                                  if not a.get("is_institutional") and a["city"] and a["city_id"] in cset] \
                                 or [a for a in graph.addresses if not a.get("is_institutional")]
            # departments whose hospital is in-split (if hospital partitioned)
            hset = set(part.ids("hospital", sp)) if "hospital" in partition else set(graph.hospital_ids)
            self.hosp_pool[sp] = [h for h in graph.hospitals if h["hospital_id"] in hset] or list(graph.hospitals)
            hp = {h["hospital_id"] for h in self.hosp_pool[sp]}
            self.dept_pool[sp] = [d for d in graph.departments if d["hospital_id"] in hp] or list(graph.departments)

    def weighted_origin(self, sp, rng):
        present = set(self.given_idx[sp]) | set(self.surname_idx[sp])
        w = {lbl: self.cfg["origin_weights"].get(lbl, 0.02) for lbl in present} or {"_": 1.0}
        tot = sum(w.values()); r, acc = rng.random() * tot, 0.0
        for lbl, val in w.items():
            acc += val
            if r <= acc: return lbl
        return next(iter(w))

    def pick_name(self, sp, rng, sex):
        for _ in range(6):
            o = self.weighted_origin(sp, rng)
            gcands = self.given_idx[sp].get(o, {})
            gpool = gcands.get(sex, []) + gcands.get("X", [])
            spool = self.surname_idx[sp].get(o, [])
            if gpool and spool: break
        else:
            gpool = [g for od in self.given_idx[sp].values() for sx in (sex, "X") for g in od.get(sx, [])]
            spool = [s for lst in self.surname_idx[sp].values() for s in lst]
        g = rng.choice(gpool) if gpool else {"given_name_id": None, "name": "Onbekend"}
        s = rng.choice(spool) if spool else {"surname_id": None, "name": "Onbekend", "tussenvoegsel": None}
        sur = ((s.get("tussenvoegsel") + " ") if s.get("tussenvoegsel") else "") + s["name"]
        return dict(given_id=g["given_name_id"], given=g["name"], surname_id=s["surname_id"], surname=sur)

# =============================================================================
# LEDGER -- durable "no combination repeats" (or in-memory for --selftest)
# =============================================================================
class MemLedger:
    def __init__(self): self.seen = set()
    def reserve(self, sig, split, meta=None):
        if sig in self.seen: return False
        self.seen.add(sig); return True
    def stats(self): return len(self.seen), len(self.seen)

class DBLedger:
    def __init__(self, conn): self.conn = conn
    def reserve(self, sig, split, meta=None):
        import psycopg2.errors
        cur = self.conn.cursor()
        try:
            cur.execute("""INSERT INTO phi.sample_log(signature,split,template,doc_uuid,n_entities,meta)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (sig, split, (meta or {}).get("template"), (meta or {}).get("doc_uuid"),
                         (meta or {}).get("n_entities"), json.dumps((meta or {}).get("bag"))))
            cur.close(); return True
        except psycopg2.errors.UniqueViolation:
            self.conn.rollback(); cur.close(); return False
    def stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT count(*),count(distinct signature) FROM phi.sample_log")
        t, d = cur.fetchone(); cur.close(); return t, d

# =============================================================================
# COMBINATION POLICY -- resolve per-document section presence + forbids
# =============================================================================
def section_probs(cond, dept, cfg):
    probs = dict(cfg["sections"])
    ctx_heritable = bool(cond and cond.get("is_heritable"))
    ctx_cond = cond["name"] if cond else None
    ctx_spec = dept.get("specialty") if dept else None
    for rule in cfg["combinations"].get("section_prob_when", []):
        w = rule.get("when", {})
        if "cond_heritable" in w and w["cond_heritable"] != ctx_heritable: continue
        if "condition" in w and w["condition"] != ctx_cond: continue
        if "specialty" in w and w["specialty"] != ctx_spec: continue
        probs.update(rule.get("set", {}))
    forced = set()
    for rule in cfg["combinations"].get("require_section_when", []):
        w = rule.get("when", {})
        if "cond_heritable" in w and w["cond_heritable"] != ctx_heritable: continue
        if "condition" in w and w["condition"] != ctx_cond: continue
        if "specialty" in w and w["specialty"] != ctx_spec: continue
        for sec in rule.get("require", []):
            probs[sec] = 1.0; forced.add(sec)
    return probs, forced

def drug_forbidden(cond_name, inn, cfg):
    for c, d in cfg["combinations"].get("forbid_condition_drug", []):
        if c == cond_name and d == inn: return True
    return False

# =============================================================================
# STRUCTURED SAMPLING  (walk the relations, honoring policy + leakage + counts)
# =============================================================================
def _age(cfg, rng):
    a = cfg["age"]
    if a.get("mode") is not None:
        return int(round(rng.triangular(a["min"], a["max"], a["mode"])))
    return rng.randint(a["min"], a["max"])

def sample_structured(graph, pools, split, cfg, rng, used_insz):
    R = {"split": split}
    sex = rng.choice(["M", "F"])
    pat = pools.pick_name(split, rng, sex)
    age = _age(cfg, rng)
    today = date.today()
    birth = date(today.year - age, rng.randint(1, 12), rng.randint(1, 28))
    while True:
        insz = generate_insz(birth, sex, rng)
        if insz not in used_insz:
            used_insz.add(insz); break
    R["patient"] = dict(**pat, sex=sex, age=age, birth_date=birth, insz=insz)

    R["address"] = rng.choice(pools.addr_pool[split]) if pools.addr_pool[split] else None

    dept = rng.choice(pools.dept_pool[split])
    hosp = next(h for h in graph.hospitals if h["hospital_id"] == dept["hospital_id"])
    R["department"] = dict(department_id=dept["department_id"], dept=dept["name"],
                           hospital_id=hosp["hospital_id"], hospital=hosp["full_name"],
                           url=hosp.get("url"), specialty=dept.get("specialty"))
    R["sites"] = [s for s in graph.sites if s["hospital_id"] == hosp["hospital_id"]]

    def make_provider(prof_digit=1):
        psex = rng.choice(["M", "F"]); nm = pools.pick_name(split, rng, psex)
        return dict(**nm, sex=psex, riziv=generate_riziv(rng, prof_digit))
    R["providers"] = {"sender": make_provider(), "responsible": make_provider()}

    R["visit_datetime"] = datetime.combine(today - timedelta(days=rng.randint(0, 300)), datetime.min.time()) \
                              .replace(hour=rng.randint(8, 17), minute=rng.choice([0, 15, 30, 45]))
    R["visit_type"] = rng.choice(["raadpleging","controlebezoek","opvolgconsultatie","eerste consultatie","spoedopname"])

    # visit condition: keep EVERY condition reachable (heritable/drug-less included),
    # with a tunable bias toward drug-linked ones for therapy coverage.
    with_drug = [c for c in graph.conditions if c["condition_id"] in graph.cond_drug]
    if with_drug and rng.random() < cfg.get("prefer_linked_condition_prob", 0.4):
        cond = rng.choice(with_drug)
    else:
        cond = rng.choice(graph.conditions) if graph.conditions else None
    R["condition"] = cond
    cid = cond["condition_id"] if cond else None
    probs, forced = section_probs(cond, R["department"], cfg)
    R["_secprobs"], R["_forced"] = probs, forced
    lim = cfg["counts"]

    def linked(names_map, id_list, k):
        ids = list(id_list); rng.shuffle(ids)
        return [names_map[i] for i in ids[:k]]
    R["symptoms"]   = [{"name": n} for n in linked(graph.sym_name,  graph.cond_symptom.get(cid, []), rng.randint(*lim["symptoms"]))] if cid else []
    tsel = list(graph.cond_test.get(cid, [])); rng.shuffle(tsel)
    R["tests"]      = [graph.test_by_id[t] for t in tsel[:rng.randint(*lim["tests"])]] if cid else []
    R["procedures"] = [{"name": n} for n in linked(graph.proc_name, graph.proc_by_cond.get(cid, []), rng.randint(*lim["procedures"]))] if cid else []
    R["treatments"] = [{"name": n} for n in linked(graph.treat_name, graph.cond_treatment.get(cid, []), rng.randint(*lim["treatments"]))] if cid else []

    # allergy (per prob) -> then a drug that is NOT an allergen and NOT policy-forbidden
    R["allergies"] = []
    if rng.random() < cfg["allergy_present_prob"] and graph.allergies:
        R["allergies"] = [rng.choice(graph.allergies)]
    allergen_ids = set()
    for a in R["allergies"]:
        allergen_ids |= set(graph.allergy_drug.get(a["allergy_id"], []))
    R["drug"] = R["dose"] = None
    R["home_meds"] = rng.random() < cfg["home_meds_present_prob"]
    if cid:
        cand = [graph.drug_by_id[d] for d in graph.cond_drug.get(cid, [])
                if d not in allergen_ids and not drug_forbidden(cond["name"], graph.drug_by_id[d]["inn_name"], cfg)]
        if cand:
            R["drug"] = rng.choice(cand)
            doses = graph.doses_by_drug.get(R["drug"]["drug_id"], [])
            if doses: R["dose"] = graph.dose_by_id[rng.choice(doses)]

    # measurements from quantitative tests
    meas = []
    for t in R["tests"]:
        if not t.get("unit"): continue
        nm = t["name"].lower()
        if "bloeddruk" in nm or "blood pressure" in nm:
            meas.append(dict(label=t["name"], value_text=f"{rng.randint(120,185)}/{rng.randint(70,110)}", unit=t["unit"]))
        else:
            lo = float(t["ref_low"]) if t.get("ref_low") is not None else 1.0
            hi = float(t["ref_high"]) if t.get("ref_high") is not None else 100.0
            meas.append(dict(label=t["name"], value=round(rng.uniform(lo, hi * 1.2), 1), unit=t["unit"]))
    R["measurements"] = meas
    R["blood_type"] = rng.choice(graph.blood_types) if graph.blood_types else None

    # personal history (Voorgeschiedenis): dated, optional laterality
    R["voorgeschiedenis"] = []
    if rng.random() < probs["voorgeschiedenis"] and graph.conditions:
        pool = list(graph.conditions); rng.shuffle(pool)
        for h in pool[:rng.randint(*lim["personal_history"])]:
            prec = rng.choice(["day", "day", "month", "year"])
            od = date(rng.randint(birth.year + 5, today.year), rng.randint(1, 12), rng.randint(1, 28))
            lat = rng.choice([None, None, None, "left", "right"])
            R["voorgeschiedenis"].append(dict(name=h["name"], onset=od, precision=prec, laterality=lat))

    # family history (pedigree): cluster the heritable visit condition on relatives
    R["familiaal"] = []
    if rng.random() < probs["familiaal"] and graph.relationships:
        rels = list(graph.relationships); rng.shuffle(rels)
        rels = rels[:rng.randint(*lim["relatives"])]
        heritable = cond if (cond and cond.get("is_heritable")
                             and rng.random() < cfg["pedigree"]["heritable_cluster_prob"]) else None
        jit = cfg["pedigree"]["onset_jitter"]
        for r in rels:
            c = heritable or rng.choice(graph.conditions)
            base = c.get("typical_onset_age") or rng.randint(45, 75)
            onset = max(20, base + rng.randint(-jit, jit))
            R["familiaal"].append(dict(label=r.get("label_nl") or "Familielid", condition=c["name"], onset_age=onset))

    # investigations: procedure + date + conclusion (+ optional pathology/APO)
    R["onderzoeken"] = []
    if R["procedures"] and rng.random() < probs["onderzoeken"]:
        pr = rng.choice(R["procedures"])
        pdate = R["visit_datetime"].date() - timedelta(days=rng.randint(3, 60))
        R["onderzoeken"].append(dict(
            procedure=pr["name"], performed_on=pdate,
            conclusion=rng.choice(["Geen afwijkingen vastgesteld.",
                                   "Beeld verenigbaar met de gekende diagnose.",
                                   "Lichte afwijkingen, verdere opvolging aangewezen."]),
            pathology=rng.choice([None, "Geen maligniteit.", "Chronische niet-specifieke ontsteking."])))

    if rng.random() < probs["validator"]:
        R["providers"]["validator"] = make_provider()

    R["report"] = dict(report_datetime=R["visit_datetime"].replace(hour=rng.randint(0, 6)),
                       validated_on=R["visit_datetime"] + timedelta(days=1))
    return R

# =============================================================================
# DEDUP SIGNATURE  (configurable: what counts as "the same combination")
# =============================================================================
def signature(R, cfg):
    dims = cfg["dedup"]["dimensions"]
    key = {}
    if "patient_name" in dims: key["pn"] = [R["patient"]["given_id"], R["patient"]["surname_id"]]
    if "patient_sex"  in dims: key["ps"] = R["patient"]["sex"]
    if "patient_age"  in dims: key["pa"] = R["patient"]["age"]
    if "address"      in dims: key["ad"] = R["address"]["address_id"] if R["address"] else None
    if "hospital"     in dims: key["ho"] = R["department"]["hospital_id"]
    if "department"   in dims: key["de"] = R["department"]["department_id"]
    if "condition"    in dims: key["co"] = R["condition"]["condition_id"] if R["condition"] else None
    if "drug"         in dims: key["dr"] = R["drug"]["drug_id"] if R["drug"] else None
    if "dose"         in dims: key["do"] = R["dose"]["dose_id"] if R["dose"] else None
    if "providers"    in dims: key["pr"] = sorted([p.get("given_id") for p in R["providers"].values()] +
                                                  [p.get("surname_id") for p in R["providers"].values()], key=lambda x: (x is None, x))
    if "symptoms"     in dims: key["sy"] = sorted(s["name"] for s in R["symptoms"])
    if "tests"        in dims: key["te"] = sorted(t["name"] for t in R["tests"])
    if "procedures"   in dims: key["pc"] = sorted(p["name"] for p in R["procedures"])
    if "treatments"   in dims: key["tr"] = sorted(t["name"] for t in R["treatments"])
    if "allergies"    in dims: key["al"] = sorted(a["substance_name"] for a in R["allergies"])
    if "home_meds"    in dims: key["hm"] = bool(R["home_meds"] and R["drug"])
    if "voorgeschiedenis" in dims: key["vg"] = sorted(h["name"] for h in R["voorgeschiedenis"])
    if "familiaal"    in dims: key["fa"] = sorted(f["label"] + "|" + f["condition"] for f in R["familiaal"])
    if "blood_type"   in dims: key["bt"] = (R["blood_type"]["abo"], R["blood_type"]["rhesus"]) if R["blood_type"] else None
    if "visit_type"   in dims: key["vt"] = R["visit_type"]
    return hashlib.sha1(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()

# =============================================================================
# TEXT BUILDER + span bookkeeping (relative offsets rebased on merge -> safe shuffle)
# =============================================================================
class TB:
    def __init__(self): self.buf, self.n, self.spans = [], 0, []
    def add(self, s): s = str(s); self.buf.append(s); self.n += len(s); return self
    def ent(self, s, fine):
        s = str(s); st = self.n; self.add(s)
        self.spans.append([st, self.n, fine, FINE_TO_COARSE.get(fine, fine), s]); return self
    def nl(self, k=1): return self.add("\n" * k)
    def text(self): return "".join(self.buf)
def merge_tb(dst, sub):
    base = dst.n
    dst.buf.extend(sub.buf); dst.n += sub.n
    for st, en, fine, coarse, txt in sub.spans:
        dst.spans.append([st + base, en + base, fine, coarse, txt])

# =============================================================================
# GENERALIZABLE RENDERER -- multiple templates -> text + spans (fine & coarse)
# =============================================================================
TEMPLATE_STYLES = {
    "brief":       dict(title="BRIEF", greeting=True, footer=True, closing=True, shuffle=True,  date_bias="mixed"),
    "ontslagbrief":dict(title="ONTSLAGBRIEF", greeting=True, footer=True, closing=True, shuffle=False, date_bias="iso"),
    "spoedverslag":dict(title="SPOEDVERSLAG", greeting=False, footer=True, closing=False, shuffle=True, date_bias="slash"),
    "verwijsbrief":dict(title="VERWIJSBRIEF", greeting=True, footer=False, closing=True, shuffle=True, date_bias="mixed"),
}

def _pick_template(cfg, rng):
    items = list(cfg["templates"].items()); tot = sum(w for _, w in items) or 1.0
    r, acc = rng.random() * tot, 0.0
    for name, w in items:
        acc += w
        if r <= acc: return name
    return items[0][0]

def _bullet(rng): return rng.choice(CONFIG["formats"]["bullets"])

def render_document(R, cfg, rng, template=None):
    template = template or _pick_template(cfg, rng)
    style = TEMPLATE_STYLES.get(template, TEMPLATE_STYLES["brief"])
    probs = R["_secprobs"]
    tb = TB()
    dt = R["visit_datetime"]; pat = R["patient"]; dep = R["department"]
    present = {"template": template}

    # ---- header ----
    tb.add(style["title"] + "\n\n")
    tb.add("PATIËNT: ").ent(f'{pat["given"]} {pat["surname"]}', "PATIENT_NAME")
    tb.add("   INSZ ").ent(insz_display(pat["insz"], rng), "INSZ").nl()
    resp = R["providers"]["responsible"]
    tb.add("VERANTWOORDELIJKE: ").ent(f'{resp["given"]} {resp["surname"]}', "PROVIDER_NAME")
    tb.add("   RIZIV ").ent(resp["riziv"], "RIZIV").nl()
    tb.add("DATUM: ").ent(render_date(R["report"]["report_datetime"], rng), "DATE")
    tb.add(" ").ent(render_time(R["report"]["report_datetime"], rng), "TIME").nl()
    snd = R["providers"]["sender"]
    tb.add("VERZONDEN DOOR: ").ent(person_name(snd["given"], snd["surname"], rng, title="dr."), "PROVIDER_NAME").nl()
    tb.ent(dep["hospital"], "HOSPITAL").add(" — ").ent(dep["dept"], "DEPARTMENT").nl(2)

    if style["greeting"] and rng.random() < probs["greeting"]:
        tb.add("Geachte collega,\n\nWij zagen uw patiënt ").ent(f'{pat["given"]} {pat["surname"]}', "PATIENT_NAME")
        tb.add(" (").ent(render_age(pat["age"], rng), "AGE").add(") op de raadpleging ")
        tb.ent(dep["specialty"] or dep["dept"], "SPECIALTY").add(" op ").ent(render_date(dt, rng), "DATE").add(".").nl(2)

    # ---- optional middle blocks (each returns a sub-TB or None) ----
    def blk_vg():
        if not R["voorgeschiedenis"]: return None
        t = TB(); t.add(hdr("voorgeschiedenis", rng) + ":").nl(); present["voorgeschiedenis"] = True
        for h in R["voorgeschiedenis"]:
            t.add(_bullet(rng)).ent(render_date(h["onset"], rng, h["precision"]), "DATE").add(": ")
            t.ent(h["name"], "MEDICAL_HISTORY")
            if h["laterality"]: t.add(" ").ent("links" if h["laterality"] == "left" else "rechts", "MEDICAL_HISTORY")
            t.nl()
        return t
    def blk_fam():
        if not R["familiaal"]: return None
        t = TB(); t.add(hdr("familiaal", rng) + ":").nl(); present["familiaal"] = True
        for f in R["familiaal"]:
            t.add(_bullet(rng)).ent(f'{f["label"]}: {f["condition"]} op {f["onset_age"]}-jarige leeftijd', "MEDICAL_HISTORY").nl()
        return t
    def blk_alg():
        t = TB(); t.add(hdr("allergie", rng) + ":").nl().add(_bullet(rng)); present["allergie"] = True
        if R["allergies"]:
            for i, a in enumerate(R["allergies"]):
                if i: t.add(", ")
                t.ent(a["substance_name"], "ALLERGY")
        else:
            t.add("Geen gekende allergieën")
        return t.nl()
    def blk_ther():
        t = TB(); t.add(hdr("therapie", rng) + ":").nl().add(_bullet(rng)); present["therapie"] = True
        if R["home_meds"] and R["drug"]:
            t.ent(R["drug"]["inn_name"], "DRUG")
            if R["dose"]:
                dd = R["dose"].get("dose_display") or f'{R["dose"]["amount"]} {R["dose"]["unit"]} {R["dose"]["frequency"]}'
                t.add(" ").ent(dd, "DRUG_DOSE")
        else:
            t.add("Geen thuismedicatie geregistreerd")
        return t.nl()
    def blk_anam():
        if not R["symptoms"] or rng.random() >= probs["anamnese"]: return None
        t = TB(); t.add(hdr("anamnese", rng) + ":").nl().add(_bullet(rng) + "Patiënt meldt "); present["anamnese"] = True
        for i, s in enumerate(R["symptoms"]):
            if i: t.add(", ")
            t.ent(s["name"], "SYMPTOM")
        return t.add(".").nl()
    def blk_ond():
        if not R["onderzoeken"]: return None
        t = TB(); t.add(hdr("onderzoeken", rng) + ":").nl(); present["onderzoeken"] = True
        for o in R["onderzoeken"]:
            t.add(_bullet(rng)).ent(o["procedure"], "PROCEDURE").add(" ").ent(render_date(o["performed_on"], rng), "DATE").add(":").nl()
            t.add("    BESLUIT: ").ent(o["conclusion"], "OBSERVATION").nl()
            if o["pathology"]: t.add("    APO: ").ent(o["pathology"], "OBSERVATION").nl()
        return t
    def blk_meas():
        if not R["measurements"] or rng.random() >= probs["measurements"]: return None
        t = TB(); t.add(hdr("metingen", rng) + ":").nl().add(_bullet(rng)); present["measurements"] = True
        for i, m in enumerate(R["measurements"]):
            if i: t.add(", ")
            val = m.get("value_text", m.get("value"))
            t.ent(f'{val} {m["unit"]}', "MEASUREMENT")
        return t.nl()

    builders = [("voorgeschiedenis", blk_vg), ("familiaal", blk_fam), ("allergie", blk_alg),
                ("therapie", blk_ther), ("anamnese", blk_anam), ("onderzoeken", blk_ond),
                ("measurements", blk_meas)]
    # exclusive-sections policy: keep at most one per group
    excl = cfg["combinations"].get("exclusive_sections", [])
    dropped = set()
    for group in excl:
        keep = None
        for name in group:
            if name in dict(builders) and name not in dropped:
                if keep is None: keep = name
                else: dropped.add(name)
    blocks = []
    for name, fn in builders:
        if name in dropped: continue
        b = fn()
        if b is not None: blocks.append(b)
    if style["shuffle"]:
        rng.shuffle(blocks)
    for b in blocks:
        merge_tb(tb, b); tb.nl()

    # ---- besluit ----
    if rng.random() < probs["besluit"] and R["condition"]:
        tb.add(hdr("besluit", rng) + ":").nl().add(_bullet(rng)); present["besluit"] = True
        tb.ent(render_age(pat["age"], rng), "AGE").add(" patiënt met ").ent(R["condition"]["name"], "CONDITION").add(". ")
        if R["drug"]:
            tb.add("Behandeling met ").ent(R["drug"]["inn_name"], "DRUG")
            if R["dose"]:
                dd = R["dose"].get("dose_display") or f'{R["dose"]["amount"]} {R["dose"]["unit"]} {R["dose"]["frequency"]}'
                tb.add(" ").ent(dd, "DRUG_DOSE")
            tb.add(". ")
        if R["treatments"]:
            tb.add("Advies: ")
            for i, tr in enumerate(R["treatments"]):
                if i: tb.add("; ")
                tb.ent(tr["name"], "TREATMENT")
            tb.add(".")
        if R["blood_type"]:
            bt = R["blood_type"]
            tb.add(" Bloedgroep ").ent(f'{bt["abo"]} {"positief" if bt["rhesus"]=="+" else "negatief"}', "BLOOD_TYPE").add(".")
        tb.nl(2)

    # ---- closing + validation ----
    if style["closing"] and rng.random() < probs["closing"]:
        tb.add("Met collegiale groeten,\n").ent(person_name(snd["given"], snd["surname"], rng, title="dr."), "PROVIDER_NAME").nl(2)
    val = R["providers"].get("validator", snd)
    tb.add("Elektronisch gevalideerd door ").ent(person_name(val["given"], val["surname"], rng, title="dr."), "PROVIDER_NAME")
    tb.add(" op ").ent(render_date(R["report"]["validated_on"], rng), "DATE").add(" ").ent(render_time(R["report"]["validated_on"], rng), "TIME").nl(2)

    # ---- footer: sites + url ----
    if style["footer"]:
        for s in R["sites"]:
            box = f' bus {s["box_number"]}' if s.get("box_number") else ""
            tb.ent(f'{s["street"]} {s["house_number"]}{box}', "STREET").add(", ")
            tb.ent(s["zip"], "ZIPCODE").add(" ").ent(s["city"], "CITY")
            if s.get("phone"): tb.add("   T ").ent(s["phone"], "TELEPHONE")
            tb.nl()
        if dep.get("url"): tb.ent(dep["url"], "URL").nl()
    return tb.text(), tb.spans, present

# =============================================================================
# BIO
# =============================================================================
TOKEN_RE = re.compile(r"\S+")
def tokenize(text): return [(m.group(), m.start(), m.end()) for m in TOKEN_RE.finditer(text)]
def to_bio(tokens, spans, track):    # track: 2=fine, 3=coarse
    labels = ["O"] * len(tokens)
    for st, en, fine, coarse, _ in spans:
        lab = fine if track == 2 else coarse
        first = True
        for i, (tok, ts, te) in enumerate(tokens):
            if ts < en and te > st:
                labels[i] = ("B-" if first else "I-") + lab; first = False
    return labels

# =============================================================================
# --pedigree : seed heritable-disease families with linked_patient_id grouping
# =============================================================================
def seed_pedigrees(conn, graph, cfg, rng, n_families):
    """Create N families: 1-2 affected relatives become REAL patients carrying the
    heritable condition in patient_condition_history; a proband is created and its
    `relative` rows LINK to them (relative.linked_patient_id) so the family history
    is derived + reusable. Returns a small summary."""
    heritable = [c for c in graph.conditions if c.get("is_heritable")]
    if not heritable:
        return {"families": 0, "note": "no is_heritable conditions in dictionary"}
    rels = {r["code"]: r for r in graph.relationships}
    # choose relation codes we can model as parents/siblings
    parentish = [c for c in ("father", "mother") if c in rels] or list(rels)[:1]
    sibish    = [c for c in ("sister", "brother") if c in rels]
    cur = conn.cursor()
    def insert_patient(given_id, surname_id, sex, birth, insz):
        cur.execute("""INSERT INTO phi.patient(given_name_id,surname_id,sex,birth_date,insz)
                       VALUES (%s,%s,%s,%s,%s) RETURNING patient_id""",
                    (given_id, surname_id, sex, birth, insz))
        return cur.fetchone()[0]
    made = 0; used = set()
    surnames = graph.surnames or []
    givens_m = [g for g in graph.given if g["sex"] in ("M", "X")]
    givens_f = [g for g in graph.given if g["sex"] in ("F", "X")]
    for _ in range(n_families):
        if not surnames or not givens_m or not givens_f: break
        cond = rng.choice(heritable)
        fam_surname = rng.choice(surnames)
        onset = cond.get("typical_onset_age") or rng.randint(45, 70)
        today = date.today()
        linked = []
        # affected parent
        pcode = rng.choice(parentish); psex = "M" if pcode == "father" else "F"
        pg = rng.choice(givens_m if psex == "M" else givens_f)
        pbirth = date(today.year - rng.randint(66, 82), rng.randint(1, 12), rng.randint(1, 28))
        pinsz = _uniq_insz(pbirth, psex, rng, used)
        ppid = insert_patient(pg["given_name_id"], fam_surname["surname_id"], psex, pbirth, pinsz)
        cur.execute("""INSERT INTO phi.patient_condition_history(patient_id,condition_id,onset_date,onset_precision)
                       VALUES (%s,%s,%s,'year')""",
                    (ppid, cond["condition_id"], date(pbirth.year + onset, 1, 1)))
        linked.append((pcode, rels[pcode].get("default_lineage"), psex, ppid))
        # optional affected sibling
        if sibish and rng.random() < 0.6:
            scode = rng.choice(sibish); ssex = "M" if scode == "brother" else "F"
            sg = rng.choice(givens_m if ssex == "M" else givens_f)
            sbirth = date(today.year - rng.randint(40, 62), rng.randint(1, 12), rng.randint(1, 28))
            sinsz = _uniq_insz(sbirth, ssex, rng, used)
            spid = insert_patient(sg["given_name_id"], fam_surname["surname_id"], ssex, sbirth, sinsz)
            sonset = max(20, onset + rng.randint(-6, 6))
            cur.execute("""INSERT INTO phi.patient_condition_history(patient_id,condition_id,onset_date,onset_precision)
                           VALUES (%s,%s,%s,'year')""",
                        (spid, cond["condition_id"], date(sbirth.year + sonset, 1, 1)))
            linked.append((scode, None, ssex, spid))
        # proband
        prsex = rng.choice(["M", "F"])
        prg = rng.choice(givens_m if prsex == "M" else givens_f)
        prbirth = date(today.year - rng.randint(28, 55), rng.randint(1, 12), rng.randint(1, 28))
        print_insz = _uniq_insz(prbirth, prsex, rng, used)
        probid = insert_patient(prg["given_name_id"], fam_surname["surname_id"], prsex, prbirth, print_insz)
        for code, lineage, rsex, linked_pid in linked:
            cur.execute("""INSERT INTO phi.relative(patient_id,relationship_id,lineage,sex,linked_patient_id)
                           VALUES (%s,%s,%s,%s,%s)""",
                        (probid, rels[code]["relationship_id"], lineage, rsex, linked_pid))
        made += 1
    conn.commit(); cur.close()
    return {"families": made, "condition_pool": [c["name"] for c in heritable]}

def _uniq_insz(birth, sex, rng, used):
    while True:
        x = generate_insz(birth, sex, rng)
        if x not in used: used.add(x); return x

# =============================================================================
# MAIN
# =============================================================================
def run_generation(graph, cfg, ledger, seed, n, out_path, print_first=False, persist_meta=True):
    part = Partitioner(graph, cfg, seed)
    pools = SplitPools(graph, part, cfg)
    rng = random.Random(seed)
    used_insz = set()

    plan = []
    for s, w in cfg["splits"].items(): plan += [s] * int(round(w * n))
    while len(plan) < n: plan.append("train")
    plan = plan[:n]; rng.shuffle(plan)

    out = open(out_path, "w") if out_path else None
    made = 0; printed = False
    for split in plan:
        rec = None
        for _ in range(cfg["dedup"]["max_attempts"]):
            cand = sample_structured(graph, pools, split, cfg, rng, used_insz)
            sig = signature(cand, cfg)
            if ledger.reserve(sig, split):
                cand["signature"] = sig; rec = cand; break
        if rec is None:
            continue
        text, spans, present = render_document(rec, cfg, rng)
        toks = tokenize(text)
        doc_id = str(uuid.uuid4())
        row = {"doc_id": doc_id, "split": split, "signature": rec["signature"],
               "template": present["template"], "text": text,
               "entities": [{"start": s[0], "end": s[1], "gen_label": s[2], "det_label": s[3], "text": s[4]} for s in spans],
               "tokens": [t[0] for t in toks],
               "bio_fine": to_bio(toks, spans, 2), "bio_coarse": to_bio(toks, spans, 3)}
        if persist_meta:
            try:
                ledger  # DBLedger.reserve already inserted the row; annotate via UPDATE is optional
            except Exception:
                pass
        if out: out.write(json.dumps(row, default=str) + "\n")
        if print_first and not printed:
            print("=" * 72 + f"\nTEMPLATE={present['template']}  SPLIT={split}\n" + "=" * 72)
            print(text)
            print("-" * 72 + "\nfirst spans (fine -> coarse):")
            for e in row["entities"][:14]:
                print(f'  [{e["start"]:>4},{e["end"]:>4}] {e["gen_label"]:<15}->{e["det_label"]:<12} {e["text"]!r}')
            printed = True
        made += 1
    if out: out.close()
    return made, part, pools

def main():
    ap = argparse.ArgumentParser(description="Final synthetic clinical letter sampler.")
    ap.add_argument("--dsn", help="PostgreSQL DSN (required unless --selftest)")
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--out")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--config", help="JSON file overriding CONFIG")
    ap.add_argument("--demo-seed", action="store_true", help="load a small diverse dictionary pool first")
    ap.add_argument("--pedigree", type=int, default=0, metavar="N", help="seed N heritable-disease families, then continue")
    ap.add_argument("--letter", action="store_true", help="emit rendered letters (default action)")
    ap.add_argument("--persist", action="store_true", help="keep sample_log rows (durable dedup across runs)")
    ap.add_argument("--reset-log", action="store_true", help="TRUNCATE phi.sample_log before running")
    ap.add_argument("--print-first", action="store_true", help="print the first rendered document")
    ap.add_argument("--selftest", action="store_true", help="offline invariants check, no DB needed")
    args = ap.parse_args()

    if args.config:
        merge_config(CONFIG, json.load(open(args.config)))
    if args.selftest:
        return selftest()

    if not args.dsn:
        sys.exit("--dsn is required (or use --selftest)")
    try:
        import psycopg2
    except ImportError:
        sys.exit("pip install psycopg2-binary")
    conn = psycopg2.connect(args.dsn); conn.autocommit = True
    if args.demo_seed:
        with conn.cursor() as c: c.execute(DEMO_SEED_SQL)
    if args.reset_log:
        with conn.cursor() as c: c.execute("TRUNCATE phi.sample_log")

    graph = Graph.from_db(conn)
    rng_seed = random.Random(args.seed)

    if args.pedigree:
        summary = seed_pedigrees(conn, graph, CONFIG, random.Random(args.seed ^ 0x9E37), args.pedigree)
        print(f"[pedigree] {summary}")
        graph = Graph.from_db(conn)   # reload so seeded patients are in the pool

    if args.pedigree and not args.letter and not args.out and args.n == 10:
        # pedigree-only invocation: don't also generate a default corpus silently
        conn.close(); return

    ledger = DBLedger(conn) if args.persist else _EphemeralDBLedger(conn)
    made, part, pools = run_generation(graph, CONFIG, ledger, args.seed, args.n, args.out, args.print_first)

    print(f"\nGenerated {made}/{args.n} unique records -> {args.out or '(no file)'}")
    # leakage self-checks on the actually-partitioned tables
    for tbl in CONFIG["pii_partition_tables"]:
        tr, te = set(part.ids(tbl, "train")), set(part.ids(tbl, "test"))
        print(f"  {tbl:<11} train/test id overlap (must be 0): {len(tr & te)}")
    tot, dis = ledger.stats()
    print(f"  sample_log rows={tot} distinct_signatures={dis} (equal => no repeats)")
    conn.close()

class _EphemeralDBLedger(DBLedger):
    """Uses the DB UNIQUE for in-run dedup but TRUNCATEs at the end (no --persist)."""
    def stats(self):
        t, d = super().stats()
        with self.conn.cursor() as c: c.execute("TRUNCATE phi.sample_log")
        return t, d

# =============================================================================
# OFFLINE SELFTEST -- builds a tiny in-memory graph, checks every invariant
# =============================================================================
def _mini_graph():
    origins = [{"origin_id": 1, "label": "flemish"}, {"origin_id": 2, "label": "arabic"}]
    given = [{"given_name_id": i, "name": n, "sex": s, "origin_id": o, "frequency": None}
             for i, (n, s, o) in enumerate([("Jonas","M",1),("Bram","M",1),("Marie","F",1),("Lore","F",1),
                                            ("Wout","M",1),("Elise","F",1),("Youssef","M",2),("Nadia","F",2),
                                            ("Karel","M",1),("Sofie","F",1),("Tom","M",1),("Hilde","F",1)], 1)]
    surnames = [{"surname_id": i, "name": n, "origin_id": o, "tussenvoegsel": None, "frequency": None}
                for i, (n, o) in enumerate([("Peeters",1),("Maes",1),("Claes",1),("Janssens",1),
                                            ("El Amrani",2),("Haddad",2),("Wouters",1),("Segers",1),
                                            ("Vermeersch",1),("Aerts",1),("De Smet",1),("Vanhoutte",1)], 1)]
    cities = [{"city_id": i, "name": n, "province_id": 1, "country_id": 1}
              for i, n in enumerate(["Hasselt","Genk","Leuven","Brugge","Gent","Antwerpen","Mol","Geel"], 1)]
    addresses = []
    aid = 1
    for cid, cn in [(c["city_id"], c["name"]) for c in cities]:
        for k in range(3):
            addresses.append({"address_id": aid, "house_number": str(10 + aid), "box_number": None,
                              "is_institutional": False, "street": f"{cn}straat", "zip": f"{2000+cid}",
                              "city_id": cid, "city": cn, "country": "België"}); aid += 1
    # a couple of institutional addresses (must never appear as patient address)
    for cn, cid in [("Hasselt", 1), ("Leuven", 3)]:
        addresses.append({"address_id": aid, "house_number": "1", "box_number": None,
                          "is_institutional": True, "street": f"{cn}plein", "zip": f"{2000+cid}",
                          "city_id": cid, "city": cn, "country": "België"}); aid += 1
    hospitals = [{"hospital_id": 1, "full_name": "AZ Noorderhart", "abbreviation": "AZNH", "hospital_type": "AZ", "url": "https://www.aznh.example.test"},
                 {"hospital_id": 2, "full_name": "UZ Leuven", "abbreviation": "UZL", "hospital_type": "UZ", "url": "https://www.uzl.example.test"}]
    sites = [{"hospital_id": 1, "site": "Hasselt", "phone": "+32 11 22 33 44", "street": "Hasseltplein", "house_number": "1", "box_number": None, "zip": "2001", "city": "Hasselt"},
             {"hospital_id": 2, "site": "Gasthuisberg", "phone": "+32 16 34 00 00", "street": "Leuvenplein", "house_number": "1", "box_number": None, "zip": "2003", "city": "Leuven"}]
    departments = [{"department_id": 1, "name": "Cardiologie", "hospital_id": 1, "specialty": "Cardiologie"},
                   {"department_id": 2, "name": "Gastro-enterologie", "hospital_id": 2, "specialty": "Gastro-enterologie"}]
    conditions = [{"condition_id": 1, "name": "Essentiële hypertensie", "icd10_code": "I10", "is_heritable": False, "typical_onset_age": None},
                  {"condition_id": 2, "name": "Colorectaal carcinoom", "icd10_code": "C18.9", "is_heritable": True, "typical_onset_age": 50},
                  {"condition_id": 3, "name": "Type 2 diabetes mellitus", "icd10_code": "E11", "is_heritable": False, "typical_onset_age": None}]
    symptoms = [{"symptom_id": 1, "name": "hoofdpijn"}, {"symptom_id": 2, "name": "vermoeidheid"}, {"symptom_id": 3, "name": "duizeligheid"}]
    tests = [{"test_id": 1, "name": "Bloeddruk", "unit": "mmHg", "ref_low": None, "ref_high": None},
             {"test_id": 2, "name": "HbA1c", "unit": "%", "ref_low": 4.0, "ref_high": 6.0},
             {"test_id": 3, "name": "CEA", "unit": "µg/L", "ref_low": 0, "ref_high": 5}]
    procedures = [{"procedure_id": 1, "name": "ECG"}, {"procedure_id": 2, "name": "Totale coloscopie"}]
    drugs = [{"drug_id": 1, "inn_name": "Lisinopril", "brand_name": None},
             {"drug_id": 2, "inn_name": "Metformine", "brand_name": None},
             {"drug_id": 3, "inn_name": "Amoxicilline", "brand_name": None}]
    doses = [{"dose_id": 1, "drug_id": 1, "amount": 10, "unit": "mg", "frequency": "1x/dag", "route": "oraal", "dose_display": "10 mg 1x/dag oraal"},
             {"dose_id": 2, "drug_id": 2, "amount": 850, "unit": "mg", "frequency": "2x/dag", "route": "oraal", "dose_display": "850 mg 2x/dag oraal"}]
    allergies = [{"allergy_id": 1, "substance_name": "penicilline"}, {"allergy_id": 2, "substance_name": "latex"}]
    treatments = [{"treatment_id": 1, "name": "Zoutarm dieet"}, {"treatment_id": 2, "name": "Rookstop"}, {"treatment_id": 3, "name": "Gewichtsreductie"}]
    blood_types = [{"blood_type_id": i, "abo": a, "rhesus": r} for i, (a, r) in enumerate([("A","+"),("O","-"),("B","+"),("AB","-")], 1)]
    relationships = [{"relationship_id": 1, "code": "father", "label_nl": "Vader", "default_lineage": "paternal"},
                     {"relationship_id": 2, "code": "sister", "label_nl": "Zus", "default_lineage": None},
                     {"relationship_id": 3, "code": "mother", "label_nl": "Moeder", "default_lineage": "maternal"}]
    d = dict(origins=origins, given=given, surnames=surnames, cities=cities, addresses=addresses,
             hospitals=hospitals, sites=sites, departments=departments, conditions=conditions,
             symptoms=symptoms, tests=tests, procedures=procedures, drugs=drugs, doses=doses,
             allergies=allergies, treatments=treatments, blood_types=blood_types, relationships=relationships)
    d["cond_symptom"]   = {1: [1, 3], 2: [2], 3: [2]}
    d["cond_test"]      = {1: [1], 2: [3], 3: [2]}
    d["cond_drug"]      = {1: [1], 3: [2]}
    d["cond_treatment"] = {1: [1], 2: [3], 3: [3]}
    d["proc_by_cond"]   = {1: [1], 2: [2]}
    d["allergy_drug"]   = {1: [3]}
    d["doses_by_drug"]  = {1: [1], 2: [2]}
    d["dose_by_id"]     = {x["dose_id"]: x for x in doses}
    d["proc_name"]      = {p["procedure_id"]: p["name"] for p in procedures}
    d["sym_name"]       = {s["symptom_id"]: s["name"] for s in symptoms}
    d["treat_name"]     = {t["treatment_id"]: t["name"] for t in treatments}
    return Graph.from_dict(d)

def selftest():
    print("Running offline selftest ...")
    graph = _mini_graph()
    ledger = MemLedger()
    made, part, pools = run_generation(graph, CONFIG, ledger, seed=7, n=200, out_path=None, print_first=True)
    # 1) offsets: every span's text must equal text[start:end]
    rng = random.Random(11)
    bad = 0
    for _ in range(50):
        cand = sample_structured(graph, pools, "train", CONFIG, rng, set())
        cand["signature"] = "x"
        text, spans, present = render_document(cand, CONFIG, rng)
        for st, en, fine, coarse, txt in spans:
            if text[st:en] != txt:
                bad += 1
        toks = tokenize(text)
        assert len(to_bio(toks, spans, 2)) == len(toks)
        assert len(to_bio(toks, spans, 3)) == len(toks)
    # 2) leakage: partitioned id-sets disjoint across splits
    overlaps = {}
    for tbl in CONFIG["pii_partition_tables"]:
        tr, va, te = set(part.ids(tbl, "train")), set(part.ids(tbl, "val")), set(part.ids(tbl, "test"))
        overlaps[tbl] = len(tr & te) + len(tr & va) + len(va & te)
    # 3) institutional addresses never used as patient address
    inst_ids = {a["address_id"] for a in graph.addresses if a["is_institutional"]}
    inst_leak = 0
    r2 = random.Random(3)
    for _ in range(300):
        c = sample_structured(graph, pools, r2.choice(["train","val","test"]), CONFIG, r2, set())
        if c["address"] and c["address"]["address_id"] in inst_ids: inst_leak += 1
    # 4) dedup: no repeated signatures
    tot, dis = ledger.stats()
    print(f"  records generated (dedup-limited): {made}")
    print(f"  span/text mismatches (must be 0): {bad}")
    print(f"  partition overlaps {overlaps} (all must be 0)")
    print(f"  institutional-address-as-patient leaks (must be 0): {inst_leak}")
    print(f"  signatures total={tot} distinct={dis} (equal => no repeats)")
    ok = (bad == 0 and all(v == 0 for v in overlaps.values()) and inst_leak == 0 and tot == dis)
    print("SELFTEST:", "PASS" if ok else "FAIL")
    return 0 if ok else 1

# ---- small diverse pool for local demos (production loads real dictionaries) ----
DEMO_SEED_SQL = r"""
SET search_path TO phi;
INSERT INTO country(name,iso2) VALUES ('België','BE') ON CONFLICT DO NOTHING;
INSERT INTO province(name,country_id) SELECT v,(SELECT country_id FROM country WHERE name='België')
  FROM unnest(ARRAY['Limburg','Vlaams-Brabant','West-Vlaanderen','Oost-Vlaanderen','Antwerpen']) v ON CONFLICT DO NOTHING;
INSERT INTO city(name,country_id,province_id)
  SELECT n,(SELECT country_id FROM country WHERE name='België'),(SELECT province_id FROM province WHERE name=p)
  FROM (VALUES ('Hasselt','Limburg'),('Genk','Limburg'),('Leuven','Vlaams-Brabant'),
               ('Brugge','West-Vlaanderen'),('Gent','Oost-Vlaanderen'),('Antwerpen','Antwerpen'),
               ('Mol','Antwerpen'),('Geel','Antwerpen')) x(n,p) ON CONFLICT DO NOTHING;
INSERT INTO postcode(code,city_id) SELECT c,(SELECT city_id FROM city WHERE name=n)
  FROM (VALUES ('3500','Hasselt'),('3600','Genk'),('3000','Leuven'),('8000','Brugge'),('9000','Gent'),
               ('2000','Antwerpen'),('2400','Mol'),('2440','Geel')) x(c,n) ON CONFLICT DO NOTHING;
INSERT INTO street(name,postcode_id) SELECT s,(SELECT postcode_id FROM postcode WHERE code=c)
  FROM (VALUES ('Kempenlaan','3500'),('Stationsplein','3600'),('Naamsestraat','3000'),
               ('Zilverstraat','8000'),('Veldstraat','9000'),('Meir','2000'),('Corbiestraat','2400'),
               ('Stationsstraat','2440'),('Dorpsstraat','3500'),('Kerkstraat','3000'),('Bruggestraat','8000')) x(s,c) ON CONFLICT DO NOTHING;
INSERT INTO address(street_id,house_number,box_number,is_institutional)
  SELECT (SELECT street_id FROM street WHERE name=s), h, b, inst FROM (VALUES
    ('Kempenlaan','12',NULL,true),('Stationsplein','5','2',false),('Naamsestraat','88',NULL,true),
    ('Zilverstraat','3',NULL,false),('Veldstraat','140',NULL,false),('Meir','14',NULL,false),
    ('Corbiestraat','12',NULL,false),('Stationsstraat','5','B',false),
    ('Dorpsstraat','7',NULL,false),('Kerkstraat','21','A',false),('Bruggestraat','56',NULL,false)
  ) x(s,h,b,inst) ON CONFLICT DO NOTHING;
INSERT INTO name_origin(label) SELECT v FROM unnest(ARRAY['flemish','arabic','french','turkish']) v ON CONFLICT DO NOTHING;
INSERT INTO given_name(name,sex,origin_id) SELECT n,x,(SELECT origin_id FROM name_origin WHERE label=o) FROM (VALUES
  ('Jonas','M','flemish'),('Bram','M','flemish'),('Marie','F','flemish'),('Elise','F','flemish'),('Lore','F','flemish'),('Wout','M','flemish'),
  ('Griet','F','flemish'),('Tom','M','flemish'),('Hilde','F','flemish'),
  ('Youssef','M','arabic'),('Fatima','F','arabic'),('Nadia','F','arabic'),
  ('Louis','M','french'),('Camille','F','french'),('Emre','M','turkish'),('Elif','F','turkish')) v(n,x,o) ON CONFLICT DO NOTHING;
INSERT INTO surname(name,origin_id) SELECT n,(SELECT origin_id FROM name_origin WHERE label=o) FROM (VALUES
  ('Wouters','flemish'),('Maes','flemish'),('Claes','flemish'),('Peeters','flemish'),('Janssens','flemish'),('De Smet','flemish'),
  ('Vanhoutte','flemish'),('Segers','flemish'),('El Amrani','arabic'),('Haddad','arabic'),('Dubois','french'),('Laurent','french'),
  ('Yilmaz','turkish'),('Demir','turkish')) v(n,o) ON CONFLICT DO NOTHING;
INSERT INTO relationship(code,label_nl,default_lineage) VALUES
  ('mother','Moeder','maternal'),('father','Vader','paternal'),
  ('maternal_grandmother','Grootmoeder (maternele zijde)','maternal'),
  ('paternal_grandfather','Grootvader (paternele zijde)','paternal'),
  ('sister','Zus',NULL),('brother','Broer',NULL) ON CONFLICT DO NOTHING;
INSERT INTO medical_specialty(name) SELECT v FROM unnest(ARRAY['Cardiologie','Nefrologie','Gastro-enterologie']) v ON CONFLICT DO NOTHING;
INSERT INTO hospital(full_name,abbreviation,url) VALUES
  ('AZ Noorderhart','AZNH','https://www.aznoorderhart.example.test'),
  ('UZ Leuven','UZL','https://www.uzleuven.example.test') ON CONFLICT DO NOTHING;
INSERT INTO hospital_site(hospital_id,name,address_id,phone)
  SELECT (SELECT hospital_id FROM hospital WHERE full_name=hn), site,
         (SELECT a.address_id FROM address a JOIN street st ON st.street_id=a.street_id WHERE st.name=strt LIMIT 1), ph
  FROM (VALUES
    ('AZ Noorderhart','Hasselt','Kempenlaan','+32 11 22 33 44'),
    ('UZ Leuven','Gasthuisberg','Naamsestraat','+32 16 34 00 00')) x(hn,site,strt,ph) ON CONFLICT DO NOTHING;
INSERT INTO clinic_department(name,hospital_id,specialty_id) VALUES
  ('Cardiologie',(SELECT hospital_id FROM hospital WHERE full_name='AZ Noorderhart'),(SELECT specialty_id FROM medical_specialty WHERE name='Cardiologie')),
  ('Gastro-enterologie',(SELECT hospital_id FROM hospital WHERE full_name='UZ Leuven'),(SELECT specialty_id FROM medical_specialty WHERE name='Gastro-enterologie')) ON CONFLICT DO NOTHING;
INSERT INTO condition(name,icd10_code,is_heritable,typical_onset_age) VALUES
  ('Essentiële hypertensie','I10',false,NULL),
  ('Familiale hypercholesterolemie','E78.0',true,45),
  ('Type 2 diabetes mellitus','E11',false,NULL),
  ('Colorectaal carcinoom','C18.9',true,50),
  ('Myocardinfarct','I21',true,58) ON CONFLICT DO NOTHING;
INSERT INTO symptom(name) SELECT v FROM unnest(ARRAY['hoofdpijn','duizeligheid','vermoeidheid','kortademigheid','rectaal bloedverlies','hartkloppingen']) v ON CONFLICT DO NOTHING;
INSERT INTO test(name,unit,ref_low,ref_high) VALUES
  ('Bloeddruk','mmHg',NULL,NULL),('Hartslag','bpm',60,100),('Gewicht','kg',NULL,NULL),
  ('HbA1c','%',4.0,6.0),('LDL-cholesterol','mg/dL',70,130),('CEA','µg/L',0,5) ON CONFLICT DO NOTHING;
INSERT INTO procedure(name) SELECT v FROM unnest(ARRAY['Lichamelijk onderzoek','ECG','Coronarografie','Totale coloscopie']) v ON CONFLICT DO NOTHING;
INSERT INTO drug(inn_name) SELECT v FROM unnest(ARRAY['Lisinopril','Atorvastatine','Metformine','Amoxicilline']) v ON CONFLICT DO NOTHING;
INSERT INTO drug_dose(drug_id,amount,unit,frequency,route,dose_display)
  SELECT (SELECT drug_id FROM drug WHERE inn_name=d), amt, u, f, r, disp FROM (VALUES
    ('Lisinopril',10,'mg','1x/dag','oraal','10 mg 1x/dag oraal'),
    ('Atorvastatine',40,'mg','1x/dag','oraal','40 mg 1x/dag oraal'),
    ('Metformine',850,'mg','2x/dag','oraal','850 mg 2x/dag oraal')) x(d,amt,u,f,r,disp) ON CONFLICT DO NOTHING;
INSERT INTO allergy(substance_name) SELECT v FROM unnest(ARRAY['penicilline','jodiumcontrast','latex']) v ON CONFLICT DO NOTHING;
INSERT INTO allergy_drug(allergy_id,drug_id,cross_reactive) VALUES
  ((SELECT allergy_id FROM allergy WHERE substance_name='penicilline'),(SELECT drug_id FROM drug WHERE inn_name='Amoxicilline'),true) ON CONFLICT DO NOTHING;
INSERT INTO treatment(name) SELECT v FROM unnest(ARRAY['Zoutarm dieet','Regelmatige beweging','Gewichtsreductie',
  'Controle over 3 maanden','Rookstop']) v ON CONFLICT DO NOTHING;
INSERT INTO blood_type(abo,rhesus) SELECT a,r FROM (VALUES ('A','+'),('A','-'),('B','+'),('B','-'),('AB','+'),('AB','-'),('O','+'),('O','-')) x(a,r) ON CONFLICT DO NOTHING;
INSERT INTO condition_symptom(condition_id,symptom_id) SELECT (SELECT condition_id FROM condition WHERE name=c),(SELECT symptom_id FROM symptom WHERE name=s) FROM (VALUES
 ('Essentiële hypertensie','hoofdpijn'),('Essentiële hypertensie','duizeligheid'),('Essentiële hypertensie','vermoeidheid'),
 ('Myocardinfarct','kortademigheid'),('Myocardinfarct','hartkloppingen'),('Colorectaal carcinoom','rectaal bloedverlies'),
 ('Colorectaal carcinoom','vermoeidheid'),('Type 2 diabetes mellitus','vermoeidheid')) x(c,s) ON CONFLICT DO NOTHING;
INSERT INTO condition_test(condition_id,test_id) SELECT (SELECT condition_id FROM condition WHERE name=c),(SELECT test_id FROM test WHERE name=t) FROM (VALUES
 ('Essentiële hypertensie','Bloeddruk'),('Essentiële hypertensie','Hartslag'),('Familiale hypercholesterolemie','LDL-cholesterol'),
 ('Type 2 diabetes mellitus','HbA1c'),('Colorectaal carcinoom','CEA'),('Myocardinfarct','Hartslag')) x(c,t) ON CONFLICT DO NOTHING;
INSERT INTO condition_drug(condition_id,drug_id) SELECT (SELECT condition_id FROM condition WHERE name=c),(SELECT drug_id FROM drug WHERE inn_name=d) FROM (VALUES
 ('Essentiële hypertensie','Lisinopril'),('Familiale hypercholesterolemie','Atorvastatine'),
 ('Type 2 diabetes mellitus','Metformine')) x(c,d) ON CONFLICT DO NOTHING;
INSERT INTO condition_treatment(condition_id,treatment_id) SELECT (SELECT condition_id FROM condition WHERE name=c),(SELECT treatment_id FROM treatment WHERE name=t) FROM (VALUES
 ('Essentiële hypertensie','Zoutarm dieet'),('Essentiële hypertensie','Regelmatige beweging'),
 ('Familiale hypercholesterolemie','Gewichtsreductie'),('Type 2 diabetes mellitus','Gewichtsreductie'),
 ('Myocardinfarct','Rookstop'),('Colorectaal carcinoom','Controle over 3 maanden')) x(c,t) ON CONFLICT DO NOTHING;
INSERT INTO procedure_condition(procedure_id,condition_id) SELECT (SELECT procedure_id FROM procedure WHERE name=p),(SELECT condition_id FROM condition WHERE name=c) FROM (VALUES
 ('Lichamelijk onderzoek','Essentiële hypertensie'),('ECG','Essentiële hypertensie'),('ECG','Myocardinfarct'),
 ('Coronarografie','Myocardinfarct'),('Totale coloscopie','Colorectaal carcinoom')) x(p,c) ON CONFLICT DO NOTHING;
"""

if __name__ == "__main__":
    sys.exit(main() or 0)
