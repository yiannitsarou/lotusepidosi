
# -*- coding: utf-8 -*-
"""
bhma7_v3_tiered_v5.py
---------------------------------
ΒΗΜΑ 7 — Ισορροπία ΕΠΙΔΟΣΗΣ με 4 TIERs (ΟΛΑ χωρίς cross-category)
ΝΕΟ: Προαιρετικό "buffer phase" *μετά* την εξάντληση των καθαρών 1↔3:
  • αν «πονάει» το #1 ⇒ προσπάθησε 1↔2 (ίδιο φύλο + ίδια γλώσσα)
  • αν «πονάει» το #3 ⇒ προσπάθησε 3↔2 (ίδιο φύλο + ίδια γλώσσα)
Acceptance gates buffer:
  - Μείωση στο στοχευμένο spread ≥ 1 (t_s1 < s1 ή t_s3 < s3 κατά περίπτωση)
  - Το άλλο metric δεν υπερβαίνει τον στόχο του τρέχοντος TIER
  - Πληθυσμός ≤25 και diff≤2
  - Gender/lang spreads ≤ caps του TIER
  - ΤΗΡΟΥΝΤΑΙ τα freezes: (Βήμα1–3, flagged, φίλοι flagged), ΔΥΑΔΕΣ ΔΕΝ ΣΠΑΝΕ
  - Όριο buffer swaps ανά TIER: MAX_BUFFER_SWAPS (π.χ. 8–10) + early stop αν δεν βελτιωθεί το max spread σε 5 συνεχόμενες δοκιμές

Tiers (χωρίς cross-category παντού):
  T1: ≤40 swaps, στόχος spread≤2, gender_cap=2, lang_cap=3
  T2: ≤40 swaps, στόχος spread≤3, gender_cap=2, lang_cap=3
  T3: ≤40 swaps, στόχος spread≤3, gender_cap=3, lang_cap=3
  T4: ≤20 swaps, στόχος spread≤3, gender_cap=4, lang_cap=4

Κύρια locks (ΑΠΟΛΥΤΩΣ αμετακίνητοι):
  • Τοποθετημένοι μέχρι ΒΗΜΑ 3
  • ΠΑΙΔΙ_ΕΚΠΑΙΔΕΥΤΙΚΟΥ='ΝΑΙ' ή ΙΔΙΑΙΤΕΡΟΤΗΤΑ='ΝΑΙ' ή ΖΩΗΡΟΣ='Ν'
  • Οι φίλοι τους (μέλη ΠΛΗΡΩΣ ΑΜΟΙΒΑΙΑΣ ΔΥΑΔΑΣ από ΒΗΜΑ 3)
"""

import re
import random
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Set, Optional

import pandas as pd
import numpy as np

# ---------------------------- CONFIG ---------------------------------

PATH_STEP6 = "STEP6_PER_SCENARIO_OUTPUT_FIXED.xlsx"
PATH_STEP3 = "STEP3_SCENARIOS.xlsx"
PATH_ROSTER = "Παραδειγμα1.xlsx"
OUTPUT_XLSX = "STEP7_TIERED.xlsx"

SCENARIOS = ["ΣΕΝΑΡΙΟ_1", "ΣΕΝΑΡΙΟ_2", "ΣΕΝΑΡΙΟ_3", "ΣΕΝΑΡΙΟ_4", "ΣΕΝΑΡΙΟ_5"]

COL_UID = "AM"
COL_NAME = "ΟΝΟΜΑ"
COL_GENDER = "ΦΥΛΟ"                      # 'Α'/'Κ'
COL_LANG = "ΚΑΛΗ_ΓΝΩΣΗ_ΕΛΛΗΝΙΚΩΝ"        # 'Ν'/'Ο'
COL_ZOIRO = "ΖΩΗΡΟΣ"                     # 'Ν'/'Ο'
COL_TEACHERKID = "ΠΑΙΔΙ_ΕΚΠΑΙΔΕΥΤΙΚΟΥ"   # 'ΝΑΙ'/'ΟΧΙ'
COL_SPECIAL = "ΙΔΙΑΙΤΕΡΟΤΗΤΑ"            # 'ΝΑΙ'/'ΟΧΙ'
COL_PERF = "ΕΠΙΔΟΣΗ"                     # '1'/'2'/'3'
COL_CLASS = "ΤΜΗΜΑ"

STEP_COL_REGEX = re.compile(r"^(ΒΗΜΑ[123]|STEP[123])", re.IGNORECASE)

MAX_CLASS_SIZE = 25
MAX_DIFF_CLASS_SIZE = 2

TIERS = [
    dict(name="T1", max_swaps=40, target_spread=2, gender_cap=2, lang_cap=3, allow_cross=False),
    dict(name="T2", max_swaps=40, target_spread=3, gender_cap=2, lang_cap=3, allow_cross=False),
    dict(name="T3", max_swaps=40, target_spread=3, gender_cap=3, lang_cap=3, allow_cross=False),
    dict(name="T4", max_swaps=20, target_spread=3, gender_cap=4, lang_cap=4, allow_cross=False),
]

# Buffer phase limits
MAX_BUFFER_SWAPS = 10
EARLY_STOP_NOIMPROVE = 5

RANDOM_SEED = 1337
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ---------------------------- HELPERS --------------------------------

def normalize_yesno(val: str) -> str:
    if val is None:
        return "ΟΧΙ"
    v = str(val).strip().upper()
    if v in {"Ν", "ΝΑΙ", "YES", "Y", "TRUE", "1"}:
        return "ΝΑΙ"
    return "ΟΧΙ"

def normalize_zoir(val: str) -> str:
    if val is None:
        return "Ο"
    v = str(val).strip().upper()
    yes = {"Ν", "N", "ΝΑΙ", "YES", "Y", "TRUE", "1", "Ν1", "Ν2", "Ν3", "N1", "N2", "N3"}
    no  = {"Ο", "O", "ΟΧΙ", "OXI", "NO", "FALSE", "0", ""}
    return "Ν" if v in yes else ("Ο" if v in no else "Ο")

def normalize_lang(val: str) -> str:
    if val is None:
        return "Ο"
    v = str(val).strip().upper()
    return "Ν" if v in {"Ν", "N", "YES", "Y", "TRUE", "1"} else "Ο"

def normalize_gender(val: str) -> str:
    if val is None:
        return "Α"
    v = str(val).strip().upper()
    if v.startswith("Α"):
        return "Α"
    if v.startswith("Κ"):
        return "Κ"
    if v in {"M", "MALE", "BOY"}:
        return "Α"
    if v in {"F", "FEMALE", "GIRL"}:
        return "Κ"
    return "Α"

def normalize_perf(val: str) -> str:
    v = str(val).strip()
    return v if v in {"1", "2", "3"} else "2"

def placed_until_step3_ids(df: pd.DataFrame) -> Set[str]:
    cols = [c for c in df.columns if STEP_COL_REGEX.match(str(c))]
    if not cols:
        return set()
    mask = df[cols].notna().any(axis=1)
    uid_col = COL_UID if COL_UID in df.columns else (COL_NAME if COL_NAME in df.columns else df.columns[0])
    return set(df.loc[mask, uid_col].astype(str))

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if COL_UID not in out.columns and COL_NAME in out.columns:
        out[COL_UID] = out[COL_NAME]
    if COL_TEACHERKID in out.columns:
        out[COL_TEACHERKID] = out[COL_TEACHERKID].map(normalize_yesno)
    else:
        out[COL_TEACHERKID] = "ΟΧΙ"
    if COL_SPECIAL in out.columns:
        out[COL_SPECIAL] = out[COL_SPECIAL].map(normalize_yesno)
    else:
        out[COL_SPECIAL] = "ΟΧΙ"
    if COL_ZOIRO in out.columns:
        out[COL_ZOIRO] = out[COL_ZOIRO].map(normalize_zoir)
    else:
        out[COL_ZOIRO] = "Ο"
    if COL_LANG in out.columns:
        out[COL_LANG] = out[COL_LANG].map(normalize_lang)
    else:
        out[COL_LANG] = "Ο"
    if COL_GENDER in out.columns:
        out[COL_GENDER] = out[COL_GENDER].map(normalize_gender)
    else:
        out[COL_GENDER] = "Α"
    if COL_PERF in out.columns:
        out[COL_PERF] = out[COL_PERF].map(normalize_perf)
    else:
        out[COL_PERF] = "2"
    return out

def class_counts(df: pd.DataFrame, class_col: str) -> Dict[str, Dict[str, int]]:
    res = {}
    for c, g in df.groupby(class_col):
        res[c] = dict(
            total=len(g),
            boys=(g[COL_GENDER] == "Α").sum(),
            girls=(g[COL_GENDER] == "Κ").sum(),
            langN=(g[COL_LANG] == "Ν").sum(),
            langO=(g[COL_LANG] == "Ο").sum(),
            perf1=(g[COL_PERF] == "1").sum(),
            perf2=(g[COL_PERF] == "2").sum(),
            perf3=(g[COL_PERF] == "3").sum(),
        )
    return res

def spread(values: List[int]) -> int:
    return int(max(values) - min(values)) if values else 0

def spreads_perf(counts: Dict[str, Dict[str, int]]) -> Tuple[int, int]:
    s1 = spread([v["perf1"] for v in counts.values()])
    s3 = spread([v["perf3"] for v in counts.values()])
    return s1, s3

def population_caps_ok(counts: Dict[str, Dict[str, int]]) -> bool:
    sizes = [v["total"] for v in counts.values()]
    if any(s > MAX_CLASS_SIZE for s in sizes):
        return False
    if (max(sizes) - min(sizes)) > MAX_DIFF_CLASS_SIZE:
        return False
    return True

def cap_violation_after_swap(counts: Dict[str, Dict[str, int]], gender_cap: int, lang_cap: int) -> bool:
    g_spread = spread([v["boys"] for v in counts.values()])  # ή girls
    l_spread = spread([v["langN"] for v in counts.values()])
    return (g_spread > gender_cap) or (l_spread > lang_cap)

@dataclass
class TierResult:
    df: pd.DataFrame
    class_col: str
    counts: Dict[str, Dict[str, int]]
    spread1: int
    spread3: int
    swaps_dyads: int = 0
    swaps_singles: int = 0
    swaps_buffer: int = 0
    meta: Dict = field(default_factory=dict)

# ---------------------- DYADS (STEP3) UTILS ---------------------------

def load_mutual_dyads_from_step3(path_step3: str) -> List[Tuple[str, str]]:
    try:
        xl = pd.ExcelFile(path_step3)
    except Exception:
        return []
    candidate_sheets = [s for s in xl.sheet_names if "DYAD" in s.upper() or "ΔΥΑΔ" in s.upper() or "FRIENDS" in s.upper()]
    if not candidate_sheets:
        candidate_sheets = xl.sheet_names[:1]
    dyads = []
    for sh in candidate_sheets:
        df = xl.parse(sh)
        if df.shape[1] < 2:
            continue
        a = str(df.columns[0]); b = str(df.columns[1])
        dyads.extend([(str(x), str(y)) for x, y in zip(df[a].astype(str), df[b].astype(str)) if pd.notna(x) and pd.notna(y)])
    return list({tuple(sorted(t)) for t in dyads})

def build_immutable_sets(roster: pd.DataFrame, dyads: List[Tuple[str, str]]) -> Tuple[Set[str], Set[str], Set[str]]:
    flagged_core_mask = (
        roster[COL_TEACHERKID].eq("ΝΑΙ") |
        roster[COL_SPECIAL].eq("ΝΑΙ") |
        roster[COL_ZOIRO].eq("Ν")
    )
    flagged_core = set(roster.loc[flagged_core_mask, COL_UID].astype(str))

    locked_friends = set()
    for a, b in dyads:
        if a in flagged_core or b in flagged_core:
            locked_friends.add(a); locked_friends.add(b)

    immutable_step1_3 = placed_until_step3_ids(roster)

    return flagged_core, locked_friends, immutable_step1_3

def filter_movable_dyads(dyads: List[Tuple[str, str]], immutable_set: Set[str]) -> List[Tuple[str, str]]:
    return [(a,b) for (a,b) in dyads if a not in immutable_set and b not in immutable_set]

# -------------------------- SWAP ENGINE -------------------------------

def same_category(row_a: pd.Series, row_b: pd.Series) -> bool:
    return (row_a[COL_GENDER] == row_b[COL_GENDER]) and (row_a[COL_LANG] == row_b[COL_LANG])

def try_apply_swap(df: pd.DataFrame, class_col: str, ids_from: List[str], ids_to: List[str]) -> pd.DataFrame:
    if len(ids_from) != len(ids_to):
        return df
    tmp = df.set_index(COL_UID)
    classes_from = tmp.loc[ids_from, class_col].tolist()
    classes_to = tmp.loc[ids_to, class_col].tolist()
    tmp.loc[ids_from, class_col] = classes_to
    tmp.loc[ids_to, class_col] = classes_from
    return tmp.reset_index()

def build_by_class_pools(df: pd.DataFrame, class_col: str, immutable_set: Set[str]) -> Dict:
    work_idx = df.set_index(COL_UID)
    by_class = {}
    for uid, r in work_idx.iterrows():
        if uid in immutable_set:
            continue
        cat = (r[COL_GENDER], r[COL_LANG])
        by_class.setdefault(r[class_col], {}).setdefault(cat, {}).setdefault(r[COL_PERF], []).append(uid)
    return by_class

def greedy_tier_pass(df: pd.DataFrame,
                     class_col: str,
                     dyads: List[Tuple[str, str]],
                     immutable_set: Set[str],
                     target_spread: int,
                     gender_cap: int,
                     lang_cap: int,
                     max_swaps: int) -> TierResult:

    work = df.copy()
    counts = class_counts(work, class_col)
    s1, s3 = spreads_perf(counts)

    swaps_dyads = 0
    swaps_singles = 0
    swaps_buffer = 0

    # Early exit αν ήδη εντός στόχου
    if s1 <= target_spread and s3 <= target_spread:
        return TierResult(work, class_col, counts, s1, s3, swaps_dyads, swaps_singles, swaps_buffer,
                          meta={"note": f"Already ≤{target_spread}"})

    # ----- ΔΥΑΔΕΣ 1↔3 -----
    movable_dyads = filter_movable_dyads(dyads, immutable_set)
    random.shuffle(movable_dyads)

    for (a, b) in movable_dyads:
        if swaps_dyads + swaps_singles >= max_swaps:
            break
        ra = work.loc[work[COL_UID] == a]
        rb = work.loc[work[COL_UID] == b]
        if ra.empty or rb.empty:
            continue
        ra = ra.iloc[0]; rb = rb.iloc[0]
        if not same_category(ra, rb):
            continue
        dyad_perf = ("1" if ra[COL_PERF] == "1" and rb[COL_PERF] == "1"
                     else "3" if ra[COL_PERF] == "3" and rb[COL_PERF] == "3"
                     else None)
        if dyad_perf is None:
            continue
        target_perf = "3" if dyad_perf == "1" else "1"

        dyad_pool = []
        for (x, y) in movable_dyads:
            if {x, y} == {a, b}:
                continue
            rx = work.loc[work[COL_UID] == x]
            ry = work.loc[work[COL_UID] == y]
            if rx.empty or ry.empty:
                continue
            rx = rx.iloc[0]; ry = ry.iloc[0]
            if not same_category(ra, rx) or not same_category(rb, ry):
                continue
            perf_xy = ("1" if rx[COL_PERF] == "1" and ry[COL_PERF] == "1"
                       else "3" if rx[COL_PERF] == "3" and ry[COL_PERF] == "3"
                       else None)
            if perf_xy != target_perf:
                continue
            if rx[class_col] == ra[class_col] and ry[class_col] == rb[class_col]:
                continue
            dyad_pool.append((x, y))

        random.shuffle(dyad_pool)
        applied = False
        for (x, y) in dyad_pool:
            test = try_apply_swap(work, class_col, [a, b], [x, y])
            t_counts = class_counts(test, class_col)
            if not population_caps_ok(t_counts):
                continue
            if cap_violation_after_swap(t_counts, gender_cap, lang_cap):
                continue
            t_s1, t_s3 = spreads_perf(t_counts)
            if max(t_s1, t_s3) <= max(s1, s3):
                work = test
                counts = t_counts
                s1, s3 = t_s1, t_s3
                swaps_dyads += 1
                applied = True
                break
        if applied and (s1 <= target_spread and s3 <= target_spread):
            return TierResult(work, class_col, counts, s1, s3, swaps_dyads, swaps_singles, swaps_buffer)

    # ----- ΜΕΜΟΝΩΜΕΝΟΙ 1↔3 -----
    by_class = build_by_class_pools(work, class_col, immutable_set)

    for _ in range(max_swaps - swaps_dyads):
        if swaps_dyads + swaps_singles >= max_swaps:
            break
        classes = list(by_class.keys())
        random.shuffle(classes)
        applied = False
        for c1 in classes:
            for c2 in classes:
                if c1 == c2:
                    continue
                cats1 = list(by_class.get(c1, {}).keys())
                random.shuffle(cats1)
                for cat in cats1:
                    pool1 = by_class.get(c1, {}).get(cat, {}).get("1", [])
                    pool3 = by_class.get(c2, {}).get(cat, {}).get("3", [])
                    if not pool1 or not pool3:
                        continue
                    u1 = pool1[-1]; u3 = pool3[-1]
                    test = try_apply_swap(work.reset_index(drop=True), class_col, [u1], [u3])
                    t_counts = class_counts(test, class_col)
                    if not population_caps_ok(t_counts):
                        continue
                    if cap_violation_after_swap(t_counts, gender_cap, lang_cap):
                        continue
                    t_s1, t_s3 = spreads_perf(t_counts)
                    if max(t_s1, t_s3) <= max(s1, s3):
                        work = test
                        counts = t_counts
                        s1, s3 = t_s1, t_s3
                        swaps_singles += 1
                        by_class[c1][cat]["1"].pop()
                        by_class[c2][cat]["3"].pop()
                        by_class.setdefault(c1, {}).setdefault(cat, {}).setdefault("3", []).append(u3)
                        by_class.setdefault(c2, {}).setdefault(cat, {}).setdefault("1", []).append(u1)
                        applied = True
                        break
                if applied: break
            if applied: break
        if applied and (s1 <= target_spread and s3 <= target_spread):
            return TierResult(work, class_col, counts, s1, s3, swaps_dyads, swaps_singles, swaps_buffer)

    # ----- BUFFER PHASE (1↔2 ή 3↔2) -----
    # Ενεργοποιείται μόνο αν δεν πετύχαμε τον στόχο
    if not (s1 <= target_spread and s3 <= target_spread):
        # ποιο "πονάει" περισσότερο;
        target_metric = "1" if (s1 > s3) else "3"
        no_improve = 0
        for _ in range(MAX_BUFFER_SWAPS):
            # early stop αν δεν βελτιώνεται το max spread για 5 διαδοχικές προσπάθειες
            if no_improve >= EARLY_STOP_NOIMPROVE:
                break
            classes = list(by_class.keys()) if 'by_class' in locals() else list(work[class_col].unique())
            random.shuffle(classes)
            applied = False
            for c1 in classes:
                for c2 in classes:
                    if c1 == c2:
                        continue
                    # ίδιες κατηγορίες
                    cats1 = list(by_class.get(c1, {}).keys()) if 'by_class' in locals() else None
                    if cats1 is None:
                        # αν δεν έχουμε by_class (π.χ. όλα immutable), σπάσε
                        cats1 = []
                    random.shuffle(cats1)
                    for cat in cats1:
                        if target_metric == "1":
                            poolA = by_class.get(c1, {}).get(cat, {}).get("1", [])
                            poolB = by_class.get(c2, {}).get(cat, {}).get("2", [])
                            if not poolA or not poolB:
                                continue
                            uA = poolA[-1]; uB = poolB[-1]
                        else:
                            poolA = by_class.get(c1, {}).get(cat, {}).get("3", [])
                            poolB = by_class.get(c2, {}).get(cat, {}).get("2", [])
                            if not poolA or not poolB:
                                continue
                            uA = poolA[-1]; uB = poolB[-1]

                        test = try_apply_swap(work.reset_index(drop=True), class_col, [uA], [uB])
                        t_counts = class_counts(test, class_col)
                        if not population_caps_ok(t_counts):
                            continue
                        if cap_violation_after_swap(t_counts, gender_cap, lang_cap):
                            continue
                        t_s1, t_s3 = spreads_perf(t_counts)

                        # Acceptance gates buffer
                        if target_metric == "1":
                            improves_target = (t_s1 < s1)
                            other_ok = (t_s3 <= target_spread)
                        else:
                            improves_target = (t_s3 < s3)
                            other_ok = (t_s1 <= target_spread)
                        if improves_target and other_ok:
                            # δέξου
                            work = test
                            counts = t_counts
                            prev_max = max(s1, s3)
                            s1, s3 = t_s1, t_s3
                            swaps_buffer += 1
                            # ενημέρωση δεξαμενών
                            if target_metric == "1":
                                by_class[c1][cat]["1"].pop()
                                by_class[c2][cat]["2"].pop()
                                by_class.setdefault(c1, {}).setdefault(cat, {}).setdefault("2", []).append(uB)
                                by_class.setdefault(c2, {}).setdefault(cat, {}).setdefault("1", []).append(uA)
                            else:
                                by_class[c1][cat]["3"].pop()
                                by_class[c2][cat]["2"].pop()
                                by_class.setdefault(c1, {}).setdefault(cat, {}).setdefault("2", []).append(uB)
                                by_class.setdefault(c2, {}).setdefault(cat, {}).setdefault("3", []).append(uA)
                            applied = True
                            if max(s1, s3) < prev_max:
                                no_improve = 0
                            else:
                                no_improve += 1
                            break
                    if applied: break
                if applied: break
            if applied and (s1 <= target_spread and s3 <= target_spread):
                break
            if not applied:
                no_improve += 1

    return TierResult(work, class_col, counts, s1, s3, swaps_dyads, swaps_singles, swaps_buffer)

# ------------------------------ MAIN ---------------------------------

def run_for_scenario(roster: pd.DataFrame,
                     df_step6: pd.DataFrame,
                     class_col: str,
                     dyads: List[Tuple[str, str]]) -> TierResult:
    roster = ensure_columns(roster)
    df = df_step6.copy()
    if COL_UID not in df.columns:
        if COL_NAME in df.columns:
            df = df.merge(roster[[COL_UID, COL_NAME, COL_GENDER, COL_LANG, COL_ZOIRO, COL_TEACHERKID, COL_SPECIAL, COL_PERF]],
                          on=COL_NAME, how="left")
            df[COL_UID] = df[COL_UID].fillna(df[COL_NAME])
        else:
            raise ValueError("Δεν βρέθηκε στήλη μοναδικού αναγνωριστικού ούτε ΟΝΟΜΑ στο df_step6.")
    else:
        df = df.merge(roster[[COL_UID, COL_GENDER, COL_LANG, COL_ZOIRO, COL_TEACHERKID, COL_SPECIAL, COL_PERF]],
                      on=COL_UID, how="left")

    flagged_core, locked_friends, immutable_step1_3 = build_immutable_sets(roster, dyads)
    immutable_set = set().union(flagged_core, locked_friends, immutable_step1_3)

    dyads_ok = filter_movable_dyads(dyads, immutable_set)

    best: Optional[TierResult] = None

    for cfg in TIERS:
        res = greedy_tier_pass(
            df=df,
            class_col=class_col,
            dyads=dyads_ok,
            immutable_set=immutable_set,
            target_spread=cfg["target_spread"],
            gender_cap=cfg["gender_cap"],
            lang_cap=cfg["lang_cap"],
            max_swaps=cfg["max_swaps"],
        )
        res.meta.update({
            "tier": cfg["name"],
            "gender_cap": cfg["gender_cap"],
            "lang_cap": cfg["lang_cap"],
            "target_spread": cfg["target_spread"],
            "allow_cross": False,
        })
        if res.spread1 <= cfg["target_spread"] and res.spread3 <= cfg["target_spread"]:
            return res
        if (best is None) or (max(res.spread1, res.spread3) < max(best.spread1, best.spread3)):
            best = res

    if best is None:
        counts = class_counts(df, class_col)
        s1, s3 = spreads_perf(counts)
        best = TierResult(df, class_col, counts, s1, s3, 0, 0, 0, meta={"tier": "NONE"})
    best.meta.setdefault("note", "Fail all tiers to hit target; returned best-so-far result.")
    return best

def write_summary(writer: pd.ExcelWriter, scenario: str, res: TierResult):
    meta = res.meta
    counts = res.counts
    df_counts = pd.DataFrame.from_dict(counts, orient="index").sort_index()
    df_meta = pd.DataFrame([{
        "Scenario": scenario,
        "TierUsed": meta.get("tier"),
        "TargetSpread": meta.get("target_spread"),
        "GenderCap": meta.get("gender_cap"),
        "LangCap": meta.get("lang_cap"),
        "AllowCross": meta.get("allow_cross"),
        "Spread1": res.spread1,
        "Spread3": res.spread3,
        "Swaps_Dyads": res.swaps_dyads,
        "Swaps_Singles": res.swaps_singles,
        "Swaps_Buffer": res.swaps_buffer,
        "Note": meta.get("note", ""),
    }])
    df_meta.to_excel(writer, sheet_name=f"{scenario}_Σύνοψη", index=False)
    df_counts.to_excel(writer, sheet_name=f"{scenario}_Μετρήσεις")

def main():
    try:
        roster = pd.read_excel(PATH_ROSTER)
        roster = ensure_columns(roster)
    except Exception as e:
        raise RuntimeError(f"Αποτυχία ανάγνωσης roster: {e}")

    dyads = load_mutual_dyads_from_step3(PATH_STEP3)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as writer:
        step6_book = pd.ExcelFile(PATH_STEP6)
        for scenario in SCENARIOS:
            try:
                df6 = step6_book.parse(scenario)
            except Exception:
                continue

            class_cols = [c for c in df6.columns if str(c).strip().upper() in {COL_CLASS.upper(), f"ΒΗΜΑ6_{scenario}".upper(), f"STEP6_{scenario}".upper()}]
            if class_cols:
                class_col = class_cols[0]
            else:
                candidates = [c for c in df6.columns if "ΤΜΗΜ" in str(c).upper() or "STEP6" in str(c).upper() or "ΒΗΜΑ6" in str(c).upper()]
                class_col = candidates[0] if candidates else COL_CLASS
                if class_col not in df6.columns:
                    df6[class_col] = np.nan

            res = run_for_scenario(roster, df6, class_col, dyads)

            df_out = res.df.copy()
            out_col = f"ΒΗΜΑ7_{scenario}"
            if out_col != class_col:
                df_out[out_col] = df_out[class_col]
            df_out.to_excel(writer, sheet_name=scenario, index=False)
            write_summary(writer, scenario, res)

    print(f"OK — Γράφτηκε αρχείο: {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
