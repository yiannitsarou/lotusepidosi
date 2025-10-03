# -*- coding: utf-8 -*-
"""
step8_fixed_final.py — FINAL (Rule: MIN total_score; tie → prefer zero-broken)
-----------------------------------------------------------------------------
ΚΑΝΟΝΑΣ ΕΠΙΛΟΓΗΣ (όπως ζητήθηκε):
1) ΠΡΩΤΑ επιλέγουμε το σενάριο με το μικρότερο total_score.
2) Αν υπάρξει ισοβαθμία στο total_score, προτιμάται σενάριο με ΜΗ σπασμένες φιλίες
   (δηλ. broken_friendships = 0). Γενικά, όσο μικρότερο broken_friendships τόσο καλύτερα.
3) Αν εξακολουθεί να υπάρχει ισοβαθμία, tie-breakers: diff_population → diff_gender_total → diff_greek.
4) Αν υπάρχει απόλυτη ισοβαθμία σε όλα τα παραπάνω, γίνεται τυχαία επιλογή (seeded).

ΣΗΜΕΙΩΣΗ: Αυτή η έκδοση καταργεί την παλιότερη «σκληρή προτεραιότητα» σε zero-broken
(δηλ. να κοιτούσαμε ΜΟΝΟ σενάρια με zero-broken). Τώρα το zero-broken λειτουργεί ΜΟΝΟ ως
tie-breaker *μετά* το total_score, όπως ορίστηκε.
"""
from __future__ import annotations
import random
from typing import Iterable, List, Tuple, Dict, Any, Optional
import pandas as pd
import numpy as np
import re

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ------------------------ Normalizations (unchanged) ------------------------
YES_TOKENS = {"Ν", "ΝΑΙ", "Y", "YES", "TRUE", "1"}
NO_TOKENS  = {"Ο", "ΟΧΙ", "N", "NO", "FALSE", "0"}

def _norm_str(x) -> str:
    return str(x).strip().upper()

def _is_yes(x) -> bool:
    return _norm_str(x) in YES_TOKENS

def _is_no(x) -> bool:
    return _norm_str(x) in NO_TOKENS

def _parse_friends_cell(x) -> List[str]:
    """Δέχεται λίστα ή string. Επιστρέφει λίστα ονομάτων (stripped)."""
    if isinstance(x, list):
        return [str(t).strip() for t in x if str(t).strip()]
    s = str(x) if x is not None else ""
    s = s.strip()
    if not s or s.upper() == "NAN":
        return []
    # Προσπάθησε python-literal list
    try:
        val = eval(s, {}, {})
        if isinstance(val, list):
            return [str(t).strip() for t in val if str(t).strip()]
    except Exception:
        pass
    # Αλλιώς split σε κοινούς διαχωριστές
    parts = re.split(r"[,\|\;/·\n]+", s)
    return [p.strip() for p in parts if p.strip()]

def _infer_num_classes_from_values(vals: Iterable[str]) -> int:
    """Επιστρέφει #τμημάτων κοιτώντας labels τύπου Α1, Α2, ..."""
    labels = sorted({str(v) for v in vals if re.match(r"^Α\d+$", str(v))})
    if not labels:
        return 2
    return len(labels)

# ------------------------ Core helpers ------------------------

def _counts_per_class(df: pd.DataFrame, scenario_col: str, label_filter=None) -> Dict[str, int]:
    """Γενικός μετρητής ανά τμήμα."""
    labels = sorted([c for c in df[scenario_col].dropna().astype(str).unique() if re.match(r"^Α\d+$", str(c))])
    res = {lab: 0 for lab in labels}
    if label_filter is None:
        for lab in labels:
            res[lab] = int((df[scenario_col] == lab).sum())
        return res
    # label_filter είναι συνάρτηση που δέχεται row και επιστρέφει bool
    mask = df.apply(label_filter, axis=1)
    for lab in labels:
        res[lab] = int(((df[scenario_col] == lab) & mask).sum())
    return res

def _boys_filter(row) -> bool:
    return _norm_str(row.get("ΦΥΛΟ")) == "Α"

def _girls_filter(row) -> bool:
    return _norm_str(row.get("ΦΥΛΟ")) == "Κ"

def _good_greek_filter(row) -> bool:
    """True αν έχει 'καλή γνώση' σύμφωνα με ΟΠΟΙΑ στήλη υπάρχει."""
    if "ΚΑΛΗ_ΓΝΩΣΗ_ΕΛΛΗΝΙΚΩΝ" in row:
        val = row.get("ΚΑΛΗ_ΓΝΩΣΗ_ΕΛΛΗΝΙΚΩΝ")
        return _is_yes(val)
    if "ΓΝΩΣΗ_ΕΛΛΗΝΙΚΩΝ" in row:
        v = _norm_str(row.get("ΓΝΩΣΗ_ΕΛΛΗΝΙΚΩΝ"))
        return v in {"ΚΑΛΗ", "Ν", "GOOD"}
    return False

def _pairwise_differences_sum(counts: Dict[str, int]) -> int:
    """Άθροισμα διαφορών όλων των ζευγαριών (για tie-breaking)."""
    values = list(counts.values())
    total_diff = 0
    n = len(values)
    for i in range(n):
        for j in range(i+1, n):
            total_diff += abs(values[i] - values[j])
    return total_diff

def _pairwise_penalty(counts: Dict[str, int], free: int, weight: int) -> int:
    """Ποινή εφαρμοσμένη ανά ζεύγος τάξεων με κατώφλι free και βάρος weight."""
    values = list(counts.values())
    penalty = 0
    n = len(values)
    for i in range(n):
        for j in range(i+1, n):
            diff = abs(values[i] - values[j])
            if diff > free:
                penalty += (diff - free) * weight
    return penalty

def _pair_conflict_penalty(aZ, aI, bZ, bI) -> int:
    """Ποινή παιδαγωγικής σύγκρουσης ανά ζεύγος."""
    if aI and bI: return 5
    if (aI and bZ) or (bI and aZ): return 4
    if aZ and bZ: return 3
    return 0

def _class_conflict_sum(class_df: pd.DataFrame) -> int:
    """Συνολική ποινή συγκρούσεων ενός τμήματος."""
    s = 0
    rows = class_df[['ΖΩΗΡΟΣ','ΙΔΙΑΙΤΕΡΟΤΗΤΑ']].fillna("").to_dict('records')
    for i in range(len(rows)):
        for j in range(i+1, len(rows)):
            a = rows[i]; b = rows[j]
            aZ = _is_yes(a.get('ΖΩΗΡΟΣ')); aI = _is_yes(a.get('ΙΔΙΑΙΤΕΡΟΤΗΤΑ'))
            bZ = _is_yes(b.get('ΖΩΗΡΟΣ')); bI = _is_yes(b.get('ΙΔΙΑΙΤΕΡΟΤΗΤΑ'))
            s += _pair_conflict_penalty(aZ, aI, bZ, bI)
    return s

def _all_conflicts_sum(df: pd.DataFrame, scenario_col: str) -> int:
    """Συνολική ποινή παιδαγωγικών συγκρούσεων."""
    s = 0
    for lab, class_df in df.groupby(scenario_col):
        if not re.match(r"^Α\d+$", str(lab)):
            continue
        s += _class_conflict_sum(class_df)
    return s

def _mutual_pairs(df: pd.DataFrame) -> List[Tuple[str,str]]:
    """Βρίσκει όλες τις *πλήρως αμοιβαίες* δυάδες από «ΦΙΛΟΙ»."""
    if "ΦΙΛΟΙ" not in df.columns:
        return []
    name2friends = {}
    for _, r in df.iterrows():
        name = str(r.get("ΟΝΟΜΑ")).strip()
        friends = set(_parse_friends_cell(r.get("ΦΙΛΟΙ")))
        name2friends[name] = friends
    pairs = set()
    names = sorted(name2friends.keys())
    for i, a in enumerate(names):
        for b in names[i+1:]:
            if a in name2friends.get(b, set()) and b in name2friends.get(a, set()):
                pairs.add(tuple(sorted((a,b))))
    return sorted(pairs)

def _broken_friendships_count(df: pd.DataFrame, scenario_col: str, critical_pairs: Optional[List[Tuple[str,str]]] = None,
                              count_unassigned_as_broken: bool=False) -> int:
    """Μετρά πόσες αμοιβαίες δυάδες ΔΕΝ κατέληξαν στο ίδιο τμήμα."""
    if critical_pairs is None:
        pairs = _mutual_pairs(df)
    else:
        pairs = [tuple(sorted((str(a).strip(), str(b).strip()))) for a,b in critical_pairs]
    name2class = {str(r["ΟΝΟΜΑ"]).strip(): r.get(scenario_col) for _, r in df.iterrows()}
    broken = 0
    for a,b in pairs:
        ca = name2class.get(a, np.nan)
        cb = name2class.get(b, np.nan)
        if pd.isna(ca) or pd.isna(cb):
            if count_unassigned_as_broken:
                broken += 1
            continue
        if str(ca) != str(cb):
            broken += 1
    return broken

# ------------------------ Public API ------------------------

def score_one_scenario(df: pd.DataFrame, scenario_col: str, num_classes: Optional[int] = None,
                       critical_pairs: Optional[List[Tuple[str,str]]]=None,
                       count_unassigned_as_broken: bool=False) -> Dict[str, Any]:
    """Υπολογίζει αναλυτικό score για ένα σενάριο με σωστή λογική ζευγαριών."""
    df = df.copy()
    if num_classes is None:
        num_classes = _infer_num_classes_from_values(df[scenario_col].values)

    # 1) Πληθυσμός
    pop_counts = _counts_per_class(df, scenario_col)
    total_pop_diff = _pairwise_differences_sum(pop_counts)  # για tie-breaking
    population_penalty = _pairwise_penalty(pop_counts, free=1, weight=3)

    # 2) Φύλο (αγόρια + κορίτσια)
    boys_counts = _counts_per_class(df, scenario_col, label_filter=_boys_filter)
    girls_counts= _counts_per_class(df, scenario_col, label_filter=_girls_filter)
    total_boys_diff = _pairwise_differences_sum(boys_counts)    # για tie-breaking
    total_girls_diff = _pairwise_differences_sum(girls_counts)  # για tie-breaking
    boys_penalty = _pairwise_penalty(boys_counts, free=1, weight=2)
    girls_penalty = _pairwise_penalty(girls_counts, free=1, weight=2)
    gender_penalty = boys_penalty + girls_penalty

    # 3) Γνώση ελληνικών
    good_counts = _counts_per_class(df, scenario_col, label_filter=_good_greek_filter)
    total_greek_diff = _pairwise_differences_sum(good_counts)  # για tie-breaking
    greek_penalty = _pairwise_penalty(good_counts, free=2, weight=1)


    # 3b) Επίδοση (1 & 3) — spread-based penalty (βάρος = 1, κατώφλι 2)
    if "ΕΠΙΔΟΣΗ" in df.columns:
        perf1_counts = _counts_per_class(df, scenario_col, label_filter=_perf1_filter)
        perf3_counts = _counts_per_class(df, scenario_col, label_filter=_perf3_filter)
        spread_perf1 = _spread_from_counts(perf1_counts)
        spread_perf3 = _spread_from_counts(perf3_counts)

        # (προαιρετικά) ΕΠΙΔ_2 — συνήθως περιττό (γιατί #2 = N - #1 - #3)
        include_perf2 = False  # άλλαξέ το σε True αν θες και το 2
        if include_perf2:
            perf2_counts = _counts_per_class(df, scenario_col, label_filter=_perf2_filter)
            spread_perf2 = _spread_from_counts(perf2_counts)
        else:
            perf2_counts = {}
            spread_perf2 = 0

        perf_pen = perf_penalty(spread_perf1, spread_perf3, spread2=spread_perf2,
                                include_perf2=include_perf2, w=1)
    else:
        perf1_counts = {}
        perf3_counts = {}
        perf2_counts = {}
        spread_perf1 = spread_perf3 = spread_perf2 = 0
        perf_pen = 0
    # 4) Παιδαγωγικές συγκρούσεις
    conflict_penalty = _all_conflicts_sum(df, scenario_col)

    # 5) Σπασμένες φιλίες
    broken = _broken_friendships_count(df, scenario_col, critical_pairs, count_unassigned_as_broken)
    broken_friendships_penalty = 5 * broken

    total = population_penalty + gender_penalty + greek_penalty + perf_pen + conflict_penalty + broken_friendships_penalty

    return {
        "scenario_col": scenario_col,
        "num_classes": num_classes,
        "population_counts": pop_counts,
        "boys_counts": boys_counts,
        "girls_counts": girls_counts,
        "good_greek_counts": good_counts,
        # Διαφορές για tie-breaking
        "diff_population": int(total_pop_diff),
        "diff_boys": int(total_boys_diff),
        "diff_girls": int(total_girls_diff), 
        "diff_gender_total": int(total_boys_diff + total_girls_diff),
        "diff_greek": int(total_greek_diff),
        # Ποινές
        "population_penalty": int(population_penalty),
        "boys_penalty": int(boys_penalty),
        "girls_penalty": int(girls_penalty),
        "gender_penalty": int(gender_penalty),
        "greek_penalty": int(greek_penalty),
        "conflict_penalty": int(conflict_penalty),
        "broken_friendships": int(broken),
        "broken_friendships_penalty": int(broken_friendships_penalty),
        "total_score": int(total),
"perf1_counts": perf1_counts,
"perf3_counts": perf3_counts,
"perf2_counts": perf2_counts,
"spread_perf1": spread_perf1,
"spread_perf3": spread_perf3,
"spread_perf2": spread_perf2,
"perf_penalty": perf_pen,

    }

def pick_best_scenario(df: pd.DataFrame, scenario_cols: List[str], num_classes: Optional[int]=None,
                       critical_pairs: Optional[List[Tuple[str,str]]]=None,
                       count_unassigned_as_broken: bool=False,
                       k_best: int=1, random_seed: int=42) -> Dict[str, Any]:
    """
    Τελικός κανόνας επιλογής:
      • 1ο κριτήριο: MIN total_score (παγκόσμια).
      • 2ο κριτήριο (tie): λιγότερα broken_friendships (δηλ. προτιμά zero-broken).
      • 3ο κριτήριο (tie): diff_population → diff_gender_total → diff_greek.
      • Τυχαιότητα ΜΟΝΟ σε απόλυτη ισοβαθμία (seeded).
    """
    if not scenario_cols:
        return {"best": None, "scores": []}
    if num_classes is None:
        num_classes = _infer_num_classes_from_values(df[scenario_cols[0]].values)

    scores = []
    for c in scenario_cols:
        if c not in df.columns:
            continue
        s = score_one_scenario(df, c, num_classes, critical_pairs, count_unassigned_as_broken)
        scores.append(s)
    if not scores:
        return {"best": None, "scores": []}

    # ΤΑΞΙΝΟΜΗΣΗ σύμφωνα με τον νέο κανόνα
    pool_sorted = sorted(
        scores,
        key=lambda s: (
            s["total_score"],
            int(s["broken_friendships"]),   # λιγότερα σπασμένα → καλύτερα
            s["diff_population"],
            s["diff_gender_total"],
            s["diff_greek"],
            str(s["scenario_col"]),
        )
    )

    head = pool_sorted[0]
    # απόλυτη ισοβαθμία σε όλα τα παραπάνω κριτήρια (εκτός του ονόματος)
    ties = [s for s in pool_sorted if (
        s["total_score"] == head["total_score"] and
        int(s["broken_friendships"]) == int(head["broken_friendships"]) and
        s["diff_population"] == head["diff_population"] and
        s["diff_gender_total"] == head["diff_gender_total"] and
        s["diff_greek"] == head["diff_greek"]
    )]

    random.seed(random_seed)
    best = random.choice(ties) if len(ties) > 1 else head

    return {"best": best, "scores": pool_sorted[:max(1, int(k_best))]}

def score_to_dataframe(df: pd.DataFrame, scenario_cols: List[str], **kwargs) -> pd.DataFrame:
    """Μετατρέπει scores σε DataFrame για εύκολη προβολή."""
    rows = []
    for c in scenario_cols:
        if c not in df.columns:
            continue
        s = score_one_scenario(df, c, **kwargs)
        rows.append({
            "SCENARIO": c,
            "TOTAL": s["total_score"],
            "POP_DIFF": s["diff_population"],
            "BOYS_DIFF": s["diff_boys"],
            "GIRLS_DIFF": s["diff_girls"],
            "GENDER_DIFF_TOTAL": s["diff_gender_total"],
            "GREEK_DIFF": s["diff_greek"],
            "POP_PEN": s["population_penalty"],
            "BOYS_PEN": s["boys_penalty"],
            "GIRLS_PEN": s["girls_penalty"],
            "GENDER_PEN": s["gender_penalty"],
            "GREEK_PEN": s["greek_penalty"],
            "CONFLICT_PEN": s["conflict_penalty"],
            "BROKEN_COUNT": s["broken_friendships"],
            "BROKEN_PEN": s["broken_friendships_penalty"],
            "PERF1_SPREAD": s.get("spread_perf1", 0),
            "PERF3_SPREAD": s.get("spread_perf3", 0),
            "PERF_PEN": s.get("perf_penalty", 0),
        })
    return pd.DataFrame(rows)

def export_scores_excel(df: pd.DataFrame, scenario_cols: List[str], out_path: str, **kwargs) -> str:
    """Εξάγει scores σε Excel αρχείο."""
    tbl = score_to_dataframe(df, scenario_cols, **kwargs)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        tbl.to_excel(w, index=False, sheet_name="Scores")
    return out_path

# === Auto helpers (unchanged) ===
def _find_scenario_col_auto(df: pd.DataFrame) -> str | None:
    import re
    # Priority: ΒΗΜΑ7 → ΒΗΜΑ6 → ΒΗΜΑ5 → explicit columns
    for c in df.columns:
        if re.match(r"^ΒΗΜΑ6_ΣΕΝΑΡΙΟ_\\d+__1$", str(c)):
            return c
    for c in df.columns:
        if re.match(r"^ΒΗΜΑ7_ΣΕΝΑΡΙΟ_\\d+__1$", str(c)):
            return c
    for c in df.columns:
        if re.match(r"^ΒΗΜΑ5_ΣΕΝΑΡΙΟ_\\d+__1$", str(c)):
            return c
    for c in ("ΒΗΜΑ7_ΤΜΗΜΑ", "ΒΗΜΑ6_ΤΜΗΜΑ", "ΤΜΗΜΑ_ΜΕΤΑ_ΒΗΜΑ7", "ΤΜΗΜΑ_ΜΕΤΑ_ΒΗΜΑ6", "ΤΜΗΜΑ"):
        if c in df.columns:
            return c
    return None


def _normalize_class_labels(df: pd.DataFrame, col: str):
    import re
    greek_A = "Α"
    df[col] = df[col].apply(lambda s: (greek_A + str(s)[1:]) if re.match(r"^A\d+$", str(s).strip()) else str(s).strip())

def _ensure_optional_cols(df: pd.DataFrame):
    defaults = {
        "ΖΩΗΡΟΣ": "Ο",
        "ΙΔΙΑΙΤΕΡΟΤΗΤΑ": "Ο",
        "ΣΥΓΚΡΟΥΣΗ": "",
        "ΦΙΛΟΙ": "",
    }
    for k,v in defaults.items():
        if k not in df.columns:
            df[k] = v

def score_one_scenario_auto(df: pd.DataFrame, scenario_col: str | None = None, **kwargs):
    """Auto-detect και score ενός σεναρίου."""
    df = df.copy()
    if scenario_col is None:
        scenario_col = _find_scenario_col_auto(df)
    if scenario_col is None:
        raise ValueError("Δεν βρέθηκε κατάλληλη στήλη τμήματος για το Βήμα 7.")
    _normalize_class_labels(df, scenario_col)
    _ensure_optional_cols(df)
    return score_one_scenario(df, scenario_col, **kwargs)

def export_best_scenario_split_by_class(scores_xlsx_path: str, out_xlsx_path: str) -> str:
    """
    Διαβάζει το STEP1_7_SCORES_AND_BEST.xlsx (ή αντίστοιχο) και δημιουργεί νέο Excel
    όπου το νικητήριο σενάριο είναι σπασμένο σε ξεχωριστό φύλλο ανά τμήμα (+ "Σύνοψη").
    Απαιτεί sheet "BEST_SCENARIO_DATA" με τα πλήρη δεδομένα του νικητή.
    """
    import pandas as pd, re
    xls = pd.ExcelFile(scores_xlsx_path)
    if "BEST_SCENARIO_DATA" not in xls.sheet_names:
        raise ValueError("Το αρχείο δεν περιέχει sheet 'BEST_SCENΑΡΙΟ_DATA'. Τρέξε πρώτα το Βήμα 7 για να το δημιουργήσεις.")

    best_df = xls.parse("BEST_SCENARIO_DATA")

    # Βρίσκουμε την τελική στήλη τμημάτων (προτεραιότητα ΒΗΜΑ6 -> ΒΗΜΑ5 -> ΒΗΜΑ4)
    def pick_final_col(df):
        pats = [r"^ΒΗΜΑ7_ΣΕΝΑΡΙΟ_\d+$", r"^ΒΗΜΑ6_ΣΕΝΑΡΙΟ_\d+$", r"^ΒΗΜΑ5_ΣΕΝΑΡΙΟ_\d+$", r"^ΒΗΜΑ4_ΣΕΝΑΡΙΟ_\d+$"]
        for pat in pats:
            cand = [c for c in df.columns if re.match(pat, str(c))]
            if cand:
                # επιλέγουμε τη στήλη με τα περισσότερα μη-κενά
                cand.sort(key=lambda c: (-int(df[c].astype(str).replace({'': None, 'nan': None}).notna().sum()), str(c)))
                return cand[0]
        # fallbacks
        for c in df.columns:
            if (str(c).startswith("ΒΗΜΑ") and "ΣΕΝΑΡΙΟ" in str(c)) or str(c).strip().upper() in {"ΤΜΗΜΑ","CLASS","SECTION"}:
                return c
        return df.columns[-1]

    final_col = pick_final_col(best_df)

    # Σύνοψη ανά τμήμα
    tmp = best_df.assign(_final=best_df[final_col].astype(str).str.strip())
    tmp = tmp.loc[tmp["_final"] != ""]
    summary = (tmp.groupby("_final").agg(Μαθητές=("ΟΝΟΜΑ","count")).reset_index().rename(columns={"_final":"ΤΜΗΜΑ"}))

    # Γράφουμε: "Σύνοψη" + 1 φύλλο ανά τμήμα
    with pd.ExcelWriter(out_xlsx_path, engine="xlsxwriter") as writer:
        summary.to_excel(writer, index=False, sheet_name="Σύνοψη")
        for cls in summary["ΤΜΗΜΑ"]:
            sub = best_df[best_df[final_col].astype(str).str.strip() == str(cls)]
            safe_name = re.sub(r"[:\\/?*\[\]]", "_", str(cls))[:31] or "ΚΕΝΟ"
            sub.to_excel(writer, index=False, sheet_name=safe_name)
    return out_xlsx_path


# ------------------------ Επιλογή across sheets (MIN total_score; tie → zero-broken) ------------------------
def pick_across_sheets_minrule(step1_7_xlsx_path: str, seed: int = 42) -> Dict[str, Any]:
    """
    Ανοίγει το STEP1_7_PER_SCENARIO_*.xlsx, υπολογίζει score για το πρώτο ΒΗΜΑ7_ΣΕΝΑΡΙΟ_* κάθε φύλλου
    και επιστρέφει το global best με τον ΝΕΟ κανόνα:
      • 1ο κριτήριο: MIN total_score (παγκόσμια).
      • 2ο κριτήριο (tie): λιγότερα broken_friendships (δηλ. προτιμά zero-broken).
      • 3ο κριτήριο (tie): diff_population → diff_gender_total → diff_greek.
      • Τυχαιότητα μόνο σε απόλυτη ισοβαθμία.
    ΔΕΝ απορρίπτει φύλλα όταν δεν υπάρχει zero-broken — απλώς τα συγκρίνει δίκαια.
    """
    import pandas as _pd, re as _re, random as _rnd
    xls = _pd.ExcelFile(step1_7_xlsx_path)
    candidates = []
    for sheet in xls.sheet_names:
        if str(sheet).strip() in {"Σύνοψη"}:
            continue
        df = _pd.read_excel(step1_7_xlsx_path, sheet_name=sheet)
        scen_cols = [c for c in df.columns if _re.match(r"^ΒΗΜΑ7_ΣΕΝΑΡΙΟ_\d+$", str(c))]
        if not scen_cols:
            scen_cols = [c for c in df.columns if _re.match(r"^ΒΗΜΑ6_ΣΕΝΑΡΙΟ_\d+$", str(c))]
        if not scen_cols:
            scen_cols = [c for c in df.columns if _re.match(r"^ΒΗΜΑ5_ΣΕΝΑΡΙΟ_\d+$", str(c))]
        col = scen_cols[0]
        res = score_one_scenario(df, col)
        candidates.append((sheet, col, res, df))

    if not candidates:
        raise RuntimeError("No Step 7/6/5 scenario columns found across sheets.")

    candidates.sort(key=lambda t: (
        t[2]["total_score"],
        int(t[2]["broken_friendships"]),
        t[2]["diff_population"],
        t[2]["diff_gender_total"],
        t[2]["diff_greek"],
        str(t[0]),  # sheet name as last tiebreaker
    ))

    head = candidates[0]
    ties = [t for t in candidates if (
        t[2]["total_score"] == head[2]["total_score"] and
        int(t[2]["broken_friendships"]) == int(head[2]["broken_friendships"]) and
        t[2]["diff_population"] == head[2]["diff_population"] and
        t[2]["diff_gender_total"] == head[2]["diff_gender_total"] and
        t[2]["diff_greek"] == head[2]["diff_greek"]
    )]

    _rnd.seed(seed)
    chosen_sheet, chosen_col, chosen_score, chosen_df = _rnd.choice(ties) if len(ties) > 1 else head

    return {
        "chosen_sheet": chosen_sheet,
        "chosen_col": chosen_col,
        "broken_pairs": int(chosen_score["broken_friendships"]),
        "total_score": int(chosen_score["total_score"]),
        "diff_population": int(chosen_score["diff_population"]),
        "diff_gender_total": int(chosen_score["diff_gender_total"]),
        "diff_greek": int(chosen_score["diff_greek"]),
    }


def _perf_value(row) -> Optional[int]:
    """Γυρίζει 1/2/3 από τη στήλη ΕΠΙΔΟΣΗ (δέχεται αριθμούς ή strings)."""
    v = row.get("ΕΠΙΔΟΣΗ", None)
    if v is None:
        return None
    try:
        import pandas as pd
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    if s in {"1", "2", "3"}:
        return int(s)
    try:
        iv = int(float(s))
        if iv in (1, 2, 3):
            return iv
    except Exception:
        return None
    return None

def _perf1_filter(row) -> bool:
    val = _perf_value(row)
    return val == 1

def _perf2_filter(row) -> bool:
    val = _perf_value(row)
    return val == 2

def _perf3_filter(row) -> bool:
    val = _perf_value(row)
    return val == 3

def _spread_from_counts(counts: Dict[str, int]) -> int:
    """spread = max - min"""
    if not counts:
        return 0
    vals = list(counts.values())
    if not vals:
        return 0
    return max(vals) - min(vals)

def perf_penalty(spread1: int, spread3: int, spread2: Optional[int] = None,
                 include_perf2: bool = False, w: int = 1) -> int:
    """Κανόνας επίδοσης: >2 ⇒ +1 ανά μονάδα (βάρος w)."""
    pen = w * max(0, spread1 - 2) + w * max(0, spread3 - 2)
    if include_perf2 and (spread2 is not None):
        pen += w * max(0, spread2 - 2)
    return int(pen)
