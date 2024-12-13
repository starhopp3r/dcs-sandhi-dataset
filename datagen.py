import os
import re
from tqdm import tqdm
import pandas as pd
from indic_transliteration import sanscript


INPUT_DIR = "dcs"
OUTPUT_FILE = "sandhi_data.csv"

token_line_pattern = re.compile(r"^(\d+(?:-\d+)?)\s+(\S+)\s+")
unsandhied_pattern = re.compile(r"Unsandhied=([^|]+)")


def iast_to_devanagari(text):
    return sanscript.transliterate(text, sanscript.IAST, sanscript.DEVANAGARI)


def token_in_ranges(ranges, idx):
    return any(start_id <= idx <= end_id for (start_id, end_id) in ranges)


def max_end_id(ranges):
    return max(end_id for start_id, end_id in ranges) if ranges else None


def finalize_chain(data_rows, original_form, parts):
    if original_form and parts:
        split_str = '+'.join(parts)
        dev_word = iast_to_devanagari(original_form)
        dev_split = iast_to_devanagari(split_str)
        data_rows.append({"word": dev_word, "split": dev_split})


def process_conllu_file(file_path, data_rows):
    current_chain_in_progress = False
    current_chain_original = ""
    current_chain_parts = []
    current_chain_ranges = []
    chain_pending_finalization = False
    last_single_token_original_form = None
    last_single_token_unsandhied_form = None
    last_single_token_standalone = True

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith('#')]


    def finalize_if_pending():
        nonlocal current_chain_in_progress, current_chain_original, current_chain_parts, current_chain_ranges, chain_pending_finalization
        if chain_pending_finalization and current_chain_in_progress:
            finalize_chain(data_rows, current_chain_original, current_chain_parts)
            current_chain_in_progress = False
            current_chain_original = ""
            current_chain_parts = []
            current_chain_ranges = []
            chain_pending_finalization = False


    def start_new_chain(original_form, ranges, initial_parts=None):
        nonlocal current_chain_in_progress, current_chain_original, current_chain_parts, current_chain_ranges, chain_pending_finalization
        current_chain_in_progress = True
        current_chain_original = original_form
        current_chain_parts = initial_parts if initial_parts else []
        current_chain_ranges = ranges
        chain_pending_finalization = False


    def try_merge_with_last_single(original_form):
        nonlocal last_single_token_original_form, last_single_token_unsandhied_form, last_single_token_standalone
        if last_single_token_original_form and last_single_token_standalone:
            merged_original = last_single_token_original_form + original_form
            merged_parts = []
            if last_single_token_unsandhied_form:
                merged_parts.append(last_single_token_unsandhied_form)
            return merged_original, merged_parts, True
        else:
            # Can't merge, remove `'` if present
            if original_form.startswith("'"):
                return original_form[1:], [], False
            else:
                return original_form, [], False

    i = 0
    while i < len(lines):
        line = lines[i]
        m = token_line_pattern.match(line)
        if m:
            token_id_str = m.group(1)
            original_form = m.group(2)
            is_multiword = '-' in token_id_str
            if is_multiword:
                start_id, end_id = map(int, token_id_str.split('-'))
                if original_form.startswith("'"):
                    # `'` multiword line
                    if chain_pending_finalization and current_chain_in_progress:
                        # Extend the existing chain with this `'` multiword token
                        current_chain_original += original_form
                        current_chain_ranges.append((start_id, end_id))
                        # We'll set chain_pending_finalization = True after reading all tokens of this new range
                        chain_pending_finalization = False
                    else:
                        # No chain pending
                        finalize_if_pending()
                        merged_original, merged_parts, merged = try_merge_with_last_single(original_form)
                        start_new_chain(merged_original, [(start_id, end_id)], merged_parts)
                else:
                    # Normal multiword line (no `'`)
                    finalize_if_pending()
                    start_new_chain(original_form, [(start_id, end_id)])
            else:
                # Single token line
                current_id = int(token_id_str)
                uns_match = unsandhied_pattern.search(line)
                single_unsandhied_form = uns_match.group(1) if uns_match else None

                if original_form.startswith("'"):
                    # `'` single token line
                    if chain_pending_finalization and current_chain_in_progress:
                        # Extend existing chain
                        current_chain_original += original_form
                        if single_unsandhied_form:
                            current_chain_parts.append(single_unsandhied_form)
                        chain_pending_finalization = True
                    else:
                        # No chain pending
                        finalize_if_pending()
                        merged_original, merged_parts, merged = try_merge_with_last_single(original_form)
                        start_new_chain(merged_original, [(current_id, current_id)], merged_parts)
                        if single_unsandhied_form:
                            current_chain_parts.append(single_unsandhied_form)
                        chain_pending_finalization = True
                    last_single_token_original_form = None
                    last_single_token_unsandhied_form = None
                    last_single_token_standalone = False
                else:
                    # Normal single token
                    # If chain pending and a normal single token line appears, finalize the chain now
                    if chain_pending_finalization and current_chain_in_progress:
                        finalize_if_pending()

                    if current_chain_in_progress and token_in_ranges(current_chain_ranges, current_id):
                        if single_unsandhied_form:
                            current_chain_parts.append(single_unsandhied_form)
                        max_id = max_end_id(current_chain_ranges)
                        if current_id == max_id:
                            chain_pending_finalization = True
                        last_single_token_standalone = False
                    else:
                        # Standalone single token
                        last_single_token_standalone = True
                        last_single_token_original_form = original_form
                        last_single_token_unsandhied_form = single_unsandhied_form

        i += 1
    # End of file
    finalize_if_pending()


def main():
    conllu_files = []
    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            if file.endswith(".conllu"):
                conllu_files.append(os.path.join(root, file))
    data_rows = []
    for file_path in tqdm(conllu_files, desc="Processing files"):
        process_conllu_file(file_path, data_rows)
    df = pd.DataFrame(data_rows, columns=["word", "split"])
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Number of entries: {len(df)}")


if __name__ == "__main__":
    main()
