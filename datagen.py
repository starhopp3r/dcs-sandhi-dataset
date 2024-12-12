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


def process_conllu_file(file_path, data_rows):
    multiword_original = None
    multiword_parts = []
    multiword_start_id = None
    multiword_end_id = None

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            m = token_line_pattern.match(line)
            if m:
                token_id_str = m.group(1)
                original_form = m.group(2)
                # Check if current line defines a multiword token
                if '-' in token_id_str:
                    # If a multiword is already in progress, finalize it first (if it had any parts)
                    if multiword_original and multiword_parts:
                        split_str = '+'.join(multiword_parts)
                        dev_word = iast_to_devanagari(multiword_original)
                        dev_split = iast_to_devanagari(split_str)
                        data_rows.append({"word": dev_word, "split": dev_split})
                    # Start a new multiword token
                    start_id, end_id = map(int, token_id_str.split('-'))
                    multiword_original = original_form
                    multiword_parts = []
                    multiword_start_id = start_id
                    multiword_end_id = end_id
                else:
                    # Single token line
                    current_id = int(token_id_str)
                    # If we are in a multiword block and this token is within the range, process it
                    if multiword_original and multiword_start_id <= current_id <= multiword_end_id:
                        unsandhied_match = unsandhied_pattern.search(line)
                        if unsandhied_match:
                            unsandhied_form = unsandhied_match.group(1)
                            multiword_parts.append(unsandhied_form)
                        # If we have reached the end of the multiword range, finalize immediately
                        if current_id == multiword_end_id:
                            # Finalize this multiword token
                            if multiword_parts:
                                split_str = '+'.join(multiword_parts)
                                dev_word = iast_to_devanagari(multiword_original)
                                dev_split = iast_to_devanagari(split_str)
                                data_rows.append({"word": dev_word, "split": dev_split})
                            # Reset after finalizing
                            multiword_original = None
                            multiword_parts = []
                            multiword_start_id = None
                            multiword_end_id = None
                    # If we're not in a multiword block or current_id not in range, ignore this token
            else:
                # This line doesn't match the token pattern, ignore
                pass
    # At file end, if a multiword was not yet finalized (for some reason),
    # finalize it if we have parts and the range was completed. 
    # This is a safety net; ideally, all multiwords should be closed right after their last token.
    if multiword_original and multiword_parts and multiword_start_id is not None and multiword_end_id is not None:
        split_str = '+'.join(multiword_parts)
        dev_word = iast_to_devanagari(multiword_original)
        dev_split = iast_to_devanagari(split_str)
        data_rows.append({"word": dev_word, "split": dev_split})


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
