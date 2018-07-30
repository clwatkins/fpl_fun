from difflib import SequenceMatcher


def flatten(deep_list):
    return [l for sublist in deep_list for l in sublist]


def compute_diff(string_1, string_2):
    return SequenceMatcher(None, string_1, string_2).ratio()
