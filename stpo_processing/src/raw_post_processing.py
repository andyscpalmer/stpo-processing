import re
from time import perf_counter

import nltk

from .constants import DEBUG
from .logging import set_local_logger

logger = set_local_logger(__name__)


def format_post(
    post, uncommon_consonants="ndthsgngkwh", special_item_signifier="32123"
):
    post_format = post
    replace_patterns_and_values = {
        "new_line": {"pattern": r"\n", "value": f" u{uncommon_consonants}u "},
        "http_url": {
            "pattern": r"https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}"
            r"\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)",
            "value": f"v{uncommon_consonants}v",
        },
        "non_http_url": {
            "pattern": r"[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}"
            r"\b(?:[-a-zA-Z0-9()@:%_\+.~#?&//=]*)",
            "value": f"w{uncommon_consonants}w",
        },
        "bsky_handle": {
            "pattern": r"[a-zA-Z0-9-_\+]+\.[a-zA-Z0-9-_\+]+\.?[a-zA-Z0-9-_\+]+",
            "value": f"x{uncommon_consonants}x",
        },
        "separator_to_space": {"pattern": r"\-|\\|\/", "value": " "},
        "currency": {
            "pattern": r"(\$|\€|\£|\¥|\₣|\₹|\₽|\₺|\원|\₯|\₱|\﷼|\₻)",
            "value": f"y{uncommon_consonants}y",
        },
        "non_word_non_space": {"pattern": r"[^\w\s]", "value": ""},
        "numbers": {"pattern": r"[0-9]", "value": f"z{uncommon_consonants}z"},
        "extra_spaces": {"pattern": r"\s+", "value": " "},
        "repeated_placeholders": {
            "pattern": rf"([uwxyz]{uncommon_consonants}[uwxyz]\s?)+",
            "value": "\g<1>",
        },
        "uncommon_consonants": {
            "pattern": uncommon_consonants,
            "value": special_item_signifier,
        },
    }

    for step in replace_patterns_and_values.keys():
        pattern = replace_patterns_and_values[step]["pattern"]
        value = replace_patterns_and_values[step]["value"]
        post_format = re.sub(pattern, value, post_format)

    post_format = post_format.strip().lower()
    return post_format


def combine_post_families(post_family_collection):
    repetitive_posts = []
    for post_families in post_family_collection:
        for family_name, family_traits in post_families.items():
            repetitive_posts.append(format_post(family_name).split())
            repetitive_posts = [*repetitive_posts, *family_traits["posts"]]
    return repetitive_posts


# async def process_single_post(post, post_family_collection, post_families):


def build_post_families(
    posts: list, min_length=1, family_cutoff=1000, family_append=100, margin=0.01
):
    """
    "family_name": {
        "unique_words": <set_of_unique_words>,
        "posts": [<list_of_posts>]
    }
    """
    post_family_collection = []
    post_families = {}
    for post in posts:
        post_format = format_post(post)
        post_words = post_format.split()
        post_words_length = len(post_words)
        if post_words_length > min_length:
            unique_post_words = set(post_words)
            unique_words_length = len(unique_post_words)
            family_name = post
            if margin > 0:
                range_cutoff = round(margin * unique_words_length) + 1
                unique_length_range = range(
                    unique_words_length - range_cutoff,
                    unique_words_length + range_cutoff,
                )
            else:
                unique_length_range = [unique_words_length]
            for post_family_name, family_traits in post_families.items():
                if len(family_traits["unique_words"]) in unique_length_range:
                    overlapping_words = family_traits["unique_words"].intersection(
                        unique_post_words
                    )
                    if len(overlapping_words) in unique_length_range:
                        if len(overlapping_words) == len(unique_post_words):
                            family_name = post_family_name
                            break
            if family_name in post_families.keys():
                post_families[family_name]["posts"].append(post_words)
            else:
                post_families[family_name] = {
                    "unique_words": unique_post_words,
                    "posts": [post_words],
                }

            if len(post_families.keys()) > family_cutoff:
                trimmed_post_families = {}
                for post_family, family_traits in post_families.items():
                    if len(family_traits["posts"]) > 1:
                        trimmed_post_families[post_family] = family_traits
                if len(trimmed_post_families.keys()) > family_append:
                    post_family_collection.append(trimmed_post_families)
                    post_families = {}
                else:
                    post_families = trimmed_post_families
    post_family_collection.append(post_families)

    repetitive_posts = combine_post_families(post_family_collection)

    return repetitive_posts


def get_post_word_separation(posts):
    separation_indexed_word_pairs = []

    for post_words in posts:
        enum_post_words = [(idx, a) for idx, a in enumerate(post_words)]

        sep_indexed_post_pairs = [
            (b[0] - a[0], a[1], b[1])
            for idx, a in enumerate(enum_post_words)
            for b in enum_post_words[idx + 1 :]
            if b[0] - a[0] < 20
        ]

        separation_indexed_word_pairs += sep_indexed_post_pairs

    return separation_indexed_word_pairs


def build_stpo_map(separation_idexed_post_words, max_separation=20):
    """
    Separation to Pair Occurrences Map (STPO Map)

    model = {
        <separation: int>: {
            <first_word: str>: {
                <second_word: str>: <occurrences: int>,
                ...
            },
            {...},
            ...
        },
        {...},
        ...
    }
    """
    separation_to_pair_occurrences = {}
    for separation, first_word, second_word in separation_idexed_post_words:
        if separation < max_separation:
            if separation not in separation_to_pair_occurrences.keys():
                separation_to_pair_occurrences[separation] = {
                    first_word: {second_word: 1}
                }
            elif first_word not in separation_to_pair_occurrences[separation].keys():
                separation_to_pair_occurrences[separation][first_word] = {
                    second_word: 1
                }
            elif (
                second_word
                not in separation_to_pair_occurrences[separation][first_word].keys()
            ):
                separation_to_pair_occurrences[separation][first_word][second_word] = 1
            else:
                separation_to_pair_occurrences[separation][first_word][second_word] += 1

    return separation_to_pair_occurrences


def combine_stpo_maps(stpo_maps: list) -> dict:
    """
    Combine list of stpo maps into single stpo map
    """
    super_stpo_map = {}
    for stpo_map in stpo_maps:
        for sep, first_words in stpo_map.items():
            if sep not in super_stpo_map.keys():
                super_stpo_map[sep] = first_words
            else:
                for first_word, second_words in first_words:
                    if first_word not in super_stpo_map[sep].keys():
                        super_stpo_map[sep][first_word] = second_words
                    else:
                        for second_word, occ in second_words.items():
                            if (
                                second_word
                                not in super_stpo_map[sep][first_word].keys()
                            ):
                                super_stpo_map[sep][first_word][second_word] = occ
                            else:
                                super_stpo_map[sep][first_word][second_word] += occ

    return super_stpo_map


def orchestrate_stpo(posts):
    logger.debug(f"Number of posts: {len(posts)}")
    repetitive_posts = build_post_families(posts)
    logger.debug(f"Number of repetitive posts: {len(repetitive_posts)}")
    separation_indexed_word_pairs = get_post_word_separation(repetitive_posts)
    stpo_map = build_stpo_map(separation_indexed_word_pairs)
    return stpo_map


def pairs_to_cfdist_map(separation_idexed_post_words):
    separation_to_cfdist = {}
    for separation, first_word, second_word in separation_idexed_post_words:
        if separation not in separation_to_cfdist.keys():
            separation_to_cfdist[separation] = nltk.ConditionalFreqDist()

        separation_to_cfdist[separation][first_word][second_word] += 1

    return separation_to_cfdist


def stpo_map_to_cfdist_map(separation_to_pair_occurrences):
    separation_to_cfdist = {}
    for separation, first_words in separation_to_pair_occurrences.items():
        if separation not in separation_to_cfdist.keys():
            separation_to_cfdist[separation] = nltk.ConditionalFreqDist()
        for first_word, second_words in first_words.items():
            for second_word, occurrences in second_words.items():
                separation_to_cfdist[separation][first_word][second_word] = occurrences

    return separation_to_cfdist


def get_post_score(post, separation_to_cfdist):
    post_format = format_post(post)
    post_words = post_format.split()
    logger.debug(post_words)
    post_len = len(post_words)

    score = 0

    length_penalty = 0
    if post_len < 8:
        length_penalty = 10 ** (6 - post_len)

    if post_len > 4:
        range_max = sorted(list(separation_to_cfdist.keys()))[-1]
        depth_divisor = range_max if range_max < post_len - 1 else post_len - 1
        depth_coefficient = 1 / depth_divisor
        for N in separation_to_cfdist.keys():
            cfd = separation_to_cfdist[N]
            sub_score = 0
            if N < post_len:
                for i in range(post_len - N):
                    sub_score += cfd[post_words[i]].freq(post_words[i + N]) / (
                        post_len - N
                    )

                    if DEBUG:
                        sub_sub_score = cfd[post_words[i]].freq(post_words[i + N]) / (
                            post_len - N
                        )
                        logger.debug(post_words[i], post_words[i + N], sub_sub_score)

                score += 10 * sub_score * depth_coefficient

                logger.debug(N, sub_score)

    score += length_penalty

    return score


# Performance testing tool
def get_config_performance(posts, test_configurations):
    collected_test_results = []
    total_posts = len(posts)

    for test_configuration in test_configurations:
        logger.debug(f"Config: {test_configuration}")
        collected_test_result = test_configuration
        start_time = perf_counter()
        post_family_collection = build_post_families(posts, **test_configuration)
        build_post_families_end = perf_counter()
        repetitive_posts = combine_post_families(post_family_collection)
        repetitive_posts_end = perf_counter()

        collected_test_result["total_posts"] = total_posts
        collected_test_result["build_post_families_time"] = round(
            build_post_families_end - start_time, 2
        )
        collected_test_result["collect_repetitive_posts_time"] = round(
            build_post_families_end - build_post_families_end, 2
        )
        collected_test_result["total_time"] = round(
            repetitive_posts_end - start_time, 2
        )
        collected_test_result["repetitive_posts_count"] = len(repetitive_posts)

        logger.debug(
            f"Total posts found: {collected_test_result['repetitive_posts_count']}"
        )
        logger.debug(f"Total time: {collected_test_result['total_time']}")
        collected_test_results.append(collected_test_result)

    return collected_test_results
