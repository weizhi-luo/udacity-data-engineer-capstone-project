from typing import List
from collections.abc import Mapping


def are_mappings_valid(mappings: List[Mapping], keys: List[str]) -> bool:
    """Validate is mappings have the keys specified

    :param mappings: a list of instances of `Mapping` to be validated
    :param keys: a list of keys that each mapping should contain
    :return: bool, indicate whether mappings are valid
    """
    return not any(mapping for mapping in mappings
                   if not is_mapping_valid(mapping, keys))


def is_mapping_valid(mapping: Mapping, keys: List[str]) -> bool:
    return set(mapping.keys()) == set(keys)
