from s3_file_data_access import get_mappings_from_file_on_s3
from data_validation import are_mappings_valid


def lambda_handler(event, context):
    s3_bucket, s3_key, expected_keys = event['s3_bucket'], event['s3_key'], \
                                       event['expected_keys']

    mappings = get_mappings_from_file_on_s3(s3_bucket, s3_key)
    if not are_mappings_valid(mappings, expected_keys):
        raise ValueError(f'File at {s3_bucket}/{s3_key} is invalid for having'
                         'mismatching keys')

    return {
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "is_valid": True
    }
