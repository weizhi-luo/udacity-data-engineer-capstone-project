import numpy as np
import json
from xarray import Dataset
from collections.abc import Mapping, Iterable

from data_model import Area


def convert_mappings_to_bytes(mappings: Iterable[Mapping]) -> bytes:
    """Convert a collection of Mappings to bytes

    :param mappings: A collection of Mappings
    :return: bytes
    """
    return bytes(
        '\n'.join(json.dumps(mapping) for mapping in mappings).encode('UTF-8'))


def convert_xarray_dataset_with_area_to_mappings(dataset: Dataset, area: Area)\
        -> Iterable[Mapping]:
    """Convert xarray dataset to a collection of mappings with area specified

    :param dataset: xarray dataset to be converted
    :type dataset: xarray Dataset
    :param area: Area to be extracted
    :type area: Area
    :return: Mappings converted from xarray dataset
    :rtype: A collection of instances of `Mapping`
    """
    dataset = get_dataset_within_area(dataset, area)
    return convert_xarray_dataset_to_mappings(dataset)


def get_dataset_within_area(dataset: Dataset, area: Area) -> Dataset:
    area_with_nearest_coordinates = \
        get_area_with_nearest_coordinates(dataset, area)

    return dataset.where(
        (dataset.latitude >= area_with_nearest_coordinates.latitude_south)
        & (dataset.latitude <= area_with_nearest_coordinates.latitude_north)
        & (dataset.longitude >= area_with_nearest_coordinates.longitude_west)
        & (dataset.longitude <= area_with_nearest_coordinates.longitude_east),
        drop=True)


def get_area_with_nearest_coordinates(dataset: Dataset, area: Area) -> Area:
    nearest_dataset = \
        dataset.sel(latitude=[area.latitude_south, area.latitude_north],
                    longitude=[area.longitude_west, area.longitude_east],
                    method='nearest')
    latitudes = nearest_dataset.coords['latitude']
    longitudes = nearest_dataset.coords['longitude']
    return Area(latitudes.max(), latitudes.min(), longitudes.max(),
                longitudes.min())


def convert_xarray_dataset_to_mappings(dataset: Dataset) -> Iterable[Mapping]:
    if 'step' in dataset.indexes:
        return [
            create_mapping_indexed_by_latitude_longitude_step(
                dataset, latitude, longitude, step)
            for longitude in dataset.longitude
            for latitude in dataset.latitude
            for step in np.atleast_1d(dataset.step)
        ]
    else:
        return [
            create_mapping_indexed_by_latitude_longitude(
                dataset, latitude, longitude)
            for longitude in dataset.longitude
            for latitude in dataset.latitude
        ]


def create_mapping_indexed_by_latitude_longitude(dataset, latitude, longitude)\
        -> Mapping:
    mapping = {
        'latitude': np.around(latitude.values, 1),
        'longitude': np.around(longitude.values, 1),
        'forecast_date_time': np.datetime_as_string(dataset.time.values),
        'value_date_time': np.datetime_as_string(
            dataset.sel(longitude=longitude, latitude=latitude)
            .valid_time.values)
    }

    for key in dataset.keys():
        mapping[key] = \
            dataset.sel(latitude=latitude, longitude=longitude)[key].item()

    return mapping


def create_mapping_indexed_by_latitude_longitude_step(
        dataset, latitude, longitude, step) -> Mapping:
    mapping = {
        'latitude': np.around(latitude.values, 1),
        'longitude': np.around(longitude.values, 1),
        'forecast_date_time': np.datetime_as_string(dataset.time.values),
        'value_date_time': np.datetime_as_string(
            dataset.sel(longitude=longitude, latitude=latitude,
                        step=step).valid_time.values)
    }

    for key in dataset.keys():
        mapping[key] = dataset.sel(latitude=latitude, longitude=longitude,
                                   step=step)[key].item()

    return mapping
