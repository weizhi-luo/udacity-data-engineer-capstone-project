class Area:
    """Represent an area within the range of coordinates"""

    def __init__(self, latitude_north, latitude_south,
                 longitude_east, longitude_west) -> None:
        """Create an instance of `Area`

        :param latitude_north: North latitude
        :param latitude_south: South latitude
        :param longitude_east: East longitude
        :param longitude_west: West longitude
        :return: An instance of 'Area'
        """
        self._latitude_north = latitude_north
        self._latitude_south = latitude_south
        self._longitude_east = longitude_east
        self._longitude_west = longitude_west

    @property
    def latitude_north(self) -> float:
        """Get north latitude of the area

        :return: North latitude of the area
        """
        return self._latitude_north

    @property
    def latitude_south(self) -> float:
        """Get south latitude of the area

        :return: South latitude of the area
        """
        return self._latitude_south

    @property
    def longitude_east(self) -> float:
        """Get east longitude of the area

        :return: East longitude of the area
        """
        return self._longitude_east

    @property
    def longitude_west(self) -> float:
        """Get west longitude of the area

        :return: West longitude of the area
        """
        return self._longitude_west
