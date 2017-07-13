"""Classes that represent the various neighborhoods used in focal functions.

Note:
    Once a parameter has been entered for any one of these classes it gets converted to a
    ``float`` if it was originally an ``int``.
"""


__all__ = ['Square', 'Circle', 'Wedge', 'Nesw', 'Annulus']


class Neighborhood(object):
    def __init__(self, name, param_1, param_2=None, param_3=None):
        """The base class of the all of the neighborhoods.

        Args:
            param_1 (int or float): The first argument of the neighborhood.
            param_2 (int or float, optional): The second argument of the neighborhood.
            param_3 (int or float, optional): The third argument of the neighborhood.

        Attributes:
            param_1 (float): The first argument.
            param_2 (float, optional): The second argument.
            param_3 (float, optional): The third argument.
            name (str): The name of the neighborhood.
        """

        self.name = name
        self.param_1 = float(param_1)

        if param_2:
            self.param_2 = float(param_2)
        else:
            self.param_2 = 0.0

        if param_3:
            self.param_3 = float(param_3)
        else:
            self.param_3 = 0.0


class Square(Neighborhood):
    def __init__(self, extent):
        """A square neighborhood.

        Args:
            extent (int or float): The extent of this neighborhood. This represents the how many
                cells past the focus the bounding box goes.

        Attributes:
            extent (int or float): The extent of this neighborhood. This represents the how many
                cells past the focus the bounding box goes.
            param_1 (float): Same as ``extent``.
            param_2 (float): Unused param for ``Square``. Is 0.0.
            param_3 (float): Unused param for ``Square``. Is 0.0.
            name (str): The name of the neighborhood which is, "square".
        """

        Neighborhood.__init__(self, name="Square", param_1=extent)
        self.extent = extent

    def __str__(self):
        return "Square(extent={})".format(self.param_1)

    def __repr__(self):
        return "Square(extent={})".format(self.param_1)


class Circle(Neighborhood):
    """A circle neighborhood.

    Args:
        radius (int or float): The radius of the circle that determines which cells fall within
            the bounding box.

    Attributes:
        radius (int or float): The radius of the circle that determines which cells fall within
            the bounding box.
        param_1 (float): Same as ``radius``.
        param_2 (float): Unused param for ``Circle``. Is 0.0.
        param_3 (float): Unused param for ``Circle``. Is 0.0.
        name (str): The name of the neighborhood which is, "circle".

    Note:
        Cells that lie exactly on the radius of the circle are apart of the neighborhood.
    """

    def __init__(self, radius):
        Neighborhood.__init__(self, name="Circle", param_1=radius)
        self.radius = radius

    def __str__(self):
        return "Circle(radius={})".format(self.param_1)

    def __repr__(self):
        return "Circle(radius={})".format(self.param_1)


class Nesw(Neighborhood):
    """A neighborhood that includes a column and row intersection for the focus.

    Args:
        extent (int or float): The extent of this neighborhood. This represents the how many
            cells past the focus the bounding box goes.

    Attributes:
        extent (int or float): The extent of this neighborhood. This represents the how many
            cells past the focus the bounding box goes.
        param_1 (float): Same as ``extent``.
        param_2 (float): Unused param for ``Nesw``. Is 0.0.
        param_3 (float): Unused param for ``Nesw``. Is 0.0.
        name (str): The name of the neighborhood which is, "nesw".
    """

    def __init__(self, extent):
        Neighborhood.__init__(self, name="Nesw", param_1=extent)
        self.extent = extent

    def __str__(self):
        return "Nesw(extent={})".format(self.param_1)

    def __repr__(self):
        return "Nesw(extent={})".format(self.param_1)


class Wedge(Neighborhood):
    """A wedge neighborhood.

    Args:
        radius (int or float): The radius of the wedge.
        start_angle (int or float): The starting angle of the wedge in degrees.
        end_angle (int or float): The ending angle of the wedge in degrees.

    Attributes:
        radius (int or float): The radius of the wedge.
        start_angle (int or float): The starting angle of the wedge in degrees.
        end_angle (int or float): The ending angle of the wedge in degrees.
        param_1 (float): Same as ``radius``.
        param_2 (float): Same as ``start_angle``.
        param_3 (float): Same as ``end_angle``.
        name (str): The name of the neighborhood which is, "wedge".
    """

    def __init__(self, radius, start_angle, end_angle):
        Neighborhood.__init__(self, name="Wedge", param_1=radius, param_2=start_angle, param_3=end_angle)
        self.radius = radius
        self.start_angle = start_angle
        self.end_angle = end_angle

    def __str__(self):
        return "Wedge(radius={}, start_angle={}, end_angle={})".format(self.param_1, self.param_2,
                                                                       self.param_3)

    def __repr__(self):
        return "Wedge(radius={}, start_angle={}, end_angle={})".format(self.param_1, self.param_2,
                                                                       self.param_3)


class Annulus(Neighborhood):
    """An Annulus neighborhood.

    Args:
        inner_radius (int or float): The radius of the inner circle.
        outer_radius (int or float): The radius of the outer circle.

    Attributes:
        inner_radius (int or float): The radius of the inner circle.
        outer_radius (int or float): The radius of the outer circle.
        param_1 (float): Same as ``inner_radius``.
        param_2 (float): Same as ``outer_radius``.
        param_3 (float): Unused param for ``Annulus``. Is 0.0.
        name (str): The name of the neighborhood which is, "annulus".
    """

    def __init__(self, inner_radius, outer_radius):
        Neighborhood.__init__(self, name="Annulus", param_1=inner_radius, param_2=outer_radius)
        self.inner_radius = inner_radius
        self.outer_radius = outer_radius

    def __str__(self):
        return "Annulus(inner_radius={}, outer_radius={})".format(self.param_1, self.param_2)

    def __repr__(self):
        return "Annulus(inner_radius={}, outer_radius={})".format(self.param_1, self.param_2)
