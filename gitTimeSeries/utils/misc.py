def listize(param):
    """
    Convierte un parámetro en un iterable (list, set, tuple) si no lo es ya
    :param param:
    :return:
    """
    return param if isinstance(param, (list, set, tuple)) else [param]
