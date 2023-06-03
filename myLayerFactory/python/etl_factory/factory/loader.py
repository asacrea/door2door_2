from importlib import import_module
from inspect import getmembers, isabstract, isclass
from extract.abs_extraction import AbsExtraction
from transform.abs_transform import AbsTransform
from load.abs_load import AbsLoad

def load_class(path, class_name, factory):
    try:
        factory_module = import_module("." + class_name, path)
    except:
        factory_module = import_module(".null_factory", path)
    
    classes = getmembers(factory_module, lambda m: isclass(m) and not isabstract(m))

    for _, _class in classes:
        if issubclass(_class, factory):
            return _class()

