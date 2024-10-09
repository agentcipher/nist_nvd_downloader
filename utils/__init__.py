import importlib
from typing import Callable, List, Dict, Any, Optional, Type
from logging import getLogger
from pydantic import BaseModel

logger = getLogger(__name__)

def get_transform_function(function_name: str) -> Callable:
    try:
        module_name, func_name = function_name.rsplit('.', 1)
        logger.info(f"Importing transformation function {func_name} from module {module_name}")
        module = importlib.import_module(module_name)
        return getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to get transformation function {function_name}: {str(e)}")
        raise ValueError(f"Failed to get transformation function: {str(e)}") from e

def load_model_class(model_class_path: str) -> Optional[Type[BaseModel]]:
    """
    Dynamically loads a Pydantic model class from a string path.

    Args:
        model_class_path: String path to the model class (e.g., 'module.submodule.ModelClass')

    Returns:
        The loaded Pydantic model class

    Raises:
        ImportError: If the module or class cannot be found
        ValueError: If the class is not a subclass of pydantic.BaseModel
    """
    try:
        module_name, class_name = model_class_path.rsplit('.', 1)
        module = importlib.import_module(module_name)
        model_class = getattr(module, class_name)

        if not issubclass(model_class, BaseModel):
            raise ValueError(f"{model_class_path} is not a subclass of pydantic.BaseModel")

        return model_class
    except Exception as e:
        logger.error(f"Error loading model class from {model_class_path}: {str(e)}")
        raise

def find_resource(resources: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    return next((r for r in resources if r['name'] == name), None)
