"""旧导入面；实际façade只转换契约，共享kernel拥有全部执行。"""

from .adapters.krim import EvaluationService as EvaluationService
from .adapters.krim import eval_view as eval_view
