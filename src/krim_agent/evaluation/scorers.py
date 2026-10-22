"""v1评分兼容导入；实际算法只有metrics中的一份。"""

from .metrics import classify as classify
from .metrics import json_equal as json_equal
from .metrics import result as result
from .metrics import score_rule as score_rule
